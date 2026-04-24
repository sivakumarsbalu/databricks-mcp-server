"""Structured logging configuration for Databricks MCP Server.

Provides JSON-formatted logging with correlation IDs, audit trails,
and sensitive data redaction.
"""

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


correlation_id: ContextVar[str] = ContextVar('correlation_id', default='')
user_id: ContextVar[str] = ContextVar('user_id', default='')


class LogLevel(Enum):
    """Log levels matching Python logging."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class AuditLogEntry:
    """Structured audit log entry for tool invocations."""

    timestamp: str
    correlation_id: str
    tool_name: str
    operation_type: str
    risk_level: str
    arguments: dict[str, Any]
    result_status: str
    execution_time_ms: float
    user_id: Optional[str] = None
    error_message: Optional[str] = None
    workspace_host: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary, filtering None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class SensitiveDataRedactor:
    """Redacts sensitive data from log entries."""

    SENSITIVE_KEYS = [
        'token', 'password', 'secret', 'api_key', 'apikey',
        'authorization', 'auth', 'credential', 'client_secret',
        'private_key', 'access_token', 'refresh_token',
    ]

    SENSITIVE_PATTERNS_IN_VALUES = [
        'dapi',  # Databricks API token prefix
        'Bearer ',
        'Basic ',
    ]

    REDACTED_VALUE = '[REDACTED]'

    @classmethod
    def redact(cls, data: Any, depth: int = 0) -> Any:
        """Recursively redact sensitive data from a structure."""
        if depth > 10:
            return data

        if isinstance(data, dict):
            redacted = {}
            for key, value in data.items():
                if cls._is_sensitive_key(key):
                    redacted[key] = cls.REDACTED_VALUE
                elif isinstance(value, str) and cls._contains_sensitive_value(value):
                    redacted[key] = cls.REDACTED_VALUE
                else:
                    redacted[key] = cls.redact(value, depth + 1)
            return redacted

        if isinstance(data, list):
            return [cls.redact(item, depth + 1) for item in data]

        if isinstance(data, str):
            if cls._contains_sensitive_value(data):
                return cls.REDACTED_VALUE
            # Redact SQL content if it contains potential secrets
            if len(data) > 1000:
                return data[:500] + '...[truncated]...' + data[-200:]

        return data

    @classmethod
    def _is_sensitive_key(cls, key: str) -> bool:
        """Check if a key name indicates sensitive data."""
        key_lower = key.lower()
        return any(sensitive in key_lower for sensitive in cls.SENSITIVE_KEYS)

    @classmethod
    def _contains_sensitive_value(cls, value: str) -> bool:
        """Check if a value contains sensitive patterns."""
        return any(pattern in value for pattern in cls.SENSITIVE_PATTERNS_IN_VALUES)


class StructuredJsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id.get() or None,
        }

        # Add user_id if set
        uid = user_id.get()
        if uid:
            log_entry["user_id"] = uid

        # Add source location
        log_entry["source"] = {
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
            }

        # Add extra fields
        extra_fields = {
            k: v for k, v in record.__dict__.items()
            if k not in ('name', 'msg', 'args', 'created', 'filename',
                        'funcName', 'levelname', 'levelno', 'lineno',
                        'module', 'msecs', 'pathname', 'process',
                        'processName', 'relativeCreated', 'stack_info',
                        'exc_info', 'exc_text', 'thread', 'threadName',
                        'taskName', 'message')
        }
        if extra_fields:
            log_entry["extra"] = SensitiveDataRedactor.redact(extra_fields)

        return json.dumps(log_entry, default=str)


class AuditLogger:
    """Specialized logger for audit trail entries."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def log_tool_invocation(
        self,
        tool_name: str,
        operation_type: str,
        risk_level: str,
        arguments: dict[str, Any],
        result_status: str,
        execution_time_ms: float,
        error_message: Optional[str] = None,
        workspace_host: Optional[str] = None,
    ) -> None:
        """Log a tool invocation to the audit trail."""
        entry = AuditLogEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            correlation_id=correlation_id.get() or str(uuid.uuid4()),
            tool_name=tool_name,
            operation_type=operation_type,
            risk_level=risk_level,
            arguments=SensitiveDataRedactor.redact(arguments),
            result_status=result_status,
            execution_time_ms=execution_time_ms,
            user_id=user_id.get() or None,
            error_message=error_message,
            workspace_host=workspace_host,
        )

        # Log as INFO for successful operations, WARNING for failures
        if result_status == "SUCCESS":
            self.logger.info(
                f"AUDIT: {tool_name}",
                extra={"audit": entry.to_dict()}
            )
        else:
            self.logger.warning(
                f"AUDIT: {tool_name} - {result_status}",
                extra={"audit": entry.to_dict()}
            )


def generate_correlation_id() -> str:
    """Generate a new correlation ID."""
    return str(uuid.uuid4())


def set_correlation_id(cid: Optional[str] = None) -> str:
    """Set the correlation ID for the current context."""
    cid = cid or generate_correlation_id()
    correlation_id.set(cid)
    return cid


def set_user_id(uid: str) -> None:
    """Set the user ID for the current context."""
    user_id.set(uid)


def configure_logging(
    level: str = "INFO",
    json_format: bool = True,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """Configure structured logging for the application.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Whether to use JSON formatting
        log_file: Optional file path for logging output

    Returns:
        Configured root logger
    """
    root_logger = logging.getLogger("databricks-mcp")
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Create formatter
    if json_format:
        formatter = StructuredJsonFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
            defaults={'correlation_id': ''}
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    return root_logger


def get_audit_logger() -> AuditLogger:
    """Get the audit logger instance."""
    logger = logging.getLogger("databricks-mcp.audit")
    return AuditLogger(logger)


# Module-level logger
logger = logging.getLogger("databricks-mcp")
audit_logger = get_audit_logger()
