"""Input validation for Databricks MCP Server tools.

Provides validation functions for common input types like paths,
identifiers, and configuration values.
"""

import re
from typing import Any, Optional, TypeVar
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of a validation check."""

    is_valid: bool
    error_message: Optional[str] = None
    sanitized_value: Any = None


T = TypeVar('T')


def validate_required(value: Any, field_name: str) -> ValidationResult:
    """Validate that a required field is present and not empty."""
    if value is None:
        return ValidationResult(False, f"'{field_name}' is required")
    if isinstance(value, str) and not value.strip():
        return ValidationResult(False, f"'{field_name}' cannot be empty")
    return ValidationResult(True, sanitized_value=value)


def validate_string(
    value: Any,
    field_name: str,
    min_length: int = 0,
    max_length: int = 10000,
    pattern: Optional[str] = None,
    required: bool = True,
) -> ValidationResult:
    """Validate a string input."""
    if value is None:
        if required:
            return ValidationResult(False, f"'{field_name}' is required")
        return ValidationResult(True, sanitized_value=None)

    if not isinstance(value, str):
        return ValidationResult(False, f"'{field_name}' must be a string")

    value = value.strip()

    if required and not value:
        return ValidationResult(False, f"'{field_name}' cannot be empty")

    if len(value) < min_length:
        return ValidationResult(False, f"'{field_name}' must be at least {min_length} characters")

    if len(value) > max_length:
        return ValidationResult(False, f"'{field_name}' must be at most {max_length} characters")

    if pattern and not re.match(pattern, value):
        return ValidationResult(False, f"'{field_name}' has invalid format")

    return ValidationResult(True, sanitized_value=value)


def validate_integer(
    value: Any,
    field_name: str,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
    required: bool = True,
    default: Optional[int] = None,
) -> ValidationResult:
    """Validate an integer input."""
    if value is None:
        if required and default is None:
            return ValidationResult(False, f"'{field_name}' is required")
        return ValidationResult(True, sanitized_value=default)

    try:
        int_value = int(value)
    except (ValueError, TypeError):
        return ValidationResult(False, f"'{field_name}' must be an integer")

    if min_value is not None and int_value < min_value:
        return ValidationResult(False, f"'{field_name}' must be at least {min_value}")

    if max_value is not None and int_value > max_value:
        return ValidationResult(False, f"'{field_name}' must be at most {max_value}")

    return ValidationResult(True, sanitized_value=int_value)


def validate_boolean(
    value: Any,
    field_name: str,
    required: bool = True,
    default: bool = False,
) -> ValidationResult:
    """Validate a boolean input."""
    if value is None:
        if required and default is None:
            return ValidationResult(False, f"'{field_name}' is required")
        return ValidationResult(True, sanitized_value=default)

    if isinstance(value, bool):
        return ValidationResult(True, sanitized_value=value)

    if isinstance(value, str):
        if value.lower() in ('true', '1', 'yes', 'on'):
            return ValidationResult(True, sanitized_value=True)
        if value.lower() in ('false', '0', 'no', 'off'):
            return ValidationResult(True, sanitized_value=False)

    return ValidationResult(False, f"'{field_name}' must be a boolean")


def validate_workspace_path(path: str, field_name: str = "path") -> ValidationResult:
    """Validate a Databricks workspace path."""
    result = validate_string(path, field_name, min_length=1, max_length=4096)
    if not result.is_valid:
        return result

    path = result.sanitized_value

    # Check for path traversal attempts
    if '..' in path:
        return ValidationResult(False, f"'{field_name}' cannot contain '..'")

    # Check for null bytes
    if '\x00' in path:
        return ValidationResult(False, f"'{field_name}' contains invalid characters")

    # Workspace paths should start with /
    if not path.startswith('/'):
        return ValidationResult(False, f"'{field_name}' must be an absolute path starting with '/'")

    return ValidationResult(True, sanitized_value=path)


def validate_dbfs_path(path: str, field_name: str = "path") -> ValidationResult:
    """Validate a DBFS path."""
    result = validate_string(path, field_name, min_length=1, max_length=4096)
    if not result.is_valid:
        return result

    path = result.sanitized_value

    # Check for path traversal attempts
    if '..' in path:
        return ValidationResult(False, f"'{field_name}' cannot contain '..'")

    # Check for null bytes
    if '\x00' in path:
        return ValidationResult(False, f"'{field_name}' contains invalid characters")

    # DBFS paths should start with /
    if not path.startswith('/'):
        # Allow dbfs: prefix
        if not path.startswith('dbfs:/'):
            return ValidationResult(False, f"'{field_name}' must start with '/' or 'dbfs:/'")

    return ValidationResult(True, sanitized_value=path)


def validate_cluster_id(cluster_id: str, field_name: str = "cluster_id") -> ValidationResult:
    """Validate a cluster ID."""
    result = validate_string(cluster_id, field_name, min_length=1, max_length=100)
    if not result.is_valid:
        return result

    # Cluster IDs are typically alphanumeric with dashes
    if not re.match(r'^[a-zA-Z0-9\-_]+$', result.sanitized_value):
        return ValidationResult(False, f"'{field_name}' contains invalid characters")

    return result


def validate_warehouse_id(warehouse_id: str, field_name: str = "warehouse_id") -> ValidationResult:
    """Validate a SQL warehouse ID."""
    result = validate_string(warehouse_id, field_name, min_length=1, max_length=100)
    if not result.is_valid:
        return result

    # Warehouse IDs are typically alphanumeric
    if not re.match(r'^[a-zA-Z0-9\-_]+$', result.sanitized_value):
        return ValidationResult(False, f"'{field_name}' contains invalid characters")

    return result


def validate_job_id(job_id: Any, field_name: str = "job_id") -> ValidationResult:
    """Validate a job ID."""
    return validate_integer(job_id, field_name, min_value=1)


def validate_run_id(run_id: Any, field_name: str = "run_id") -> ValidationResult:
    """Validate a run ID."""
    return validate_integer(run_id, field_name, min_value=1)


def validate_catalog_name(name: str, field_name: str = "catalog_name") -> ValidationResult:
    """Validate a Unity Catalog name."""
    result = validate_string(name, field_name, min_length=1, max_length=255)
    if not result.is_valid:
        return result

    # Catalog names should be alphanumeric with underscores
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', result.sanitized_value):
        return ValidationResult(
            False,
            f"'{field_name}' must start with a letter and contain only letters, numbers, and underscores"
        )

    return result


def validate_schema_name(name: str, field_name: str = "schema_name") -> ValidationResult:
    """Validate a schema name."""
    return validate_catalog_name(name, field_name)


def validate_table_full_name(full_name: str, field_name: str = "full_name") -> ValidationResult:
    """Validate a fully qualified table name (catalog.schema.table)."""
    result = validate_string(full_name, field_name, min_length=5, max_length=767)
    if not result.is_valid:
        return result

    name = result.sanitized_value

    # Check for SQL injection patterns
    dangerous_patterns = [';', '--', '/*', '*/', "'", '"']
    for pattern in dangerous_patterns:
        if pattern in name:
            return ValidationResult(False, f"'{field_name}' contains invalid characters")

    # Should have exactly 2 dots for catalog.schema.table format
    parts = name.split('.')
    if len(parts) != 3:
        return ValidationResult(
            False,
            f"'{field_name}' must be in format 'catalog.schema.table'"
        )

    # Validate each part
    for i, part_name in enumerate(['catalog', 'schema', 'table']):
        part = parts[i]
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', part):
            return ValidationResult(
                False,
                f"'{field_name}' has invalid {part_name} name"
            )

    return ValidationResult(True, sanitized_value=name)


def validate_language(language: str, field_name: str = "language") -> ValidationResult:
    """Validate a programming language selection."""
    valid_languages = ['PYTHON', 'SCALA', 'SQL', 'R', 'python', 'scala', 'sql', 'r']

    result = validate_string(language, field_name, required=False)
    if not result.is_valid:
        return result

    if result.sanitized_value and result.sanitized_value not in valid_languages:
        return ValidationResult(
            False,
            f"'{field_name}' must be one of: PYTHON, SCALA, SQL, R"
        )

    return ValidationResult(True, sanitized_value=result.sanitized_value.upper() if result.sanitized_value else 'PYTHON')


def validate_export_format(format_str: str, field_name: str = "format") -> ValidationResult:
    """Validate an export format selection."""
    valid_formats = ['SOURCE', 'HTML', 'JUPYTER', 'DBC']

    result = validate_string(format_str, field_name, required=False)
    if not result.is_valid:
        return result

    if result.sanitized_value:
        format_upper = result.sanitized_value.upper()
        if format_upper not in valid_formats:
            return ValidationResult(
                False,
                f"'{field_name}' must be one of: SOURCE, HTML, JUPYTER, DBC"
            )
        return ValidationResult(True, sanitized_value=format_upper)

    return ValidationResult(True, sanitized_value='SOURCE')


class InputValidator:
    """Utility class for validating tool input arguments."""

    def __init__(self):
        self.errors: list[str] = []
        self.sanitized: dict[str, Any] = {}

    def validate(self, result: ValidationResult, field_name: str) -> bool:
        """Add validation result and return whether it was valid."""
        if result.is_valid:
            self.sanitized[field_name] = result.sanitized_value
            return True
        else:
            self.errors.append(result.error_message or f"Invalid {field_name}")
            return False

    def is_valid(self) -> bool:
        """Check if all validations passed."""
        return len(self.errors) == 0

    def get_error_message(self) -> str:
        """Get combined error message."""
        return "; ".join(self.errors)

    def get_sanitized(self) -> dict[str, Any]:
        """Get dictionary of sanitized values."""
        return self.sanitized
