# Databricks MCP Server

Talk to your company's data using plain English! This tool connects Claude Code to Databricks, letting anyone query databases, create reports, and work with data without needing to write code.

---

## What is This?

Imagine being able to ask questions like:
- "Show me last month's sales data"
- "How many customers signed up this week?"
- "Create a chart of revenue by region"

...and getting answers directly from your company's database. That's what this tool does!

### Key Concepts Explained

| Term | What It Means | Real-World Analogy |
|------|---------------|-------------------|
| **Databricks** | A cloud platform where your company stores and analyzes data | Like a giant, super-powered Excel in the cloud |
| **MCP Server** | A bridge that lets Claude Code talk to Databricks | Like a translator between two people speaking different languages |
| **Cluster** | A group of computers that work together to process data | Like having multiple calculators working on the same math problem |
| **SQL Warehouse** | A service for running database queries | Like a librarian who can instantly find any book you ask for |
| **Notebook** | A document where you can write and run code | Like a Word document, but it can also do calculations |
| **Table** | Data organized in rows and columns | Just like a spreadsheet or Excel table |
| **Catalog** | A collection of databases | Like a library that contains many bookshelves |
| **Schema** | A folder that groups related tables together | Like a bookshelf that holds related books |

---

## What Can You Do With This?

### For Business Users

Ask questions in plain English:
```
"Show me the top 10 customers by revenue"
"What were our sales last quarter?"
"List all products with low inventory"
"How many orders did we process today?"
```

### For Analysts

Run SQL queries and explore data:
```
"Run this SQL: SELECT product_name, SUM(quantity) FROM orders GROUP BY product_name"
"Show me the structure of the customers table"
"Preview data from the sales.transactions table"
"Search for tables related to 'inventory'"
```

### For Data Teams

Manage Databricks resources:
```
"List all running clusters"
"Start the analytics cluster"
"Create a new notebook for monthly reporting"
"Run the daily ETL job"
```

---

## Quick Start Guide

### What You'll Need

Before starting, make sure you have:

1. **Access to a Databricks workspace** - Ask your IT team or data team if you're not sure
2. **Python installed** - Version 3.10 or newer ([Download Python](https://www.python.org/downloads/))
3. **Claude Code installed** - The AI coding assistant you're using

### Step 1: Download and Install

Open your terminal (Command Prompt on Windows, Terminal on Mac) and run:

```bash
# Go to where you want to install (change this path as needed)
cd ~/Documents

# If you have the files, navigate to them
cd databricks-mcp-server

# Install the required software
pip install -e .
```

**Alternative: Using uv (faster, recommended for developers)**
```bash
cd databricks-mcp-server
uv sync
```

### Step 2: Get Your Databricks Credentials

You need two pieces of information from Databricks:

#### A. Your Workspace URL
This is the web address you use to access Databricks. It looks like:
- `https://mycompany.cloud.databricks.com` (AWS)
- `https://adb-1234567890.12.azuredatabricks.net` (Azure)
- `https://mycompany.cloud.databricks.com` (GCP)

#### B. Your Access Token
This is like a password that lets the tool connect to Databricks.

**How to get your token:**

1. Log into your Databricks workspace in a web browser
2. Click your **profile icon** (top-right corner)
3. Click **"User Settings"**
4. Click **"Developer"** in the left sidebar
5. Click **"Access tokens"**
6. Click **"Generate new token"**
7. Give it a name like "Claude Code" and click **Generate**
8. **IMPORTANT**: Copy the token immediately! You won't be able to see it again.

The token looks like: `dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX` (a long string starting with `dapi`)

### Step 3: Configure Claude Code

Now tell Claude Code how to connect to Databricks.

1. Find your Claude Code settings file:
   - **Mac/Linux**: `~/.claude/settings.json`
   - **Windows**: `C:\Users\YourName\.claude\settings.json`

2. Open the file in any text editor (Notepad, VS Code, etc.)

3. Add the following (replace the placeholder values with your actual information):

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "databricks_mcp.server"],
      "cwd": "/full/path/to/databricks-mcp-server/src",
      "env": {
        "DATABRICKS_HOST": "https://your-workspace-url-here.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi-your-token-here"
      }
    }
  }
}
```

**Example with placeholder values:**
```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "databricks_mcp.server"],
      "cwd": "/Users/john/Documents/databricks-mcp-server/src",
      "env": {
        "DATABRICKS_HOST": "https://mycompany.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi_your_actual_token_here"
      }
    }
  }
}
```

### Step 4: Restart Claude Code

Close Claude Code completely and reopen it. The Databricks connection should now be active!

### Step 5: Test It Out!

Try asking Claude:
- "List my Databricks clusters"
- "Show all available catalogs"
- "What tables are available?"

---

## Usage Examples (From Simple to Advanced)

### Beginner: Asking Simple Questions

Just type what you want to know:

```
"What data do we have available?"
→ Claude will list all catalogs and schemas

"Show me the customers table"
→ Claude will display the table structure and sample data

"How many rows are in the orders table?"
→ Claude will run a COUNT query and tell you
```

### Intermediate: Running Specific Queries

Ask for specific data:

```
"Show me all orders from last week"

"Get the top 5 products by sales amount"

"Find all customers in California"

"Calculate the average order value by month"
```

### Advanced: Managing Databricks Resources

For power users:

```
"Create a new Python notebook called 'Monthly Sales Report' in my folder"

"Start the production cluster"

"Run the data-refresh job and tell me when it's done"

"Export the analysis notebook as a Jupyter file"
```

---

## Common Tasks

### Looking at Your Data

| What You Want | What to Say |
|--------------|-------------|
| See what databases exist | "List all catalogs" |
| See tables in a database | "Show tables in the sales schema" |
| See a table's columns | "Describe the customers table" |
| See sample data | "Preview data from sales.orders" |
| Search for a table | "Find tables containing 'customer'" |

### Running Queries

| What You Want | What to Say |
|--------------|-------------|
| Simple query | "Run: SELECT * FROM customers LIMIT 10" |
| Count records | "How many rows in the orders table?" |
| Filter data | "Show orders where amount > 1000" |
| Group data | "Show sales by region" |
| Join tables | "Show customer names with their orders" |

### Working with Notebooks

| What You Want | What to Say |
|--------------|-------------|
| See your notebooks | "List notebooks in my folder" |
| Read a notebook | "Show me the analysis notebook" |
| Create a notebook | "Create a new Python notebook called 'Report'" |
| Run a notebook | "Execute the monthly-report notebook" |
| Download a notebook | "Export notebook as Jupyter format" |

### Managing Compute Resources

| What You Want | What to Say |
|--------------|-------------|
| See available clusters | "List all clusters" |
| Start a cluster | "Start the analytics cluster" |
| Stop a cluster | "Stop cluster xyz" |
| Check cluster status | "Is the production cluster running?" |

---

## Troubleshooting

### "I can't connect to Databricks"

**Check these things:**

1. **Is your workspace URL correct?**
   - It should start with `https://`
   - It should NOT have a trailing slash
   - Example: `https://mycompany.cloud.databricks.com` (correct)
   - NOT: `https://mycompany.cloud.databricks.com/` (wrong - has trailing slash)

2. **Is your token valid?**
   - Tokens can expire - try generating a new one
   - Make sure you copied the entire token (it's long!)
   - Check there are no extra spaces before or after

3. **Can you access Databricks in your browser?**
   - If you can't log into Databricks normally, the tool won't work either
   - You might be on a VPN requirement - check with your IT team

### "Permission denied" errors

This usually means your Databricks account doesn't have access to what you're trying to do.

- **For tables**: Ask your data team to grant you access
- **For clusters**: Ask for permission to use/start clusters
- **For notebooks**: Check if you have access to that folder

### "No cluster/warehouse found"

To run queries, you need either:
- A **running cluster**, OR
- A **running SQL warehouse**

Ask Claude to "list my clusters" or "list my warehouses" to see what's available. If nothing is running, ask to start one (if you have permission).

### "Claude doesn't seem to understand my request"

Try being more specific:
- Instead of: "Show me the data"
- Try: "Show me the first 10 rows from the customers table in the sales catalog"

---

## Security Configuration (v0.2.0+)

The server includes enterprise-grade security features to protect your data.

### Environment Profiles

Set the `DATABRICKS_MCP_PROFILE` environment variable to configure security defaults:

| Profile | Description | Default Behavior |
|---------|-------------|------------------|
| `development` | For local development | All tools enabled, minimal restrictions |
| `staging` | For testing environments | Safe mode on, code execution disabled |
| `production` | For production use | **Read-only mode**, all destructive operations blocked |

```json
{
  "env": {
    "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
    "DATABRICKS_TOKEN": "dapi...",
    "DATABRICKS_MCP_PROFILE": "production"
  }
}
```

### Security Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_MCP_PROFILE` | Environment profile | `development`, `staging`, `production` |
| `DATABRICKS_MCP_READ_ONLY` | Enable read-only mode | `true` or `false` |
| `DATABRICKS_MCP_SAFE_MODE` | Block destructive SQL/operations | `true` or `false` |
| `DATABRICKS_MCP_ALLOW_CODE_EXECUTION` | Allow arbitrary code execution | `true` or `false` |
| `DATABRICKS_MCP_ALLOW_SQL_EXECUTION` | Allow arbitrary SQL execution | `true` or `false` |
| `DATABRICKS_MCP_ALLOWED_TOOLS` | Comma-separated list of allowed tools | `databricks_list_tables,databricks_preview_table` |
| `DATABRICKS_MCP_BLOCKED_TOOLS` | Comma-separated list of blocked tools | `databricks_delete_notebook,databricks_dbfs_delete` |
| `DATABRICKS_MCP_MAX_SQL_ROWS` | Maximum rows returned from SQL queries | `1000` |

### Security Modes

**Read-Only Mode** (`DATABRICKS_MCP_READ_ONLY=true`)
- Only read operations are allowed (list, get, describe, preview)
- All write, delete, and execute operations are blocked

**Safe Mode** (`DATABRICKS_MCP_SAFE_MODE=true`)
- Destructive SQL operations (DROP, DELETE, TRUNCATE) are blocked
- Destructive file/notebook operations require additional checks
- Recursive deletes on system paths are prevented

### Tool Risk Levels

Tools are classified by risk level:

| Risk Level | Description | Examples |
|------------|-------------|----------|
| 🟢 **SAFE** | Read-only operations | `list_clusters`, `describe_table`, `preview_table` |
| 🟡 **MODERATE** | Write operations with limited impact | `create_notebook`, `start_cluster` |
| 🟠 **DESTRUCTIVE** | Operations that delete data | `delete_notebook`, `dbfs_delete` |
| 🔴 **CRITICAL** | Arbitrary code/SQL execution | `execute_code`, `execute_sql` |

### Audit Logging

All tool invocations are logged with:
- Timestamp and correlation ID
- Tool name, operation type, and risk level
- Sanitized arguments (sensitive data redacted)
- Execution time and result status
- Workspace host

Logs are output in JSON format for easy integration with log aggregation systems.

### SQL Query Validation

In safe mode, SQL queries are validated before execution:
- ✅ Allowed: `SELECT`, `SHOW`, `DESCRIBE`, `EXPLAIN`
- ❌ Blocked: `DROP`, `DELETE`, `TRUNCATE`, `INSERT`, `UPDATE`, `ALTER`, `CREATE`

### Example: Production-Safe Configuration

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "databricks_mcp.server"],
      "cwd": "/path/to/databricks-mcp-server/src",
      "env": {
        "DATABRICKS_HOST": "https://prod.cloud.databricks.com",
        "DATABRICKS_TOKEN": "dapi...",
        "DATABRICKS_MCP_PROFILE": "production",
        "DATABRICKS_MCP_READ_ONLY": "true",
        "DATABRICKS_MCP_SAFE_MODE": "true",
        "DATABRICKS_MCP_ALLOWED_TOOLS": "databricks_list_clusters,databricks_list_tables,databricks_describe_table,databricks_preview_table,databricks_list_catalogs,databricks_list_schemas,databricks_search_tables,databricks_get_query_history"
      }
    }
  }
}
```

---

## Security Notes

**Keep your token safe!**

- Never share your token with others
- Don't commit it to Git or post it online
- If you think your token was exposed, delete it in Databricks and create a new one

**What this tool can access:**

- Only data YOU have permission to see in Databricks
- It uses YOUR credentials, so it has the same access as you do
- It cannot access anything you couldn't access by logging into Databricks yourself
- In read-only mode, write and delete operations are blocked at the server level

---

## Getting Help

### From Your Team
- **Data questions**: Ask your data team or analytics team
- **Access issues**: Ask your IT team or Databricks admin
- **Databricks training**: Ask if your company offers Databricks training

### Online Resources
- [Databricks Documentation](https://docs.databricks.com/)
- [Databricks Community](https://community.databricks.com/)
- [SQL Tutorial](https://www.w3schools.com/sql/) - Learn basic SQL

---

## Glossary

| Term | Definition |
|------|------------|
| **API** | Application Programming Interface - a way for programs to talk to each other |
| **Catalog** | The top level of organization in Unity Catalog, like a folder containing databases |
| **Cluster** | A group of computers working together to process your data |
| **DBFS** | Databricks File System - a place to store files in Databricks |
| **ETL** | Extract, Transform, Load - the process of moving and preparing data |
| **Job** | An automated task that runs on a schedule or when triggered |
| **MCP** | Model Context Protocol - the technology that lets Claude talk to other tools |
| **Notebook** | An interactive document for writing and running code |
| **PySpark** | A Python library for processing big data |
| **Query** | A request for data, usually written in SQL |
| **Schema** | A container for tables within a catalog (like a subfolder) |
| **SQL** | Structured Query Language - the language used to query databases |
| **SQL Warehouse** | A compute resource optimized for running SQL queries |
| **Table** | Data organized in rows and columns, like a spreadsheet |
| **Token** | A secret key that proves your identity to Databricks |
| **Unity Catalog** | Databricks' system for organizing and securing data |
| **Volume** | A storage location for files in Unity Catalog |
| **Workspace** | Your Databricks environment, accessed via a web URL |

---

## For Developers

### Running Tests

```bash
uv run pytest
```

### Running the Server Manually

```bash
uv run databricks-mcp
```

### Project Structure

```
databricks-mcp-server/
├── src/databricks_mcp/
│   ├── server.py           # Main MCP server
│   ├── config.py           # Authentication handling
│   └── tools/              # All the Databricks tools
│       ├── clusters.py     # Cluster management
│       ├── notebooks.py    # Notebook operations
│       ├── sql.py          # SQL queries
│       ├── jobs.py         # Job management
│       ├── dbfs.py         # File operations
│       └── unity_catalog.py # Data catalog
├── tests/                  # Unit tests
├── pyproject.toml          # Project configuration
└── README.md               # This file
```

---

## License

MIT - Feel free to use and modify!
