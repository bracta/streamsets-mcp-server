# Streamsets MCP Server

A Model Context Protocol (MCP) server that provides secure access to Streamsets Control Hub APIs for managing data pipelines and connections.

## Purpose

This MCP server provides a secure interface for AI assistants to interact with Streamsets Control Hub, enabling job management, monitoring, and connection administration through natural language.

## Features

### Current Implementation

#### Job Runner Tools
- **`list_jobs`** - List all jobs with filtering by organization and status
- **`get_job_details`** - Get detailed information about a specific job
- **`get_job_status`** - Get the current status of a job
- **`start_job`** - Start a job with optional runtime parameters
- **`stop_job`** - Stop a running job
- **`get_job_history`** - Get the run history for a specific job

#### Connection Tools
- **`list_connections`** - List all connections with filtering by organization and type
- **`get_connection_details`** - Get detailed information about a specific connection
- **`get_connection_types`** - Get available connection types for an organization
- **`get_pipelines_using_connection`** - Get pipelines that are using a specific connection

## Prerequisites

- Docker Desktop with MCP Toolkit enabled
- Docker MCP CLI plugin (`docker mcp` command)
- Streamsets Control Hub account with API credentials
- Access to Streamsets Control Hub API endpoint

## Installation

See the step-by-step instructions provided with the files.

## Configuration

The server requires three environment variables:
- `STREAMSETS_HOST_PREFIX` - The base URL for your Streamsets instance (e.g., https://cloud.streamsets.com)
- `STREAMSETS_CRED_ID` - Your API credential ID
- `STREAMSETS_CRED_TOKEN` - Your API authentication token

## Usage Examples

In Claude Desktop, you can ask:

### Job Management
- "List all active jobs in my organization"
- "Show me failed jobs"
- "Get details about job ID abc123"
- "What's the status of job xyz789?"
- "Start the ETL pipeline job"
- "Stop job abc123"
- "Show me the run history for job def456"

### Connection Management
- "List all database connections"
- "Show me AWS S3 connections"
- "Get details about connection conn123"
- "What connection types are available?"
- "Which pipelines are using connection abc456?"
- "Show me all connections with their types"

### Combined Queries
- "List all jobs and their current status"
- "Show me failed jobs and their error messages"
- "Which connections are being used by active pipelines?"

## Architecture

```
Claude Desktop → MCP Gateway → Streamsets MCP Server → Streamsets Control Hub API
                      ↓
               Docker Desktop Secrets
         (STREAMSETS_HOST_PREFIX, STREAMSETS_CRED_ID,
                STREAMSETS_CRED_TOKEN)
```

## Development

### Local Testing

```bash
# Set environment variables for testing
export STREAMSETS_HOST_PREFIX="https://your-instance.streamsets.com"
export STREAMSETS_CRED_ID="your-cred-id"
export STREAMSETS_CRED_TOKEN="your-token"

# Run directly
python streamsets_server.py

# Test MCP protocol
echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | python streamsets_server.py
```

### Adding New Tools

1. Add the function to `streamsets_server.py`
2. Decorate with `@mcp.tool()`
3. Update the catalog entry with the new tool name
4. Rebuild the Docker image

## API Authentication

The server uses Streamsets API credentials for authentication. Each request includes:
- `X-SS-App-Component-Id`: Your credential ID
- `X-SS-App-Auth-Token`: Your authentication token
- `X-SS-REST-CALL`: Set to "true"
- `X-Requested-By`: Set to "mcp-server"

## Troubleshooting

### Tools Not Appearing
- Verify Docker image built successfully
- Check catalog and registry files
- Ensure Claude Desktop config includes custom catalog
- Restart Claude Desktop

### Authentication Errors
- Verify secrets with `docker mcp secret list`
- Ensure secret names match in code and catalog
- Check that credentials are valid in Streamsets Control Hub
- Verify the HOST_PREFIX URL is correct

### API Errors
- Check that the HOST_PREFIX includes the full URL (https://...)
- Ensure your credentials have appropriate permissions
- Verify network connectivity to Streamsets Control Hub
- Check API response for specific error messages

## Security Considerations

- All credentials stored in Docker Desktop secrets
- Never hardcode credentials
- Running as non-root user
- Sensitive data never logged
- API tokens transmitted securely over HTTPS
- Credentials are scoped to specific organization permissions

## License

MIT License