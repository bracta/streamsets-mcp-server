# StreamSets MCP Server

A comprehensive Model Context Protocol (MCP) server that provides seamless integration with StreamSets Control Hub APIs, enabling complete data pipeline management and creation through conversational AI.

## 🚀 Features

### **Pipeline Management (Read Operations)**
- **Job Management**: List, start, stop, and monitor job execution
- **Pipeline Operations**: Browse, search, and analyze pipeline configurations
- **Connection Management**: Manage data connections and integrations
- **Metrics & Analytics**: Comprehensive performance and usage analytics
- **Enterprise Integration**: Deployment management, security audits, and alerts

### **Pipeline Building (Write Operations)** 🆕
- **Interactive Pipeline Creation**: Build pipelines through conversation
- **Stage Library**: Access to 25+ StreamSets stages (Origins, Processors, Destinations, Executors)
- **Visual Flow Management**: Connect stages with data and event streams
- **Persistent Sessions**: Pipeline builders persist across conversations
- **Smart Validation**: Automatic validation of pipeline logic and connections

## 📊 API Coverage

**44 Tools** covering **9 StreamSets Services**:

- **Job Runner API** (11 tools) - Job lifecycle management
- **Pipeline Repository API** (7 tools) - Pipeline CRUD operations
- **Connection API** (4 tools) - Data connection management
- **Provisioning API** (5 tools) - Infrastructure and deployment
- **Notification API** (2 tools) - Alert and notification management
- **Topology API** (1 tool) - System topology information
- **Metrics APIs** (7 tools) - Performance and usage analytics
- **Security API** (1 tool) - Security audit trails
- **Pipeline Builder** (6 tools) - Interactive pipeline creation

## 🏗️ Pipeline Builder Capabilities

### Create Complete Data Pipelines
```bash
# 1. Initialize a new pipeline builder
sdc_create_pipeline_builder title="My ETL Pipeline" engine_type="data_collector"

# 2. Browse available stages
sdc_list_available_stages category="origins"

# 3. Add stages to your pipeline
sdc_add_pipeline_stage pipeline_id="pipeline_builder_1" stage_label="Dev Raw Data Source"
sdc_add_pipeline_stage pipeline_id="pipeline_builder_1" stage_label="Expression Evaluator"
sdc_add_pipeline_stage pipeline_id="pipeline_builder_1" stage_label="Trash"

# 4. Connect stages with data flows
sdc_connect_pipeline_stages pipeline_id="pipeline_builder_1" source_stage_id="stage_1" target_stage_id="stage_2"
sdc_connect_pipeline_stages pipeline_id="pipeline_builder_1" source_stage_id="stage_2" target_stage_id="stage_3"

# 5. Visualize your pipeline flow
sdc_get_pipeline_flow pipeline_id="pipeline_builder_1"

# 6. Build and publish (coming soon)
# sdc_build_pipeline pipeline_id="pipeline_builder_1"
# sdc_publish_pipeline pipeline_id="pipeline_builder_1"
```

### Persistent Pipeline Sessions
- **Cross-Conversation**: Continue building pipelines across multiple conversations
- **Auto-Save**: All changes automatically saved to disk
- **Session Management**: List, view, and delete pipeline builder sessions
- **Storage Location**: `~/.streamsets_mcp/pipeline_builders/`

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- StreamSets Control Hub account with API credentials
- Claude Desktop (for MCP integration)

### Setup
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/streamsets-mcp-server.git
   cd streamsets-mcp-server
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   ```bash
   export STREAMSETS_HOST_PREFIX="https://your-instance.streamsets.com"
   export STREAMSETS_CRED_ID="your-credential-id"
   export STREAMSETS_CRED_TOKEN="your-auth-token"
   ```

4. **Test the server**
   ```bash
   python streamsets_server.py
   ```

### Docker Deployment

#### Setup for MCP Integration
```bash
# Build the image
docker build -t streamsets-mcp-server .

# Create persistent volume for pipeline builders
docker volume create streamsets-pipeline-data
```

#### Manual Testing
```bash
# Test run with volume persistence
docker run --rm -it \
  -e STREAMSETS_HOST_PREFIX="https://your-instance.streamsets.com" \
  -e STREAMSETS_CRED_ID="your-credential-id" \
  -e STREAMSETS_CRED_TOKEN="your-auth-token" \
  -v streamsets-pipeline-data:/data \
  streamsets-mcp-server
```

### Claude Desktop Integration

#### Option 1: Direct Python (Local Development)
```json
{
  "mcpServers": {
    "streamsets": {
      "command": "python",
      "args": ["/path/to/streamsets_server.py"],
      "env": {
        "STREAMSETS_HOST_PREFIX": "https://your-instance.streamsets.com",
        "STREAMSETS_CRED_ID": "your-credential-id",
        "STREAMSETS_CRED_TOKEN": "your-auth-token"
      }
    }
  }
}
```

#### Option 2: Docker with Persistence (Production)
```json
{
  "mcpServers": {
    "streamsets": {
      "command": "docker",
      "args": [
        "run", "--rm", "-i",
        "-v", "streamsets-pipeline-data:/data",
        "-e", "STREAMSETS_HOST_PREFIX=https://your-instance.streamsets.com",
        "-e", "STREAMSETS_CRED_ID=your-credential-id",
        "-e", "STREAMSETS_CRED_TOKEN=your-auth-token",
        "streamsets-mcp-server"
      ]
    }
  }
}
```


## 📖 Usage Examples

### Job Management
```bash
# List all jobs
sdc_list_jobs organization="your-org" status="ACTIVE"

# Get detailed job information
sdc_get_job_details job_id="your-job-id"

# Start/stop jobs
sdc_start_job job_id="your-job-id"
sdc_stop_job job_id="your-job-id"

# Bulk operations
sdc_start_multiple_jobs job_ids="job1,job2,job3"
```

### Pipeline Operations
```bash
# Search pipelines
sdc_search_pipelines search_query="name==ETL*"

# Get pipeline details
sdc_get_pipeline_details pipeline_id="your-pipeline-id"

# Export/import pipelines
sdc_export_pipelines commit_ids="commit1,commit2"
```

### Metrics & Analytics
```bash
# Job performance metrics
sdc_get_job_metrics job_id="your-job-id"

# System health overview
sdc_get_job_count_by_status

# Executor infrastructure metrics
sdc_get_executor_metrics executor_type="COLLECTOR" label="prod"

# Security audit trails
sdc_get_security_audit_metrics org_id="your-org" audit_type="login"
```

## 🔧 Configuration

### Environment Variables

#### Required (StreamSets Authentication)
- `STREAMSETS_HOST_PREFIX` - StreamSets Control Hub URL
- `STREAMSETS_CRED_ID` - API Credential ID
- `STREAMSETS_CRED_TOKEN` - Authentication Token

#### Optional (Pipeline Builder Persistence)
- `PIPELINE_STORAGE_PATH` - Custom storage directory for pipeline builders

### Pipeline Builder Storage
Pipeline builders are automatically persisted across conversations and container restarts:

#### Storage Locations (Priority Order)
1. **Custom Path**: `PIPELINE_STORAGE_PATH` environment variable
2. **Docker Volume**: `/data/pipeline_builders` (when running in Docker)
3. **Default Path**: `~/.streamsets_mcp/pipeline_builders/`

#### Configuration Options
- **Format**: Pickle files for session persistence
- **Management**: Automatic file management with error handling
- **Fallback**: Memory-only mode if no writable storage available

#### Docker Persistence
When using Docker, pipeline builders persist in named volumes:
```bash
# Data persists in Docker volume 'streamsets-pipeline-data'
docker volume create streamsets-pipeline-data

# Run with persistent volume
docker run --rm -it -v streamsets-pipeline-data:/data streamsets-mcp-server
```

#### Troubleshooting
- **No Persistence**: Check storage directory permissions
- **Docker Issues**: Ensure volume mounts are configured correctly
- **Memory Mode**: Server logs will indicate if persistence is disabled

## 📚 Documentation

- **API Reference**: See `CLAUDE.md` for detailed tool documentation
- **Stage Library**: Built-in documentation for 25+ StreamSets stages
- **Configuration**: `custom.yaml` for MCP server registry
- **Swagger Specs**: API specifications in `/swagger/` directory

## 🧪 Development

### Project Structure
```
streamsets-mcp-server/
├── streamsets_server.py      # Main MCP server implementation
├── custom.yaml              # MCP server configuration
├── CLAUDE.md                # Comprehensive documentation
├── requirements.txt         # Python dependencies
├── Dockerfile              # Container deployment
├── swagger/                # API specifications
└── README.md               # This file
```

### Adding New Tools
1. Define tool function with `@mcp.tool()` decorator
2. Add comprehensive error handling and logging
3. Update `custom.yaml` with tool metadata
4. Document in `CLAUDE.md`

### Testing
```bash
# Syntax validation
python -m py_compile streamsets_server.py

# Tool count verification
grep -c "@mcp.tool()" streamsets_server.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **StreamSets** for the comprehensive Control Hub APIs
- **Anthropic** for the Model Context Protocol framework
- **FastMCP** for the Python MCP server implementation

## 📧 Support

For issues and questions:
- Create an issue on GitHub
- Check the documentation in `CLAUDE.md`
- Review the API specifications in `/swagger/`

---

**Transform your data pipeline workflows with conversational AI! 🚀**