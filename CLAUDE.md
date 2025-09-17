# Streamsets MCP Server - Implementation Guide

## Overview

This MCP server provides integration with Streamsets Control Hub APIs, enabling management of data pipelines and connections through Claude Desktop.

## API Authentication

The server uses Streamsets API Credentials authentication:
- Credential ID (`X-SS-App-Component-Id`)
- Authentication Token (`X-SS-App-Auth-Token`)

These are passed as headers on every API request along with:
- `X-SS-REST-CALL: true`
- `X-Requested-By: mcp-server`

## Implemented Tools

### Job Runner API Tools

#### Basic Job Management
1. **list_jobs** - Lists jobs with optional filtering
   - Parameters: organization, status, offset, limit
   - Returns formatted list of jobs with status

2. **get_job_details** - Gets complete job information
   - Parameters: job_id
   - Returns detailed job configuration and status

3. **get_job_status** - Gets current job execution status
   - Parameters: job_id
   - Returns status, color, run count, metrics

4. **start_job** - Starts a job execution
   - Parameters: job_id, runtime_params (JSON)
   - Returns job status after start

5. **stop_job** - Stops a running job
   - Parameters: job_id
   - Returns job status after stop

6. **get_job_history** - Gets job run history
   - Parameters: job_id, limit
   - Returns list of historical runs with status

#### Advanced Job Operations (Phase 1 Enhancement)
7. **get_job_logs** - Gets log messages for debugging
   - Parameters: job_id, run_count, component, offset, limit, order
   - Returns formatted log entries with timestamps

8. **force_stop_job** - Emergency force stop for unresponsive jobs
   - Parameters: job_id
   - Returns job status after force stop

9. **reset_job_offset** - Resets pipeline offsets for data recovery
   - Parameters: job_id
   - Returns success confirmation

10. **acknowledge_job_error** - Clears job error state
    - Parameters: job_id
    - Returns job status after error acknowledgment

#### Bulk Operations
11. **start_multiple_jobs** - Starts multiple jobs simultaneously
    - Parameters: job_ids (comma-separated), runtime_params (JSON)
    - Returns success/failure summary for each job

12. **stop_multiple_jobs** - Stops multiple jobs simultaneously
    - Parameters: job_ids (comma-separated)
    - Returns success/failure summary for each job

13. **get_bulk_job_status** - Gets status for multiple jobs
    - Parameters: job_ids (comma-separated)
    - Returns status summary for each job

#### Job Label Management
14. **get_job_labels** - Gets labels for a specific job
    - Parameters: job_id
    - Returns list of job labels

15. **list_all_job_labels** - Lists all job labels used across the system
    - Parameters: none
    - Returns alphabetical list of all available labels

16. **update_job_labels** - Updates labels for a specific job
    - Parameters: job_id, labels (comma-separated)
    - Returns confirmation and updated label list

17. **search_jobs_by_labels** - Searches jobs by labels using RSQL or simple filtering
    - Parameters: label_filter, organization, offset, limit
    - Returns jobs matching label criteria

18. **get_job_tags_bulk** - Gets tags/labels for multiple jobs
    - Parameters: job_ids (comma-separated)
    - Returns bulk tag information for job sets

### Pipeline Repository API Tools (Phase 2 Enhancement)

#### Pipeline Discovery & Management
19. **list_pipelines** - Lists pipelines with filtering options
    - Parameters: organization, filter_text, only_published, offset, limit, order_by
    - Returns formatted list of pipelines with metadata

20. **get_pipeline_details** - Gets detailed pipeline information (latest version)
    - Parameters: pipeline_id, only_published
    - Returns pipeline configuration and details

21. **get_pipeline_commit** - Gets specific pipeline commit details
    - Parameters: commit_id, include_library_definitions
    - Returns version-specific pipeline information

22. **get_pipeline_history** - Gets version history for a pipeline
    - Parameters: pipeline_id, limit, only_published
    - Returns chronological list of pipeline versions

23. **search_pipelines** - Advanced pipeline search with RSQL support
    - Parameters: organization, search_query, offset, limit
    - Returns filtered pipeline results

#### Pipeline DevOps & Deployment
24. **export_pipelines** - Exports pipelines for backup/migration
    - Parameters: commit_ids (comma-separated), include_fragments, include_credentials
    - Returns export data for deployment

25. **import_pipelines** - Imports pipelines from export data
    - Parameters: pipeline_data (JSON), commit_message, import_as_new
    - Returns import success summary

### Connection API Tools

26. **list_connections** - Lists connections with filtering
    - Parameters: organization, connection_type, offset, limit
    - Returns formatted list of connections

27. **get_connection_details** - Gets complete connection information
    - Parameters: connection_id
    - Returns detailed connection configuration

28. **get_connection_types** - Lists available connection types
    - Parameters: organization
    - Returns list of connection type labels and IDs

29. **get_pipelines_using_connection** - Lists pipelines using a connection
    - Parameters: connection_id, limit
    - Returns list of pipeline commits using the connection

### Enterprise Integration API Tools (Phase 3 Enhancement)

#### Infrastructure & Deployment Management
30. **list_deployments** - Lists infrastructure deployments with filtering
    - Parameters: organization, filter_text, offset, limit
    - Returns deployment inventory with status and configuration

31. **get_deployment_details** - Gets detailed deployment information
    - Parameters: deployment_id
    - Returns comprehensive deployment configuration and state

32. **start_deployment** - Starts an infrastructure deployment
    - Parameters: deployment_id
    - Returns deployment start confirmation

33. **stop_deployment** - Stops an infrastructure deployment
    - Parameters: deployment_id, force
    - Returns deployment stop confirmation (supports force stop)

#### Monitoring & Alert Management
34. **list_alerts** - Lists system alerts with filtering
    - Parameters: alert_status, filter_text, offset, limit
    - Returns alerts with severity, status, and timestamps

#### Security & Access Control
35. **get_job_acl** - Gets Access Control List for a job
    - Parameters: job_id
    - Returns job permissions and resource owner information

36. **update_job_acl** - Updates Access Control List for a job
    - Parameters: job_id, permissions_json
    - Returns updated ACL confirmation

#### Data Lineage & Governance
37. **get_topology_details** - Gets data lineage topology for pipeline commits
    - Parameters: commit_id
    - Returns data flow visualization and dependency mapping

### Metrics and Analytics API Tools

#### Job Performance Metrics
38. **get_job_metrics** - Gets detailed performance metrics for a specific job
    - Parameters: job_id, run_count, sdc_id
    - Returns throughput, error rates, runtime metrics with formatted display

39. **get_problematic_jobs** - Gets jobs with error/failed status for monitoring
    - Parameters: organization, job_status, executor_type, offset, limit
    - Returns failed jobs with error details and timestamps

40. **get_job_count_by_status** - Gets job distribution by status for system health
    - Parameters: none
    - Returns job counts by status with visual indicators

41. **get_executor_metrics** - Gets infrastructure performance metrics
    - Parameters: executor_type, label, order_by
    - Returns CPU, memory, uptime metrics for executors

#### Pipeline Analytics
42. **get_pipeline_metrics** - Gets pipeline creation and usage analytics
    - Parameters: organization, start_time, end_time, group, limit
    - Returns time-based pipeline usage and adoption metrics

#### Security & User Analytics
43. **get_user_login_metrics** - Gets user engagement and login analytics
    - Parameters: org_id, start_time, end_time, active_users_only
    - Returns active/inactive user analysis with login patterns

44. **get_security_audit_metrics** - Gets security audit trail for compliance
    - Parameters: org_id, start_time, end_time, audit_type, limit
    - Returns login audits and user action trails

## Error Handling

All tools include comprehensive error handling:
- Configuration validation (missing env vars)
- HTTP status error handling with response codes
- JSON parsing error handling
- Timeout handling (30 second default)
- User-friendly error messages

## Response Formatting

Responses use emojis for visual clarity:
- ✅ Success operations
- ❌ Errors or failures
- 📊 Job information
- 🔌 Connection information
- 📋 Pipeline information
- ⚠️ Warnings

## Technical Details

### Environment Variables
- `STREAMSETS_HOST_PREFIX` - Base URL (e.g., https://cloud.streamsets.com)
- `STREAMSETS_CRED_ID` - API Credential ID
- `STREAMSETS_CRED_TOKEN` - API Authentication Token

### API Endpoints Used

#### Job Runner
- `/jobrunner/rest/v1/jobs` - List jobs
- `/jobrunner/rest/v1/job/{jobId}` - Get job details
- `/jobrunner/rest/v1/job/{jobId}/currentStatus` - Get current status
- `/jobrunner/rest/v1/job/{jobId}/start` - Start job
- `/jobrunner/rest/v1/job/{jobId}/stop` - Stop job
- `/jobrunner/rest/v1/saql/jobs/search/{jobId}/runs` - Get run history

#### Connections
- `/connection/rest/v1/connections` - List connections
- `/connection/rest/v1/connection/{connectionId}` - Get connection details
- `/connection/rest/v1/connections/types` - Get connection types
- `/connection/rest/v1/connection/{connId}/getPipelineCommits` - Get pipeline commits

### Data Transformation

The server transforms API responses:
- Timestamps converted from milliseconds to ISO format
- JSON runtime parameters parsed and displayed
- Status information formatted for readability
- Nested objects flattened for display

## Future Enhancements

Potential additions based on available APIs:
- Job creation and deletion
- Connection creation and updates
- ACL management for jobs and connections
- Job template management
- Bulk operations (start/stop multiple jobs)
- Metrics and monitoring endpoints
- SDC (Data Collector) management
- Pipeline offset management
- Job upgrade operations

## Testing

### Manual Testing Commands

```bash
# Test listing jobs
curl -X GET "https://your-instance/jobrunner/rest/v1/jobs" \
  -H "X-SS-App-Component-Id: $CRED_ID" \
  -H "X-SS-App-Auth-Token: $CRED_TOKEN" \
  -H "X-SS-REST-CALL: true" \
  -H "X-Requested-By: curl"

# Test getting job status
curl -X GET "https://your-instance/jobrunner/rest/v1/job/{jobId}/currentStatus" \
  -H "X-SS-App-Component-Id: $CRED_ID" \
  -H "X-SS-App-Auth-Token: $CRED_TOKEN" \
  -H "X-SS-REST-CALL: true" \
  -H "X-Requested-By: curl"
```

## Development Resources

### Python SDK Reference
The `streamsets_python_source.py` file contains the complete Streamsets Python SDK source code and serves as:
- **API Usage Examples**: Real-world patterns for calling Streamsets APIs correctly
- **Authentication Reference**: Proper header and credential handling
- **Response Processing**: How to parse and handle API responses
- **Pipeline Builder Classes**: Foundation for future pipeline creation capabilities
- **Error Handling Patterns**: Best practices for robust API integration

Key insights from the Python SDK:
- Job logs require `run_count` parameter from current job status
- PipelineBuilder classes available for programmatic pipeline creation
- Complex data transformations and model mappings

## Known Limitations

1. No support for file uploads (offset files, job imports)
2. No WebSocket support for real-time status updates
3. Limited to REST API operations (no GraphQL endpoints)
4. No support for complex search queries (RSQL)
5. Pagination limited to offset/limit (no cursor support)

## Security Notes

- Credentials stored securely in Docker secrets
- No credential logging or output
- HTTPS required for all API communication
- Token-based authentication (no username/password)
- Permissions inherited from API credentials