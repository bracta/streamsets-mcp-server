#!/usr/bin/env python3
"""
Streamsets MCP Server - Provides access to Streamsets Control Hub APIs
"""
import os
import sys
import logging
from datetime import datetime, timezone
import httpx
import json
import pickle
from pathlib import Path
from mcp.server.fastmcp import FastMCP

# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("streamsets-server")

# Initialize MCP server - NO PROMPT PARAMETER!
mcp = FastMCP("streamsets")

# Configuration
HOST_PREFIX = os.environ.get("STREAMSETS_HOST_PREFIX", "")
CRED_ID = os.environ.get("STREAMSETS_CRED_ID", "")
CRED_TOKEN = os.environ.get("STREAMSETS_CRED_TOKEN", "")

# === PIPELINE BUILDER SESSION MANAGEMENT ===
class PipelineBuilderSession:
    """Manages pipeline builder state and operations."""
    def __init__(self, title, description="", engine_type="data_collector", engine_id=""):
        self.pipeline_id = None
        self.title = title
        self.description = description
        self.engine_type = engine_type
        self.engine_id = engine_id
        self.stages = {}  # stage_id -> stage_info
        self.connections = []  # list of {source, target, type}
        self.runtime_parameters = {}
        self.labels = []
        self.created_time = datetime.now(tz=timezone.utc)
        self.built_pipeline = None
        self.published = False

    def add_stage(self, stage_id, stage_info):
        """Add a stage to the session."""
        self.stages[stage_id] = stage_info

    def add_connection(self, source_stage_id, target_stage_id, connection_type="data"):
        """Add a connection between stages."""
        connection = {
            "source": source_stage_id,
            "target": target_stage_id,
            "type": connection_type
        }
        self.connections.append(connection)

    def get_summary(self):
        """Get a summary of the pipeline builder session."""
        return {
            "pipeline_id": self.pipeline_id,
            "title": self.title,
            "description": self.description,
            "engine_type": self.engine_type,
            "engine_id": self.engine_id,
            "stage_count": len(self.stages),
            "connection_count": len(self.connections),
            "created_time": self.created_time.isoformat(),
            "built": self.built_pipeline is not None,
            "published": self.published
        }

# Pipeline Builder Persistence Configuration
def get_storage_directory():
    """Get the pipeline storage directory based on environment configuration."""
    # Check for custom storage path (Docker volume support)
    custom_path = os.environ.get("PIPELINE_STORAGE_PATH")
    if custom_path:
        path = Path(custom_path)
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using custom storage path: {path}")
            return path
        except Exception as e:
            logger.warning(f"Cannot use custom storage path {path}: {e}")

    # Check if running in Docker (volume mount exists)
    docker_path = Path("/data/pipeline_builders")
    try:
        if docker_path.parent.exists() and docker_path.parent.is_dir():
            docker_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using Docker volume storage: {docker_path}")
            return docker_path
    except Exception as e:
        logger.warning(f"Cannot use Docker volume path {docker_path}: {e}")

    # Default to user home directory
    default_path = Path.home() / ".streamsets_mcp" / "pipeline_builders"
    try:
        default_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using default storage path: {default_path}")
        return default_path
    except Exception as e:
        logger.error(f"Cannot create default storage path {default_path}: {e}")
        return None

# Initialize storage paths with enhanced error handling
PIPELINE_STORAGE_DIR = get_storage_directory()
PIPELINE_COUNTER_FILE = PIPELINE_STORAGE_DIR / "counter.txt" if PIPELINE_STORAGE_DIR else None

# Validate storage initialization
if PIPELINE_STORAGE_DIR is None:
    logger.error("⚠️ Pipeline persistence disabled - no writable storage directory available")
    logger.error("⚠️ Pipeline builders will only exist in memory for this session")
else:
    logger.info(f"✅ Pipeline persistence enabled at: {PIPELINE_STORAGE_DIR}")

# Global pipeline builder storage (loaded from files)
active_pipeline_builders = {}
pipeline_counter = 0

def load_pipeline_counter():
    """Load the pipeline counter from file."""
    global pipeline_counter
    if PIPELINE_COUNTER_FILE is None:
        pipeline_counter = 0
        return

    try:
        if PIPELINE_COUNTER_FILE.exists():
            with open(PIPELINE_COUNTER_FILE, 'r') as f:
                pipeline_counter = int(f.read().strip())
        else:
            pipeline_counter = 0
    except Exception as e:
        logger.warning(f"Could not load pipeline counter: {e}")
        pipeline_counter = 0

def save_pipeline_counter():
    """Save the pipeline counter to file."""
    if PIPELINE_COUNTER_FILE is None:
        return

    try:
        with open(PIPELINE_COUNTER_FILE, 'w') as f:
            f.write(str(pipeline_counter))
    except Exception as e:
        logger.error(f"Could not save pipeline counter: {e}")

def get_pipeline_file_path(pipeline_id):
    """Get the file path for storing a pipeline builder session."""
    if PIPELINE_STORAGE_DIR is None:
        return None
    return PIPELINE_STORAGE_DIR / f"{pipeline_id}.pkl"

def save_pipeline_session(pipeline_id, session):
    """Save a pipeline builder session to disk."""
    if PIPELINE_STORAGE_DIR is None:
        logger.debug(f"Persistence disabled - pipeline session {pipeline_id} not saved to disk")
        return

    try:
        file_path = get_pipeline_file_path(pipeline_id)
        with open(file_path, 'wb') as f:
            pickle.dump(session, f)
        logger.info(f"Saved pipeline session {pipeline_id} to {file_path}")
    except Exception as e:
        logger.error(f"Could not save pipeline session {pipeline_id}: {e}")

def load_pipeline_session(pipeline_id):
    """Load a pipeline builder session from disk."""
    if PIPELINE_STORAGE_DIR is None:
        return None

    try:
        file_path = get_pipeline_file_path(pipeline_id)
        if file_path.exists():
            with open(file_path, 'rb') as f:
                session = pickle.load(f)
            logger.info(f"Loaded pipeline session {pipeline_id} from {file_path}")
            return session
    except Exception as e:
        logger.error(f"Could not load pipeline session {pipeline_id}: {e}")
    return None

def load_all_pipeline_sessions():
    """Load all pipeline builder sessions from disk."""
    global active_pipeline_builders
    active_pipeline_builders = {}

    if PIPELINE_STORAGE_DIR is None:
        logger.debug("Persistence disabled - no pipeline sessions loaded from disk")
        return

    try:
        for file_path in PIPELINE_STORAGE_DIR.glob("pipeline_builder_*.pkl"):
            pipeline_id = file_path.stem
            session = load_pipeline_session(pipeline_id)
            if session:
                active_pipeline_builders[pipeline_id] = session

        logger.info(f"Loaded {len(active_pipeline_builders)} pipeline sessions from disk")
    except Exception as e:
        logger.error(f"Error loading pipeline sessions: {e}")

def delete_pipeline_session_file(pipeline_id):
    """Delete a pipeline builder session file from disk."""
    if PIPELINE_STORAGE_DIR is None:
        return

    try:
        file_path = get_pipeline_file_path(pipeline_id)
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted pipeline session file {pipeline_id}")
    except Exception as e:
        logger.error(f"Could not delete pipeline session file {pipeline_id}: {e}")

def get_next_pipeline_id():
    """Generate next pipeline ID."""
    global pipeline_counter
    pipeline_counter += 1
    save_pipeline_counter()
    return f"pipeline_builder_{pipeline_counter}"

def get_pipeline_session(pipeline_id):
    """Get pipeline builder session by ID (load from disk if not in memory)."""
    # Check memory first
    if pipeline_id in active_pipeline_builders:
        return active_pipeline_builders[pipeline_id]

    # Try to load from disk
    session = load_pipeline_session(pipeline_id)
    if session:
        active_pipeline_builders[pipeline_id] = session
        return session

    return None

def list_active_sessions():
    """List all active pipeline builder sessions (including those on disk)."""
    # Load all sessions from disk first
    load_all_pipeline_sessions()
    return {pid: session.get_summary() for pid, session in active_pipeline_builders.items()}

# Initialize persistence on startup
load_pipeline_counter()
load_all_pipeline_sessions()

# === UTILITY FUNCTIONS ===
def get_auth_headers():
    """Build authentication headers for Streamsets API."""
    return {
        "Content-Type": "application/json",
        "X-Requested-By": "mcp-server",
        "X-SS-REST-CALL": "true",
        "X-SS-App-Component-Id": CRED_ID,
        "X-SS-App-Auth-Token": CRED_TOKEN
    }

def format_job_info(job):
    """Format job information for display."""
    result = []
    result.append(f"📊 Job: {job.get('name', 'N/A')}")
    result.append(f"  ID: {job.get('id', 'N/A')}")
    result.append(f"  Pipeline: {job.get('pipelineName', 'N/A')}")
    result.append(f"  Creator: {job.get('creator', 'N/A')}")
    
    if job.get('currentJobStatus'):
        status = job['currentJobStatus']
        result.append(f"  Status: {status.get('status', 'UNKNOWN')}")
        result.append(f"  Color: {status.get('color', 'GRAY')}")
    
    if job.get('labels'):
        result.append(f"  Labels: {', '.join(job['labels'])}")
    
    return "\n".join(result)

def format_connection_info(conn):
    """Format connection information for display."""
    result = []
    result.append(f"🔌 Connection: {conn.get('name', 'N/A')}")
    result.append(f"  ID: {conn.get('id', 'N/A')}")
    result.append(f"  Type: {conn.get('connectionType', 'N/A')}")
    result.append(f"  Created By: {conn.get('createdBy', 'N/A')}")
    
    if conn.get('tags'):
        tags = [tag.get('tag', '') for tag in conn['tags'] if isinstance(tag, dict)]
        if tags:
            result.append(f"  Tags: {', '.join(tags)}")
    
    return "\n".join(result)

# === JOB RUNNER TOOLS ===

@mcp.tool()
async def list_jobs(organization: str = "", status: str = "", offset: str = "0", limit: str = "20") -> str:
    """List jobs from Streamsets Control Hub with optional filtering by status."""
    logger.info(f"Listing jobs for org={organization}, status={status}")
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/jobs"
        params = {}
        
        if organization.strip():
            params["organization"] = organization
        if status.strip():
            params["jobStatus"] = status
        
        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20
        params["orderBy"] = "NAME"
        params["order"] = "ASC"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            jobs = response.json()
            
            if not jobs:
                return "✅ No jobs found"
            
            results = [f"✅ Found {len(jobs)} jobs:\n"]
            for job in jobs:
                results.append(format_job_info(job))
                results.append("")  # Empty line between jobs
            
            return "\n".join(results)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_details(job_id: str = "") -> str:
    """Get detailed information about a specific job."""
    logger.info(f"Getting job details for {job_id}")
    
    if not job_id.strip():
        return "❌ Error: job_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()
            
            job = response.json()
            
            result = ["✅ Job Details:\n"]
            result.append(format_job_info(job))
            
            # Add runtime parameters if present
            if job.get('runtimeParameters'):
                result.append("\n  Runtime Parameters:")
                try:
                    params = json.loads(job['runtimeParameters'])
                    for key, value in params.items():
                        result.append(f"    {key}: {value}")
                except:
                    result.append(f"    {job['runtimeParameters']}")
            
            return "\n".join(result)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_status(job_id: str = "") -> str:
    """Get the current status of a specific job."""
    logger.info(f"Getting job status for {job_id}")
    
    if not job_id.strip():
        return "❌ Error: job_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/currentStatus"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()
            
            status = response.json()
            
            result = ["✅ Job Status:\n"]
            result.append(f"📊 Job ID: {status.get('jobId', 'N/A')}")
            result.append(f"  Status: {status.get('status', 'UNKNOWN')}")
            result.append(f"  Color: {status.get('color', 'GRAY')}")
            result.append(f"  Run Count: {status.get('runCount', 0)}")
            
            if status.get('startTime'):
                start = datetime.fromtimestamp(status['startTime']/1000, tz=timezone.utc)
                result.append(f"  Start Time: {start.isoformat()}")
            
            if status.get('finishTime'):
                finish = datetime.fromtimestamp(status['finishTime']/1000, tz=timezone.utc)
                result.append(f"  Finish Time: {finish.isoformat()}")
            
            if status.get('errorMessage'):
                result.append(f"  ⚠️ Error: {status['errorMessage']}")
            
            if status.get('inputRecordCount'):
                result.append(f"  Records - Input: {status['inputRecordCount']}, Output: {status.get('outputRecordCount', 0)}, Error: {status.get('errorRecordCount', 0)}")
            
            return "\n".join(result)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def start_job(job_id: str = "", runtime_params: str = "") -> str:
    """Start a job with optional runtime parameters (JSON string)."""
    logger.info(f"Starting job {job_id}")
    
    if not job_id.strip():
        return "❌ Error: job_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/start"
        
        body = {}
        if runtime_params.strip():
            try:
                body = json.loads(runtime_params)
            except json.JSONDecodeError:
                return "❌ Error: runtime_params must be valid JSON"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                json=body,
                timeout=30
            )
            response.raise_for_status()
            
            status = response.json()
            
            return f"✅ Job started successfully!\n  Status: {status.get('status', 'UNKNOWN')}\n  Color: {status.get('color', 'GRAY')}"
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error starting job: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def stop_job(job_id: str = "") -> str:
    """Stop a running job."""
    logger.info(f"Stopping job {job_id}")
    
    if not job_id.strip():
        return "❌ Error: job_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/stop"
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()
            
            status = response.json()
            
            return f"✅ Job stopped successfully!\n  Status: {status.get('status', 'UNKNOWN')}\n  Color: {status.get('color', 'GRAY')}"
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error stopping job: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_history(job_id: str = "", limit: str = "10") -> str:
    """Get the run history for a specific job."""
    logger.info(f"Getting job history for {job_id}")
    
    if not job_id.strip():
        return "❌ Error: job_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/saql/jobs/search/{job_id}/runs"
        
        params = {
            "offset": 0,
            "len": int(limit) if limit.strip() else 10,
            "orderBy": "run_count"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            runs = data.get('data', []) if isinstance(data, dict) else data
            
            if not runs:
                return "✅ No run history found"
            
            results = [f"✅ Job Run History ({len(runs)} runs):\n"]
            
            for run in runs:
                results.append(f"Run #{run.get('runCount', 'N/A')}")
                results.append(f"  Status: {run.get('status', 'UNKNOWN')}")
                results.append(f"  Color: {run.get('color', 'GRAY')}")
                
                if run.get('startTime'):
                    start = datetime.fromtimestamp(run['startTime']/1000, tz=timezone.utc)
                    results.append(f"  Start: {start.isoformat()}")
                
                if run.get('finishTime'):
                    finish = datetime.fromtimestamp(run['finishTime']/1000, tz=timezone.utc)
                    results.append(f"  Finish: {finish.isoformat()}")
                
                if run.get('errorMessage'):
                    results.append(f"  ⚠️ Error: {run['errorMessage'][:100]}")
                
                results.append("")
            
            return "\n".join(results)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job history: {e}")
        return f"❌ Error: {str(e)}"

# === CONNECTION TOOLS ===

@mcp.tool()
async def list_connections(organization: str = "", connection_type: str = "", offset: str = "0", limit: str = "20") -> str:
    """List connections from Streamsets Control Hub with optional filtering."""
    logger.info(f"Listing connections for org={organization}, type={connection_type}")
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/connection/rest/v1/connections"
        params = {}
        
        if organization.strip():
            params["organization"] = organization
        if connection_type.strip():
            params["connectionType"] = connection_type
        
        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20
        params["orderBy"] = "NAME"
        params["order"] = "ASC"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            connections = data.get('data', []) if isinstance(data, dict) else data
            
            if not connections:
                return "✅ No connections found"
            
            results = [f"✅ Found {len(connections)} connections:\n"]
            for conn in connections:
                results.append(format_connection_info(conn))
                results.append("")  # Empty line between connections
            
            return "\n".join(results)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing connections: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_connection_details(connection_id: str = "") -> str:
    """Get detailed information about a specific connection."""
    logger.info(f"Getting connection details for {connection_id}")
    
    if not connection_id.strip():
        return "❌ Error: connection_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/connection/rest/v1/connection/{connection_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()
            
            conn = response.json()
            
            result = ["✅ Connection Details:\n"]
            result.append(format_connection_info(conn))
            
            if conn.get('description'):
                result.append(f"  Description: {conn['description']}")
            
            if conn.get('lastModifiedBy'):
                result.append(f"  Last Modified By: {conn['lastModifiedBy']}")
                if conn.get('lastModifiedTime'):
                    modified = datetime.fromtimestamp(conn['lastModifiedTime']/1000, tz=timezone.utc)
                    result.append(f"  Last Modified Time: {modified.isoformat()}")
            
            if conn.get('sdcVersion'):
                result.append(f"  SDC Version: {conn['sdcVersion']}")
            
            return "\n".join(result)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting connection details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_connection_types(organization: str = "") -> str:
    """Get available connection types for an organization."""
    logger.info(f"Getting connection types for org={organization}")
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/connection/rest/v1/connections/types"
        params = {}
        
        if organization.strip():
            params["organization"] = organization
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            types = response.json()
            
            if not types:
                return "✅ No connection types found"
            
            results = [f"✅ Available Connection Types ({len(types)}):\n"]
            for conn_type in types:
                results.append(f"  • {conn_type.get('label', 'N/A')} ({conn_type.get('type', 'N/A')})")
            
            return "\n".join(results)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting connection types: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_pipelines_using_connection(connection_id: str = "", limit: str = "20") -> str:
    """Get pipelines that are using a specific connection."""
    logger.info(f"Getting pipelines using connection {connection_id}")
    
    if not connection_id.strip():
        return "❌ Error: connection_id is required"
    
    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"
    
    try:
        url = f"{HOST_PREFIX}/connection/rest/v1/connection/{connection_id}/getPipelineCommits"
        
        params = {
            "latest": True,
            "offset": 0,
            "len": int(limit) if limit.strip() else 20
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            pipelines = response.json()
            
            if not pipelines:
                return "✅ No pipelines using this connection"
            
            results = [f"✅ Pipelines using connection ({len(pipelines)}):\n"]
            for pipeline in pipelines:
                results.append(f"  📋 {pipeline.get('pipelineName', 'N/A')}")
                results.append(f"     ID: {pipeline.get('pipelineId', 'N/A')}")
                results.append(f"     Version: {pipeline.get('pipelineVersion', 'N/A')}")
                
                if pipeline.get('lastModifiedOn'):
                    modified = datetime.fromtimestamp(pipeline['lastModifiedOn']/1000, tz=timezone.utc)
                    results.append(f"     Last Modified: {modified.isoformat()}")
                
                results.append("")
            
            return "\n".join(results)
            
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting pipelines using connection: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_logs(job_id: str = "", run_count: str = "", component: str = "", offset: str = "0", limit: str = "50", order: str = "DESC") -> str:
    """Get log messages for a specific job with optional filtering."""
    logger.info(f"Getting job logs for {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    # Based on the Python SDK pattern, it appears logs might require an active job with a run_count
    # Let's try to get the current job status first if no run_count is provided
    if not run_count.strip():
        try:
            status_url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/currentStatus"
            async with httpx.AsyncClient() as client:
                status_response = await client.get(
                    status_url,
                    headers=get_auth_headers(),
                    timeout=30
                )
                status_response.raise_for_status()
                status_data = status_response.json()
                if status_data.get('runCount'):
                    run_count = str(status_data['runCount'])
                    logger.info(f"Auto-detected run_count: {run_count}")
        except Exception as e:
            logger.warning(f"Could not auto-detect run_count: {e}")
            # Continue without run_count

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/logMessages"
        params = {}

        # jobId is required for this API despite being marked optional in swagger
        params["jobId"] = job_id

        # Set proper defaults according to swagger spec
        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else -1  # Default is -1 according to swagger
        params["order"] = order if order in ["ASC", "DESC"] else "DESC"

        # Add optional parameters only if provided
        if run_count.strip():
            try:
                params["runCount"] = int(run_count)
            except ValueError:
                return "❌ Error: run_count must be a valid integer"

        if component.strip():
            params["component"] = component

        # Log the request for debugging
        logger.info(f"Making request to {url} with params: {params}")

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            logs = response.json()

            if not logs:
                return "✅ No log messages found"

            results = [f"✅ Job Log Messages ({len(logs)} entries):\n"]

            for log in logs:
                # Handle case where log might be a string instead of dict
                if isinstance(log, str):
                    results.append(f"📝 {log}")
                    results.append("")
                    continue

                # Handle dict log entries
                if isinstance(log, dict):
                    timestamp_ms = log.get('timestamp', 0)
                    if timestamp_ms and timestamp_ms > 0:
                        timestamp = datetime.fromtimestamp(timestamp_ms/1000, tz=timezone.utc)
                        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        timestamp_str = "Unknown time"

                    level = log.get('level', 'INFO')
                    message = log.get('message', 'N/A')

                    results.append(f"[{timestamp_str}] {level}: {message}")

                    if log.get('component'):
                        results.append(f"  Component: {log['component']}")
                    if log.get('runCount'):
                        results.append(f"  Run: #{log['runCount']}")
                else:
                    # Handle unexpected log format
                    results.append(f"📝 Raw log: {str(log)}")

                results.append("")  # Empty line between log entries

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        error_text = e.response.text if hasattr(e.response, 'text') else str(e.response.content)
        logger.error(f"Full error response: {error_text}")
        return f"❌ API Error: {e.response.status_code} - {error_text[:500]}"
    except Exception as e:
        logger.error(f"Error getting job logs: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def force_stop_job(job_id: str = "") -> str:
    """Force stop a job (emergency stop when normal stop fails)."""
    logger.info(f"Force stopping job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/forceStop"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            status = response.json()

            return f"✅ Job force stopped successfully!\n  Status: {status.get('status', 'UNKNOWN')}\n  Color: {status.get('color', 'GRAY')}"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error force stopping job: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def reset_job_offset(job_id: str = "") -> str:
    """Reset all pipeline offsets for a job (data recovery)."""
    logger.info(f"Resetting offset for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/resetOffset"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            return f"✅ Job offset reset successfully for job {job_id}"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error resetting job offset: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def acknowledge_job_error(job_id: str = "") -> str:
    """Acknowledge error for a job to clear error state."""
    logger.info(f"Acknowledging error for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/acknowledgeError"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            status = response.json()

            return f"✅ Job error acknowledged successfully!\n  Status: {status.get('status', 'UNKNOWN')}\n  Color: {status.get('color', 'GRAY')}"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error acknowledging job error: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_bulk_job_status(job_ids: str = "") -> str:
    """Get status for multiple jobs (comma-separated job IDs)."""
    logger.info(f"Getting bulk job status for {job_ids}")

    if not job_ids.strip():
        return "❌ Error: job_ids is required (comma-separated list)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        job_id_list = [jid.strip() for jid in job_ids.split(',') if jid.strip()]

        if not job_id_list:
            return "❌ Error: No valid job IDs provided"

        results = [f"✅ Bulk Job Status ({len(job_id_list)} jobs):\n"]

        async with httpx.AsyncClient() as client:
            for job_id in job_id_list:
                try:
                    url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/currentStatus"
                    response = await client.get(
                        url,
                        headers=get_auth_headers(),
                        timeout=30
                    )
                    response.raise_for_status()

                    status = response.json()
                    results.append(f"📊 {job_id}: {status.get('status', 'UNKNOWN')} ({status.get('color', 'GRAY')})")

                except Exception as e:
                    results.append(f"❌ {job_id}: Error - {str(e)[:50]}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error getting bulk job status: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def start_multiple_jobs(job_ids: str = "", runtime_params: str = "") -> str:
    """Start multiple jobs (comma-separated job IDs) with optional runtime parameters."""
    logger.info(f"Starting multiple jobs: {job_ids}")

    if not job_ids.strip():
        return "❌ Error: job_ids is required (comma-separated list)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        job_id_list = [jid.strip() for jid in job_ids.split(',') if jid.strip()]

        if not job_id_list:
            return "❌ Error: No valid job IDs provided"

        # Parse runtime parameters if provided
        body = {}
        if runtime_params.strip():
            try:
                body = json.loads(runtime_params)
            except json.JSONDecodeError:
                return "❌ Error: runtime_params must be valid JSON"

        results = [f"✅ Starting {len(job_id_list)} jobs:\n"]
        success_count = 0
        failed_jobs = []

        async with httpx.AsyncClient() as client:
            for job_id in job_id_list:
                try:
                    url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/start"
                    response = await client.post(
                        url,
                        headers=get_auth_headers(),
                        json=body,
                        timeout=30
                    )
                    response.raise_for_status()

                    status = response.json()
                    results.append(f"✅ {job_id}: Started - {status.get('status', 'UNKNOWN')}")
                    success_count += 1

                except Exception as e:
                    error_msg = str(e)[:100]
                    results.append(f"❌ {job_id}: Failed - {error_msg}")
                    failed_jobs.append(job_id)

        # Summary
        results.append(f"\n📊 Summary: {success_count}/{len(job_id_list)} jobs started successfully")
        if failed_jobs:
            results.append(f"⚠️ Failed jobs: {', '.join(failed_jobs)}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error starting multiple jobs: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def stop_multiple_jobs(job_ids: str = "") -> str:
    """Stop multiple jobs (comma-separated job IDs)."""
    logger.info(f"Stopping multiple jobs: {job_ids}")

    if not job_ids.strip():
        return "❌ Error: job_ids is required (comma-separated list)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        job_id_list = [jid.strip() for jid in job_ids.split(',') if jid.strip()]

        if not job_id_list:
            return "❌ Error: No valid job IDs provided"

        results = [f"✅ Stopping {len(job_id_list)} jobs:\n"]
        success_count = 0
        failed_jobs = []

        async with httpx.AsyncClient() as client:
            for job_id in job_id_list:
                try:
                    url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/stop"
                    response = await client.post(
                        url,
                        headers=get_auth_headers(),
                        timeout=30
                    )
                    response.raise_for_status()

                    status = response.json()
                    results.append(f"✅ {job_id}: Stopped - {status.get('status', 'UNKNOWN')}")
                    success_count += 1

                except Exception as e:
                    error_msg = str(e)[:100]
                    results.append(f"❌ {job_id}: Failed - {error_msg}")
                    failed_jobs.append(job_id)

        # Summary
        results.append(f"\n📊 Summary: {success_count}/{len(job_id_list)} jobs stopped successfully")
        if failed_jobs:
            results.append(f"⚠️ Failed jobs: {', '.join(failed_jobs)}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error stopping multiple jobs: {e}")
        return f"❌ Error: {str(e)}"

# === PIPELINE REPOSITORY TOOLS ===

def format_pipeline_info(pipeline):
    """Format pipeline information for display."""
    result = []
    result.append(f"📋 Pipeline: {pipeline.get('pipelineName', 'N/A')}")
    result.append(f"  ID: {pipeline.get('pipelineId', 'N/A')}")
    result.append(f"  Version: {pipeline.get('version', 'N/A')}")
    result.append(f"  Creator: {pipeline.get('creator', 'N/A')}")

    if pipeline.get('commitTime'):
        commit_time = datetime.fromtimestamp(pipeline['commitTime']/1000, tz=timezone.utc)
        result.append(f"  Commit Time: {commit_time.isoformat()}")

    if pipeline.get('commitMessage'):
        result.append(f"  Message: {pipeline['commitMessage']}")

    if pipeline.get('labels'):
        result.append(f"  Labels: {', '.join(pipeline['labels'])}")

    return "\n".join(result)

@mcp.tool()
async def list_pipelines(organization: str = "", filter_text: str = "", only_published: str = "true", offset: str = "0", limit: str = "20", order_by: str = "NAME") -> str:
    """List pipelines from Streamsets Control Hub with filtering options."""
    logger.info(f"Listing pipelines for org={organization}, filter={filter_text}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipelines"
        params = {}

        if organization.strip():
            params["organization"] = organization
        if filter_text.strip():
            params["filterText"] = filter_text

        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20
        params["orderBy"] = order_by if order_by in ["NAME", "COMMIT_TIME", "VERSION"] else "NAME"
        params["order"] = "ASC"
        params["onlyPublished"] = only_published.lower() == "true"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            pipelines = data.get('data', []) if isinstance(data, dict) else data

            if not pipelines:
                return "✅ No pipelines found"

            results = [f"✅ Found {len(pipelines)} pipelines:\n"]
            for pipeline in pipelines:
                results.append(format_pipeline_info(pipeline))
                results.append("")  # Empty line between pipelines

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_pipeline_details(pipeline_id: str = "", only_published: str = "true") -> str:
    """Get detailed information about a specific pipeline (latest version)."""
    logger.info(f"Getting pipeline details for {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipeline/{pipeline_id}/latest"
        params = {
            "onlyPublished": only_published.lower() == "true"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            pipeline = response.json()

            result = ["✅ Pipeline Details:\n"]
            result.append(format_pipeline_info(pipeline))

            # Add pipeline configuration details
            if pipeline.get('pipelineConfig'):
                config = pipeline['pipelineConfig']
                result.append(f"\n  Configuration:")
                result.append(f"    Description: {config.get('description', 'N/A')}")
                if config.get('stages'):
                    result.append(f"    Stages: {len(config['stages'])}")
                if config.get('executionMode'):
                    result.append(f"    Execution Mode: {config['executionMode']}")

            return "\n".join(result)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting pipeline details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_pipeline_commit(commit_id: str = "", include_library_definitions: str = "true") -> str:
    """Get detailed information about a specific pipeline commit."""
    logger.info(f"Getting pipeline commit details for {commit_id}")

    if not commit_id.strip():
        return "❌ Error: commit_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipelineCommit/{commit_id}"
        params = {
            "includeLibraryDefinitions": include_library_definitions.lower() == "true"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            pipeline = response.json()

            result = ["✅ Pipeline Commit Details:\n"]
            result.append(format_pipeline_info(pipeline))
            result.append(f"\n  Commit ID: {commit_id}")

            if pipeline.get('commitTags'):
                tags = [tag.get('tag', '') for tag in pipeline['commitTags']]
                result.append(f"  Tags: {', '.join(tags)}")

            return "\n".join(result)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting pipeline commit: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_pipeline_history(pipeline_id: str = "", limit: str = "10", only_published: str = "true") -> str:
    """Get version history for a specific pipeline."""
    logger.info(f"Getting pipeline history for {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipeline/{pipeline_id}/log"
        params = {
            "offset": 0,
            "len": int(limit) if limit.strip() else 10,
            "order": "DESC",
            "onlyPublished": only_published.lower() == "true"
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            versions = data.get('data', []) if isinstance(data, dict) else data

            if not versions:
                return "✅ No pipeline history found"

            results = [f"✅ Pipeline History ({len(versions)} versions):\n"]

            for i, version in enumerate(versions):
                results.append(f"Version #{i+1}")
                results.append(f"  Commit ID: {version.get('commitId', 'N/A')}")
                results.append(f"  Version: {version.get('version', 'N/A')}")
                results.append(f"  Committer: {version.get('committer', 'N/A')}")

                if version.get('commitTime'):
                    commit_time = datetime.fromtimestamp(version['commitTime']/1000, tz=timezone.utc)
                    results.append(f"  Commit Time: {commit_time.isoformat()}")

                if version.get('commitMessage'):
                    results.append(f"  Message: {version['commitMessage']}")

                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting pipeline history: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def search_pipelines(organization: str = "", search_query: str = "", offset: str = "0", limit: str = "20") -> str:
    """Search pipelines using advanced query language (RSQL support)."""
    logger.info(f"Searching pipelines with query: {search_query}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/saql/pipelines/search"
        params = {}

        if organization.strip():
            params["organization"] = organization
        if search_query.strip():
            params["search"] = search_query

        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            pipelines = data.get('data', []) if isinstance(data, dict) else data

            if not pipelines:
                return "✅ No pipelines found matching search criteria"

            results = [f"✅ Search Results ({len(pipelines)} pipelines):\n"]
            for pipeline in pipelines:
                results.append(format_pipeline_info(pipeline))
                results.append("")  # Empty line between pipelines

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error searching pipelines: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def export_pipelines(commit_ids: str = "", include_fragments: str = "false", include_credentials: str = "false") -> str:
    """Export pipelines by commit IDs (comma-separated) for backup/migration."""
    logger.info(f"Exporting pipelines: {commit_ids}")

    if not commit_ids.strip():
        return "❌ Error: commit_ids is required (comma-separated list)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        commit_id_list = [cid.strip() for cid in commit_ids.split(',') if cid.strip()]

        if not commit_id_list:
            return "❌ Error: No valid commit IDs provided"

        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipelines/exportPipelineCommits"
        params = {
            "fragments": include_fragments.lower() == "true",
            "includePlainTextCredentials": include_credentials.lower() == "true"
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                params=params,
                json=commit_id_list,
                timeout=30
            )
            response.raise_for_status()

            export_data = response.json()

            results = [f"✅ Pipeline Export Successful ({len(commit_id_list)} commits):\n"]

            if export_data.get('pipelineCommits'):
                results.append(f"  Exported Pipelines: {len(export_data['pipelineCommits'])}")

            if export_data.get('fragmentCommits') and include_fragments.lower() == "true":
                results.append(f"  Exported Fragments: {len(export_data['fragmentCommits'])}")

            # Note: In a real implementation, you might want to save this to a file
            results.append(f"\n⚠️ Export data size: ~{len(str(export_data))} characters")
            results.append("ℹ️ Use this data with import_pipelines for deployment")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error exporting pipelines: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def import_pipelines(pipeline_data: str = "", commit_message: str = "Imported via MCP", import_as_new: str = "false") -> str:
    """Import pipelines from exported JSON data for deployment."""
    logger.info(f"Importing pipelines with message: {commit_message}")

    if not pipeline_data.strip():
        return "❌ Error: pipeline_data is required (JSON export data)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Parse the pipeline data
        try:
            import_data = json.loads(pipeline_data)
        except json.JSONDecodeError:
            return "❌ Error: pipeline_data must be valid JSON"

        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/pipelines/importPipelineCommit"
        params = {
            "commitMessage": commit_message,
            "importAsNew": import_as_new.lower() == "true",
            "removeMissingConnection": False
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                params=params,
                json=import_data,
                timeout=60  # Longer timeout for import operations
            )
            response.raise_for_status()

            result_data = response.json()

            results = ["✅ Pipeline Import Successful:\n"]

            if isinstance(result_data, list):
                results.append(f"  Imported Pipelines: {len(result_data)}")
                for i, pipeline in enumerate(result_data[:5]):  # Show first 5
                    results.append(f"    {i+1}. {pipeline.get('pipelineName', 'Unknown')} (v{pipeline.get('version', 'N/A')})")
                if len(result_data) > 5:
                    results.append(f"    ... and {len(result_data) - 5} more")
            else:
                results.append(f"  Imported: {result_data.get('pipelineName', 'Pipeline')} (v{result_data.get('version', 'N/A')})")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error importing pipelines: {e}")
        return f"❌ Error: {str(e)}"

# === JOB LABEL MANAGEMENT TOOLS ===

@mcp.tool()
async def get_job_labels(job_id: str = "") -> str:
    """Get labels for a specific job."""
    logger.info(f"Getting labels for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/labels"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            labels = response.json()

            if not labels:
                return f"✅ Job {job_id} has no labels"

            results = [f"✅ Job Labels for {job_id}:\n"]
            for i, label in enumerate(labels, 1):
                results.append(f"  {i}. {label}")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job labels: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def list_all_job_labels() -> str:
    """List all job labels used across the system."""
    logger.info("Listing all job labels in the system")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/jobs/labels"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            labels = response.json()

            if not labels:
                return "✅ No job labels found in the system"

            results = [f"✅ All Job Labels ({len(labels)} total):\n"]
            for i, label in enumerate(sorted(labels), 1):
                results.append(f"  {i}. {label}")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing all job labels: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def update_job_labels(job_id: str = "", labels: str = "") -> str:
    """Update labels for a specific job (comma-separated labels)."""
    logger.info(f"Updating labels for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Parse labels
        label_list = [label.strip() for label in labels.split(',') if label.strip()] if labels.strip() else []

        # First get the current job to preserve other properties
        get_url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}"

        async with httpx.AsyncClient() as client:
            # Get current job
            get_response = await client.get(
                get_url,
                headers=get_auth_headers(),
                timeout=30
            )
            get_response.raise_for_status()

            job_data = get_response.json()

            # Update labels
            job_data['labels'] = label_list

            # Update job
            update_url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}"
            update_response = await client.post(
                update_url,
                headers=get_auth_headers(),
                json=job_data,
                timeout=30
            )
            update_response.raise_for_status()

            updated_job = update_response.json()

            result = [f"✅ Job labels updated successfully for {job_id}:\n"]
            if updated_job.get('labels'):
                result.append("  Updated Labels:")
                for i, label in enumerate(updated_job['labels'], 1):
                    result.append(f"    {i}. {label}")
            else:
                result.append("  No labels set (all labels removed)")

            return "\n".join(result)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error updating job labels: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def search_jobs_by_labels(label_filter: str = "", organization: str = "", offset: str = "0", limit: str = "20") -> str:
    """Search jobs by labels using RSQL query or simple label filter."""
    logger.info(f"Searching jobs by labels: {label_filter}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Use advanced search if label_filter looks like RSQL, otherwise use simple job listing
        if any(op in label_filter for op in ['==', '!=', '=in=', '=out=']) if label_filter else False:
            # Use RSQL search
            url = f"{HOST_PREFIX}/jobrunner/rest/v1/saql/jobs/search"
            params = {
                "search": label_filter,
                "offset": int(offset) if offset.strip() else 0,
                "len": int(limit) if limit.strip() else 20
            }
            if organization.strip():
                params["organization"] = organization
        else:
            # Use simple job listing with label filter
            url = f"{HOST_PREFIX}/jobrunner/rest/v1/jobs"
            params = {
                "offset": int(offset) if offset.strip() else 0,
                "len": int(limit) if limit.strip() else 20,
                "orderBy": "NAME",
                "order": "ASC"
            }
            if organization.strip():
                params["organization"] = organization
            if label_filter.strip():
                params["jobLabel"] = label_filter

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            jobs = data.get('data', []) if isinstance(data, dict) else data

            if not jobs:
                return f"✅ No jobs found matching label criteria: {label_filter}"

            results = [f"✅ Jobs matching label criteria ({len(jobs)} found):\n"]
            for job in jobs:
                results.append(f"📊 Job: {job.get('name', 'N/A')}")
                results.append(f"  ID: {job.get('id', 'N/A')}")
                if job.get('labels'):
                    results.append(f"  Labels: {', '.join(job['labels'])}")
                if job.get('currentJobStatus'):
                    status = job['currentJobStatus']
                    results.append(f"  Status: {status.get('status', 'UNKNOWN')}")
                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error searching jobs by labels: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_tags_bulk(job_ids: str = "") -> str:
    """Get tags/labels for multiple jobs (comma-separated job IDs)."""
    logger.info(f"Getting bulk job tags for: {job_ids}")

    if not job_ids.strip():
        return "❌ Error: job_ids is required (comma-separated list)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        job_id_list = [jid.strip() for jid in job_ids.split(',') if jid.strip()]

        if not job_id_list:
            return "❌ Error: No valid job IDs provided"

        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/jobTagsBulk"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                json=job_id_list,
                timeout=30
            )
            response.raise_for_status()

            tags_data = response.json()

            if not tags_data:
                return f"✅ No tags found for jobs: {job_ids}"

            results = [f"✅ Bulk Job Tags ({len(tags_data)} jobs):\n"]

            for job_id, tag_info in tags_data.items():
                results.append(f"📊 Job ID: {job_id}")
                if isinstance(tag_info, dict) and tag_info.get('tags'):
                    tags = tag_info['tags']
                    if isinstance(tags, list):
                        results.append(f"  Tags: {', '.join(tags)}")
                    else:
                        results.append(f"  Tags: {tags}")
                else:
                    results.append("  No tags found")
                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting bulk job tags: {e}")
        return f"❌ Error: {str(e)}"

# === ENTERPRISE INTEGRATION TOOLS (Phase 3) ===

def format_deployment_info(deployment):
    """Format deployment information for display."""
    result = []
    result.append(f"🚀 Deployment: {deployment.get('name', 'N/A')}")
    result.append(f"  ID: {deployment.get('id', 'N/A')}")
    result.append(f"  Type: {deployment.get('type', 'N/A')}")
    result.append(f"  Status: {deployment.get('status', 'UNKNOWN')}")

    if deployment.get('created'):
        created_time = datetime.fromtimestamp(deployment['created']/1000, tz=timezone.utc)
        result.append(f"  Created: {created_time.isoformat()}")

    if deployment.get('engineConfiguration'):
        config = deployment['engineConfiguration']
        result.append(f"  Engine Version: {config.get('engineVersion', 'N/A')}")
        result.append(f"  Instance Count: {config.get('instanceCount', 'N/A')}")

    return "\n".join(result)

@mcp.tool()
async def list_deployments(organization: str = "", filter_text: str = "", offset: str = "0", limit: str = "20") -> str:
    """List infrastructure deployments with filtering options."""
    logger.info(f"Listing deployments for org={organization}, filter={filter_text}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/provisioning/rest/v1/csp/deployments"
        params = {}

        if organization.strip():
            params["organization"] = organization
        if filter_text.strip():
            params["filterText"] = filter_text

        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20
        params["orderBy"] = "NAME"
        params["order"] = "ASC"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            deployments = data.get('data', []) if isinstance(data, dict) else data

            if not deployments:
                return "✅ No deployments found"

            results = [f"✅ Found {len(deployments)} deployments:\n"]
            for deployment in deployments:
                results.append(format_deployment_info(deployment))
                results.append("")  # Empty line between deployments

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing deployments: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_deployment_details(deployment_id: str = "") -> str:
    """Get detailed information about a specific deployment."""
    logger.info(f"Getting deployment details for {deployment_id}")

    if not deployment_id.strip():
        return "❌ Error: deployment_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/provisioning/rest/v1/csp/deployment/{deployment_id}"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            deployment = response.json()

            result = ["✅ Deployment Details:\n"]
            result.append(format_deployment_info(deployment))

            # Add additional details
            if deployment.get('state'):
                result.append(f"\n  State: {deployment['state']}")

            if deployment.get('cloudProvider'):
                result.append(f"  Cloud Provider: {deployment['cloudProvider']}")

            if deployment.get('region'):
                result.append(f"  Region: {deployment['region']}")

            if deployment.get('tags'):
                result.append(f"  Tags: {deployment['tags']}")

            return "\n".join(result)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting deployment details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def start_deployment(deployment_id: str = "") -> str:
    """Start an infrastructure deployment."""
    logger.info(f"Starting deployment {deployment_id}")

    if not deployment_id.strip():
        return "❌ Error: deployment_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/provisioning/rest/v1/csp/deployment/{deployment_id}/start"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                timeout=60  # Longer timeout for deployment operations
            )
            response.raise_for_status()

            return f"✅ Deployment {deployment_id} start initiated successfully"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error starting deployment: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def stop_deployment(deployment_id: str = "", force: str = "false") -> str:
    """Stop an infrastructure deployment."""
    logger.info(f"Stopping deployment {deployment_id}, force={force}")

    if not deployment_id.strip():
        return "❌ Error: deployment_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        if force.lower() == "true":
            url = f"{HOST_PREFIX}/provisioning/rest/v1/csp/deployment/{deployment_id}/forceStop"
            params = {"confirm": True}
        else:
            url = f"{HOST_PREFIX}/provisioning/rest/v1/csp/deployment/{deployment_id}/stop"
            params = {}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=60  # Longer timeout for deployment operations
            )
            response.raise_for_status()

            action = "force stopped" if force.lower() == "true" else "stopped"
            return f"✅ Deployment {deployment_id} {action} successfully"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error stopping deployment: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def list_alerts(alert_status: str = "", filter_text: str = "", offset: str = "0", limit: str = "20") -> str:
    """List alerts with optional filtering by status."""
    logger.info(f"Listing alerts with status={alert_status}, filter={filter_text}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/notification/rest/v1/alerts"
        params = {}

        if alert_status.strip():
            params["alertStatus"] = alert_status
        if filter_text.strip():
            params["filterText"] = filter_text

        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20
        params["orderBy"] = "CREATED_TIME"
        params["order"] = "DESC"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            alerts = data.get('data', []) if isinstance(data, dict) else data

            if not alerts:
                return "✅ No alerts found"

            results = [f"✅ Found {len(alerts)} alerts:\n"]
            for alert in alerts:
                results.append(f"⚠️ Alert: {alert.get('title', 'N/A')}")
                results.append(f"  ID: {alert.get('id', 'N/A')}")
                results.append(f"  Status: {alert.get('status', 'UNKNOWN')}")
                results.append(f"  Severity: {alert.get('severity', 'UNKNOWN')}")

                if alert.get('createdTime'):
                    created = datetime.fromtimestamp(alert['createdTime']/1000, tz=timezone.utc)
                    results.append(f"  Created: {created.isoformat()}")

                if alert.get('description'):
                    results.append(f"  Description: {alert['description'][:100]}...")

                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error listing alerts: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_acl(job_id: str = "") -> str:
    """Get Access Control List (ACL) for a specific job."""
    logger.info(f"Getting ACL for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/acl"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            acl = response.json()

            results = [f"✅ Job ACL for {job_id}:\n"]

            if acl.get('resourceOwner'):
                results.append(f"  Owner: {acl['resourceOwner']}")

            if acl.get('permissions'):
                results.append("\n  Permissions:")
                for perm in acl['permissions']:
                    subject = perm.get('subjectId', 'Unknown')
                    actions = perm.get('actions', [])
                    results.append(f"    {subject}: {', '.join(actions)}")
            else:
                results.append("  No specific permissions set")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job ACL: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_topology_details(commit_id: str = "") -> str:
    """Get data lineage topology details for a pipeline commit."""
    logger.info(f"Getting topology details for commit {commit_id}")

    if not commit_id.strip():
        return "❌ Error: commit_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/topology/rest/v1/topology/{commit_id}"
        params = {"validate": True}

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            topology = response.json()

            results = [f"✅ Topology Details for {commit_id}:\n"]

            if topology.get('topologyDefinition'):
                topo_def = topology['topologyDefinition']
                results.append(f"  Name: {topo_def.get('name', 'N/A')}")
                results.append(f"  Description: {topo_def.get('description', 'N/A')}")

                if topo_def.get('jobs'):
                    results.append(f"\n  Associated Jobs ({len(topo_def['jobs'])}):")
                    for job in topo_def['jobs'][:5]:  # Show first 5
                        results.append(f"    • {job.get('instanceName', 'N/A')}")
                    if len(topo_def['jobs']) > 5:
                        results.append(f"    ... and {len(topo_def['jobs']) - 5} more")

                if topo_def.get('dataSources'):
                    results.append(f"\n  Data Sources ({len(topo_def['dataSources'])}):")
                    for source in topo_def['dataSources'][:3]:  # Show first 3
                        results.append(f"    • {source.get('type', 'Unknown')}: {source.get('name', 'N/A')}")
                    if len(topo_def['dataSources']) > 3:
                        results.append(f"    ... and {len(topo_def['dataSources']) - 3} more")

            if topology.get('commitTime'):
                commit_time = datetime.fromtimestamp(topology['commitTime']/1000, tz=timezone.utc)
                results.append(f"\n  Commit Time: {commit_time.isoformat()}")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting topology details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def update_job_acl(job_id: str = "", permissions_json: str = "") -> str:
    """Update Access Control List (ACL) for a specific job."""
    logger.info(f"Updating ACL for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not permissions_json.strip():
        return "❌ Error: permissions_json is required (ACL JSON data)"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Parse the permissions JSON
        try:
            acl_data = json.loads(permissions_json)
        except json.JSONDecodeError:
            return "❌ Error: permissions_json must be valid JSON"

        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/acl"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                json=acl_data,
                timeout=30
            )
            response.raise_for_status()

            updated_acl = response.json()

            results = [f"✅ Job ACL updated successfully for {job_id}:\n"]

            if updated_acl.get('resourceOwner'):
                results.append(f"  Owner: {updated_acl['resourceOwner']}")

            if updated_acl.get('permissions'):
                results.append("\n  Updated Permissions:")
                for perm in updated_acl['permissions']:
                    subject = perm.get('subjectId', 'Unknown')
                    actions = perm.get('actions', [])
                    results.append(f"    {subject}: {', '.join(actions)}")
            else:
                results.append("  No permissions set")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error updating job ACL: {e}")
        return f"❌ Error: {str(e)}"

# === METRICS AND ANALYTICS TOOLS ===

def format_job_metrics(metrics, job_id):
    """Format job metrics information for display."""
    results = [f"📊 Job Metrics for {job_id}:\n"]

    if metrics.get('gauges'):
        gauges = metrics['gauges']
        results.append("  Performance Metrics:")

        # Throughput metrics
        if 'BatchInputRecords.gauge' in gauges:
            results.append(f"    Input Records: {gauges['BatchInputRecords.gauge']['value']:,}")
        if 'BatchOutputRecords.gauge' in gauges:
            results.append(f"    Output Records: {gauges['BatchOutputRecords.gauge']['value']:,}")
        if 'BatchErrorRecords.gauge' in gauges:
            results.append(f"    Error Records: {gauges['BatchErrorRecords.gauge']['value']:,}")

        # Runtime metrics
        if 'RuntimeStatsGauge.gauge' in gauges:
            runtime_stats = gauges['RuntimeStatsGauge.gauge']['value']
            if isinstance(runtime_stats, dict):
                if 'timeInCurrentStage' in runtime_stats:
                    time_ms = runtime_stats['timeInCurrentStage']
                    time_hours = time_ms / (1000 * 60 * 60)
                    results.append(f"    Runtime: {time_hours:.2f} hours")

    if metrics.get('counters'):
        counters = metrics['counters']
        results.append("\n  Counters:")
        for counter_name, counter_data in list(counters.items())[:5]:  # Show first 5
            if isinstance(counter_data, dict) and 'count' in counter_data:
                results.append(f"    {counter_name}: {counter_data['count']:,}")

    if metrics.get('meters'):
        meters = metrics['meters']
        results.append("\n  Rates:")
        for meter_name, meter_data in list(meters.items())[:3]:  # Show first 3
            if isinstance(meter_data, dict) and 'mean_rate' in meter_data:
                results.append(f"    {meter_name}: {meter_data['mean_rate']:.2f}/sec")

    return "\n".join(results)

@mcp.tool()
async def get_job_metrics(job_id: str = "", run_count: str = "", sdc_id: str = "") -> str:
    """Get detailed performance metrics for a specific job."""
    logger.info(f"Getting metrics for job {job_id}")

    if not job_id.strip():
        return "❌ Error: job_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/metrics/job/{job_id}"
        params = {}

        if run_count.strip():
            try:
                params["runCount"] = int(run_count)
            except ValueError:
                return "❌ Error: run_count must be a valid integer"

        if sdc_id.strip():
            params["sdcId"] = sdc_id

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            metrics = response.json()

            if not metrics:
                return f"✅ No metrics available for job {job_id}"

            return format_job_metrics(metrics, job_id)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        error_text = e.response.text if hasattr(e.response, 'text') else str(e.response.content)
        return f"❌ API Error: {e.response.status_code} - {error_text[:500]}"
    except Exception as e:
        logger.error(f"Error getting job metrics: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_problematic_jobs(organization: str = "", job_status: str = "", executor_type: str = "", offset: str = "0", limit: str = "20") -> str:
    """Get jobs with error/failed status for operational monitoring."""
    logger.info(f"Getting problematic jobs for org={organization}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/metrics/problematicJobs"
        params = {}

        if organization.strip():
            params["organization"] = organization
        if job_status.strip():
            params["jobStatus"] = job_status
        if executor_type.strip():
            params["executorType"] = executor_type

        params["offset"] = int(offset) if offset.strip() else 0
        params["len"] = int(limit) if limit.strip() else 20

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            jobs = response.json()

            if not jobs:
                return "✅ No problematic jobs found"

            results = [f"⚠️ Problematic Jobs ({len(jobs)} found):\n"]

            for job in jobs:
                results.append(f"❌ Job: {job.get('name', 'N/A')}")
                results.append(f"  ID: {job.get('id', 'N/A')}")
                results.append(f"  Status: {job.get('status', 'UNKNOWN')}")
                if job.get('errorMessage'):
                    results.append(f"  Error: {job['errorMessage'][:100]}...")
                if job.get('lastErrorTime'):
                    error_time = datetime.fromtimestamp(job['lastErrorTime']/1000, tz=timezone.utc)
                    results.append(f"  Last Error: {error_time.isoformat()}")
                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting problematic jobs: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_job_count_by_status() -> str:
    """Get distribution of jobs by their current status for system health overview."""
    logger.info("Getting job count by status")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/metrics/jobCountByStatus"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                timeout=30
            )
            response.raise_for_status()

            status_counts = response.json()

            if not status_counts:
                return "✅ No job status data available"

            results = ["✅ Job Status Distribution:\n"]
            total_jobs = 0

            # Sort by count descending for better readability
            sorted_statuses = sorted(status_counts.items(), key=lambda x: x[1], reverse=True)

            for status, count in sorted_statuses:
                total_jobs += count
                # Add status emojis for better visualization
                emoji = {
                    'ACTIVE': '🟢',
                    'INACTIVE': '🔴',
                    'STARTING': '🟡',
                    'STOPPING': '🟠',
                    'FINISHED': '✅',
                    'KILLED': '❌',
                    'FAILED': '🔴',
                    'ERROR': '⚠️'
                }.get(status, '🟦')

                results.append(f"  {emoji} {status}: {count:,} jobs")

            results.append(f"\n📊 Total Jobs: {total_jobs:,}")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting job count by status: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_executor_metrics(executor_type: str = "", organization: str = "", label: str = "", limit: str = "50") -> str:
    """Get infrastructure executor performance metrics (CPU, memory, uptime) with filtering by executor type and labels."""
    logger.info(f"Getting executor metrics for type={executor_type}, label={label}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Use the correct metrics/executors endpoint
        url = f"{HOST_PREFIX}/jobrunner/rest/v1/metrics/executors"
        params = {}

        if executor_type.strip():
            # Validate executor type
            valid_types = ["COLLECTOR", "TRANSFORMER", "EDGE", "SNOWPARK"]
            if executor_type.upper() not in valid_types:
                return f"❌ Error: executor_type must be one of: {', '.join(valid_types)}"
            params["executorType"] = executor_type.upper()

        if organization.strip():
            params["organization"] = organization

        if label.strip():
            params["label"] = label

        if limit.strip():
            params["len"] = int(limit)

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            # Handle pagination wrapper response
            if isinstance(data, dict) and "data" in data:
                executors = data["data"]
                total_count = data.get("totalCount", len(executors))
            else:
                executors = data if isinstance(data, list) else []
                total_count = len(executors)

            if not executors:
                filters = []
                if executor_type.strip():
                    filters.append(f"type: {executor_type.upper()}")
                if label.strip():
                    filters.append(f"label: {label}")
                filter_text = f" ({', '.join(filters)})" if filters else ""
                return f"📊 Executor Metrics{filter_text}:\n\n✅ No executors found"

            # Build results
            results = []
            filters = []
            if executor_type.strip():
                filters.append(f"type: {executor_type.upper()}")
            if label.strip():
                filters.append(f"label: {label}")
            filter_text = f" ({', '.join(filters)})" if filters else ""

            results.append(f"📊 Executor Infrastructure Metrics{filter_text}:\n")
            results.append(f"📈 Total Executors: {total_count}\n")

            for i, executor in enumerate(executors, 1):
                executor_id = executor.get('id', 'N/A')
                executor_type_actual = executor.get('executorType', 'N/A')
                version = executor.get('version', 'N/A')
                responding = executor.get('responding', False)

                # Status icon based on responding status
                status_icon = "🟢" if responding else "🔴"
                status_text = "Online" if responding else "Offline"

                results.append(f"{i}. {status_icon} Executor: {executor_id[:8]}...")
                results.append(f"   🔧 Type: {executor_type_actual}")
                results.append(f"   📊 Version: {version}")
                results.append(f"   🌐 Status: {status_text}")

                # HTTP URL
                if executor.get('httpUrl'):
                    results.append(f"   🔗 URL: {executor['httpUrl']}")

                # Memory metrics
                if executor.get('totalMemory') and executor.get('usedMemory'):
                    total_memory_gb = executor['totalMemory'] / (1024**3)
                    used_memory_gb = executor['usedMemory'] / (1024**3)
                    memory_percent = executor.get('memoryUsedPercentage', 0) * 100
                    results.append(f"   💾 Memory: {used_memory_gb:.1f}GB / {total_memory_gb:.1f}GB ({memory_percent:.1f}%)")

                # CPU metrics
                if executor.get('cpuLoad') is not None:
                    cpu_percent = executor['cpuLoad'] * 100
                    results.append(f"   ⚡ CPU Load: {cpu_percent:.1f}%")

                # Pipeline count
                if executor.get('pipelinesCount') is not None:
                    pipeline_count = executor['pipelinesCount']
                    results.append(f"   📋 Pipelines: {pipeline_count}")

                # Labels/Tags
                reported_labels = executor.get('reportedLabels', [])
                if reported_labels:
                    labels_str = ', '.join(reported_labels)
                    results.append(f"   🏷️  Labels: {labels_str}")

                # Uptime
                if executor.get('startUpTime'):
                    startup_time = datetime.fromtimestamp(executor['startUpTime']/1000, tz=timezone.utc)
                    uptime_seconds = (datetime.now(tz=timezone.utc) - startup_time).total_seconds()
                    uptime_hours = uptime_seconds / 3600
                    if uptime_hours < 24:
                        results.append(f"   ⏰ Uptime: {uptime_hours:.1f} hours")
                    else:
                        uptime_days = uptime_hours / 24
                        results.append(f"   ⏰ Uptime: {uptime_days:.1f} days")

                # Last report time
                if executor.get('lastReportedTime'):
                    last_report = datetime.fromtimestamp(executor['lastReportedTime']/1000, tz=timezone.utc)
                    results.append(f"   📡 Last Report: {last_report.isoformat()}")

                # Resource limits
                max_cpu = executor.get('maxCpuLoad', 0)
                max_memory = executor.get('maxMemoryUsed', 0)
                max_pipelines = executor.get('maxPipelinesRunning', 0)
                if max_cpu or max_memory or max_pipelines:
                    results.append(f"   ⚠️  Limits: CPU {max_cpu}%, Memory {max_memory}%, Pipelines {max_pipelines}")

                results.append("")  # Empty line between executors

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting executor metrics: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_pipeline_metrics(organization: str = "", start_time: str = "", end_time: str = "", group: str = "", limit: str = "20") -> str:
    """Get pipeline creation and usage analytics with time-based filtering."""
    logger.info(f"Getting pipeline metrics for org={organization}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        url = f"{HOST_PREFIX}/pipelinestore/rest/v1/metrics/pipelines"
        params = {}

        if organization.strip():
            params["organization"] = organization
        if group.strip():
            params["group"] = group

        # Handle time range parameters (expect Unix timestamps)
        if start_time.strip():
            try:
                params["startTime"] = int(start_time)
            except ValueError:
                return "❌ Error: start_time must be a Unix timestamp (milliseconds)"
        if end_time.strip():
            try:
                params["endTime"] = int(end_time)
            except ValueError:
                return "❌ Error: end_time must be a Unix timestamp (milliseconds)"

        params["len"] = int(limit) if limit.strip() else 20
        params["offset"] = 0
        params["orderBy"] = "COMMIT_TIME"
        params["order"] = "DESC"
        params["onlyPublished"] = True

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            pipelines = data.get('data', []) if isinstance(data, dict) else data

            if not pipelines:
                return "✅ No pipeline metrics data available"

            results = [f"✅ Pipeline Usage Analytics ({len(pipelines)} pipelines):\n"]

            for pipeline in pipelines:
                results.append(f"📋 Pipeline: {pipeline.get('pipelineName', 'N/A')}")
                results.append(f"  Creator: {pipeline.get('creator', 'N/A')}")
                results.append(f"  Version: {pipeline.get('version', 'N/A')}")

                if pipeline.get('commitTime'):
                    commit_time = datetime.fromtimestamp(pipeline['commitTime']/1000, tz=timezone.utc)
                    results.append(f"  Created: {commit_time.isoformat()}")

                if pipeline.get('lastModifiedOn'):
                    modified_time = datetime.fromtimestamp(pipeline['lastModifiedOn']/1000, tz=timezone.utc)
                    results.append(f"  Modified: {modified_time.isoformat()}")

                if pipeline.get('executionMode'):
                    results.append(f"  Execution Mode: {pipeline['executionMode']}")

                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting pipeline metrics: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_user_login_metrics(org_id: str = "", start_time: str = "", end_time: str = "", active_users_only: str = "true") -> str:
    """Get user login analytics and engagement metrics."""
    logger.info(f"Getting user login metrics for org {org_id}")

    if not org_id.strip():
        return "❌ Error: org_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Choose endpoint based on active_users_only parameter
        if active_users_only.lower() == "true":
            endpoint = "loggedInUsers"
            title = "Active Users (With Login Activity)"
        else:
            endpoint = "notLoggedInUsers"
            title = "Inactive Users (No Login Activity)"

        url = f"{HOST_PREFIX}/security/rest/v1/metrics/{org_id}/{endpoint}"
        params = {}

        # Handle time range parameters
        if start_time.strip():
            try:
                params["startTime"] = int(start_time)
            except ValueError:
                return "❌ Error: start_time must be a Unix timestamp (milliseconds)"
        if end_time.strip():
            try:
                params["endTime"] = int(end_time)
            except ValueError:
                return "❌ Error: end_time must be a Unix timestamp (milliseconds)"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            users = response.json()

            if not users:
                return f"✅ No {title.lower()} found"

            results = [f"✅ {title} ({len(users)} users):\n"]

            for user in users:
                emoji = "🟢" if active_users_only.lower() == "true" else "🔴"
                results.append(f"{emoji} User: {user.get('userName', 'N/A')}")
                results.append(f"  Email: {user.get('email', 'N/A')}")
                results.append(f"  Display Name: {user.get('displayName', 'N/A')}")

                if user.get('lastLoginTime'):
                    login_time = datetime.fromtimestamp(user['lastLoginTime']/1000, tz=timezone.utc)
                    results.append(f"  Last Login: {login_time.isoformat()}")

                if user.get('groups'):
                    results.append(f"  Groups: {', '.join(user['groups'])}")

                results.append("")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting user login metrics: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def get_security_audit_metrics(org_id: str = "", start_time: str = "", end_time: str = "", audit_type: str = "login", limit: str = "50") -> str:
    """Get security audit trail for compliance and monitoring."""
    logger.info(f"Getting security audit metrics for org {org_id}")

    if not org_id.strip():
        return "❌ Error: org_id is required"

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # Choose endpoint based on audit type - using correct Security Metrics API endpoints
        if audit_type.lower() == "login":
            endpoint = "loginAudits"
            title = "Login Security Audit"
        else:
            endpoint = "actionAudits"
            title = "User Action Audit"

        url = f"{HOST_PREFIX}/security/rest/v1/metrics/{org_id}/{endpoint}"
        params = {
            "offset": 0,
            "len": int(limit) if limit.strip() else 50,
            "sortField": "loginTimestamp" if audit_type.lower() == "login" else "time",
            "sortOrder": "DESC"
        }

        # Handle time range parameters (Unix timestamps in milliseconds)
        if start_time.strip():
            try:
                params["startTime"] = int(start_time)
            except ValueError:
                return "❌ Error: start_time must be a Unix timestamp (milliseconds)"

        if end_time.strip():
            try:
                params["endTime"] = int(end_time)
            except ValueError:
                return "❌ Error: end_time must be a Unix timestamp (milliseconds)"

        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=get_auth_headers(),
                params=params,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            # Debug logging to see what we actually get
            logger.info(f"Security audit API response type: {type(data)}")
            if isinstance(data, dict):
                logger.info(f"Response keys: {list(data.keys())}")
            elif isinstance(data, list):
                logger.info(f"Response list length: {len(data)}")

            if not data:
                return f"📊 {title}:\n\n✅ No audit records found for the specified time range"

            results = [f"📊 {title}:\n"]

            # Handle pagination wrapper response format based on swagger
            if isinstance(data, dict) and "data" in data:
                audit_records = data["data"]
                total_count = data.get("totalCount", len(audit_records))
                results.append(f"📈 Total Records: {total_count}")
                if data.get("offset", 0) > 0:
                    results.append(f"📍 Showing records from offset: {data['offset']}")
            elif isinstance(data, list):
                audit_records = data
            else:
                audit_records = []
                # If we get an unexpected format, show what we got
                results.append(f"🔍 Debug: Unexpected response format: {type(data)}")
                if hasattr(data, '__dict__'):
                    results.append(f"🔍 Debug: Response content: {str(data)[:200]}...")

            if not audit_records:
                results.append("✅ No audit records found for the specified criteria")
                return "\n".join(results)

            results.append(f"📋 Showing {len(audit_records)} records\n")

            for i, record in enumerate(audit_records, 1):
                if audit_type.lower() == "login":
                    # Format login audit record based on LoginAuditJson schema
                    login_time = record.get("loginTimestamp", 0)
                    success = record.get("success", False)
                    user_id = record.get("userId", "unknown")
                    ip_address = record.get("ipAddress", "unknown")
                    user_agent = record.get("userAgent", "")

                    status_icon = "✅" if success else "❌"
                    formatted_time = format_timestamp(login_time)

                    results.append(f"{i}. {status_icon} Login Attempt")
                    results.append(f"   👤 User: {user_id}")
                    results.append(f"   🌐 IP: {ip_address}")
                    results.append(f"   ⏰ Time: {formatted_time}")

                    if user_agent:
                        results.append(f"   🖥️  User Agent: {user_agent[:50]}...")

                    if not success:
                        details = record.get("details", "Login failed")
                        results.append(f"   ⚠️ Details: {details}")

                    # Check for logout information
                    logout_time = record.get("logoutTimestamp")
                    if logout_time:
                        logout_formatted = format_timestamp(logout_time)
                        logout_reason = record.get("logoutReason", "Normal logout")
                        results.append(f"   🚪 Logout: {logout_formatted} ({logout_reason})")

                else:
                    # Format action audit record based on UserActionAuditJson schema
                    action = record.get("action", "unknown")
                    requester = record.get("requesterId", "unknown")
                    timestamp = record.get("time", 0)
                    affected_user = record.get("affectsUser")
                    field_type = record.get("fieldType")
                    ip_address = record.get("ipAddress")

                    formatted_time = format_timestamp(timestamp)

                    results.append(f"{i}. 🔧 Action: {action}")
                    results.append(f"   👤 Requester: {requester}")
                    if affected_user:
                        results.append(f"   🎯 Affected User: {affected_user}")
                    if field_type:
                        results.append(f"   📝 Field Type: {field_type}")
                    if ip_address:
                        results.append(f"   🌐 IP: {ip_address}")
                    results.append(f"   ⏰ Time: {formatted_time}")

                results.append("")  # Empty line between records

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error getting security audit metrics: {e}")
        return f"❌ Error: {str(e)}"

# === PIPELINE BUILDER TOOLS ===

@mcp.tool()
async def sdc_create_pipeline_builder(title: str = "New Pipeline", description: str = "", engine_type: str = "data_collector", engine_id: str = "") -> str:
    """Initialize a new pipeline builder session for creating StreamSets pipelines."""
    logger.info(f"Creating pipeline builder: {title}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    if not title.strip():
        return "❌ Error: title is required"

    try:
        # Validate engine_type
        valid_engine_types = ["data_collector", "transformer", "snowflake"]
        if engine_type not in valid_engine_types:
            return f"❌ Error: engine_type must be one of: {', '.join(valid_engine_types)}"

        # For data_collector, we need to verify the engine_id exists
        if engine_type == "data_collector" and engine_id.strip():
            # Verify the SDC exists by calling the topology API
            sdc_url = f"{HOST_PREFIX}/provisioning/rest/v1/dpm/topology/systemSdcs"
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    sdc_url,
                    headers=get_auth_headers(),
                    timeout=30
                )
                response.raise_for_status()
                sdcs = response.json()

                # Check if the engine_id exists
                sdc_ids = [sdc.get('id') for sdc in sdcs if isinstance(sdcs, list)]
                if isinstance(sdcs, dict) and 'data' in sdcs:
                    sdc_ids = [sdc.get('id') for sdc in sdcs['data']]

                if engine_id not in sdc_ids:
                    return f"❌ Error: engine_id '{engine_id}' not found. Available SDCs: {', '.join(sdc_ids[:5])}{'...' if len(sdc_ids) > 5 else ''}"

        # Create new pipeline builder session
        pipeline_id = get_next_pipeline_id()
        session = PipelineBuilderSession(
            title=title,
            description=description,
            engine_type=engine_type,
            engine_id=engine_id
        )
        session.pipeline_id = pipeline_id

        # Store in global storage and save to disk
        active_pipeline_builders[pipeline_id] = session
        save_pipeline_session(pipeline_id, session)

        results = []
        results.append(f"✅ Pipeline Builder Created: {title}")
        results.append(f"📋 Pipeline ID: {pipeline_id}")
        results.append(f"🔧 Engine Type: {engine_type}")
        if engine_id:
            results.append(f"🖥️  Engine ID: {engine_id}")
        if description:
            results.append(f"📝 Description: {description}")
        results.append(f"⏰ Created: {session.created_time.isoformat()}")
        results.append("")
        results.append("📋 Next Steps:")
        results.append("1. Use sdc_list_available_stages to see available stages")
        results.append("2. Use sdc_add_pipeline_stage to add stages")
        results.append("3. Use sdc_connect_pipeline_stages to connect stages")
        results.append("4. Use sdc_build_pipeline to build the final pipeline")
        results.append("5. Use sdc_publish_pipeline to publish to Control Hub")

        return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error verifying engine: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Error creating pipeline builder: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_list_pipeline_builders() -> str:
    """List all active pipeline builder sessions."""
    logger.info("Listing active pipeline builders")

    try:
        if not active_pipeline_builders:
            return "📋 No active pipeline builder sessions"

        results = []
        results.append(f"📋 Active Pipeline Builder Sessions ({len(active_pipeline_builders)}):\n")

        for i, (pipeline_id, session) in enumerate(active_pipeline_builders.items(), 1):
            summary = session.get_summary()

            results.append(f"{i}. 🏗️  {summary['title']}")
            results.append(f"   ID: {pipeline_id}")
            results.append(f"   Engine: {summary['engine_type']}")
            if summary['engine_id']:
                results.append(f"   Engine ID: {summary['engine_id']}")
            results.append(f"   Stages: {summary['stage_count']}")
            results.append(f"   Connections: {summary['connection_count']}")

            status_icons = []
            if summary['built']:
                status_icons.append("🔨 Built")
            if summary['published']:
                status_icons.append("📤 Published")
            if not status_icons:
                status_icons.append("🚧 In Progress")

            results.append(f"   Status: {', '.join(status_icons)}")
            results.append(f"   Created: {summary['created_time']}")

            if summary['description']:
                results.append(f"   Description: {summary['description']}")
            results.append("")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error listing pipeline builders: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_get_pipeline_builder_details(pipeline_id: str = "") -> str:
    """Get detailed information about a specific pipeline builder session."""
    logger.info(f"Getting pipeline builder details: {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        results = []
        results.append(f"🏗️  Pipeline Builder Details: {session.title}")
        results.append(f"📋 ID: {pipeline_id}")
        results.append(f"🔧 Engine Type: {session.engine_type}")
        if session.engine_id:
            results.append(f"🖥️  Engine ID: {session.engine_id}")
        if session.description:
            results.append(f"📝 Description: {session.description}")
        results.append(f"⏰ Created: {session.created_time.isoformat()}")

        # Status
        status_items = []
        if session.built_pipeline:
            status_items.append("🔨 Built")
        if session.published:
            status_items.append("📤 Published")
        if not status_items:
            status_items.append("🚧 In Progress")
        results.append(f"📊 Status: {', '.join(status_items)}")

        # Stages
        results.append(f"\n📋 Stages ({len(session.stages)}):")
        if session.stages:
            for stage_id, stage_info in session.stages.items():
                results.append(f"  • {stage_id}: {stage_info.get('label', 'N/A')} ({stage_info.get('type', 'N/A')})")
        else:
            results.append("  (No stages added yet)")

        # Connections
        results.append(f"\n🔗 Connections ({len(session.connections)}):")
        if session.connections:
            for conn in session.connections:
                conn_type = "📊" if conn['type'] == "data" else "⚡"
                results.append(f"  • {conn_type} {conn['source']} → {conn['target']}")
        else:
            results.append("  (No connections defined yet)")

        # Runtime Parameters
        if session.runtime_parameters:
            results.append(f"\n⚙️  Runtime Parameters ({len(session.runtime_parameters)}):")
            for key, value in session.runtime_parameters.items():
                results.append(f"  • {key}: {value}")

        # Labels
        if session.labels:
            results.append(f"\n🏷️  Labels: {', '.join(session.labels)}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error getting pipeline builder details: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_delete_pipeline_builder(pipeline_id: str = "") -> str:
    """Delete a pipeline builder session and free up resources."""
    logger.info(f"Deleting pipeline builder: {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    try:
        session = active_pipeline_builders.get(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        # Remove from storage and delete file
        del active_pipeline_builders[pipeline_id]
        delete_pipeline_session_file(pipeline_id)

        return f"✅ Pipeline builder '{session.title}' ({pipeline_id}) deleted successfully"

    except Exception as e:
        logger.error(f"Error deleting pipeline builder: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_get_pipeline_storage_info() -> str:
    """Get information about pipeline builder persistence and storage location."""
    logger.info("Getting pipeline storage information")

    try:
        results = []
        results.append("💾 Pipeline Builder Persistence Info:")
        results.append(f"📁 Storage Directory: {PIPELINE_STORAGE_DIR}")
        results.append(f"📄 Counter File: {PIPELINE_COUNTER_FILE}")
        results.append(f"🔢 Current Counter: {pipeline_counter}")

        # Check directory existence and files
        if PIPELINE_STORAGE_DIR.exists():
            session_files = list(PIPELINE_STORAGE_DIR.glob("pipeline_builder_*.pkl"))
            results.append(f"📊 Session Files: {len(session_files)}")
            results.append(f"💽 Memory Sessions: {len(active_pipeline_builders)}")

            if session_files:
                results.append(f"\n📋 Stored Session Files:")
                for i, file_path in enumerate(session_files, 1):
                    pipeline_id = file_path.stem
                    file_size = file_path.stat().st_size
                    file_modified = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    results.append(f"  {i}. {pipeline_id} ({file_size} bytes, modified: {file_modified})")
        else:
            results.append("❌ Storage directory does not exist")

        results.append(f"\n📝 Persistence Features:")
        results.append("  ✅ Sessions persist across conversations")
        results.append("  ✅ Automatic save on modifications")
        results.append("  ✅ Counter persistence")
        results.append("  ✅ Auto-load on startup")

        results.append(f"\n💡 Storage Location:")
        results.append(f"  Sessions are stored in your home directory")
        results.append(f"  Path: {PIPELINE_STORAGE_DIR}")
        results.append(f"  Files are automatically managed by the MCP server")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error getting pipeline storage info: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_list_available_stages(engine_type: str = "data_collector", category: str = "", search: str = "") -> str:
    """List available stages from the StreamSets stage library for pipeline building."""
    logger.info(f"Listing available stages for {engine_type}")

    if not HOST_PREFIX:
        return "❌ Error: STREAMSETS_HOST_PREFIX not configured"

    try:
        # For now, provide a curated list of common data_collector stages
        # In a full implementation, this would query the stage library API

        if engine_type != "data_collector":
            return f"❌ Error: Stage library for '{engine_type}' not yet implemented. Use 'data_collector'."

        # Common StreamSets Data Collector stages by category
        stage_library = {
            "origins": [
                {"label": "Dev Raw Data Source", "name": "com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource", "description": "Generates random test data"},
                {"label": "Directory", "name": "com_streamsets_pipeline_stage_origin_spooldir_SpoolDirSource", "description": "Reads files from a directory"},
                {"label": "File Tail", "name": "com_streamsets_pipeline_stage_origin_tailfile_FileTailSource", "description": "Reads data from files as they are written"},
                {"label": "JDBC Query Consumer", "name": "com_streamsets_pipeline_stage_origin_jdbc_JdbcSource", "description": "Reads data from JDBC databases"},
                {"label": "HTTP Client", "name": "com_streamsets_pipeline_stage_origin_http_HttpClientSource", "description": "Reads data from HTTP endpoints"},
                {"label": "Amazon S3", "name": "com_streamsets_pipeline_stage_origin_s3_AmazonS3Source", "description": "Reads objects from Amazon S3"},
                {"label": "Kafka Consumer", "name": "com_streamsets_pipeline_stage_origin_kafka_KafkaSource", "description": "Reads messages from Apache Kafka"},
            ],
            "processors": [
                {"label": "Expression Evaluator", "name": "com_streamsets_pipeline_stage_processor_expression_ExpressionProcessor", "description": "Evaluates expressions and creates new fields"},
                {"label": "Field Converter", "name": "com_streamsets_pipeline_stage_processor_fieldconverter_FieldConverterProcessor", "description": "Converts field types"},
                {"label": "Field Mapper", "name": "com_streamsets_pipeline_stage_processor_fieldmapper_FieldMapperProcessor", "description": "Maps fields to new names or locations"},
                {"label": "Stream Selector", "name": "com_streamsets_pipeline_stage_processor_selector_SelectorProcessor", "description": "Routes records to different streams based on conditions"},
                {"label": "Field Remover", "name": "com_streamsets_pipeline_stage_processor_fieldfilter_FieldFilterProcessor", "description": "Removes specified fields from records"},
                {"label": "Record Deduplicator", "name": "com_streamsets_pipeline_stage_processor_dedup_RecordDedupProcessor", "description": "Removes duplicate records"},
                {"label": "JavaScript Evaluator", "name": "com_streamsets_pipeline_stage_processor_javascript_JavaScriptProcessor", "description": "Processes records using JavaScript"},
            ],
            "destinations": [
                {"label": "Trash", "name": "com_streamsets_pipeline_stage_destination_devnull_NullTarget", "description": "Discards records (for testing)"},
                {"label": "Local FS", "name": "com_streamsets_pipeline_stage_destination_localfilesystem_LocalFileSystemDestination", "description": "Writes files to local filesystem"},
                {"label": "JDBC Producer", "name": "com_streamsets_pipeline_stage_destination_jdbc_JdbcTarget", "description": "Writes data to JDBC databases"},
                {"label": "Amazon S3", "name": "com_streamsets_pipeline_stage_destination_s3_AmazonS3Target", "description": "Writes objects to Amazon S3"},
                {"label": "Kafka Producer", "name": "com_streamsets_pipeline_stage_destination_kafka_KafkaTarget", "description": "Writes messages to Apache Kafka"},
                {"label": "HTTP Client", "name": "com_streamsets_pipeline_stage_destination_http_HttpClientTarget", "description": "Sends HTTP requests"},
            ],
            "executors": [
                {"label": "Pipeline Finisher Executor", "name": "com_streamsets_pipeline_stage_executor_finisher_PipelineFinisherExecutor", "description": "Stops the pipeline when triggered"},
                {"label": "Email Executor", "name": "com_streamsets_pipeline_stage_executor_email_EmailExecutor", "description": "Sends email notifications"},
                {"label": "Shell Executor", "name": "com_streamsets_pipeline_stage_executor_shell_ShellExecutor", "description": "Executes shell commands"},
            ]
        }

        # Filter by category if specified
        if category:
            if category.lower() not in stage_library:
                valid_categories = list(stage_library.keys())
                return f"❌ Error: category must be one of: {', '.join(valid_categories)}"
            filtered_library = {category.lower(): stage_library[category.lower()]}
        else:
            filtered_library = stage_library

        # Filter by search term if specified
        if search:
            search_lower = search.lower()
            for cat_name, stages in filtered_library.items():
                filtered_library[cat_name] = [
                    stage for stage in stages
                    if search_lower in stage['label'].lower() or search_lower in stage['description'].lower()
                ]

        # Build results
        results = []
        results.append(f"📚 Available Stages for {engine_type.title()}:\n")

        total_stages = sum(len(stages) for stages in filtered_library.values())
        if total_stages == 0:
            results.append(f"❌ No stages found matching criteria")
            return "\n".join(results)

        for category_name, stages in filtered_library.items():
            if not stages:
                continue

            category_icon = {
                "origins": "📥",
                "processors": "⚙️",
                "destinations": "📤",
                "executors": "⚡"
            }.get(category_name, "📋")

            results.append(f"{category_icon} {category_name.title()} ({len(stages)} stages):")

            for i, stage in enumerate(stages, 1):
                results.append(f"  {i}. {stage['label']}")
                results.append(f"     Type: {stage['name']}")
                results.append(f"     Description: {stage['description']}")
                results.append("")

        results.append("💡 Usage: Use sdc_add_pipeline_stage with the 'label' or 'name' from above")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error listing available stages: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_add_pipeline_stage(pipeline_id: str = "", stage_label: str = "", stage_name: str = "", stage_type: str = "", configuration: str = "{}") -> str:
    """Add a stage to an existing pipeline builder session."""
    logger.info(f"Adding stage to pipeline {pipeline_id}: {stage_label or stage_name}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not stage_label.strip() and not stage_name.strip():
        return "❌ Error: Either stage_label or stage_name is required"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        # Parse configuration if provided
        stage_config = {}
        if configuration.strip():
            try:
                stage_config = json.loads(configuration)
            except json.JSONDecodeError:
                return "❌ Error: configuration must be valid JSON"

        # Generate stage ID
        stage_count = len(session.stages) + 1
        stage_id = f"stage_{stage_count}"

        # Use stage_label if provided, otherwise use stage_name
        display_label = stage_label if stage_label.strip() else stage_name

        # Create stage info
        stage_info = {
            "stage_id": stage_id,
            "label": display_label,
            "name": stage_name if stage_name.strip() else f"auto_generated_{stage_count}",
            "type": stage_type,
            "configuration": stage_config,
            "added_time": datetime.now(tz=timezone.utc).isoformat()
        }

        # Determine stage category based on common patterns
        stage_category = "unknown"
        if stage_label:
            label_lower = stage_label.lower()
            if any(x in label_lower for x in ["source", "consumer", "dev raw", "directory", "tail", "s3"]):
                stage_category = "origin"
            elif any(x in label_lower for x in ["producer", "target", "destination", "trash"]):
                stage_category = "destination"
            elif any(x in label_lower for x in ["executor", "finisher", "email", "shell"]):
                stage_category = "executor"
            else:
                stage_category = "processor"

        stage_info["category"] = stage_category

        # Add to session and save to disk
        session.add_stage(stage_id, stage_info)
        save_pipeline_session(pipeline_id, session)

        results = []
        results.append(f"✅ Stage Added: {display_label}")
        results.append(f"🏗️  Pipeline: {session.title} ({pipeline_id})")
        results.append(f"📋 Stage ID: {stage_id}")
        results.append(f"🔧 Category: {stage_category}")
        if stage_name:
            results.append(f"📦 Name: {stage_name}")
        if stage_type:
            results.append(f"🏷️  Type: {stage_type}")
        if stage_config:
            results.append(f"⚙️  Configuration: {len(stage_config)} parameters")

        results.append(f"\n📊 Pipeline Status:")
        results.append(f"  Total Stages: {len(session.stages)}")
        results.append(f"  Connections: {len(session.connections)}")

        results.append(f"\n📋 Next Steps:")
        results.append("• Add more stages with sdc_add_pipeline_stage")
        results.append("• Connect stages with sdc_connect_pipeline_stages")
        results.append("• View details with sdc_get_pipeline_builder_details")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error adding stage to pipeline: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_remove_pipeline_stage(pipeline_id: str = "", stage_id: str = "") -> str:
    """Remove a stage from a pipeline builder session."""
    logger.info(f"Removing stage {stage_id} from pipeline {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not stage_id.strip():
        return "❌ Error: stage_id is required"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        if stage_id not in session.stages:
            return f"❌ Error: Stage '{stage_id}' not found in pipeline"

        # Get stage info before removal
        stage_info = session.stages[stage_id]
        stage_label = stage_info.get('label', stage_id)

        # Remove the stage
        del session.stages[stage_id]

        # Remove any connections involving this stage
        original_connections = len(session.connections)
        session.connections = [
            conn for conn in session.connections
            if conn['source'] != stage_id and conn['target'] != stage_id
        ]
        removed_connections = original_connections - len(session.connections)

        # Save changes to disk
        save_pipeline_session(pipeline_id, session)

        results = []
        results.append(f"✅ Stage Removed: {stage_label}")
        results.append(f"🏗️  Pipeline: {session.title} ({pipeline_id})")
        results.append(f"📋 Stage ID: {stage_id}")

        if removed_connections > 0:
            results.append(f"🔗 Removed {removed_connections} connections involving this stage")

        results.append(f"\n📊 Pipeline Status:")
        results.append(f"  Total Stages: {len(session.stages)}")
        results.append(f"  Connections: {len(session.connections)}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error removing stage from pipeline: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_connect_pipeline_stages(pipeline_id: str = "", source_stage_id: str = "", target_stage_id: str = "", connection_type: str = "data") -> str:
    """Connect two stages in a pipeline builder session (data flow or event stream)."""
    logger.info(f"Connecting stages in pipeline {pipeline_id}: {source_stage_id} -> {target_stage_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not source_stage_id.strip():
        return "❌ Error: source_stage_id is required"

    if not target_stage_id.strip():
        return "❌ Error: target_stage_id is required"

    # Validate connection type
    valid_connection_types = ["data", "event"]
    if connection_type not in valid_connection_types:
        return f"❌ Error: connection_type must be one of: {', '.join(valid_connection_types)}"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        # Verify both stages exist
        if source_stage_id not in session.stages:
            return f"❌ Error: Source stage '{source_stage_id}' not found in pipeline"

        if target_stage_id not in session.stages:
            return f"❌ Error: Target stage '{target_stage_id}' not found in pipeline"

        # Get stage information
        source_stage = session.stages[source_stage_id]
        target_stage = session.stages[target_stage_id]

        # Validate connection logic
        source_category = source_stage.get('category', 'unknown')
        target_category = target_stage.get('category', 'unknown')

        # Basic validation rules
        if source_category == "destination" and connection_type == "data":
            return f"❌ Error: Destination stages cannot be data sources. Consider using 'event' connection type."

        if target_category == "origin" and connection_type == "data":
            return f"❌ Error: Origin stages cannot be data targets. Consider connecting to a processor or destination."

        # Check for duplicate connections
        existing_connection = next(
            (conn for conn in session.connections
             if conn['source'] == source_stage_id and conn['target'] == target_stage_id and conn['type'] == connection_type),
            None
        )
        if existing_connection:
            return f"❌ Error: {connection_type.title()} connection already exists between these stages"

        # Add the connection and save to disk
        session.add_connection(source_stage_id, target_stage_id, connection_type)
        save_pipeline_session(pipeline_id, session)

        # Determine connection symbols
        connection_symbol = "📊 →" if connection_type == "data" else "⚡ ⇝"

        results = []
        results.append(f"✅ Stages Connected Successfully")
        results.append(f"🏗️  Pipeline: {session.title} ({pipeline_id})")
        results.append(f"🔗 Connection: {source_stage.get('label', source_stage_id)} {connection_symbol} {target_stage.get('label', target_stage_id)}")
        results.append(f"📋 Type: {connection_type.title()} flow")

        # Show validation info
        if connection_type == "data":
            results.append(f"📊 Data flows from {source_category} to {target_category}")
        else:
            results.append(f"⚡ Event stream from {source_category} to {target_category}")

        results.append(f"\n📊 Pipeline Status:")
        results.append(f"  Total Stages: {len(session.stages)}")
        results.append(f"  Total Connections: {len(session.connections)}")

        # Show current pipeline flow
        data_connections = [conn for conn in session.connections if conn['type'] == 'data']
        event_connections = [conn for conn in session.connections if conn['type'] == 'event']

        if data_connections:
            results.append(f"\n📊 Data Flow ({len(data_connections)} connections):")
            for i, conn in enumerate(data_connections, 1):
                source_label = session.stages[conn['source']].get('label', conn['source'])
                target_label = session.stages[conn['target']].get('label', conn['target'])
                results.append(f"  {i}. {source_label} → {target_label}")

        if event_connections:
            results.append(f"\n⚡ Event Streams ({len(event_connections)} connections):")
            for i, conn in enumerate(event_connections, 1):
                source_label = session.stages[conn['source']].get('label', conn['source'])
                target_label = session.stages[conn['target']].get('label', conn['target'])
                results.append(f"  {i}. {source_label} ⇝ {target_label}")

        results.append(f"\n📋 Next Steps:")
        results.append("• Add more connections with sdc_connect_pipeline_stages")
        results.append("• Build pipeline with sdc_build_pipeline")
        results.append("• View details with sdc_get_pipeline_builder_details")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error connecting pipeline stages: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_disconnect_pipeline_stages(pipeline_id: str = "", source_stage_id: str = "", target_stage_id: str = "", connection_type: str = "data") -> str:
    """Remove a connection between two stages in a pipeline builder session."""
    logger.info(f"Disconnecting stages in pipeline {pipeline_id}: {source_stage_id} -> {target_stage_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    if not source_stage_id.strip():
        return "❌ Error: source_stage_id is required"

    if not target_stage_id.strip():
        return "❌ Error: target_stage_id is required"

    # Validate connection type
    valid_connection_types = ["data", "event"]
    if connection_type not in valid_connection_types:
        return f"❌ Error: connection_type must be one of: {', '.join(valid_connection_types)}"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        # Find the connection to remove
        connection_to_remove = None
        for i, conn in enumerate(session.connections):
            if (conn['source'] == source_stage_id and
                conn['target'] == target_stage_id and
                conn['type'] == connection_type):
                connection_to_remove = i
                break

        if connection_to_remove is None:
            return f"❌ Error: No {connection_type} connection found between '{source_stage_id}' and '{target_stage_id}'"

        # Remove the connection and save to disk
        removed_connection = session.connections.pop(connection_to_remove)
        save_pipeline_session(pipeline_id, session)

        # Get stage labels for display
        source_label = session.stages.get(source_stage_id, {}).get('label', source_stage_id)
        target_label = session.stages.get(target_stage_id, {}).get('label', target_stage_id)

        # Determine connection symbols
        connection_symbol = "📊 →" if connection_type == "data" else "⚡ ⇝"

        results = []
        results.append(f"✅ Connection Removed")
        results.append(f"🏗️  Pipeline: {session.title} ({pipeline_id})")
        results.append(f"❌ Removed: {source_label} {connection_symbol} {target_label}")
        results.append(f"📋 Type: {connection_type.title()} flow")

        results.append(f"\n📊 Pipeline Status:")
        results.append(f"  Total Stages: {len(session.stages)}")
        results.append(f"  Total Connections: {len(session.connections)}")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error disconnecting pipeline stages: {e}")
        return f"❌ Error: {str(e)}"

@mcp.tool()
async def sdc_get_pipeline_flow(pipeline_id: str = "") -> str:
    """Visualize the current pipeline flow showing all stages and connections."""
    logger.info(f"Getting pipeline flow for {pipeline_id}")

    if not pipeline_id.strip():
        return "❌ Error: pipeline_id is required"

    try:
        session = get_pipeline_session(pipeline_id)
        if not session:
            return f"❌ Error: Pipeline builder '{pipeline_id}' not found"

        results = []
        results.append(f"🏗️  Pipeline Flow: {session.title}")
        results.append(f"📋 ID: {pipeline_id}")

        if not session.stages:
            results.append("\n❌ No stages in pipeline yet")
            return "\n".join(results)

        # Group stages by category
        stages_by_category = {}
        for stage_id, stage_info in session.stages.items():
            category = stage_info.get('category', 'unknown')
            if category not in stages_by_category:
                stages_by_category[category] = []
            stages_by_category[category].append((stage_id, stage_info))

        # Display stages by category
        results.append(f"\n📊 Stages ({len(session.stages)} total):")

        category_icons = {
            "origin": "📥",
            "processor": "⚙️",
            "destination": "📤",
            "executor": "⚡"
        }

        for category in ["origin", "processor", "destination", "executor", "unknown"]:
            if category in stages_by_category:
                icon = category_icons.get(category, "❓")
                stages = stages_by_category[category]
                results.append(f"\n{icon} {category.title()} Stages ({len(stages)}):")
                for stage_id, stage_info in stages:
                    results.append(f"  • {stage_id}: {stage_info.get('label', 'N/A')}")

        # Display connections
        if session.connections:
            results.append(f"\n🔗 Connections ({len(session.connections)} total):")

            data_connections = [conn for conn in session.connections if conn['type'] == 'data']
            event_connections = [conn for conn in session.connections if conn['type'] == 'event']

            if data_connections:
                results.append(f"\n📊 Data Flow ({len(data_connections)} connections):")
                for i, conn in enumerate(data_connections, 1):
                    source_label = session.stages[conn['source']].get('label', conn['source'])
                    target_label = session.stages[conn['target']].get('label', conn['target'])
                    results.append(f"  {i}. {source_label} → {target_label}")

            if event_connections:
                results.append(f"\n⚡ Event Streams ({len(event_connections)} connections):")
                for i, conn in enumerate(event_connections, 1):
                    source_label = session.stages[conn['source']].get('label', conn['source'])
                    target_label = session.stages[conn['target']].get('label', conn['target'])
                    results.append(f"  {i}. {source_label} ⇝ {target_label}")

            # Show pipeline flow visualization
            results.append(f"\n🌊 Pipeline Flow Visualization:")

            # Find origin stages (stages with no incoming data connections)
            stages_with_incoming = set(conn['target'] for conn in data_connections)
            origin_stages = [stage_id for stage_id in session.stages.keys()
                           if stage_id not in stages_with_incoming and
                           session.stages[stage_id].get('category') == 'origin']

            if origin_stages:
                results.append("📥 Data Entry Points:")
                for origin_stage in origin_stages:
                    _trace_pipeline_path(session, origin_stage, results, "  ", set())
            else:
                results.append("⚠️  No clear data entry points found")

        else:
            results.append(f"\n❌ No connections defined yet")
            results.append("💡 Use sdc_connect_pipeline_stages to connect stages")

        return "\n".join(results)

    except Exception as e:
        logger.error(f"Error getting pipeline flow: {e}")
        return f"❌ Error: {str(e)}"

def _trace_pipeline_path(session, current_stage_id, results, indent, visited):
    """Helper function to trace pipeline paths recursively."""
    if current_stage_id in visited:
        return

    visited.add(current_stage_id)
    stage_info = session.stages[current_stage_id]
    stage_label = stage_info.get('label', current_stage_id)
    category = stage_info.get('category', 'unknown')

    # Icon based on category
    category_icon = {
        "origin": "📥",
        "processor": "⚙️",
        "destination": "📤",
        "executor": "⚡"
    }.get(category, "❓")

    results.append(f"{indent}{category_icon} {stage_label}")

    # Find outgoing data connections
    outgoing_connections = [conn for conn in session.connections
                          if conn['source'] == current_stage_id and conn['type'] == 'data']

    # Recursively trace connected stages
    for conn in outgoing_connections:
        target_stage_id = conn['target']
        _trace_pipeline_path(session, target_stage_id, results, indent + "  ", visited)

# === JOB CREATION & DELETION TOOLS ===

@mcp.tool()
async def sdc_create_job_from_template(
    template_job_id: str,
    job_name: str,
    runtime_parameters: str = "{}",
    inherit_permissions: bool = False
) -> str:
    """
    Create a new job instance from an existing job template.

    Args:
        template_job_id: ID of the job template to use
        job_name: Name for the new job instance
        runtime_parameters: JSON string of runtime parameters for the job
        inherit_permissions: Whether to inherit permissions from template

    Returns:
        Formatted job creation result with new job details
    """
    try:
        if not validate_config():
            return "❌ Error: Missing StreamSets configuration"

        # Parse runtime parameters
        try:
            runtime_params = json.loads(runtime_parameters) if runtime_parameters else {}
        except json.JSONDecodeError:
            return "❌ Error: runtime_parameters must be valid JSON"

        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{template_job_id}/createAndStartJobInstances"

        # Build the creation info payload
        creation_info = {
            "runtimeParameters": [runtime_params] if runtime_params else [{}],
            "jobNames": [job_name]
        }

        params = {
            "inheritPermissions": str(inherit_permissions).lower()
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                params=params,
                json=creation_info,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            if isinstance(data, list) and data:
                job_data = data[0]  # Get first created job

                results = [f"✅ Job created successfully from template {template_job_id}:\n"]
                results.append(f"📊 Job ID: {job_data.get('jobId', 'N/A')}")
                results.append(f"📋 Name: {job_data.get('jobName', job_name)}")
                results.append(f"🏷️ Template ID: {template_job_id}")

                if job_data.get('status'):
                    results.append(f"🔄 Status: {job_data['status']}")

                if job_data.get('color'):
                    results.append(f"🎨 Color: {job_data['color']}")

                if job_data.get('runCount'):
                    results.append(f"🔢 Run Count: {job_data['runCount']}")

                if job_data.get('timeStamp'):
                    created_time = datetime.fromtimestamp(job_data['timeStamp']/1000, tz=timezone.utc)
                    results.append(f"⏰ Created: {created_time.isoformat()}")

                if runtime_params:
                    results.append(f"⚙️ Runtime Parameters: {len(runtime_params)} parameters")

                return "\n".join(results)
            else:
                return f"✅ Job creation initiated from template {template_job_id}"

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"❌ Unexpected error: {str(e)}"

@mcp.tool()
async def sdc_create_job_async(
    template_job_id: str,
    job_name: str,
    runtime_parameters: str = "{}",
    inherit_permissions: bool = False
) -> str:
    """
    Create a new job instance from a template asynchronously (non-blocking).

    Args:
        template_job_id: ID of the job template to use
        job_name: Name for the new job instance
        runtime_parameters: JSON string of runtime parameters for the job
        inherit_permissions: Whether to inherit permissions from template

    Returns:
        Formatted job creation result with async operation details
    """
    try:
        if not validate_config():
            return "❌ Error: Missing StreamSets configuration"

        # Parse runtime parameters
        try:
            runtime_params = json.loads(runtime_parameters) if runtime_parameters else {}
        except json.JSONDecodeError:
            return "❌ Error: runtime_parameters must be valid JSON"

        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{template_job_id}/createAndStartJobInstancesAsync"

        # Build the creation info payload
        creation_info = {
            "runtimeParameters": [runtime_params] if runtime_params else [{}],
            "jobNames": [job_name]
        }

        params = {
            "inheritPermissions": str(inherit_permissions).lower()
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                params=params,
                json=creation_info,
                timeout=30
            )
            response.raise_for_status()

            data = response.json()

            results = [f"✅ Async job creation initiated from template {template_job_id}:\n"]
            results.append(f"📋 Job Name: {job_name}")
            results.append(f"🏷️ Template ID: {template_job_id}")
            results.append(f"🔄 Mode: Asynchronous")
            results.append(f"⚙️ Inherit Permissions: {inherit_permissions}")

            if runtime_params:
                results.append(f"⚙️ Runtime Parameters: {len(runtime_params)} parameters")

            results.append("\n📝 Note: Job creation is running in background. Use sdc_list_jobs to check status.")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"❌ Unexpected error: {str(e)}"

@mcp.tool()
async def sdc_duplicate_job(
    job_id: str,
    new_job_name: str,
    copy_acl: bool = True,
    copy_labels: bool = True
) -> str:
    """
    Duplicate an existing job with a new name.

    Args:
        job_id: ID of the job to duplicate
        new_job_name: Name for the duplicated job
        copy_acl: Whether to copy ACL permissions from original job
        copy_labels: Whether to copy labels from original job

    Returns:
        Formatted job duplication result
    """
    try:
        if not validate_config():
            return "❌ Error: Missing StreamSets configuration"

        url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}/duplicate"

        duplicate_info = {
            "name": new_job_name,
            "copyAcl": copy_acl,
            "copyLabels": copy_labels
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                headers=get_auth_headers(),
                json=duplicate_info,
                timeout=30
            )
            response.raise_for_status()

            results = [f"✅ Job duplicated successfully:\n"]
            results.append(f"📊 Original Job ID: {job_id}")
            results.append(f"📋 New Job Name: {new_job_name}")
            results.append(f"🔒 Copy ACL: {copy_acl}")
            results.append(f"🏷️ Copy Labels: {copy_labels}")

            results.append("\n📝 Note: Use sdc_list_jobs to find the new job ID")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"❌ Unexpected error: {str(e)}"

@mcp.tool()
async def sdc_delete_job(job_id: str) -> str:
    """
    Delete a job permanently. WARNING: This action cannot be undone.

    Args:
        job_id: ID of the job to delete

    Returns:
        Confirmation of job deletion
    """
    try:
        if not validate_config():
            return "❌ Error: Missing StreamSets configuration"

        # First get job details for confirmation
        job_details_url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}"

        async with httpx.AsyncClient() as client:
            # Get job details first
            details_response = await client.get(
                job_details_url,
                headers=get_auth_headers(),
                timeout=30
            )

            if details_response.status_code == 404:
                return f"❌ Job {job_id} not found"

            details_response.raise_for_status()
            job_data = details_response.json()
            job_name = job_data.get('jobName', 'Unknown')

            # Delete the job
            delete_url = f"{HOST_PREFIX}/jobrunner/rest/v1/job/{job_id}"
            delete_response = await client.delete(
                delete_url,
                headers=get_auth_headers(),
                timeout=30
            )
            delete_response.raise_for_status()

            results = [f"✅ Job deleted successfully:\n"]
            results.append(f"📊 Job ID: {job_id}")
            results.append(f"📋 Job Name: {job_name}")
            results.append(f"⚠️ Status: Permanently deleted")

            results.append("\n🔄 Note: This action cannot be undone")

            return "\n".join(results)

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error: {e.response.status_code}")
        if e.response.status_code == 404:
            return f"❌ Job {job_id} not found"
        return f"❌ API Error: {e.response.status_code} - {e.response.text[:200]}"
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return f"❌ Unexpected error: {str(e)}"

# === SERVER STARTUP ===
if __name__ == "__main__":
    logger.info("Starting Streamsets MCP server...")
    
    # Check configuration
    if not HOST_PREFIX:
        logger.warning("STREAMSETS_HOST_PREFIX not set - please configure")
    if not CRED_ID:
        logger.warning("STREAMSETS_CRED_ID not set - authentication will fail")
    if not CRED_TOKEN:
        logger.warning("STREAMSETS_CRED_TOKEN not set - authentication will fail")
    
    try:
        mcp.run(transport='stdio')
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)