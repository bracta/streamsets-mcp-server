#!/bin/bash

# StreamSets MCP Server Docker Startup Script
# FOR TESTING/DEVELOPMENT ONLY - Not for MCP integration!
#
# This script builds and starts the Docker container as a standalone service
# For MCP integration, use the Docker command in claude_desktop_config.json

set -e

echo "🚀 Starting StreamSets MCP Server with Docker (Testing Mode)"
echo "⚠️  NOTE: This is for testing only. For MCP integration, see README.md"

# Check if required environment variables are set
if [ -z "$STREAMSETS_HOST_PREFIX" ] || [ -z "$STREAMSETS_CRED_ID" ] || [ -z "$STREAMSETS_CRED_TOKEN" ]; then
    echo "❌ Error: Required environment variables not set:"
    echo "   - STREAMSETS_HOST_PREFIX"
    echo "   - STREAMSETS_CRED_ID"
    echo "   - STREAMSETS_CRED_TOKEN"
    echo ""
    echo "Please set these variables and try again:"
    echo "export STREAMSETS_HOST_PREFIX=\"https://your-instance.streamsets.com\""
    echo "export STREAMSETS_CRED_ID=\"your-credential-id\""
    echo "export STREAMSETS_CRED_TOKEN=\"your-auth-token\""
    exit 1
fi

# Create data directory if it doesn't exist
mkdir -p ./data/pipeline_builders

echo "📦 Building Docker image..."
docker-compose build

echo "🔄 Starting container with persistent volumes..."
docker-compose up -d

echo "✅ StreamSets MCP Server started successfully!"
echo ""
echo "📊 Container status:"
docker-compose ps

echo ""
echo "📝 View logs:"
echo "   docker-compose logs -f streamsets-mcp"
echo ""
echo "🛑 Stop the server:"
echo "   docker-compose down"
echo ""
echo "💾 Pipeline data is persisted in: ./data/pipeline_builders"