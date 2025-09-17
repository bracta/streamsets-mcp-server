# Use Python slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set Python unbuffered mode
ENV PYTHONUNBUFFERED=1

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the server code
COPY streamsets_server.py .

# Create non-root user and persistence directory
RUN useradd -m -u 1000 mcpuser && \
    chown -R mcpuser:mcpuser /app && \
    mkdir -p /data/pipeline_builders && \
    chown -R mcpuser:mcpuser /data

# Create volume mount point for pipeline builder persistence
VOLUME ["/data"]

# Switch to non-root user
USER mcpuser

# Run the server
CMD ["python", "streamsets_server.py"]