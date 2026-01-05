FROM selenium/standalone-chrome:latest

# Metadata
LABEL maintainer="ETL Pipeline"
LABEL description="WhatsApp ETL with Selenium and MongoDB"

WORKDIR /app

USER root

# Install Python and essential tools
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip in the venv
RUN /opt/venv/bin/python -m pip install --upgrade pip

# Copy and install Python dependencies
COPY requirements.txt .
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/whatsapp_session && \
    chown -R seluser:seluser /app

# Environment variables with defaults
ENV ETL_INTERVAL=7200
ENV PYTHONUNBUFFERED=1

# Health check - verify both Selenium and MongoDB connectivity
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:4444/wd/hub/status || exit 1

# Switch to non-root user
USER seluser

# Expose ports
EXPOSE 4444 5900 7900

# Use Python scheduler instead of bash loop
CMD ["/bin/bash", "-c", "\
    set -e && \
    echo 'Starting Selenium Grid...' && \
    /opt/bin/entry_point.sh & \
    echo 'Waiting for Selenium to be ready...' && \
    timeout=30 && \
    count=0 && \
    while [ $count -lt $timeout ]; do \
        if curl -s http://localhost:4444/wd/hub/status > /dev/null 2>&1; then \
            echo 'Selenium is ready!' && \
            break; \
        fi; \
        count=$((count + 1)); \
        echo \"Waiting... ($count/$timeout)\"; \
        sleep 2; \
    done && \
    if [ $count -eq $timeout ]; then \
        echo 'ERROR: Selenium failed to start' && \
        exit 1; \
    fi && \
    echo 'Starting ETL Scheduler...' && \
    exec /opt/venv/bin/python scheduler.py --interval $ETL_INTERVAL"]
