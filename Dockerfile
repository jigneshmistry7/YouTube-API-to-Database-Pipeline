# YouTube Data Pipeline Dockerfile
FROM python:3.9-slim as builder

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --user -r requirements.txt

# Runtime stage
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r youtube && useradd -r -g youtube youtube

# Copy Python dependencies from builder
COPY --from=builder /root/.local /home/youtube/.local

# Copy application code
COPY . .

# Set path for user-installed Python packages
ENV PATH=/home/youtube/.local/bin:$PATH \
    PYTHONPATH=/app/src

# Create necessary directories
RUN mkdir -p /app/logs /app/data /app/backups \
    && chown -R youtube:youtube /app

# Switch to non-root user
USER youtube

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health', timeout=2)"

# Expose port for monitoring (if applicable)
EXPOSE 8080

# Default command
CMD ["python", "main.py"]