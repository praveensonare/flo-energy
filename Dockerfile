# ============================================================
# Flo Energy – Meter Reading ETL Agent
# Multi-stage build for a lean production image
# ============================================================

# ---- Builder stage ----
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies (needed for psycopg2 native extension)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---- Runtime stage ----
FROM python:3.12-slim AS runtime

LABEL maintainer="flo-energy"
LABEL description="NEM12/NEM13 Meter Reading ETL Agent powered by Claude AI"

# Runtime PostgreSQL client library
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app

# Copy application source
COPY src/ src/
COPY main.py .

# Data directory (mount point for input files)
RUN mkdir -p /data /output

# Non-root user for security
RUN useradd --uid 1001 --create-home appuser \
    && chown -R appuser:appuser /app /data /output
USER appuser

# Prometheus metrics port
EXPOSE 8000

ENTRYPOINT ["python", "main.py"]
CMD ["--help"]
