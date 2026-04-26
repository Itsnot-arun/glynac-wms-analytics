# =============================================================================
#   Glynac WMS — Referral Analytics Pipeline
#   Springer Capital | Data Engineering Intern Take-Home Test
# =============================================================================

# Use official Python slim image for smaller footprint
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Copy dependency manifest first (for better layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY pipeline.py .
COPY data/ ./data/

# Create output directory (where reports are written)
RUN mkdir -p output

# Run the pipeline when the container starts
CMD ["python", "pipeline.py"]
