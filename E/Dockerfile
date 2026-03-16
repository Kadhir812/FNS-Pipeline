# Dockerfile for E (Extract) service
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .


# (Optional) If you use a .env file, mount it at runtime or use --env-file in docker run/docker-compose
# Do NOT copy .env here if it contains secrets

# Mount the models directory from T as a volume at runtime using docker run or docker-compose, not in the Dockerfile
# Example:
#   docker run --env-file .env -v "d:/big data pipeline/T/models:/app/T/models" extract-service

# Set the entrypoint to your main script
CMD ["python", "Extract.py"]
