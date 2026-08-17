# Base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion scripts
COPY ingestion/ ./ingestion/

# Set environment variables
ENV STORAGE_ACCOUNT_NAME=stlogisticsplatform
ENV AZURE_CLIENT_ID=""
ENV AZURE_CLIENT_SECRET=""
ENV AZURE_TENANT_ID=""

# Default command runs the API ingestion
CMD ["python", "ingestion/api/opensky_ingestion.py"]