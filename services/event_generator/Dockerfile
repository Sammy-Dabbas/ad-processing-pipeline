# Event Generator - High-volume ad event simulation
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ad_event_generator.py .

# Create data directory for volume mount
RUN mkdir -p /app/data

# Run the event generator
CMD ["python", "ad_event_generator.py"]
