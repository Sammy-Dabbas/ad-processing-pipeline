FROM python:3.13.7-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Copy requirements and install dependencies
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY consumer/ad_event_consumer.py .
COPY infrastructure/ ./infrastructure/

# Create data directory (volume will mount here)
RUN mkdir -p data

CMD ["python", "ad_event_consumer.py"]