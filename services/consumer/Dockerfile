FROM python:3.13.7-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# No external dependencies needed - uses only standard library
COPY consumer.py .

# Create data directory (volume will mount here)
RUN mkdir -p data

CMD ["python", "consumer.py"]