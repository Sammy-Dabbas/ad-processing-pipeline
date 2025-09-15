FROM python:3.13.7-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Copy requirements FIRST (rarely changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code LAST (changes frequently)  
# Copy the needed directory structure
COPY reader.py .
# Create data directory (volume will mount here)
RUN mkdir -p data

# Run the application
CMD ["python", "reader.py"]