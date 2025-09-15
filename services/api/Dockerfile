FROM python:3.13.7-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Copy requirements FIRST (rarely changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code (changes frequently)
COPY main.py .
COPY routes/ ./routes/
COPY static/ ./static/

# Create data directory (volume will mount here)
RUN mkdir -p data

# Expose port for FastAPI
EXPOSE 8000

# Run with uvicorn (not python directly)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]