# Dockerfile
FROM python:3.9

WORKDIR /app

# Copy requirements.txt and install dependencies
COPY app/requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code
COPY . .

# Command to run the ingestion script
CMD ["python", "app/app.py"]
