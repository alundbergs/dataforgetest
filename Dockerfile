# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app directory to the working directory
COPY ./app ./app

# Expose the application port
EXPOSE 8080

# Install psutil
# RUN apt-get update && apt-get install -y gcc python3-dev && pip install psutil
# Run the FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
