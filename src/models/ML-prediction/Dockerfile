FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY ./src/models/ML-prediction/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy prediction code
COPY ./src/models/ML-prediction/ /app

# Create models directory
RUN mkdir -p /models

# Run prediction service
CMD ["python3", "predict_model.py"]