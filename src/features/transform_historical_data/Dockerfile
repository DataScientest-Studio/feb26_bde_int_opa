FROM python:3.11-slim

# Ensure logs are printed immediately
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies (needed for psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency file first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your source code
COPY src/ ./src/

# Run your script
CMD ["python3", "src/features/transform_historical_data/generate_explanatory_variables_from_historical_data.py"]