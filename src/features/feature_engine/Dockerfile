# -------- Base image --------
FROM python:3.11-slim

# -------- System deps (for psycopg2) --------
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# -------- Set working dir --------
WORKDIR /app

# -------- Copy requirements --------
COPY requirements.txt .

# -------- Install Python deps --------
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# -------- Copy project code --------
COPY src/ ./src/

# -------- Run feature engine --------
CMD ["python3", "src/features/feature_engine/generate_explanatory_variables_from_streaming_data.py"]