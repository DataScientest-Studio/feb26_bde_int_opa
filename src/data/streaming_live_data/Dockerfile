# -------- Base image --------
FROM python:3.11-slim

# -------- Environment --------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# -------- System dependencies --------
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# -------- Work directory --------
WORKDIR /app

# -------- Install Python deps --------
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r requirements.txt

# -------- Copy application --------
COPY src/ src/

# -------- Run script --------
CMD ["python3", "src/data/streaming_live_data/extract_and_load_live_klines.py"]