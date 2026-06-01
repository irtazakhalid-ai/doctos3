FROM python:3.13-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY kaggle_to_s3.py .

CMD ["python", "-u", "kaggle_to_s3.py"]
