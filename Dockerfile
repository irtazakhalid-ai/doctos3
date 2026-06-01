FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard.py .
COPY kaggle_to_s3.py .

EXPOSE 8080

CMD ["python", "-u", "dashboard.py"]
