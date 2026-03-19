FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming_consumer.py .

CMD ["python", "streaming_consumer.py"]
