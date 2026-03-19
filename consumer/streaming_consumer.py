import os
import json
import logging
import time
from datetime import datetime
from io import BytesIO

import boto3
from botocore.client import Config
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "taxi-rides")
BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "100"))
MINIO_ENDPOINT          = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY        = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY        = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET            = os.getenv("MINIO_BUCKET", "nyc-taxi")

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' já existe.")
    except Exception:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' criado.")

def upload_batch(s3, records: list):
    """Serializa lista de dicts como JSONL e faz upload para MinIO raw/."""
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp      = datetime.utcnow().strftime("%Y%m%d_%H%M%S%f")
    key = f"raw/ingestion_date={ingestion_date}/{timestamp}.jsonl"

    jsonl_bytes = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    buffer = BytesIO(jsonl_bytes)

    s3.upload_fileobj(buffer, MINIO_BUCKET, key)
    logger.info(f"✅ Upload: s3://{MINIO_BUCKET}/{key} ({len(records)} registros, {len(jsonl_bytes)} bytes)")

def main():
    consumer = None
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="minio-raw-consumer",
                consumer_timeout_ms=5000,
            )
            logger.info("✅ Conectado ao Kafka.")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka indisponível, tentativa {attempt + 1}/10. Aguardando 5s...")
            time.sleep(5)

    if consumer is None:
        raise RuntimeError("Não foi possível conectar ao Kafka após 10 tentativas.")

    s3 = get_s3_client()
    ensure_bucket(s3, MINIO_BUCKET)

    batch = []
    logger.info(f"Consumindo tópico '{KAFKA_TOPIC}' em batches de {BATCH_SIZE}...")

    while True:
        try:
            for message in consumer:
                batch.append(message.value)
                if len(batch) >= BATCH_SIZE:
                    upload_batch(s3, batch)
                    batch = []
        except Exception as e:
            logger.error(f"Erro no loop de consumo: {e}")
            if batch:
                try:
                    upload_batch(s3, batch)
                except Exception as upload_err:
                    logger.error(f"Falha no upload do batch parcial: {upload_err}")
                batch = []
            time.sleep(3)

if __name__ == "__main__":
    main()
