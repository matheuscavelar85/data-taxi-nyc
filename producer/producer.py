import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi-rides")
DATA_PATH = os.getenv("DATA_PATH", "/data/train.csv")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "0.1"))  # segundos

def create_producer():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            print(f"Conectando ao Kafka (tentativa {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=3,
                retry_backoff_ms=100
            )
            print("✓ Conectado ao Kafka com sucesso!")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"✗ Erro: {e}. Aguardando 5s...")
                time.sleep(5)
            else:
                raise

def publish_rides(producer, df_chunk):
    for _, row in df_chunk.iterrows():
        # Cria uma chave única
        key = str(hash(tuple(row)))  
        value = {
            "pickup_datetime": str(row["pickup_datetime"]),
            "pickup_longitude": float(row["pickup_longitude"]),
            "pickup_latitude": float(row["pickup_latitude"]),
            "dropoff_longitude": float(row["dropoff_longitude"]),
            "dropoff_latitude": float(row["dropoff_latitude"]),
            "passenger_count": int(row["passenger_count"]),
            "fare_amount": float(row["fare_amount"])
        }
        producer.send(KAFKA_TOPIC, key=key, value=value)
        time.sleep(PUBLISH_INTERVAL)

def main():
    print(f"Lendo dados de {DATA_PATH}")
    producer = create_producer()
    
    chunksize = 10000
    for chunk in pd.read_csv(DATA_PATH, chunksize=chunksize):
        publish_rides(producer, chunk)
    
    producer.flush()
    print("Publicação concluída.")

if __name__ == "__main__":
    main()
