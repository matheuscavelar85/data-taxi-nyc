from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import boto3
import psycopg2
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "nyc-taxi")
RAW_PREFIX       = "raw/"
DB_HOST     = os.getenv("DB_HOST",     "postgres-dbt")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "analytics_warehouse")
DB_USER     = os.getenv("DB_USER",     "dbt_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dbt_password")

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def check_minio_raw(**ctx):
    """
    Verifica se o bucket existe e contém arquivos JSONL na camada raw.
    Se vazio → skip_pipeline; se tem dados → load_minio_to_staging.
    """
    s3 = _s3_client()

    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except Exception:
        logger.warning(f"Bucket '{MINIO_BUCKET}' não existe. Criando...")
        s3.create_bucket(Bucket=MINIO_BUCKET)
        logger.info("Bucket criado. Pipeline será ignorado neste ciclo (sem dados ainda).")
        return "skip_pipeline"

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=RAW_PREFIX, MaxKeys=1)
    has_files = bool(response.get("Contents"))

    if not has_files:
        logger.info("Nenhum arquivo JSONL no MinIO ainda. Aguardando próximo ciclo.")
        return "skip_pipeline"

    paginator = s3.get_paginator("list_objects_v2")
    total = sum(
        1
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=RAW_PREFIX)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".jsonl")
    )
    logger.info(f"✅ MinIO raw contém {total} arquivos JSONL. Iniciando carga.")
    ctx["ti"].xcom_push(key="raw_file_count", value=total)
    return "load_minio_to_staging"


def load_minio_to_staging(**ctx):
    """
    Lê TODOS os JSONL do MinIO raw/ e popula raw.taxi_rides_staging (truncate + insert).
    """
    import json
    import psycopg2.extras

    s3 = _s3_client()
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.taxi_rides_staging (
                source_file           TEXT,
                vendor_id             TEXT,
                pickup_datetime       TEXT,
                dropoff_datetime      TEXT,
                passenger_count       INTEGER,
                trip_distance         FLOAT,
                pickup_longitude      FLOAT,
                pickup_latitude       FLOAT,
                rate_code_id          TEXT,
                store_and_fwd_flag    TEXT,
                dropoff_longitude     FLOAT,
                dropoff_latitude      FLOAT,
                payment_type          TEXT,
                fare_amount           FLOAT,
                extra                 FLOAT,
                mta_tax               FLOAT,
                tip_amount            FLOAT,
                tolls_amount          FLOAT,
                improvement_surcharge FLOAT,
                total_amount          FLOAT,
                ingestion_timestamp   TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        cur.execute("TRUNCATE TABLE raw.taxi_rides_staging;")
        conn.commit()

    paginator = s3.get_paginator("list_objects_v2")
    total_records = 0
    total_files   = 0

    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue

            response = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
            lines    = response["Body"].read().decode("utf-8").splitlines()
            records  = [json.loads(l) for l in lines if l.strip()]

            if not records:
                continue

            def _int(v):
                try: return int(v)
                except: return None

            def _float(v):
                try: return float(v)
                except: return None

            rows = [(
                key,
                r.get("vendor_id"),
                r.get("pickup_datetime"),
                r.get("dropoff_datetime"),
                _int(r.get("passenger_count")),
                _float(r.get("trip_distance")),
                _float(r.get("pickup_longitude")),
                _float(r.get("pickup_latitude")),
                r.get("rate_code_id"),
                r.get("store_and_fwd_flag"),
                _float(r.get("dropoff_longitude")),
                _float(r.get("dropoff_latitude")),
                r.get("payment_type"),
                _float(r.get("fare_amount")),
                _float(r.get("extra")),
                _float(r.get("mta_tax")),
                _float(r.get("tip_amount")),
                _float(r.get("tolls_amount")),
                _float(r.get("improvement_surcharge")),
                _float(r.get("total_amount")),
            ) for r in records]

            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO raw.taxi_rides_staging (
                        source_file, vendor_id, pickup_datetime, dropoff_datetime,
                        passenger_count, trip_distance, pickup_longitude, pickup_latitude,
                        rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
                        payment_type, fare_amount, extra, mta_tax,
                        tip_amount, tolls_amount, improvement_surcharge, total_amount
                    ) VALUES %s
                """, rows, page_size=500)
            conn.commit()

            total_records += len(records)
            total_files   += 1
            logger.info(f"  → {key}: {len(records)} registros")

    conn.close()
    logger.info(f"✅ Staging carregada: {total_records} registros de {total_files} arquivos.")
    ctx["ti"].xcom_push(key="staged_records", value=total_records)


def validate_staging(**ctx):
    """Valida que a staging table tem registros antes de rodar dbt."""
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.taxi_rides_staging;")
        count = cur.fetchone()[0]
    conn.close()

    if count == 0:
        raise ValueError("raw.taxi_rides_staging está vazia. dbt não será executado.")
    logger.info(f"✅ Staging validada: {count} registros prontos para dbt.")


with DAG(
    dag_id="taxi_pipeline",
    description="Kafka → MinIO raw → PostgreSQL staging → dbt silver/gold",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    tags=["taxi", "streaming", "dbt"],
) as dag:

    t_check_minio = BranchPythonOperator(
        task_id="check_minio_raw",
        python_callable=check_minio_raw,
    )

    t_skip = EmptyOperator(task_id="skip_pipeline")

    t_load_staging = PythonOperator(
        task_id="load_minio_to_staging",
        python_callable=load_minio_to_staging,
    )

    t_validate = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging,
    )

    t_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt && \
            /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt
        """,
        env={
            "DB_HOST": DB_HOST,
            "DB_PORT": str(DB_PORT),
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        },
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dbt && \
            /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt
        """,
        env={
            "DB_HOST": DB_HOST,
            "DB_PORT": str(DB_PORT),
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        },
    )

    t_check_minio >> [t_skip, t_load_staging]
    t_load_staging >> t_validate >> t_dbt_run >> t_dbt_test
