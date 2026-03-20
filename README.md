# 🚕 NYC Taxi Data Pipeline — Complete Documentation
### Internal Use | Presentation | Technical Reference

---

## 📋 Executive Overview

An end-to-end data engineering pipeline that simulates a real production environment, processing 50,000 historical NYC Taxi records as if they were real-time events. The architecture implements the modern Data Engineering stack: streaming ingestion via Kafka, Data Lake storage with MinIO, declarative transformation with dbt, and full orchestration with Apache Airflow — all containerized with Docker Compose.

**What the pipeline does, from start to finish:**
1. Reads a CSV with 50K NYC taxi rides
2. Publishes each record as an event to Kafka (simulating real streaming)
3. A Consumer reads those events in batches and saves them as JSONL files in MinIO (Data Lake)
4. Airflow, every 10 minutes, detects new files in MinIO, loads them into PostgreSQL, and triggers dbt transformations
5. dbt transforms the raw data into analytics-ready layers (Silver and Gold)

---

## 🏗️ Full Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           NYC TAXI DATA PIPELINE                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ┌─────────────┐    JSON events     ┌──────────────────────────────────┐     ║
║  │  train.csv  │──────────────────▶ │           APACHE KAFKA           │     ║
║  │  (50K regs) │   100ms/record     │       Topic: taxi-rides          │     ║
║  │  Producer   │                    │       Broker: kafka:9092         │     ║
║  └─────────────┘                    └──────────────┬───────────────────┘     ║
║                                                    │ batch of 100 msgs       ║
║                                                    ▼                         ║
║                                    ┌──────────────────────────────────┐      ║
║                                    │         STREAMING CONSUMER       │      ║
║                                    │  • Accumulates 100 messages      │      ║
║                                    │  • Serializes to JSONL           │      ║
║                                    │  • Uploads to MinIO              │      ║
║                                    └──────────────┬───────────────────┘      ║
║                                                   │                          ║
║                                                   ▼                          ║
║                                    ┌──────────────────────────────────┐      ║
║                                    │     MINIO  (Data Lake)           │      ║
║                                    │  S3-compatible | port 9000       │      ║
║                                    │  Console UI   | port 9001        │      ║
║                                    │                                  │      ║
║                                    │  nyc-taxi/                       │      ║
║                                    │  └─ raw/                         │      ║
║                                    │     └─ 20260318_155514.jsonl     │      ║
║                                    │        20260318_155524.jsonl     │      ║
║                                    │        ... (1 file / 100 records)│      ║
║                                    └──────────────┬───────────────────┘      ║
║                                                   │                          ║
║  ╔════════════════════════════════════════════════╪═══════════════════════╗  ║
║  ║              APACHE AIRFLOW (port 8082)        │                       ║  ║
║  ║              DAG: taxi_pipeline                │                       ║  ║
║  ║              Schedule: */10 * * * *            │                       ║  ║
║  ║                                                ▼                       ║  ║
║  ║   ┌─────────────────┐     ┌──────────────────────────────────────┐     ║  ║
║  ║   │ check_minio_raw │     │        load_minio_to_staging         │     ║  ║
║  ║   │ BranchPython    │────▶│        PythonOperator                │     ║  ║
║  ║   │ • Checks if     │     │  • Reads all JSONL files from MinIO  │     ║  ║
║  ║   │   JSONL files   │     │  • TRUNCATE + INSERT into PostgreSQL │     ║  ║
║  ║   │   exist in raw/ │     │  • Tracks source_file per record     │     ║  ║
║  ║   └─────────────────┘     └──────────────────┬───────────────────┘     ║  ║
║  ║         │ (empty)                            │                         ║  ║
║  ║         ▼                                    ▼                         ║  ║
║  ║   ┌───────────────┐        ┌──────────────────────────────────────┐    ║  ║
║  ║   │ skip_pipeline │        │          validate_staging            │    ║  ║
║  ║   │ EmptyOperator │        │          PythonOperator              │    ║  ║
║  ║   └───────────────┘        │  • COUNT(*) on staging               │    ║  ║
║  ║                            │  • Fails if empty                    │    ║  ║
║  ║                            └──────────────────┬───────────────────┘    ║  ║
║  ║                                               │                        ║  ║
║  ║                                               ▼                        ║  ║
║  ║                            ┌──────────────────────────────────────┐    ║  ║
║  ║                            │             dbt_run                  │    ║  ║
║  ║                            │             BashOperator             │    ║  ║
║  ║                            │  • Runs all dbt models               │    ║  ║
║  ║                            │  • Raw → Silver → Gold               │    ║  ║
║  ║                            └──────────────────┬───────────────────┘    ║  ║
║  ║                                               │                        ║  ║
║  ║                                               ▼                        ║  ║
║  ║                            ┌──────────────────────────────────────┐    ║  ║
║  ║                            │             dbt_test                 │    ║  ║
║  ║                            │             BashOperator             │    ║  ║
║  ║                            │  • Runs data quality tests           │    ║  ║
║  ║                            │  • Validates uniqueness, nulls,      │    ║  ║
║  ║                            │    ranges                            │    ║  ║
║  ║                            └──────────────────────────────────────┘    ║  ║
║  ╚════════════════════════════════════════════════════════════════════════╝  ║
║                                                   │                          ║
║                                                   ▼                          ║
║  ╔════════════════════════════════════════════════════════════════════════╗  ║
║  ║              POSTGRESQL 14  —  analytics_warehouse                     ║  ║
║  ║              port 5433 (host) | port 5432 (internal)                   ║  ║
║  ║                                                                        ║  ║
║  ║  schema: raw                    schema: dbt_dev_silver_raw             ║  ║
║  ║  └─ taxi_rides_staging          └─ base_taxi_rides (VIEW)              ║  ║
║  ║     (physical table)                                                   ║  ║
║  ║     source: MinIO JSONL         schema: dbt_dev_silver_silver          ║  ║
║  ║                                 └─ stg_taxi_rides (VIEW)               ║  ║
║  ║                                    cleaning + enrichment               ║  ║
║  ║                                                                        ║  ║
║  ║                                 schema: dbt_dev_silver_gold            ║  ║
║  ║                                 ├─ fct_rides         (TABLE)           ║  ║
║  ║                                 ├─ dim_locations     (TABLE)           ║  ║
║  ║                                 └─ agg_daily_metrics (TABLE)           ║  ║
║  ╚════════════════════════════════════════════════════════════════════════╝  ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## 📦 Components & Technologies

| Component | Technology | Version | Port | Role |
|---|---|---|---|---|
| **Messaging** | Apache Kafka | 7.4.0 | 9092 | Real-time event queue |
| **Coordination** | Zookeeper | 7.4.0 | 2181 | Kafka cluster management |
| **Data Lake** | MinIO | latest | 9000/9001 | Raw file storage (S3-compatible) |
| **Warehouse** | PostgreSQL | 14-alpine | 5433 | Analytics database with multiple schemas |
| **Transformation** | dbt-postgres | 1.7.0 | — | Declarative ELT in SQL |
| **Orchestration** | Apache Airflow | 2.7.0 | 8082 | DAGs, scheduling, retry, monitoring |
| **Runtime** | Python | 3.9 | — | Producer, Consumer, and DAG |
| **Infrastructure** | Docker Compose | v2 | — | Containerized environment |

---

## 🔬 Detailed Component Breakdown

---

### 1️⃣ PRODUCER (`producer/producer.py`)

**What it does:** Reads the `data/train.csv` file with 50,000 NYC taxi ride records and publishes each record as a JSON event to Kafka, simulating a real-time ingestion system.

**How it works, step by step:**

```
train.csv (50K rows)
       │
       ▼
pd.read_csv(chunksize=10,000)   ← reads in chunks to avoid RAM overload
       │
       ▼
For each record in the chunk:
  ├─ Creates a unique key: hash(tuple(row))
  ├─ Builds the JSON payload with 7 fields
  ├─ producer.send(topic, key=key, value=payload)
  └─ time.sleep(0.1)            ← rate control: ~10 events/second
```

**JSON payload published to Kafka:**
```json
{
  "pickup_datetime":    "2015-01-10 14:32:00",
  "pickup_longitude":   -73.982,
  "pickup_latitude":    40.764,
  "dropoff_longitude":  -73.991,
  "dropoff_latitude":   40.750,
  "passenger_count":    2,
  "fare_amount":        12.50
}
```

**Technical details:**
- **Serialization:** JSON → UTF-8 bytes via `value_serializer=lambda v: json.dumps(v).encode('utf-8')`
- **Message key:** `hash(tuple(row))` — ensures identical records go to the same Kafka partition
- **Retry:** 3 attempts with 100ms backoff on send failure
- **Reconnection:** Loop of up to 10 connection attempts to Kafka with 5s wait between each — necessary because Kafka takes ~20-30s to be ready after `docker compose up`
- **Real throughput:** ~10 events/second (0.1s interval), configurable via `PUBLISH_INTERVAL`

**Environment variables:**
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Kafka broker address |
| `KAFKA_TOPIC` | taxi-rides | Topic name |
| `DATA_PATH` | /data/train.csv | CSV file path |
| `PUBLISH_INTERVAL` | 0.1 | Seconds between publications |

---

### 2️⃣ KAFKA (`confluentinc/cp-kafka:7.4.0`)

**What it does:** Acts as the backbone of the streaming pipeline — receives events from the Producer and keeps them available for consumption by the Consumer.

**Why Kafka and not a simple queue (RabbitMQ, SQS)?**
- **Replay:** Kafka stores messages on disk. If the Consumer crashes, it resumes from where it left off using `auto_offset_reset="earliest"`
- **Ordering:** Messages within a partition are guaranteed to be ordered
- **Scalability:** Can have multiple consumers reading in parallel via consumer groups
- **Decoupling:** Producer and Consumer are completely independent — one can stop without affecting the other

**Relevant docker-compose configuration:**
```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"    # auto-creates the taxi-rides topic
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1  # single-broker environment (development)
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092  # internal Docker address
KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE: "false"     # disables telemetry
```

---

### 3️⃣ CONSUMER (`consumer/streaming_consumer.py`)

**What it does:** Consumes events from Kafka in batches of 100 records, serializes them to JSONL (JSON Lines) format, and uploads to MinIO — creating the raw layer of the Data Lake.

**How it works, step by step:**

```
Kafka (topic taxi-rides)
       │
       ▼
KafkaConsumer (group_id="minio-raw-consumer")
  • auto_offset_reset="earliest"  → reprocesses from the start if restarted
  • enable_auto_commit=True       → automatically commits offset
  • consumer_timeout_ms=5000      → exits inner loop after 5s without messages
       │
       ▼
Infinite loop (while True):
  ├─ Accumulates messages in batch[]
  ├─ When len(batch) >= 100:
  │     ├─ Generates path: raw/ingestion_date=2026-03-18/20260318_155514123456.jsonl
  │     ├─ Serializes: "\n".join(json.dumps(r) for r in records)
  │     ├─ BytesIO(jsonl_bytes) → in-memory buffer (no temp file)
  │     ├─ s3.upload_fileobj(buffer, bucket, key)
  │     └─ batch = []  ← clears for next batch
  └─ On exception:
        ├─ Attempts to save partial batch to MinIO (no data loss)
        ├─ batch = []
        └─ time.sleep(3) and restarts the loop
```

**JSONL file format in MinIO:**
```
{"pickup_datetime": "2015-01-10 14:32:00", "pickup_longitude": -73.982, ...}
{"pickup_datetime": "2015-01-10 14:33:15", "pickup_longitude": -73.991, ...}
... (100 lines per file)
```

**MinIO directory structure:**
```
nyc-taxi/
└── raw/
    └── ingestion_date=2026-03-18/
        ├── 20260318_155514123456.jsonl   (100 records)
        ├── 20260318_155524786321.jsonl   (100 records)
        └── ...  (~500 files for 50K records)
```

**Why JSONL and not CSV or Parquet?**
- JSONL is schema-flexible: each line is a valid, independent JSON object
- Easy to read line-by-line without loading the entire file into memory
- Human-readable for debugging
- Natively compatible with `json.loads()` with no extra dependencies

**Why `BytesIO` instead of a temp file?**
- Avoids disk I/O — the buffer lives entirely in RAM
- Faster and no risk of temp file accumulation in the container

**Environment variables:**
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | kafka:9092 | Broker address |
| `KAFKA_TOPIC` | taxi-rides | Topic to consume |
| `BATCH_SIZE` | 100 | Records per JSONL file |
| `MINIO_ENDPOINT` | http://minio:9000 | MinIO API URL |
| `MINIO_BUCKET` | nyc-taxi | Destination bucket |

---

### 4️⃣ MINIO (Data Lake)

**What it does:** S3-compatible object storage that receives JSONL files from the Consumer and makes them available for Airflow to process.

**Why MinIO as an intermediate layer (instead of inserting directly into Postgres)?**

| With MinIO (current architecture) | Without MinIO (direct insert) |
|---|---|
| Consumer and Airflow completely decoupled | Consumer coupled to the database |
| Raw data preserved indefinitely | Raw data overwritten on each run |
| Reprocessing possible at any time | Impossible to reprocess without Kafka |
| `source_file` tracks exactly which file originated each record | No source traceability |
| Consumer can scale without affecting the database | Database bottleneck affects streaming |
| Faithfully simulates real cloud architecture (S3, GCS, ADLS) | Overly simplified architecture |

**Access:**
- S3 API: `http://localhost:9000` (used by internal services via boto3)
- Web Console: `http://localhost:9001` (visual interface to browse files)
- Credentials: `minioadmin / minioadmin`

---

### 5️⃣ AIRFLOW DAG (`dags/taxi_pipeline_dag.py`)

**What it does:** Orchestrates the entire data flow between MinIO and PostgreSQL, ensuring dbt transformations only run when valid data is available.

**DAG configuration:**
```python
schedule_interval = "*/10 * * * *"   # every 10 minutes
start_date        = datetime(2026, 1, 1)
catchup           = False             # does not reprocess past runs
retries           = 2                 # retries 2x before marking as failed
retry_delay       = timedelta(minutes=3)
execution_timeout = timedelta(minutes=30)
```

**Task breakdown:**

#### Task 1: `check_minio_raw` — BranchPythonOperator
```
Purpose: Decide whether the pipeline should run or skip this cycle

Logic:
  1. Creates boto3 client for MinIO
  2. s3.head_bucket() → if bucket does not exist:
       → s3.create_bucket()
       → return "skip_pipeline"
  3. s3.list_objects_v2(MaxKeys=1) → if no files:
       → return "skip_pipeline"
  4. If files exist:
       → paginator counts total .jsonl files
       → xcom_push(key="raw_file_count", value=total)
       → return "load_minio_to_staging"

The BranchPythonOperator uses the return value (string with task_id)
to decide which task to execute next.
```

#### Task 2: `skip_pipeline` — EmptyOperator
```
Purpose: Clean branch endpoint when there is no data
Executes no logic — just signals that the run was intentionally skipped.
Without this, Airflow would mark downstream tasks as "skipped" in a dirty way.
```

#### Task 3: `load_minio_to_staging` — PythonOperator
```
Purpose: Transfer all data from MinIO to PostgreSQL

Detailed logic:
  1. psycopg2.connect(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
  2. CREATE SCHEMA IF NOT EXISTS raw
  3. CREATE TABLE IF NOT EXISTS raw.taxi_rides_staging (20 columns)
     → idempotent: does not fail if it already exists
  4. TRUNCATE TABLE raw.taxi_rides_staging
     → "full refresh" strategy: each run reloads everything from MinIO
     → MinIO is the source of truth; staging is always a faithful copy
  5. For each paginator page (lists all .jsonl in MinIO):
     For each .jsonl file:
       a. s3.get_object() → downloads content
       b. UTF-8 decode → splitlines()
       c. json.loads() per line → list of dicts
       d. Builds list of tuples with explicit casting:
          _int(v): tries int(), returns None on failure
          _float(v): tries float(), returns None on failure
       e. execute_values(cur, INSERT..., rows, page_size=500)
          → batch insert of 500 rows per call (performance)
       f. conn.commit() per file
  6. conn.close()
  7. xcom_push(key="staged_records", value=total_records)

Inserted fields (20 columns):
  source_file, vendor_id, pickup_datetime, dropoff_datetime,
  passenger_count, trip_distance, pickup_longitude, pickup_latitude,
  rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude,
  payment_type, fare_amount, extra, mta_tax, tip_amount,
  tolls_amount, improvement_surcharge, total_amount
```

**About `source_file`:** every record in staging knows exactly which JSONL file it came from. Given any record in the final warehouse, you can trace it back to the raw file in MinIO and to the original Kafka message — full lineage.

#### Task 4: `validate_staging` — PythonOperator
```
Purpose: Quality gate before running costly transformations

Logic:
  SELECT COUNT(*) FROM raw.taxi_rides_staging
  If count == 0 → raise ValueError("staging is empty") → task fails
                → dbt_run and dbt_test are NOT executed
  If count > 0  → logs the count and returns successfully
```

#### Task 5: `dbt_run` — BashOperator
```
Purpose: Execute all dbt transformations

Command executed:
  /home/airflow/.local/bin/dbt run \
    --profiles-dir /opt/airflow/dbt \
    --project-dir /opt/airflow/dbt

Why use the full path (/home/airflow/.local/bin/dbt)?
  The BashOperator does not load the airflow user's PATH.
  dbt is installed in ~/.local/bin, which is not in bash's PATH.
  Using the absolute path resolves this without modifying the system PATH.

Injected environment variables:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
  → dbt reads them via env_var() in profiles.yml
```

#### Task 6: `dbt_test` — BashOperator
```
Purpose: Validate data quality after transformation

Runs all tests defined in schema.yml:
  - ride_id uniqueness (no duplicates)
  - Non-nullability of critical fields
  - Valid ranges for geographic coordinates
  - Referential integrity between models
```

**Dependency graph:**
```
check_minio_raw ──┬──▶ skip_pipeline
                  └──▶ load_minio_to_staging ──▶ validate_staging ──▶ dbt_run ──▶ dbt_test
```

---

### 6️⃣ DBT — RAW LAYER (`models/base/base_taxi_rides.sql`)

**What it does:** Creates a VIEW on top of the physical table `raw.taxi_rides_staging`, serving as a contract between raw storage and subsequent transformations.

```sql
{{ config(
    materialized='view',
    schema='raw'   -- results in schema: dbt_dev_silver_raw
) }}

select
    pickup_datetime,         -- TEXT (comes this way from staging)
    pickup_longitude,        -- FLOAT
    pickup_latitude,         -- FLOAT
    dropoff_longitude,       -- FLOAT
    dropoff_latitude,        -- FLOAT
    passenger_count,         -- INTEGER
    fare_amount,             -- FLOAT
    ingestion_timestamp,     -- TIMESTAMPTZ
    current_timestamp as dbt_loaded_at  -- when dbt read this data
from raw.taxi_rides_staging
```

**Why a VIEW and not a table?**
- A VIEW stores no data — it is a saved query that executes when called
- Zero storage cost: does not duplicate data
- Always reflects the current state of staging (no refresh needed)
- Abstraction layer: if the table changes name or location, only this file needs updating

**How dbt resolves the schema name:**
`profiles.yml` defines default schema: `dbt_dev_silver`
Model defines `schema='raw'`
dbt concatenates: `dbt_dev_silver` + `_` + `raw` = **`dbt_dev_silver_raw`**

Final schema: `dbt_dev_silver_raw.base_taxi_rides`

---

### 7️⃣ DBT — SILVER LAYER (`models/staging/stg_taxi_rides.sql`)

**What it does:** The highest-value layer in the pipeline — transforms raw, unreliable data into clean, correctly typed data enriched with business metrics.

**Transformations applied in detail:**

**1. Unique ride ID generation (ride_id):**
```sql
md5(
    coalesce(pickup_datetime, '') || '-' ||
    coalesce(cast(pickup_longitude as varchar), '') || '-' ||
    coalesce(cast(pickup_latitude as varchar), '') || '-' ||
    coalesce(cast(passenger_count as varchar), '')
) as ride_id
```
Combines 4 identifier fields and applies MD5 → 32-character hex hash.
`coalesce(..., '')` guards against NULLs: without it, any NULL in the concatenation would produce NULL as the result, and the MD5 of NULL is NULL — breaking uniqueness.

**2. Type correction (TEXT → TIMESTAMP cast):**
```sql
cast(pickup_datetime as timestamp)::date          as pickup_date
extract(hour from cast(pickup_datetime as timestamp)) as pickup_hour
extract(dow  from cast(pickup_datetime as timestamp)) as pickup_day_of_week
```
`pickup_datetime` arrives as TEXT from staging (inserted that way by Airflow).
PostgreSQL cannot apply `extract()` on TEXT — it requires an explicit TIMESTAMP.
`extract(dow)` returns 0=Sunday, 1=Monday, ..., 6=Saturday.

**3. Validation and cleaning (no record loss):**
```sql
case when passenger_count <= 0 then null else passenger_count end as passenger_count_clean
case when fare_amount      <= 0 then null else fare_amount      end as fare_amount_clean
```
Records with impossible values have the field nullified instead of deleting the row.
This preserves the record for auditing and allows counting how many values were invalid.

**4. Approximate geographic distance calculation:**
```sql
round(cast(
    111.0 * sqrt(
        power(pickup_latitude - dropoff_latitude, 2) +
        power((pickup_longitude - dropoff_longitude) * cos(radians(pickup_latitude)), 2)
    ) as numeric
), 2) as trip_distance_km
```
Euclidean formula on geographic coordinates:
- `111.0` km = 1 degree of latitude (geographic constant)
- `cos(radians(latitude))` corrects longitudinal distortion (longitudes get closer at higher latitudes)
- Rounded to 2 decimal places
- No PostGIS required — no extra dependencies

**5. NYC borough classification via bounding boxes:**
```sql
case
    when pickup_longitude between -74.02 and -73.93
     and pickup_latitude  between 40.70  and 40.82  then 'Manhattan'
    when pickup_longitude between -74.03 and -73.92
     and pickup_latitude  between 40.58  and 40.70  then 'Brooklyn'
    when pickup_longitude between -73.93 and -73.83
     and pickup_latitude  between 40.79  and 40.87  then 'Bronx'
    when pickup_longitude between -73.97 and -73.82
     and pickup_latitude  between 40.65  and 40.80  then 'Queens'
    when pickup_longitude between -74.26 and -74.05
     and pickup_latitude  between 40.51  and 40.65  then 'Staten Island'
    else 'Unknown'
end as borough_approximation
```
Each borough has a manually defined bounding box.
Simple, efficient, no need for joins with geospatial tables.

**6. Geographic filter in the WHERE clause:**
```sql
where location_validation = 'valid'
-- pickup inside bbox: lon [-74.3, -73.7] × lat [40.5, 40.9]
```
Removes rides with GPS coordinates outside New York (corrupted, simulated, or collection errors).
This explains why `fct_rides` has ~783 records despite ~50K in staging.

**Resulting schema:** `dbt_dev_silver_silver.stg_taxi_rides`

---

### 8️⃣ DBT — GOLD LAYER: `fct_rides` (Fact Table)

**What it does:** The main fact table — consolidates all valid rides into a physical materialized table, optimized for analytical queries.

**Columns and metrics:**

| Column | Type | Description |
|---|---|---|
| `ride_id` | TEXT | Unique MD5 ID |
| `pickup_datetime` | TEXT | Original date/time |
| `pickup_date` | DATE | Extracted date |
| `pickup_hour` | FLOAT | Hour of day (0-23) |
| `pickup_day_of_week` | FLOAT | Day of week (0=Sun) |
| `borough_approximation` | TEXT | Origin borough |
| `pickup/dropoff_longitude/latitude` | FLOAT | Coordinates |
| `passenger_count` | INTEGER | Passengers (validated) |
| `fare_amount` | FLOAT | Fare (validated) |
| `trip_distance_km` | NUMERIC | Calculated distance |
| `trip_duration_minutes` | INTEGER | 15 min (estimated) |
| `implied_speed_kmh` | NUMERIC | `distance / duration * 60` |
| `fare_per_km` | NUMERIC | `fare_amount / trip_distance_km` |
| `ingestion_timestamp` | TIMESTAMPTZ | When it entered MinIO |
| `dbt_processed_at` | TIMESTAMPTZ | When processed in silver |
| `dbt_gold_at` | TIMESTAMPTZ | When it entered gold |

**Materialization:** TABLE (physical data stored, not recalculated on each query).

**Schema:** `dbt_dev_silver_gold.fct_rides`

---

### 9️⃣ DBT — GOLD LAYER: `dim_locations` (Dimension Table)

**What it does:** Dimension table with all unique pickup and dropoff locations, enriched with urban zone classification.

**Deduplication logic:**
```
pickup_locations  (longitude, latitude, 'pickup')
        +
dropoff_locations (longitude, latitude, 'dropoff')
        │
        ▼ UNION ALL → all_locations
        │
        ▼ ROW_NUMBER() OVER (PARTITION BY longitude, latitude ORDER BY location_type)
          → numbers each occurrence of each coordinate
        │
        ▼ WHERE rn = 1
          → keeps only 1 record per unique coordinate pair
        │
        ▼ + md5(longitude || '-' || latitude) as location_id
          + zone classification (Manhattan South/Midtown/North, Brooklyn, Bronx, Queens)
```

**Schema:** `dbt_dev_silver_gold.dim_locations`

---

### 🔟 DBT — GOLD LAYER: `agg_daily_metrics` (Aggregation Table)

**What it does:** Aggregates all rides by date, computing business metrics ready for dashboards and executive reports.

**Metrics per `pickup_date`:**

| Column | Calculation | Meaning |
|---|---|---|
| `total_rides` | COUNT(DISTINCT ride_id) | Unique trips on the day |
| `total_passengers` | COUNT(DISTINCT passenger_count) | Occupancy variation |
| `total_fare_revenue` | SUM(fare_amount) | Total revenue for the day |
| `avg_fare_amount` | AVG(fare_amount) | Average fare |
| `min/max_fare_amount` | MIN/MAX | Fare range |
| `avg_trip_distance_km` | AVG(trip_distance_km) | Average distance |
| `total_trip_distance_km` | SUM(trip_distance_km) | Total km driven |
| `avg_passengers_per_ride` | AVG(passenger_count) | Average occupancy |
| `avg_fare_per_km` | SUM(fare)/SUM(dist) | Pricing efficiency |

**Schema:** `dbt_dev_silver_gold.agg_daily_metrics`

---

### 1️⃣1️⃣ DBT — CONFIGURATION (`dbt_project.yml` and `profiles.yml`)

**`profiles.yml` — Database connection:**
```yaml
taxi_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host:     "{{ env_var('DB_HOST', 'postgres-dbt') }}"
      port:     "{{ env_var('DB_PORT', '5432') | int }}"
      dbname:   "{{ env_var('DB_NAME', 'analytics_warehouse') }}"
      user:     "{{ env_var('DB_USER', 'dbt_user') }}"
      password: "{{ env_var('DB_PASSWORD', 'dbt_password') }}"
      schema:   dbt_dev_silver   ← default schema (prefix for all models)
      threads:  4                ← up to 4 models in parallel
```

dbt's `env_var()` works like `os.getenv()` — reads environment variables with a fallback. This allows the same `profiles.yml` to work both in the standalone `dbt` container and inside Airflow.

**`dbt_project.yml` — Materialization per layer:**
```yaml
models:
  taxi_pipeline:
    staging:                    # folder models/staging/
      +materialized: view       # silver = views (zero storage, always current)
      +schema: dbt_dev_silver
    marts:                      # folder models/marts/
      +materialized: table      # gold = physical tables (fast queries)
      +schema: dbt_dev_gold
```

**How dbt builds schema names:**
dbt uses the schema from `profiles.yml` as a prefix and concatenates it with the `+schema` from `dbt_project.yml`:

| Layer | profiles schema | + model schema | = final schema |
|---|---|---|---|
| Base | dbt_dev_silver | raw | dbt_dev_silver_raw |
| Silver | dbt_dev_silver | dbt_dev_silver | dbt_dev_silver_silver |
| Gold | dbt_dev_silver | dbt_dev_gold | dbt_dev_silver_gold |

**Available vars in dbt_project.yml:**
```yaml
vars:
  minio_endpoint:    "http://minio:9000"
  minio_bucket:      "nyc-taxi"
  raw_prefix:        "raw"
  filter_start_date: '2015-01-01'   # configurable business filters
  filter_end_date:   '2015-01-31'
  filter_min_lon:    -74.05         # NYC bounding box
  filter_max_lon:    -73.75
  filter_min_lat:     40.58
  filter_max_lat:     40.90
```

---

## 🔄 Complete Data Flow — Step by Step

```
STEP 1 — CSV → Kafka
  train.csv (50K rows)
  Producer reads in chunks of 10,000 via pandas
  Serializes each row as JSON → UTF-8 bytes
  producer.send("taxi-rides", key=hash(row), value=json_bytes)
  Waits 100ms between each send
  Rate: ~10 events/second

STEP 2 — Kafka → MinIO
  Consumer in continuous loop consuming "taxi-rides"
  Accumulates messages in internal buffer (Python list)
  When buffer reaches 100 msgs:
    → serializes to JSONL (1 JSON per line, separated by \n)
    → generates path: raw/ingestion_date=YYYY-MM-DD/TIMESTAMP.jsonl
    → BytesIO(jsonl_bytes) → upload via boto3 S3-compatible
    → buffer = []
  Result: ~500 files for 50K records (~15KB each)

STEP 3 — Airflow (every 10 min)
  check_minio_raw:
    → boto3 lists objects in nyc-taxi/raw/
    → if empty: skip_pipeline (run ends without error)
    → if files exist: load_minio_to_staging

STEP 4 — MinIO → PostgreSQL (raw)
  load_minio_to_staging:
    → TRUNCATE raw.taxi_rides_staging (full refresh)
    → For each .jsonl in MinIO:
        download → parse JSON → cast types → execute_values (batch 500)
    → Result: ~50,000 rows with traceable source_file

STEP 5 — Validation
  validate_staging:
    → SELECT COUNT(*) FROM raw.taxi_rides_staging
    → If 0: raise ValueError → pipeline stops
    → If > 0: continues

STEP 6 — dbt run
  base_taxi_rides: VIEW on top of staging (0.01s)
  stg_taxi_rides:  VIEW with cleaning + casts + enrichment (0.05s)
  fct_rides:       Materialized TABLE (0.1s)
  dim_locations:   TABLE (0.05s)
  agg_daily_metrics: TABLE (0.05s)
  Total: ~0.2-0.5s for 5 models

STEP 7 — dbt test
  Validates uniqueness, nulls, ranges, integrity
  If it fails: alert in Airflow, no impact on already-created tables

FINAL RESULT
  raw.taxi_rides_staging:              ~50,000 rows
  dbt_dev_silver_silver.stg_taxi_rides: ~783 rows (after geo filter)
  dbt_dev_silver_gold.fct_rides:        ~783 rows
  dbt_dev_silver_gold.dim_locations:    ~1,557 rows
  dbt_dev_silver_gold.agg_daily_metrics: ~649 rows
  Query latency: < 100ms
```

---

## 📐 Architectural Decisions — Full Justifications

| Decision | Alternative | Reason |
|---|---|---|
| **Kafka** | RabbitMQ, Redis | Replay, guaranteed ordering, consumer groups |
| **MinIO** as intermediate | Direct insert to PG | Decoupling, raw data preserved, reprocessability |
| **PostgreSQL** | DuckDB | ACID, multi-user, production-ready |
| **dbt** | Python/Pandas | Declarative SQL, automatic tests, lineage |
| **Airflow** | Cron | Dependency graph, retry, SLAs, visual history |
| **Docker Compose** | Kubernetes | Simplicity for local development |
| **Full refresh** staging | Incremental | MinIO is source of truth; simplicity > complexity |
| **JSONL** in Data Lake | Parquet, CSV | Schema-flexible, readable, no dependencies |
| **BranchPythonOperator** | Sensor | Flow control without keeping a worker waiting |
| **execute_values** | executemany | 10-50x faster for batch inserts in psycopg2 |

---

## 📊 Full PostgreSQL Schema

```
analytics_warehouse
│
├── raw                                 (physical schema)
│   └── taxi_rides_staging              TABLE
│       ├── source_file                 TEXT        which JSONL file it came from
│       ├── vendor_id                   TEXT
│       ├── pickup_datetime             TEXT        string, cast in dbt
│       ├── dropoff_datetime            TEXT
│       ├── passenger_count             INTEGER
│       ├── trip_distance               FLOAT
│       ├── pickup_longitude            FLOAT
│       ├── pickup_latitude             FLOAT
│       ├── rate_code_id                TEXT
│       ├── store_and_fwd_flag          TEXT
│       ├── dropoff_longitude           FLOAT
│       ├── dropoff_latitude            FLOAT
│       ├── payment_type                TEXT
│       ├── fare_amount                 FLOAT
│       ├── extra                       FLOAT
│       ├── mta_tax                     FLOAT
│       ├── tip_amount                  FLOAT
│       ├── tolls_amount                FLOAT
│       ├── improvement_surcharge       FLOAT
│       ├── total_amount                FLOAT
│       └── ingestion_timestamp         TIMESTAMPTZ DEFAULT NOW()
│
├── dbt_dev_silver_raw                  (dbt schema)
│   └── base_taxi_rides                 VIEW → SELECT * FROM raw.taxi_rides_staging
│
├── dbt_dev_silver_silver               (dbt schema)
│   └── stg_taxi_rides                  VIEW → cleaned and enriched data
│
└── dbt_dev_silver_gold                 (dbt schema)
    ├── fct_rides                       TABLE → main fact table
    ├── dim_locations                   TABLE → unique locations
    └── agg_daily_metrics               TABLE → daily metrics
```

---

## 🚀 How to Run

### 1. Start everything
```bash
git clone <repository-url>
cd taxi_pipeline
docker compose up -d
sleep 30 && docker compose ps
```

### 2. Monitor ingestion
```bash
docker compose logs -f consumer
# ✅ Upload: s3://nyc-taxi/raw/ingestion_date=2026-03-18/20260318_155514.jsonl (100 records, 14832 bytes)
```

### 3. Check MinIO
```
http://localhost:9001   →   minioadmin / minioadmin
Bucket: nyc-taxi → raw/
```

### 4. Trigger DAG
```
http://localhost:8082   →   airflow / airflow
DAG: taxi_pipeline → ▶ button (Trigger DAG)
```

### 5. Validate layers
```bash
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS raw_records FROM raw.taxi_rides_staging;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS silver_rides FROM dbt_dev_silver_silver.stg_taxi_rides;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS fct_rides FROM dbt_dev_silver_gold.fct_rides;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS dim_locations FROM dbt_dev_silver_gold.dim_locations;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS daily_metrics FROM dbt_dev_silver_gold.agg_daily_metrics;"
```

### 6. Analytical query
```bash
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT borough_approximation, COUNT(*) AS rides, ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare FROM dbt_dev_silver_gold.fct_rides GROUP BY borough_approximation ORDER BY rides DESC;"
```

---

## 🔍 Troubleshooting

### View Airflow task log
```bash
docker exec -it $(docker ps --filter "name=airflow-scheduler" --format "{{.ID}}") \
  ls /opt/airflow/logs/dag_id=taxi_pipeline/

docker exec -it $(docker ps --filter "name=airflow-scheduler" --format "{{.ID}}") \
  cat "/opt/airflow/logs/dag_id=taxi_pipeline/run_id=<RUN_ID>/task_id=<TASK>/attempt=1.log"
```

### Recreate Postgres (corrupted schema)
```bash
docker compose stop postgres-dbt && docker compose rm -f postgres-dbt
docker volume rm taxi_pipeline_postgres-dbt-data
docker compose up -d postgres-dbt
```

### Rebuild Airflow (Dockerfile change)
```bash
docker compose stop airflow-webserver airflow-scheduler
docker compose rm -f airflow-webserver airflow-scheduler
docker compose build --no-cache airflow-webserver airflow-scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

### Full reset
```bash
docker compose down
docker volume rm taxi_pipeline_postgres-dbt-data taxi_pipeline_minio-data taxi_pipeline_postgres-airflow-data
docker compose up -d
```

---

## 🔐 Credentials & Ports

| Service | URL / Port | User | Password |
|---|---|---|---|
| Airflow UI | http://localhost:8082 | airflow | airflow |
| MinIO Console | http://localhost:9001 | minioadmin | minioadmin |
| MinIO API | http://localhost:9000 | minioadmin | minioadmin |
| PostgreSQL analytics | localhost:5433 | dbt_user | dbt_password |
| PostgreSQL airflow | localhost:5432 | airflow | airflow |
| Kafka | localhost:9092 | — | — |
| Zookeeper | localhost:2181 | — | — |

---

## 📊 Performance Metrics

| Metric | Observed value |
|---|---|
| Producer → Kafka rate | ~10 events/second |
| Consumer → MinIO throughput | 100 records/batch, ~1-2s/batch |
| MinIO → Staging load (50K records) | ~2-5 seconds |
| dbt run (5 models) | ~0.2-0.5 seconds |
| Gold layer query | < 100ms |
| Total memory footprint | < 2GB |
| Records after geo filter | ~783 out of ~50K (NYC bbox) |

---

## 📝 Version History

### v2.0 — 03/18/2026
- ✅ MinIO added as intermediate Data Lake (Kafka → MinIO → PG)
- ✅ Consumer rewritten: saves JSONL to MinIO instead of inserting directly to PG
- ✅ Airflow DAG with 6 tasks + branch logic (skip when MinIO is empty)
- ✅ Resolved Docker-in-Docker: dbt installed directly in the Airflow container
- ✅ Resolved PATH: BashOperator uses `/home/airflow/.local/bin/dbt`
- ✅ Fixed `init.sql`: staging schema aligned with the DAG
- ✅ Fixed TEXT→TIMESTAMP cast in `stg_taxi_rides.sql`
- ✅ Fixed NULL concatenation in `md5()` for ride_id
- ✅ PASS=5 ERROR=0 SKIP=0 across all dbt models

### v1.0
- Initial pipeline: Kafka → Consumer → PostgreSQL directly
- dbt with DuckDB as analytics engine

---

**Last updated**: March 18, 2026
**Status**: ✅ Operational — End-to-end pipeline working | PASS=5 dbt models