# 🚕 NYC Taxi Data Pipeline

**Pipeline de dados em tempo real: Kafka → MinIO → PostgreSQL → dbt**

---

## 🏗️ Arquitetura

```
┌────────────────────────────────────────────────────────────────────┐
│                         CAMADA DE INGESTÃO                         │
│                                                                    │
│  ┌──────────────┐         ┌──────────────┐       ┌───────────┐     │
│  │ Dataset CSV  │────────▶│  Producer    │──────▶│  Kafka    │     │
│  │ (50K regs)   │         │ (Python)     │       │ taxi-rides│     │
│  └──────────────┘         └──────────────┘       └───────────┘     │
│                                                       ▼            │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│                  CAMADA DE PROCESSAMENTO STREAMING                 │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  Consumer (kafka-python)                                 │      │
│  │  • Consome batches de 100 records do Kafka               │      │
│  │  • Serializa para JSONL e salva no MinIO (raw/)          │      │
│  └──────────────────────────────────────────────────────────┘      │
│                         ▼                                          │
│  ┌──────────────────────────────────────────────────────────┐      │
│  │  MinIO (Data Lake — S3-compatible)                       │      │
│  │  └─ nyc-taxi/raw/*.jsonl                                 │      │
│  └──────────────────────────────────────────────────────────┘      │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│                     ORQUESTRAÇÃO (AIRFLOW)                         │
│                                                                    │
│  DAG: taxi_pipeline (a cada 10 minutos)                            │
│  ├─ check_minio_raw      → Verifica arquivos JSONL no MinIO        │
│  ├─ load_minio_to_staging → MinIO → raw.taxi_rides_staging (PG)    │
│  ├─ validate_staging      → Garante dados antes do dbt             │
│  ├─ dbt_run               → Executa modelos silver + gold          │
│  └─ dbt_test              → Testes de qualidade de dados           │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────────┐
│              CAMADA DE TRANSFORMAÇÃO (DBT + PostgreSQL)            │
│                                                                    │
│  Raw Layer     → base_taxi_rides  (view sobre raw.taxi_rides_staging)
│      ▼                                                             │
│  Silver Layer  → stg_taxi_rides   (limpeza, validação, enriquecimento)
│      ▼                                                             │
│  Gold Layer    → fct_rides        (fact table)                     │
│                  dim_locations    (dimensão de locais)             │
│                  agg_daily_metrics (agregações diárias)            │
└────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Tech Stack

| Componente | Tecnologia | Função |
|---|---|---|
| **Mensageria** | Apache Kafka 7.4.0 | Streaming de eventos |
| **Data Lake** | MinIO (S3-compatible) | Armazenamento raw (JSONL) |
| **Warehouse** | PostgreSQL 14 | Analytics warehouse (mais fácil de configurar localmente que o DuckDB)|
| **Transformação** | dbt 1.7.0 | ELT declarativo (mais fácil de configurar localmente que Spark) |
| **Orquestração** | Apache Airflow 2.7.0 | DAGs e scheduling |
| **Infraestrutura** | Docker Compose | Ambiente containerizado |

---

## 🚀 Como Executar

### Pré-requisitos
```bash
docker --version          # >= 20.10
docker compose version    # >= 2.0
```

Baixar os dados em: https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data (arquivo train.csv)

### 1. Subir a infraestrutura
```bash
git clone <repository-url>
cd taxi_pipeline
docker compose up -d
sleep 30 && docker compose ps
```

### 2. Validar fluxo de dados
```bash
# Ver dados chegando no MinIO
docker compose logs -f consumer

# Ver logs do consumer (deve mostrar uploads)
docker-compose logs -f consumer --tail=20

# Conferir staging table
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) FROM raw.taxi_rides_staging;"
```

### 3. Rodar transformações (sem Airflow)
```bash
docker compose run --rm dbt dbt run
# Esperado: PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```

### 4. Acessar Airflow
Cadastrar usuário de serviço hard-coded (evita erro de loop no airflow-init)

```bash
docker-compose exec airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
````

```
URL:  http://localhost:8082
User: admin
Pass: admin
```

### 5. Consultar dados analíticos
```bash
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT borough_approximation, COUNT(*) AS rides, ROUND(AVG(fare_amount)::numeric, 2) AS avg_fare FROM dbt_dev_silver_gold.fct_rides GROUP BY borough_approximation ORDER BY rides DESC;"
```

---

## 📁 Estrutura do Projeto

```
taxi_pipeline/
├── producer/                   # CSV → Kafka
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/                   # Kafka → MinIO (JSONL)
│   ├── streaming_consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/                        # Transformações ELT
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── base/               # Raw layer
│       │   └── base_taxi_rides.sql
│       ├── staging/            # Silver layer
│       │   └── stg_taxi_rides.sql
│       └── marts/              # Gold layer
│           ├── fct_rides.sql
│           ├── dim_locations.sql
│           └── agg_daily_metrics.sql
├── dags/
│   └── taxi_pipeline_dag.py    # Airflow DAG
├── postgres/
│   └── init.sql                # Schema inicial
├── data/
│   └── train.csv               # 50K registros NYC Taxi
├── Dockerfile.airflow
└── docker-compose.yml
```

---

## 🔍 Validação das Camadas

```bash
# Raw
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS raw_registros FROM raw.taxi_rides_staging;"

# Silver
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS silver_rides FROM dbt_dev_silver_silver.stg_taxi_rides;"

# Gold
docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS fct_rides FROM dbt_dev_silver_gold.fct_rides;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS dim_locations FROM dbt_dev_silver_gold.dim_locations;"

docker compose exec postgres-dbt psql -U dbt_user -d analytics_warehouse \
  -c "SELECT COUNT(*) AS daily_metrics FROM dbt_dev_silver_gold.agg_daily_metrics;"
```

---

## 🔐 Credenciais & Portas

| Serviço | URL / Porta | Usuário | Senha |
|---|---|---|---|
| Airflow UI | http://localhost:8082 | airflow | airflow |
| MinIO Console | http://localhost:9001 | minioadmin | minioadmin |
| PostgreSQL (analytics) | localhost:5433 | dbt_user | dbt_password |
| PostgreSQL (airflow) | localhost:5432 | airflow | airflow |
| Kafka | localhost:9092 | — | — |

---
