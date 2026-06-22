# 🎧 Spotify Streaming Data Pipeline

A production-grade, end-to-end streaming data pipeline that simulates Spotify user listening events, streams them through Apache Kafka, stores raw data in AWS S3, loads into Snowflake, and transforms through a medallion architecture using dbt — all orchestrated by Apache Airflow.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS_S3-FF9900?logo=amazons3&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

---

## 📐 Architecture

```
┌─────────────┐     ┌───────────┐     ┌──────────┐     ┌───────────┐
│   Producer   │────▶│   Kafka   │────▶│ Consumer │────▶│  AWS S3   │
│ (Simulator)  │     │  Broker   │     │(Micro-   │     │ (Bronze)  │
└─────────────┘     └───────────┘     │  batch)  │     └─────┬─────┘
                                      └──────────┘           │
                                                             │ @hourly
                                                    ┌────────▼────────┐
                                                    │  Airflow DAG    │
                                                    │  (Extract+Load) │
                                                    └────────┬────────┘
                                                             │
                                                    ┌────────▼────────┐
                                                    │   Snowflake     │
                                                    │  ┌───────────┐  │
                                                    │  │  Bronze   │  │
                                                    │  │ RAW_EVENTS│  │
                                                    │  └─────┬─────┘  │
                                                    │   dbt  │  run   │
                                                    │  ┌─────▼─────┐  │
                                                    │  │  Silver   │  │
                                                    │  │clean_data │  │
                                                    │  └─────┬─────┘  │
                                                    │   dbt  │  run   │
                                                    │  ┌─────▼─────┐  │
                                                    │  │   Gold    │  │
                                                    │  │top_songs  │  │
                                                    │  │engagement │  │
                                                    │  └───────────┘  │
                                                    └─────────────────┘
```

### Data Flow

| Layer | Storage | Description |
|-------|---------|-------------|
| **Ingestion** | Kafka → S3 | Real-time events micro-batched as NDJSON files |
| **Bronze** | Snowflake `BRONZE.RAW_EVENTS` | Raw events loaded hourly via Airflow |
| **Silver** | Snowflake view `clean_data` | Cleaned, typed, deduplicated events |
| **Gold** | Snowflake tables `top_songs`, `user_engagement` | Business-ready aggregations |

---

## 🗂️ Project Structure

```
spotify-data-pipeline/
├── src/                          # Python source code
│   ├── config.py                 # Centralized configuration
│   ├── producer/                 # Kafka event producer
│   │   ├── main.py               # Producer entry point
│   │   ├── event_generator.py    # Pure event generation logic
│   │   └── fixtures/songs.json   # Externalized song catalog
│   └── consumer/                 # Kafka → S3 consumer
│       └── main.py               # Consumer entry point
├── dags/                         # Airflow DAG definitions
│   └── bronze_ingestion_dag.py   # S3 → Snowflake ETL
├── spotify_dbt/                  # dbt transformation project
│   ├── models/
│   │   ├── sources.yml           # Source definitions + tests
│   │   ├── silver/               # Staging transformations
│   │   └── gold/                 # Business aggregations
│   └── dbt_project.yml
├── tests/                        # Python test suite
├── .github/workflows/ci.yml     # CI/CD pipeline
├── docker-compose.yml            # Infrastructure services
├── pyproject.toml                # Project config + tooling
├── Makefile                      # Developer commands
└── .env.example                  # Environment template
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose
- AWS account with S3 access
- Snowflake account

### 1. Clone & Configure

```bash
git clone https://github.com/yourusername/spotify-data-pipeline.git
cd spotify-data-pipeline

# Create environment file from template
cp .env.example .env
# Edit .env with your actual credentials
```

### 2. Start Infrastructure

```bash
# Start Kafka, Zookeeper, Kafdrop, Airflow, and PostgreSQL
make up

# Verify services are running
docker compose ps
```

| Service | URL | Purpose |
|---------|-----|---------|
| Kafdrop | http://localhost:9000 | Kafka topic browser |
| Airflow | http://localhost:8080 | DAG management UI |

### 3. Install Python Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
make install
```

### 4. Run the Pipeline

```bash
# Terminal 1: Start the event producer
make producer

# Terminal 2: Start the S3 consumer
make consumer

# Airflow DAG runs automatically on @hourly schedule
# Or trigger manually via the Airflow UI
```

### 5. Run dbt Transformations

```bash
# Run all models (Silver → Gold)
make dbt-run

# Run schema tests
make dbt-test

# Generate documentation
make dbt-docs
```

---

## 🧪 Development

### Run Tests

```bash
make test          # Run test suite
make test-cov      # Run with coverage report
```

### Code Quality

```bash
make lint          # Run linter (Ruff)
make format        # Auto-format code
make typecheck     # Run type checker (MyPy)
```

### Available Make Commands

```bash
make help          # Show all available commands
```

---

## 🐳 Docker Services

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| `zookeeper` | `confluentinc/cp-zookeeper:7.4.1` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.4.1` | 9092, 29092 | Message broker |
| `kafdrop` | `obsidiandynamics/kafdrop` | 9000 | Kafka UI |
| `postgres` | `postgres:15` | 5432 | Airflow metadata DB |
| `airflow-webserver` | `apache/airflow:2.9.3` | 8080 | Airflow UI |
| `airflow-scheduler` | `apache/airflow:2.9.3` | — | DAG scheduler |

---

## 🏗️ Key Design Decisions

### Medallion Architecture
The pipeline follows the **Bronze → Silver → Gold** medallion pattern:
- **Bronze**: Raw, unprocessed data — the system of record
- **Silver**: Cleaned, typed, validated data — single source of truth
- **Gold**: Business-level aggregations — ready for dashboards and analytics

### Micro-Batch Consumer
Instead of writing each Kafka message individually to S3, the consumer buffers messages and writes in configurable batches (default: 10). This balances latency vs. S3 PUT cost efficiency.

### Idempotent dbt Models
Silver models are materialized as **views** (always current), while Gold models are materialized as **tables** (performant reads). All models include schema tests for data quality enforcement.

### Centralized Configuration
All environment variables are loaded and validated through a single `src/config.py` module, following the [12-Factor App](https://12factor.net/config) methodology.

---

## 📊 dbt Models

| Model | Layer | Materialization | Description |
|-------|-------|-----------------|-------------|
| `clean_data` | Silver | View | Cleans timestamps, standardizes casing, filters nulls |
| `top_songs` | Gold | Table | Play/skip/playlist counts + skip rate per song |
| `user_engagement` | Gold | Table | Daily engagement metrics by user, device, country |

---

## 🔒 Security

- Credentials are managed via environment variables (`.env`)
- `.env` is excluded from version control via `.gitignore`
- `.env.example` provides a safe template with placeholder values
- AWS IAM keys should be scoped to minimum required S3 permissions

---

## 🛣️ Roadmap

- [ ] Add Great Expectations for data quality validation
- [ ] Implement Terraform for AWS infrastructure provisioning
- [ ] Add monitoring and alerting (Datadog / CloudWatch)
- [ ] Implement CDC (Change Data Capture) for incremental loads
- [ ] Add Grafana dashboard for pipeline observability
- [ ] Schema registry for Kafka event validation (Avro/Protobuf)

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|-----------|
| **Streaming** | Apache Kafka |
| **Orchestration** | Apache Airflow |
| **Storage** | AWS S3 |
| **Data Warehouse** | Snowflake |
| **Transformation** | dbt (Data Build Tool) |
| **Language** | Python 3.10+ |
| **Containerization** | Docker, Docker Compose |
| **CI/CD** | GitHub Actions |
| **Linting** | Ruff, MyPy |
| **Testing** | pytest |

---

## 📄 License

This project is licensed under the MIT License.
