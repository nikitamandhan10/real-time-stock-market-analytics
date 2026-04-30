# Real-Time Stock Market Analytics 📈

An end-to-end, real-time data engineering pipeline that ingests live stock market data, processes it through a streaming + batch lakehouse architecture, and prepares it for analytics — all running locally via Docker Compose.

Built as a hands-on project to learn modern data engineering tooling: **Kafka, Flink, Spark, Airflow, and MinIO (S3)** wired together using the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🏗️ Architecture

```
                 ┌─────────────────┐
                 │  Stock Data API │
                 │   (Producer)    │
                 └────────┬────────┘
                          │
                          ▼
        ┌──────────────────────────────────┐
        │        Apache Kafka              │
        │  (Zookeeper + Schema Registry)   │
        └─────────┬────────────────┬───────┘
                  │                │
                  ▼                ▼
         ┌────────────────┐  ┌──────────────┐
         │  Apache Flink  │  │ Apache Spark │
         │ (Stream Proc.) │  │   (Batch)    │
         └───────┬────────┘  └──────┬───────┘
                 │                  │
                 └────────┬─────────┘
                          ▼
              ┌──────────────────────┐
              │   MinIO (S3)         │
              │  Bronze │ Silver │ Gold
              └──────────────────────┘
                          ▲
                          │ orchestrates
                          │
                  ┌───────────────┐
                  │ Apache Airflow│
                  └───────────────┘
```

**Data flow:**
1. **Producer** fetches live stock data and publishes to Kafka topics.
2. **Kafka** acts as the durable event backbone, with Schema Registry enforcing message contracts.
3. **Flink** consumes the stream for real-time computations (windowed aggregations, moving averages, anomaly detection).
4. **Spark** handles heavier batch transformations on stored data.
5. **MinIO** stores data across three layers — Bronze (raw), Silver (cleaned), Gold (aggregated/business-ready).
6. **Airflow** orchestrates the batch jobs and dependencies between layers.

---

## 🧰 Tech Stack

| Layer              | Tool                            |
| ------------------ | ------------------------------- |
| Streaming          | Apache Kafka 7.5.0 + Zookeeper  |
| Schema Management  | Confluent Schema Registry       |
| Stream Processing  | Apache Flink                    |
| Batch Processing   | Apache Spark 3.5.0              |
| Orchestration      | Apache Airflow 2.7.3            |
| Object Storage     | MinIO (S3-compatible)           |
| Metadata DB        | PostgreSQL 15                   |
| Language           | Python 3.11                     |
| Containerization   | Docker + Docker Compose         |

---

## 📁 Project Structure

```
real-time-stock-market-analytics/
├── config/                  # Configuration files
├── flink/
│   └── jobs/                # Flink streaming jobs
├── kafka/
│   └── producers/           # Kafka producer scripts
├── docker-compose.yml       # Full local stack
├── requirements.txt         # Python dependencies
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

- Docker & Docker Compose installed
- At least **8 GB RAM** allocated to Docker (this stack has many services)
- Python 3.11+ (for running producers locally)

### 1. Clone the repository

```bash
git clone https://github.com/nikitamandhan10/real-time-stock-market-analytics.git
cd real-time-stock-market-analytics
```

### 2. Spin up the stack

```bash
docker-compose up -d
```

This will start: Zookeeper, Kafka, Schema Registry, MinIO, Postgres, Airflow (init/webserver/scheduler), Spark (master + worker), and Flink (jobmanager + taskmanager).

> ⏳ The first run will take a few minutes — Docker pulls images and Airflow runs DB migrations.

### 3. Verify services are running

| Service           | URL                             | Credentials             |
| ----------------- | ------------------------------- | ----------------------- |
| Airflow UI        | http://localhost:8080           | `admin` / `admin`       |
| Spark Master UI   | http://localhost:8081           | —                       |
| Flink Dashboard   | http://localhost:8083           | —                       |
| MinIO Console     | http://localhost:9001           | `minioadmin` / `minioadmin` |
| Schema Registry   | http://localhost:8084           | —                       |
| Kafka Broker      | `localhost:9092`                | —                       |

### 4. Install Python dependencies (for running producers)

```bash
pip install -r requirements.txt
```

### 5. Run the Kafka producer

```bash
python kafka/producers/<your_producer_script>.py
```

Messages will start flowing into Kafka. You can verify with any Kafka CLI tool or from the Flink/Spark consumer side.

---

## 🔍 What's Inside

### Kafka Producers (`kafka/producers/`)
Python scripts that fetch live stock data and publish to Kafka topics, keyed by stock symbol so that all events for a given ticker land in the same partition (preserves ordering).

### Flink Jobs (`flink/jobs/`)
Streaming jobs that consume from Kafka and perform real-time aggregations — things like rolling price averages, volume spikes, and price-change anomaly detection.

### Medallion Layers (MinIO)
- **Bronze** — raw events as they arrive from Kafka.
- **Silver** — cleaned and validated records.
- **Gold** — aggregated, analytics-ready datasets.

---

## 🛑 Stopping the stack

```bash
docker-compose down
```

To also wipe persistent data (MinIO buckets, Postgres):

```bash
docker-compose down -v
```

---

## 🧠 Learnings & Concepts Covered

This project was built to get hands-on with:

- ✅ Kafka fundamentals — topics, partitions, consumer groups, offsets
- ✅ Schema Registry & schema evolution
- ✅ Flink streaming — windowing, watermarks, stateful processing
- ✅ Spark batch processing on object storage
- ✅ Medallion architecture (Bronze/Silver/Gold)
- ✅ Airflow DAGs for orchestrating batch jobs
- ✅ Docker Compose for multi-service local development

---

## 🗺️ Roadmap

- [ ] Add exactly-once semantics to Flink jobs
- [ ] Integrate Avro schemas with Schema Registry
- [ ] Add a Streamlit / Grafana dashboard for live visualizations
- [ ] CI/CD via GitHub Actions
- [ ] Deploy to a cloud environment (AWS MSK + EKS)
- [ ] Add unit + integration tests

