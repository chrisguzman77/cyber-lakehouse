# Cyber Lakehouse – Network Intrusion Detection Pipeline

A local lakehouse-style data engineering + ML pipeline for detecting malicious network traffic using Apache Spark, Delta Lake, and Python.
Built to demonstrate data engineering fundamentals, ML integration, and production-style structure in a cybersecurity context.

---

## Project Overview

This project implements a Bronze → Silver → Gold lakehouse architecture to ingest, clean, feature-engineer, and analyze network flow data, followed by:

- Training a machine-learning model

- Scoring predictions at scale

- Querying results using Spark SQL

The system is fully containerized and runs locally using Docker.

---

## Architecture
```
Raw CSVs
   ↓
Bronze (raw Delta)
   ↓
Silver (cleaned Delta)
   ↓
Gold (features + analytics Delta)
   ↓
ML Training (Random Forest)
   ↓
Predictions (Delta)
   ↓
SQL Analytics
```
---

## Tech Stack
Category	| Tools
|----|----|
Data Processing	| Apache Spark (PySpark)
Storage	| Delta Lake
ML	| scikit-learn
Orchestration	| Docker + Docker Compose
Language	| Python 3.11
Quality	| pytest, ruff
Dataset	| UNSW-NB15 (network intrusion data)

---

## Repository Structure
```
cyber-lakehouse/
│
├── src/
│   ├── jobs/                  # Spark jobs (Bronze → Gold, ML)
│   │   ├── 01_ingest_bronze.py
│   │   ├── 02_clean_silver.py
│   │   ├── 03_build_gold_features.py
│   │   ├── 04_train_model.py
│   │   └── 05_score_predictions.py
│   │
│   ├── utils/                 # Shared helpers
│   │   ├── io.py
│   │   ├── schema.py
│   │   └── spark_session.py
│
├── src/sql/
│   ├── analytics.sql          # Analytics queries
│   └── quality_checks.sql     # Data quality checks
│
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│
├── models/                    # Trained model artifacts (gitignored)
├── tests/                     # Unit tests
├── docker/
│   └── spark.Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Running the Pipeline
1️⃣ Start Spark container
```
docker compose up -d
```
2️⃣ Run the lakehouse jobs
```
# Bronze ingestion
docker compose exec spark python src/jobs/01_ingest_bronze.py

# Silver cleaning
docker compose exec spark python src/jobs/02_clean_silver.py

# Gold feature engineering
docker compose exec spark python src/jobs/03_build_gold_features.py
```
3️⃣ Train the ML model
```
docker compose exec spark python src/jobs/04_train_model.py
```

- Trains a Random Forest classifier

- Saves model artifact locally (not committed to Git)

4️⃣ Score predictions
```
docker compose exec spark python src/jobs/05_score_predictions.py
```

Outputs:

- Prediction probabilities

- Attack classification

- Model version metadata

- Timestamps

All written back to Delta tables.

---


## Analytics (Spark SQL)

Run analytics queries against Delta tables:
```
docker compose exec spark \
  /usr/local/lib/python3.11/site-packages/pyspark/bin/spark-sql \
  --packages io.delta:delta-spark_2.12:3.3.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -f src/sql/analytics.sql
```

Example insights:

- Attack counts by category

- Prediction distributions

- Most suspicious network flows

---


## Testing & Quality

Run locally:
```
uv run ruff check .
uv run ruff format .
uv run pytest

```
✔ Import hygiene
✔ Schema validation
✔ Data quality rules

---

## Security & Engineering Practices

- Immutable data layers (Bronze/Silver/Gold)

- Schema validation before writes

- Deterministic ML pipeline

- Reproducible containerized environment

- Clear separation of concerns (jobs vs utils)

---

## Why This Project?

This project demonstrates:

- Practical data engineering workflows

- Spark + Delta Lake integration

- ML embedded in a data pipeline

- Cybersecurity-focused analytics

- Production-style repo structure

It’s designed as a portfolio-grade project for roles in:

- Data Engineering

- Security Engineering

- ML Engineering

- Cyber Operations / SOC automation
