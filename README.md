# COVID-19 Data Pipeline

An end-to-end ETL pipeline that automates daily COVID-19 data collection, transformation, and storage for analytics use cases.

This project simulates a real-world batch data pipeline, starting from a public API and ending in a structured PostgreSQL database, fully automated using Apache Airflow.

---

## 🚀 Overview

The pipeline performs the following steps daily:

1. Extracts COVID-19 data from the **disease.sh public API**
2. Cleans and transforms the raw JSON data using **Pandas**
3. Loads the processed data into **PostgreSQL** using a star schema
4. Orchestrates the workflow with **Apache Airflow**, running inside Docker containers

---

## 🏗️ Architecture

```
Public API → Python ETL → PostgreSQL → Airflow Scheduler
```

---

## 📁 Project Structure

```
src/                    # ETL logic (extract, transform, load)
dags/                   # Airflow DAGs
config/                 # API and database configuration files
sql/                    # Table creation scripts
data/                   # Raw and processed data files
docker-compose.yml
```

---

## 🛠️ Technologies Used

- Python (Requests, Pandas, SQLAlchemy)
- PostgreSQL
- Apache Airflow
- Docker & Docker Compose
- SQL

---

## ⚙️ How to Run

1. Clone the repository
2. Start all services using Docker:
   ```bash
   docker-compose up -d
   ```
3. Open Airflow UI at http://localhost:8080
4. Trigger the DAG: `covid_etl_pipeline`

The pipeline will fetch fresh data and load it into PostgreSQL automatically.

---

## 🧠 Data Modeling

The database follows a star schema design:

- **dim_countries** – country-level information
- **fact_covid_daily** – daily COVID statistics (cases, deaths, tests, recoveries)

This structure is optimized for analytics and reporting.

---

## 📊 Key Features

1. Automated daily data ingestion
2. Clean separation of extract, transform, and load steps
3. Idempotent loads with basic upsert handling
4. Logging and error handling for observability
5. Fully containerized environment for easy setup

---

## 📘 What I Learned

1. Building ETL pipelines from API sources
2. Cleaning and transforming semi-structured data
3. Designing analytical schemas in PostgreSQL
4. Orchestrating batch workflows using Apache Airflow
5. Managing multi-service applications with Docker

This project was built as part of my data engineering learning journey to gain hands-on experience with production-style pipelines and orchestration tools.
