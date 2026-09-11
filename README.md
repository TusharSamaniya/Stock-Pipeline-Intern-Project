# 📈 Dockerized Data Pipeline with Apache Airflow

An end-to-end, automated, containerized **ETL (Extract, Transform, Load) Data Pipeline** designed to fetch, process, validate, and store daily stock market data. The orchestration is handled by **Apache Airflow**, with data extraction from the **Alpha Vantage API**, transformation via **Pandas**, and storage in **PostgreSQL**—all orchestrated effortlessly using **Docker & Docker Compose**.

---

## 🏗️ Architecture Overview

```
                          ┌────────────────────────┐
                          │   Alpha Vantage API    │
                          └───────────┬────────────┘
                                      │
                                (1) Extract (JSON)
                                      ▼
                        ┌───────────────────────────┐
                        │    fetch_data.py Task     │
                        │ (scripts/raw_data.json)   │
                        └─────────────┬─────────────┘
                                      │
                              (2) Transform & Clean
                                      ▼
                        ┌───────────────────────────┐
                        │   transform_data.py Task  │
                        │(scripts/transformed_data) │
                        └─────────────┬─────────────┘
                                      │
                                (3) Load (SQL)
                                      ▼
                        ┌───────────────────────────┐
                        │     load_data.py Task     │
                        └─────────────┬─────────────┘
                                      │
                          ┌───────────▼────────────┐
                          │  PostgreSQL Database   │
                          │   (Table: stocks)      │
                          └────────────────────────┘
```

### Key Workflow Highlights
1. **Extract (`fetch_data.py`)**: Queries the Alpha Vantage API for daily stock prices (`TIME_SERIES_DAILY`) for a specified symbol (e.g., `AAPL`), with built-in API rate-limit detection and retries.
2. **Transform (`transform_data.py`)**: Flattens nested JSON responses, handles missing values, removes duplicates, casts numeric types, and outputs a clean tabular dataset.
3. **Load (`load_data.py`)**: Bulk-loads data into PostgreSQL with idempotent `ON CONFLICT (symbol, date) DO NOTHING` clauses to prevent duplicate entries on pipeline re-runs.
4. **Orchestrate (`dags/stock_pipeline.py`)**: Manages the schedule, execution dependencies, retries, and task alerting via Apache Airflow.

---

## 🚀 Tech Stack

- **Orchestration**: Apache Airflow 2.7.0 (LocalExecutor)
- **Containerization**: Docker & Docker Compose
- **Programming Language**: Python 3.9+
- **Data Transformation**: Pandas
- **Database**: PostgreSQL 13
- **Database Admin UI**: pgAdmin 4
- **External Data Source**: Alpha Vantage API

---

## 📂 Directory Structure

```text
stock_pipeline_project/
├── config/
│   └── .env                   # API keys and DB connection credentials
├── dags/
│   ├── stock_pipeline.py      # Production DAG (Fetch >> Transform >> Load)
│   └── test_dag.py            # Simple verification/smoke-test DAG
├── db/
│   └── schema.sql             # SQL table definitions (auto-initialized on startup)
├── docs/
│   └── developer_notes.md     # Development documentation and notes
├── scripts/
│   ├── fetch_data.py          # API data extraction logic
│   ├── transform_data.py      # Data cleaning and transformation logic
│   └── load_data.py           # Database ingestion logic
├── docker-compose.yml         # Multi-container orchestration (Airflow, Postgres, pgAdmin)
├── Dockerfile                 # Custom Docker image definition
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

---

## 🛠️ Getting Started & Setup

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with WSL2 enabled on Windows)
- [Git](https://git-scm.com/)
- Free API Key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

### 2. Clone the Repository
```bash
git clone https://github.com/<your-username>/Dockerized-Data-Pipeline-with-Airflow.git
cd Dockerized-Data-Pipeline-with-Airflow/stock_pipeline_project
```

### 3. Configure Environment Variables
Create a file named `.env` in the `config/` directory:

```env
API_KEY=your_alpha_vantage_api_key_here
DATABASE_URL=postgresql://airflow:airflow@postgres:5432/stocks
```

---

## 🐳 Running the Pipeline with Docker

### 1. Start the Containers
Run Docker Compose to build and start all required services:
```bash
docker compose up -d
```

This starts:
- **PostgreSQL**: Port `5432`
- **Airflow Webserver**: Port `8080`
- **Airflow Scheduler**: Background scheduler container
- **Airflow Init**: Initializes the metadata database & default user
- **pgAdmin**: Port `5050`

### 2. Access the UIs
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080)
  - **Username**: `admin`
  - **Password**: `admin`
- **pgAdmin Web UI**: [http://localhost:5050](http://localhost:5050)
  - **Email**: `admin@admin.com`
  - **Password**: `admin`

### 3. Triggering the DAG
1. Open the Airflow UI at [http://localhost:8080](http://localhost:8080).
2. Unpause/Toggle the `stock_pipeline` DAG.
3. Trigger the DAG manually or let it run on its daily schedule (`@daily`).
4. (Optional) Customize the target ticker symbol dynamically by navigating to **Admin -> Variables** and setting `stock_symbol` (e.g., `MSFT`, `GOOGL`, `TSLA`).

---

## 🗄️ Database Schema

The database table `stocks` is automatically initialized on the first startup via [schema.sql](db/schema.sql):

```sql
CREATE TABLE IF NOT EXISTS stocks (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    volume BIGINT,
    UNIQUE(symbol, date)
);
```

You can verify the loaded records directly using `psql`:
```bash
docker exec -it <postgres_container_name> psql -U airflow -d stocks -c "SELECT * FROM stocks ORDER BY date DESC LIMIT 10;"
```

---

## 🧪 Local Testing (Without Docker)

If you wish to test individual scripts locally:

```bash
# Create and activate virtual environment
python -m venv venv
source venv/Scripts/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run scripts sequentially
python scripts/fetch_data.py
python scripts/transform_data.py
python scripts/load_data.py
```

---

## 🔒 Best Practices Implemented

- **Idempotency**: Safe re-executions via `ON CONFLICT DO NOTHING` prevent duplicate database records.
- **Resilience & Rate-Limiting**: Graceful handling of API rate limits and connection retries.
- **Security**: Strict separation of secrets using environment variables (`.env`).
- **Modularity**: Standalone scripts that work both independently via CLI and as Airflow Python operators.
