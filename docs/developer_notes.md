# 🛠️ Developer Notes & Engineering Journal

This document serves as an in-depth technical reference, architectural log, and developer guide for the **Dockerized Data Pipeline with Airflow** project.

---

## 1. Architectural Decisions & Trade-offs

### 1.1 Airflow Execution Mode (`LocalExecutor`)
* **Decision**: We chose `LocalExecutor` over `SequentialExecutor` (default SQLite) and `CeleryExecutor`.
* **Reasoning**:
  * `SequentialExecutor` runs only one task instance at a time and cannot execute parallel workflows.
  * `CeleryExecutor` requires message brokers (Redis/RabbitMQ) and worker pools, introducing unnecessary container and operational overhead for single-node development.
  * `LocalExecutor` leverages PostgreSQL as the backend to spawn parallel task processes directly on the scheduler container, balancing simplicity and real concurrency.

### 1.2 Data Intermediaries (Disk Staging vs. XComs)
* **Decision**: Pipeline stages pass state via staged filesystem artifacts (`raw_data.json` and `transformed_data.csv`) mounted through `/opt/airflow/scripts` rather than using Airflow XComs.
* **Reasoning**:
  * Airflow XComs store task output metadata directly inside the Airflow metadata database (PostgreSQL backend).
  * Storing full daily stock time-series DataFrames or large JSON responses in XCom quickly bloats the metadata database and degrades scheduler performance.
  * Shared volume staging keeps tasks decoupled, allows easy debugging/auditing of intermediate payloads, and preserves database performance.

### 1.3 Data Ingestion & Idempotency
* **Decision**: PostgreSQL schema defines a compound unique constraint:
  ```sql
  UNIQUE(symbol, date)
  ```
  The ingestion script executes:
  ```sql
  INSERT INTO stocks (...) VALUES (...) ON CONFLICT (symbol, date) DO NOTHING;
  ```
* **Reasoning**:
  * Guaranteed **idempotency**: If a DAG run fails midway or is re-triggered across multiple days, re-running tasks will not duplicate records or throw constraint violations.

---

## 2. Module & Code Walkthrough

### 2.1 Extraction (`scripts/fetch_data.py`)
* **Target Endpoint**: `https://www.alphavantage.co/query?function=TIME_SERIES_DAILY`
* **Handling Alpha Vantage Quotas**:
  * Free tier allows **25 requests/day** and **5 requests/min**.
  * The script inspects the response body for:
    1. `"Error Message"`: Logs an error if an invalid ticker symbol or bad parameter is passed.
    2. `"Note"`: Detects rate-limit warnings and applies a 30-second backoff (`time.sleep(30)`) before recursively retrying.
* **Output**: `scripts/raw_data.json`

### 2.2 Transformation (`scripts/transform_data.py`)
* **Input**: `scripts/raw_data.json`
* **Data Processing & Cleaning**:
  * Unpacks nested JSON (`data["Time Series (Daily)"]`).
  * Normalizes column names into database-friendly formats (`1. open` $\rightarrow$ `open`, etc.).
  * Typecasting:
    * `open`, `high`, `low`, `close` $\rightarrow$ `float` (stored as `NUMERIC(10,2)`)
    * `volume` $\rightarrow$ `int` (stored as `BIGINT`)
  * Data quality checks:
    * `df.drop_duplicates(subset=["date"])`
    * `df.dropna(subset=["date", "open", "high", "low", "close", "volume"])`
    * Chronological sorting: `df.sort_values("date", inplace=True)`
* **Output**: `scripts/transformed_data.csv`

### 2.3 Loading (`scripts/load_data.py`)
* **Input**: `scripts/transformed_data.csv`
* **Target Table**: `stocks`
* **Connection**: Managed with `psycopg2` using connection strings from `config/.env`.
* **Execution**: Iterates through records, binds parameterized SQL queries to prevent SQL injection, and executes transaction commits with robust `try/except/finally` blocks.

### 2.4 DAG Orchestration (`dags/stock_pipeline.py`)
* **Task Pipeline Graph**:
  ```text
  [fetch_data] ---> [transform_data] ---> [load_data]
  ```
* **Airflow Variables**:
  * Fetches `stock_symbol` from `Variable.get("stock_symbol", default_var="AAPL")`.
  * Enables zero-downtime reconfiguration of target stock tickers directly from the Airflow UI without modifying Python code.
* **Retry Strategy**:
  * `retries = 2`, `retry_delay = timedelta(minutes=1)`. Transient network issues or rate-limiting are resolved automatically.

---

## 3. Docker Environment & Networking

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network Bridge                    │
│                                                             │
│   ┌──────────────┐         ┌──────────────┐                 │
│   │   postgres   │◄────────┤ airflow-init │ (Exits on done) │
│   │  (Port 5432) │         └──────────────┘                 │
│   └──────┬───────┘                                          │
│          ▲                 ┌──────────────┐                 │
│          ├─────────────────┤   airflow    │ (Port 8080)     │
│          │                 │ (Webserver)  │                 │
│          │                 └──────────────┘                 │
│          │                                                  │
│          │                 ┌──────────────┐                 │
│          ├─────────────────┤   airflow-   │                 │
│          │                 │  scheduler   │                 │
│          │                 └──────────────┘                 │
│          │                                                  │
│          │                 ┌──────────────┐                 │
│          └─────────────────┤   pgadmin    │ (Port 5050)     │
│                            └──────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

* **Service Initialization Order**:
  1. `postgres` boots up, checks health via `pg_isready`.
  2. `airflow-init` waits for healthy `postgres`, runs `airflow db init`, seeds the admin user, and terminates.
  3. `airflow` (webserver) and `airflow-scheduler` start up only after `airflow-init` finishes successfully.

---

## 4. Troubleshooting & Debugging Guide

### 4.1 Checking Container Logs
```bash
# View all logs
docker compose logs -f

# View logs for a specific service
docker compose logs -f airflow-scheduler
docker compose logs -f postgres
```

### 4.2 Inspecting Airflow Task Failures
1. Navigate to the Airflow UI: [http://localhost:8080](http://localhost:8080).
2. Click on `stock_pipeline` DAG $\rightarrow$ **Grid** or **Graph** view.
3. Click on the failed task instance (red box) $\rightarrow$ **Log** tab.
4. Alternatively, examine raw log files on the host filesystem at:
   `stock_pipeline_project/logs/dag_id=stock_pipeline/...`

### 4.3 Direct Database Access
Access the PostgreSQL container shell:
```bash
docker exec -it stock_pipeline_project-postgres-1 psql -U airflow -d stocks
```
Useful SQL verification queries:
```sql
-- Count total records
SELECT COUNT(*) FROM stocks;

-- Check summary by symbol
SELECT symbol, COUNT(*), MIN(date), MAX(date) FROM stocks GROUP BY symbol;

-- View latest 5 records
SELECT * FROM stocks ORDER BY date DESC LIMIT 5;
```

---

## 5. Roadmap & Future Improvements

1. **Bulk Ingestion Optimization**:
   * Replace row-by-row `cur.execute` in `load_data.py` with `psycopg2.extras.execute_values` or Pandas `df.to_sql(method='multi')` for significant speedups on large datasets.
2. **Dynamic Task Mapping**:
   * Utilize Airflow 2.3+ Dynamic Task Mapping (`expand()`) to extract, clean, and load multiple stock symbols concurrently (e.g., `['AAPL', 'MSFT', 'GOOGL', 'AMZN']`).
3. **Data Quality Framework**:
   * Integrate **Great Expectations** or **Soda Core** as a dedicated verification task between transformation and loading to enforce schema drift and range validation rules.
4. **Cloud Migration**:
   * Transition local disk storage to **AWS S3 / GCP Cloud Storage** and load into a cloud data warehouse such as **Snowflake / Google BigQuery / AWS Redshift**.
