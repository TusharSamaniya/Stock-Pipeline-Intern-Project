import sys
import os
from datetime import datetime, timedelta

# Add /opt/airflow to sys.path to allow importing modules from the /opt/airflow/scripts directory
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.models import Variable

# Import our pipeline functions
from scripts.fetch_data import fetch_stock_data
from scripts.transform_data import transform_stock_data
from scripts.load_data import load_data_to_db

# Default settings applied to every task in this DAG
default_args = {
    "owner": "tushar",
    "retries": 2,                          # If a task fails, try 2 more times
    "retry_delay": timedelta(minutes=1),   # Wait 1 minute between retries
}

with DAG(
    dag_id="stock_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["stock_market"]
) as dag:

    # Get the stock symbol from Airflow UI Variables (default to 'AAPL' if not set)
    stock_symbol = Variable.get("stock_symbol", default_var="AAPL")

    # Task 1: Fetch
    fetch_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_stock_data,
        op_kwargs={"symbol": stock_symbol}
    )

    # Task 2: Transform
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_stock_data
    )

    # Task 3: Load
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data_to_db
    )

    # Set dependencies
    fetch_task >> transform_task >> load_task
