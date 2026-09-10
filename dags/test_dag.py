from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Airflow is working successfully!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )
