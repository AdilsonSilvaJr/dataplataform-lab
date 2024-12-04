from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_dag',
    default_args=default_args,
    description='A simple Apache Spark DAG',
    schedule_interval=timedelta(days=1),
)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/aiflow/app/spark.py',
    conn_id='spark_default',
    dag=dag,
)
