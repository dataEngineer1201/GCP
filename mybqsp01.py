import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from datetime import datetime, timedelta

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('mybqc', default_args=default_args, schedule_interval=None) as dag:
    submit_spark_job = DataprocSubmitPySparkJobOperator(
        task_id='submit_spark_job',
        main='gs://sam01/bqsp.py',  # Specify the path to your PySpark script
        cluster_name='spbq',  # Specify your Dataproc cluster name
        region='us-central1',  # Specify your GCP region
        arguments=[],
    )

# Set task dependencies
submit_spark_job
