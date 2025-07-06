from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import boto3
from utils.config import AWS_REGION,S3_BUCKET,GLUE_JOB_NAME

glue_client = boto3.client("glue")

def trigger_glue_job():
    s3_input=f"s3://{S3_BUCKET}/input/competitors_raw_data.csv"
    s3_output=f"s3://{S3_BUCKET}/output/"
    glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={'--s3_input': s3_input, '--s3_output': s3_output}
    )

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

with DAG('parquet_pipeline_dag', schedule_interval='@hourly', default_args=default_args) as dag:
    trigger_glue =PythonOperator(
        task_id='trigger_glue_conversion',
        python_callable=trigger_glue_job
    )
