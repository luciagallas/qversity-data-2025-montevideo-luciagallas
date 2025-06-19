from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os

# Configuración del archivo y S3
RAW_DATA_PATH = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
BUCKET_NAME = "qversity-raw-public-data"
FILE_KEY = "mobile_customers_messy_dataset.json"

def check_if_file_exists():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3.head_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    print("✅ El archivo existe en S3.")

def download_file_from_s3():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    s3.download_file(BUCKET_NAME, FILE_KEY, RAW_DATA_PATH)
    print(f"✅ Archivo descargado exitosamente a: {RAW_DATA_PATH}")

default_args = {
    'owner': 'Lucía',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='bronze_ingestion_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG que verifica y descarga el JSON desde S3 público a data/raw/',
    tags=['bronze']
) as dag:

    check_task = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_if_file_exists
    )

    download_task = PythonOperator(
        task_id='download_file_from_s3',
        python_callable=download_file_from_s3
    )

    check_task >> download_task
