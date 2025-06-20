from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os
import json

# Configuración de paths y conexión
RAW_DATA_PATH = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
POSTGRES_CONN_STR = "postgresql+psycopg2://qversity-admin:qversity-admin@postgres:5432/qversity"
TABLE_NAME = "customers_raw"
SCHEMA_NAME = "bronze"

def load_to_postgres():
    # Leer el archivo JSON
    df = pd.read_json(RAW_DATA_PATH)

    # Agregar timestamp de ingesta
    df["ingestion_timestamp"] = datetime.utcnow()

    # Convertir columnas con estructuras complejas (listas, diccionarios) a string
    columnas_complejas = ["contracted_services", "payment_history"]
    for col in columnas_complejas:
        if col in df.columns:
            df[col] = df[col].apply(json.dumps)

    # Crear engine SQLAlchemy y cargar en Postgres
    engine = create_engine(POSTGRES_CONN_STR)
    df.to_sql(
        name=TABLE_NAME,
        con=engine,
        schema=SCHEMA_NAME,
        if_exists="replace",
        index=False
    )

default_args = {
    'owner': 'Lucía',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id="load_to_postgres_bronze_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Carga datos crudos JSON a Postgres (bronze)",
    tags=["bronze"]
) as dag:

    load_task = PythonOperator(
        task_id="load_json_to_postgres",
        python_callable=load_to_postgres
    )
