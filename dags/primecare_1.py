import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

with DAG(
    dag_id='upload_local_to_blob',
    start_date=datetime(2025, 5, 30),
    schedule='@daily',    # Ejecuta diariamente
    catchup=False,
    tags=['azure','blob'],
    user_defined_macros={'basename': os.path.basename}
) as dag:

    upload_task = LocalFilesystemToWasbOperator(
    task_id='upload_claims',
    file_path='/opt/airflow/data/claims.parquet',
    container_name='datalake',
    blob_name='raw/airflow/G3/claims.parquet',  # Particionado por fecha
    wasb_conn_id='azureblob_wasb',
    create_container=True,
    # retries=3,                               # Reintentos
    # retry_delay=timedelta(minutes=5),        # Delay entre reintentos
    # execution_timeout=timedelta(minutes=30)  # Timeout
    )

    upload_task = LocalFilesystemToWasbOperator(
    task_id='upload_patients',
    file_path='/opt/airflow/data/patients.parquet',
    container_name='datalake',
    blob_name='raw/airflow/G3/patients.parquet',  # Particionado por fecha
    wasb_conn_id='azureblob_wasb',
    create_container=True,
    # retries=3,                               # Reintentos
    # retry_delay=timedelta(minutes=5),        # Delay entre reintentos
    # execution_timeout=timedelta(minutes=30)  # Timeout
    )

    upload_task = LocalFilesystemToWasbOperator(
    task_id='upload_CIE10',
    file_path='/opt/airflow/data/cie10_catalog.parquet',
    container_name='datalake',
    blob_name='raw/airflow/G3/cie10_catalog.parquet',  # Particionado por fecha
    wasb_conn_id='azureblob_wasb',
    create_container=True,
    # retries=3,                               # Reintentos
    # retry_delay=timedelta(minutes=5),        # Delay entre reintentos
    # execution_timeout=timedelta(minutes=30)  # Timeout
    )