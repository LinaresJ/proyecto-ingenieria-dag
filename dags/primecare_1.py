import os
from airflow import DAG
from datetime import datetime
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
    task_id='upload_files',
    file_path='/opt/airflow/data/claims.csv',
    container_name='bronze',
    blob_name='claims/{{ ds }}/claims.csv',  # Particionado por fecha
    wasb_conn_id='azureblob_wasb',
    create_container=True,
    retries=3,                               # Reintentos
    retry_delay=timedelta(minutes=5),        # Delay entre reintentos
    execution_timeout=timedelta(minutes=30)  # Timeout
    )