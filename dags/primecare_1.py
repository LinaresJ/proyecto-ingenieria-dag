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
        file_path='/opt/airflow/data/claims.csv',            # Puedes usar glob
        container_name='bronze',    # Nombre del container
        # blob_name='demo/{{ ds }}/{{ basename(file_path) }}',
        blob_name='claims.csv',  # Nombre del blob donde se guardará el archivo
        wasb_conn_id='azureblob_wasb',              # Debe coincidir con tu Conn Id
        create_container=True                     # Crea el contenedor si no existe
    )