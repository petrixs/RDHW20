from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime

bucket_name = 'petr-tsesar-bucket'

source_path = '/file_storage/stg/{{ ds }}/sales/sales_{{ds}}.avro'

destination_path = 'src1/sales/v1/{{ ds }}/'

dag = DAG(
    'upload_to_gcs2',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True
)

upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src=source_path,
    dst=f'{destination_path}',
    bucket=bucket_name,
    gcp_conn_id='google-cloud-connection',
    dag=dag,
)

upload_to_gcs