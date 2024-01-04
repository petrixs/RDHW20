from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

def check_response_201(response):
    return response.status_code == 201

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 9),
    'end_date': datetime(2022, 8, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'process_sales',
    default_args=default_args,
    description='DAG for processing sales data',
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True,
)

extract_data = SimpleHttpOperator(
    task_id='extract_data_from_api',
    http_conn_id='extract_data_from_api',
    endpoint='/',
    method='POST',
    data='{"raw_dir": "/Users/user/PycharmProjects/pythonProject2/file_storage/raw/{{ ds }}", "date": "{{ ds }}"}',
    headers={"Content-Type": "application/json"},
    response_check=check_response_201,
    dag=dag,
)

convert_to_avro = SimpleHttpOperator(
    task_id='convert_to_avro',
    http_conn_id='convert_to_avro',
    endpoint='/',
    method='POST',
    data='{"raw_dir": "/Users/user/PycharmProjects/pythonProject2/file_storage/raw/{{ ds }}","stg_dir": "/Users/user/PycharmProjects/pythonProject2/file_storage/stg/{{ ds }}"}',
    headers={"Content-Type": "application/json"},
    response_check=check_response_201,
    dag=dag,
)

extract_data >> convert_to_avro