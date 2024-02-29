from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = 'de-07-petro-tsesar'
RAW_BUCKET = 'petr-tsesar-bucket'

SILVER_DATASET = 'silver'
CUSTOMERS_TABLE_NAME = 'customers'
PROFILES_TABLE_NAME = 'user_profiles'
IMPORT_FILE_TEMPLATE = "raw/user_profiles/user_profiles.json"


def add_column_if_not_exists():
    client = BigQueryHook(gcp_conn_id='GC').get_client()

    table_ref = bigquery.TableReference.from_string(
        f"{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}"
    )

    table = client.get_table(table_ref)

    column_names = [field.name for field in table.schema]
    if 'birth_date' not in column_names:
        from google.cloud.bigquery import SchemaField
        new_schema = table.schema[:]
        new_schema.append(SchemaField('birth_date', 'DATE'))
        table.schema = new_schema
        client.update_table(table, ['schema'])


process_user_profiles = DAG(
    'process_user_profiles',
    schedule_interval=None,
    catchup=False
)

fill_silver_profiles = GCSToBigQueryOperator(
    task_id='fill_silver_profiles',
    bucket=RAW_BUCKET,
    source_objects=[IMPORT_FILE_TEMPLATE],
    destination_project_dataset_table=f'{PROJECT_ID}:{SILVER_DATASET}.{PROFILES_TABLE_NAME}',
    schema_fields=[
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    dag=process_user_profiles,
    gcp_conn_id='GC'
)

add_birthdate_if_not_exists_task = PythonOperator(
    task_id='add_birthdate_task',
    python_callable=add_column_if_not_exists,
)

update_customers_data = BigQueryExecuteQueryOperator(
    task_id='update_customers_data',
    sql=f"""
        MERGE INTO `{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}` AS c
        USING (
            SELECT
                email,
                state,
                SPLIT(full_name, ' ')[OFFSET(0)] AS first_name,
                SPLIT(full_name, ' ')[SAFE_OFFSET(1)] AS last_name,
                birth_date
            FROM `{PROJECT_ID}.{SILVER_DATASET}.{PROFILES_TABLE_NAME}`
        ) AS u
        ON c.email = u.email
        WHEN MATCHED THEN
            UPDATE SET
                c.state = u.state,
                c.first_name = u.first_name,
                c.last_name = u.last_name,
                c.birth_date = u.birth_date;
    """,
    use_legacy_sql=False,
    dag=process_user_profiles,
    gcp_conn_id='GC'
)

fill_silver_profiles >> add_birthdate_if_not_exists_task >> update_customers_data


