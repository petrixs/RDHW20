from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = 'de-07-petro-tsesar'
RAW_BUCKET = 'petr-tsesar-bucket'

BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'

CUSTOMERS_TABLE_NAME = 'customers'

date = "{{execution_date.strftime('%Y-%m-%-d')}}"
IMPORT_DIR_TEMPLATE = "raw/customers/"

process_customers = DAG(
    dag_id='process_customers',
    start_date=datetime(2022, 8, 1),
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_runs=1,
    end_date=datetime(2022, 8, 6)
)

conditional_clear_bronze_customers_for_date = BigQueryExecuteQueryOperator(
    task_id='conditional_clear_bronze_customers_for_date',
    sql=f"""
        DECLARE table_exists BOOL DEFAULT (
            SELECT
                COUNT(1) > 0
            FROM `{PROJECT_ID}.{BRONZE_DATASET}.__TABLES_SUMMARY__`
            WHERE table_id = '{CUSTOMERS_TABLE_NAME}'
        );

        IF table_exists THEN
            DELETE FROM `{PROJECT_ID}.{BRONZE_DATASET}.{CUSTOMERS_TABLE_NAME}`
            WHERE DATE(RegistrationDate) = DATE("{date}");
        END IF;
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_customers,
)

fill_bronze_customers = GCSToBigQueryOperator(
    task_id='fill_bronze_sales',
    bucket=RAW_BUCKET,
    source_objects=[
        f"raw/customers/{date}/{date}*.csv"
    ],
    schema_fields=[
        {'name': 'Id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    destination_project_dataset_table=f'{PROJECT_ID}:{BRONZE_DATASET}.{CUSTOMERS_TABLE_NAME}',
    write_disposition='WRITE_APPEND',
    skip_leading_rows=1,
    field_delimiter=',',
    gcp_conn_id='GC',
    dag=process_customers,
)


fill_silver_customers = BigQueryExecuteQueryOperator(
    task_id='fill_silver_customers',
    sql=f"""
        DECLARE table_exists BOOL DEFAULT (
          SELECT
            COUNT(1) > 0
          FROM
            `{PROJECT_ID}.{SILVER_DATASET}.__TABLES_SUMMARY__`
          WHERE
            table_id = '{CUSTOMERS_TABLE_NAME}');
    
        IF NOT table_exists THEN 
          EXECUTE IMMEDIATE "CREATE TABLE `{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}` (client_id INT64, first_name STRING, last_name STRING, email STRING NOT NULL, registration_date DATE NOT NULL, state STRING)"; 
        END IF;
        
        DELETE FROM `{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}` WHERE registration_date=DATE("{date}");
        
        INSERT INTO `{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}` (client_id, first_name, last_name, email, registration_date, state)
        SELECT
            CAST(Id AS INT64) AS Id,
            FirstName,
            LastName,
            Email,
            DATE(RegistrationDate) AS RegistrationDate,
            State
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{CUSTOMERS_TABLE_NAME}`
        WHERE RegistrationDate = "{ date }"
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_customers,
)

conditional_clear_bronze_customers_for_date >> fill_bronze_customers >> fill_silver_customers
