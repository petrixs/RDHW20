from airflow import DAG
from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryDeleteTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = 'de-07-petro-tsesar'
RAW_BUCKET = 'petr-tsesar-bucket'

BRONZE_DATASET = 'bronze'
SILVER_DATASET = 'silver'

SALES_TABLE_NAME = 'sales'

date = "{{execution_date.strftime('%Y-%m-%-d')}}"
IMPORT_FILENAMES_TEMPLATE = f"raw/sales/{date}/{date}__sales.csv"

process_sales = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 9, 1),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True,
    end_date=datetime(2022, 10, 1)
)

load_data_to_temp_table = GCSToBigQueryOperator(
    task_id='load_data_to_temp_table',
    bucket=RAW_BUCKET,
    source_objects=[IMPORT_FILENAMES_TEMPLATE],
    destination_project_dataset_table=f"{PROJECT_ID}:{BRONZE_DATASET}.temp_{date}_{SALES_TABLE_NAME}",
    schema_fields=[
        {'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Product', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Price', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='GC',
    dag=process_sales,
)


delete_temp_table = BigQueryDeleteTableOperator(
    task_id='delete_temp_table',
    deletion_dataset_table=f"{PROJECT_ID}.{BRONZE_DATASET}.temp_{date}_{SALES_TABLE_NAME}",
    gcp_conn_id='GC',
    dag=process_sales,
)

create_bronze_table_if_not_exists = BigQueryExecuteQueryOperator(
    task_id='create_bronze_table_if_not_exists',
    sql=f"""
        CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{BRONZE_DATASET}.{SALES_TABLE_NAME}` (
            CustomerId STRING,
            PurchaseDate STRING,
            Product STRING,
            Price STRING,
            _dag_exec_date DATE
        )
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_sales,
)

conditional_clear_bronze_sales_for_date = BigQueryExecuteQueryOperator(
    task_id='conditional_clear_bronze_sales_for_date',
    sql=f"""
        DECLARE table_exists BOOL DEFAULT (
            SELECT
                COUNT(1) > 0
            FROM `{PROJECT_ID}.{BRONZE_DATASET}.__TABLES__`
            WHERE table_id = '{SALES_TABLE_NAME}'
        );

        IF table_exists THEN
            DELETE FROM `{PROJECT_ID}.{BRONZE_DATASET}.{SALES_TABLE_NAME}`
            WHERE PurchaseDate = "{date}";
        END IF;
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_sales,
)

fill_bronze_sales = BigQueryExecuteQueryOperator(
    task_id='transfer_data_to_bronze',
    sql=f"""
        INSERT INTO `{PROJECT_ID}.{BRONZE_DATASET}.{SALES_TABLE_NAME}` (CustomerId, PurchaseDate, Product, Price, _dag_exec_date)
        SELECT CustomerId, PurchaseDate, Product, Price, DATE('{date}') AS _dag_exec_date
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.temp_{date}_{SALES_TABLE_NAME}`
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_sales,
)

transform_and_fill_silver_sales = BigQueryExecuteQueryOperator(
    task_id='transform_and_fill_silver_sales',
    sql=f"""
       DECLARE table_exists BOOL DEFAULT (
          SELECT
            COUNT(1) > 0
          FROM
            `{PROJECT_ID}.{SILVER_DATASET}.__TABLES_SUMMARY__`
          WHERE
            table_id = '{SALES_TABLE_NAME}');
    
        IF NOT table_exists THEN 
          EXECUTE IMMEDIATE "CREATE TABLE `{PROJECT_ID}.{SILVER_DATASET}.{SALES_TABLE_NAME}` (client_id INT64, purchase_date DATE, product_name STRING, price DECIMAL(10, 2)) PARTITION BY purchase_date"; 
        END IF; 
        
        DELETE FROM `{PROJECT_ID}.{SILVER_DATASET}.{SALES_TABLE_NAME}` WHERE purchase_date=DATE("{date}");

        INSERT INTO `{PROJECT_ID}.{SILVER_DATASET}.{SALES_TABLE_NAME}` (client_id, purchase_date, product_name, price)
        SELECT
          CAST(CustomerId AS INT64) AS client_id,
          DATE(_dag_exec_date) AS purchase_date,
          Product AS product_name,
          CAST(REGEXP_REPLACE(Price, r'[^0-9]', '') AS DECIMAL) AS price
        FROM `{PROJECT_ID}.{BRONZE_DATASET}.{SALES_TABLE_NAME}`
        WHERE _dag_exec_date = DATE("{ date }")
    """,
    use_legacy_sql=False,
    gcp_conn_id='GC',
    dag=process_sales,
)

load_data_to_temp_table >> create_bronze_table_if_not_exists >> conditional_clear_bronze_sales_for_date >> fill_bronze_sales >> delete_temp_table >> transform_and_fill_silver_sales
