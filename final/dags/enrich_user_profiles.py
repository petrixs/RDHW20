from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, \
    BigQueryCreateEmptyTableOperator

PROJECT_ID = 'de-07-petro-tsesar'

SILVER_DATASET = 'silver'
GOLD_DATASET = 'gold'

CUSTOMERS_TABLE_NAME = 'customers'
PROFILES_TABLE_NAME = 'user_profiles'

USER_PROFILES_ENRICHED_TABLE_NAME = 'user_profiles_enriched'

user_profiles_enriched = DAG(
    'enrich_user_profiles',
    schedule_interval=None,
    catchup=False,
)

create_user_profiles_enriched_table = BigQueryCreateEmptyTableOperator(
    task_id='create_user_profiles_enriched_table',
    project_id=PROJECT_ID,
    dataset_id=GOLD_DATASET,
    table_id=USER_PROFILES_ENRICHED_TABLE_NAME,
    schema_fields=[
        {'name': 'client_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    if_exists='ignore',
    dag=user_profiles_enriched,
    gcp_conn_id='GC',
)

merge_user_profiles_query = BigQueryExecuteQueryOperator(
    task_id='merge_user_profiles',
    sql=f"""
        MERGE `{PROJECT_ID}.{GOLD_DATASET}.{USER_PROFILES_ENRICHED_TABLE_NAME}` AS target
        USING (
            SELECT
                c.client_id,
                COALESCE(c.first_name, SPLIT(p.full_name, ' ')[OFFSET(0)]) AS first_name,
                COALESCE(c.last_name, SPLIT(p.full_name, ' ')[SAFE_OFFSET(1)]) AS last_name,
                c.email,
                c.registration_date,
                COALESCE(c.state, p.state) AS state,
                COALESCE(c.birth_date, p.birth_date) AS birth_date,
                p.phone_number
            FROM `{PROJECT_ID}.{SILVER_DATASET}.{CUSTOMERS_TABLE_NAME}` c
            LEFT JOIN `{PROJECT_ID}.{SILVER_DATASET}.{PROFILES_TABLE_NAME}` p ON c.email = p.email
        ) AS source
        ON target.email = source.email
        WHEN MATCHED THEN
            UPDATE SET
                target.client_id = source.client_id,
                target.first_name = source.first_name,
                target.last_name = source.last_name,
                target.registration_date = source.registration_date,
                target.state = source.state,
                target.birth_date = source.birth_date,
                target.phone_number = source.phone_number
        WHEN NOT MATCHED THEN
            INSERT (client_id, first_name, last_name, email, registration_date, state, birth_date, phone_number)
            VALUES (source.client_id, source.first_name, source.last_name, source.email, source.registration_date, source.state, source.birth_date, source.phone_number)
    """,
    use_legacy_sql=False,
    dag=user_profiles_enriched,
    gcp_conn_id='GC',
)

create_user_profiles_enriched_table >> merge_user_profiles_query
