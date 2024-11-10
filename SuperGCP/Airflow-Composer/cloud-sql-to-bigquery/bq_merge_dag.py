from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'BQ_MERGE_DAG',
    default_args=default_args,
    description='Perform a MERGE operation in BigQuery',
    schedule_interval=None,
)

sql_merge_query = '''
MERGE INTO `first-project-428309.customers_dataset.customer_male_final` AS target
USING `first-project-428309.customers_dataset.customers_male` AS source
ON target.CustomerID = source.CustomerID

-- When a match is found, update the existing records
WHEN MATCHED THEN
  UPDATE SET
    target.FullName = source.FullName,
    target.Email = source.Email,
    target.Phone = source.Phone,
    target.Address = source.Address,
    target.Username = source.Username,
    target.Password = source.Password,
    target.DateOfBirth = source.DateOfBirth,
    target.Gender = source.Gender,
    target.CreatedAt = source.CreatedAt,
    target.UpdatedAt = CURRENT_TIMESTAMP()

-- When no match is found, insert new records
WHEN NOT MATCHED THEN
  INSERT (CustomerID, FullName, Email, Phone, Address, Username, Password, DateOfBirth, Gender, CreatedAt, UpdatedAt)
  VALUES (source.CustomerID, source.FullName, source.Email, source.Phone, source.Address, source.Username, source.Password, source.DateOfBirth, source.Gender, source.CreatedAt, CURRENT_TIMESTAMP())'''

bigquery_execute_merge_query = BigQueryExecuteQueryOperator(
    task_id="execute_merge_query",
    sql=sql_merge_query,
    use_legacy_sql=False,
    location='us',
    dag=dag
)
