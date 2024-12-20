from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

## Databricks path for cleaning data
cleaning_task = {
    "notebook_path": "/Users/wilsonredman@outlook.com/clean_data"
}

## Args when creating the DAG
default_args = {
    "owner": "Wilson",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 12, 12)
}

with DAG("0e59bc5e89eb_dag",
    default_args = default_args,
    schedule_interval = "@daily" ## Repeat daily
    ) as dag:
    
    clean_data = DatabricksSubmitRunOperator(
        task_id = "clean_pinterest_data", ## Task Name
        databricks_conn_id = "databricks_default",
        existing_cluster_id = "1108-162752-8okw8dgg", ## Databricks cluster ID
        notebook_task = cleaning_task
    )
    clean_data ## Only single node in DAG