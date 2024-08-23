from os import remove
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

"""
DAG to extract Reddit data, load into Google Cloud Storage, and copy to BigQuery
"""

# Output name of the extracted file. This will be passed to each
# DAG task so they know which file to process
output_name = datetime.now().strftime("%Y%m%d")

# Run our DAG daily and ensure the DAG run will kick off
# once Airflow is started, as it will try to "catch up"
schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="elt_reddit_pipeline_gcp",
    description="Reddit ELT using GCP",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["RedditETL"],
) as dag:

    extract_reddit_data = BashOperator(
        task_id="extract_reddit_data",
        bash_command=f"python /opt/airflow/extraction/extract_reddit_etl.py {output_name}",
        dag=dag,
    )
    extract_reddit_data.doc_md = "Extract Reddit data and store as CSV"

    upload_to_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command=f"python /opt/airflow/extraction/upload_gcp_gcs_etl.py {output_name}",
        dag=dag,
    )
    upload_to_gcs.doc_md = "Upload Reddit CSV data to Google Cloud Storage bucket"

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"python /opt/airflow/extraction/upload_gcp_bigquery_etl.py {output_name}",
        dag=dag,
    )
    load_to_bigquery.doc_md = "Load data from GCS CSV file to BigQuery table"

# Define task dependencies
extract_reddit_data >> upload_to_gcs >> load_to_bigquery
