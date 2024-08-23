import configparser
import pathlib
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from validation import validate_input

"""
Part of DAG. Upload GCS CSV data to BigQuery. Takes one argument of format YYYYMMDD. 
This is the name of the file to copy from GCS. Script will load data into a temporary table in BigQuery, 
delete records with the same post ID from the main table, then insert these from the temp table 
(along with new data) into the main table.
"""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store our configuration variables
GCP_PROJECT = parser.get("gcp_config", "project_id")
GCP_CREDENTIALS_FILE = parser.get("gcp_config", "credentials_file")
BUCKET_NAME = parser.get("gcp_config", "bucket_name")
DATASET_ID = parser.get("gcp_config", "dataset_id")
TABLE_NAME = "bigquery"
TEMP_TABLE_NAME = "bigquery_temp"

# Check command line argument passed
try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error: {e}")
    sys.exit(1)

# Our GCS file path
file_path = f"gs://{BUCKET_NAME}/{output_name}.csv"

# BigQuery schema for the table
schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("num_comments", "INTEGER"),
    bigquery.SchemaField("score", "INTEGER"),
    bigquery.SchemaField("author", "STRING"),
    bigquery.SchemaField("created_utc", "TIMESTAMP"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("upvote_ratio", "FLOAT"),
    bigquery.SchemaField("over_18", "BOOLEAN"),
    bigquery.SchemaField("edited", "BOOLEAN"),
    bigquery.SchemaField("spoiler", "BOOLEAN"),
    bigquery.SchemaField("stickied", "BOOLEAN"),
]

def main():
    """Upload file from GCS to BigQuery Table"""
    validate_input(output_name)
    bq_client = connect_to_bigquery()
    create_table_if_not_exists(bq_client)
    load_data_into_bigquery(bq_client)

def connect_to_bigquery():
    """Connect to BigQuery using service account credentials"""
    try:
        credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_FILE)
        bq_client = bigquery.Client(credentials=credentials, project=GCP_PROJECT)
        return bq_client
    except Exception as e:
        print(f"Unable to connect to BigQuery. Error: {e}")
        sys.exit(1)

def create_table_if_not_exists(bq_client):
    """Create the BigQuery table if it doesn't exist."""
    try:
        dataset_ref = bq_client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_NAME)

        try:
            bq_client.get_table(table_ref)  # Check if table exists
            print(f"Table {TABLE_NAME} already exists.")
        except Exception:
            # Table does not exist, so create it
            table = bigquery.Table(table_ref, schema=schema)
            bq_client.create_table(table)
            print(f"Table {TABLE_NAME} created successfully.")

    except Exception as e:
        print(f"Error checking or creating table in BigQuery. Error: {e}")
        sys.exit(1)

def load_data_into_bigquery(bq_client):
    """Load data from GCS into BigQuery"""
    try:
        dataset_ref = bq_client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_NAME)
        temp_table_ref = dataset_ref.table(TEMP_TABLE_NAME)

        # Create or replace the temporary table
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq_client.load_table_from_uri(
            file_path, temp_table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete

        # Delete records in the main table with the same ID
        delete_query = f"""
        DELETE FROM `{GCP_PROJECT}.{DATASET_ID}.{TABLE_NAME}`
        WHERE id IN (SELECT id FROM `{GCP_PROJECT}.{DATASET_ID}.{TEMP_TABLE_NAME}`);
        """
        bq_client.query(delete_query).result()

        # Insert records from the temp table to the main table
        insert_query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET_ID}.{TABLE_NAME}`
        SELECT * FROM `{GCP_PROJECT}.{DATASET_ID}.{TEMP_TABLE_NAME}`;
        """
        bq_client.query(insert_query).result()

        # Drop the temporary table
        bq_client.delete_table(temp_table_ref, not_found_ok=True)
        
        print(f"Data loaded successfully into {TABLE_NAME}.")
        
    except Exception as e:
        print(f"Error loading data into BigQuery. Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
