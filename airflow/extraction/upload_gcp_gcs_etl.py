import configparser
import pathlib
import sys
from google.cloud import storage
from validation import validate_input

"""
Part of DAG. Take Reddit data and upload to Google Cloud Storage bucket. 
Takes one command line argument of format YYYYMMDD.
This represents the file downloaded from Reddit, which will be in the /tmp folder.
"""

# Load GCP credentials and configurations
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
parser.read(f"{script_path}/configuration.conf")
BUCKET_NAME = parser.get("gcp_config", "bucket_name")

try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error: {e}")
    sys.exit(1)

# Name for our GCS file
FILENAME = f"{output_name}.csv"
DESTINATION_BLOB_NAME = FILENAME  # This is the name the file will have in GCS


def main():
    """Upload input file to Google Cloud Storage bucket"""
    validate_input(output_name)
    upload_file_to_gcs()


def upload_file_to_gcs():
    """Upload file to Google Cloud Storage Bucket"""
    try:
        # Initialize a storage client
        client = storage.Client()

        # Get the bucket object
        bucket = client.bucket(BUCKET_NAME)

        # Create a blob object from the filepath
        blob = bucket.blob(DESTINATION_BLOB_NAME)

        # Upload the file to a destination
        blob.upload_from_filename(f"/tmp/{FILENAME}")
        print(f"File {FILENAME} uploaded to {BUCKET_NAME}.")
    except Exception as e:
        print(f"Failed to upload file to GCS. Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
