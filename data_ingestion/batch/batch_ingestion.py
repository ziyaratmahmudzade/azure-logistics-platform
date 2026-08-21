import os
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Configuration
STORAGE_ACCOUNT_NAME = "stlogisticsplatform"
LOCAL_FILE_PATH = "ingestion/batch/shipments.csv"
FILE_NAME = "shipments.csv"


def get_adls_client():
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=credential
    )
    return service_client


def upload_to_bronze(local_file_path, file_name):
    now = datetime.now(timezone.utc)
    folder_path = f"shipments/batch/{now.year}/{now.month:02d}/{now.day:02d}"

    client = get_adls_client()
    fs_client = client.get_file_system_client("bronze")

    # Create directory
    dir_client = fs_client.get_directory_client(folder_path)
    dir_client.create_directory()

    # Upload file
    file_client = dir_client.get_file_client(file_name)
    with open(local_file_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    print(f"Uploaded to bronze/{folder_path}/{file_name}")
    print(f"File size: {os.path.getsize(local_file_path)} bytes")


if __name__ == "__main__":
    print("Starting batch ingestion...")
    upload_to_bronze(LOCAL_FILE_PATH, FILE_NAME)
    print("Batch ingestion complete.")