import requests
import json
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Configuration
STORAGE_ACCOUNT_NAME = "stlogisticsplatform"
API_URL = "https://opensky-network.org/api/states/all"


def get_adls_client():
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=credential
    )
    return service_client


def fetch_flight_data():
    print("Fetching live flight data from OpenSky API...")
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def land_to_bronze(data):
    now = datetime.now(timezone.utc)
    folder_path = f"tracking/api/{now.year}/{now.month:02d}/{now.day:02d}"
    file_name = f"{now.hour:02d}-{now.minute:02d}-{now.second:02d}.json"

    client = get_adls_client()
    fs_client = client.get_file_system_client("bronze")

    # Create directory
    dir_client = fs_client.get_directory_client(folder_path)
    dir_client.create_directory()

    # Write file
    file_client = dir_client.get_file_client(file_name)
    file_content = json.dumps(data, indent=2)
    file_client.upload_data(file_content, overwrite=True)

    print(f"Data landed at bronze/{folder_path}/{file_name}")
    print(f"Total aircraft tracked: {len(data.get('states', []))}")


if __name__ == "__main__":
    data = fetch_flight_data()
    land_to_bronze(data)