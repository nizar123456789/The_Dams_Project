
from azure.storage.blob import BlobServiceClient
import pandas as pd

connection_string = "DefaultEndpointsProtocol=https;AccountName=damsdatalake;AccountKey=Y+517P3wt5MGyXVzkQO/ikq/inXutw+vCrwjBMrDss13NhxlifoKV3m/d6hgenJQ38UHEYfOIRG7+AStGFKVJw==;EndpointSuffix=core.windows.net"
container_name = "rawdata"

def upload_file(data):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Convert DataFrame to CSV string and upload it
    csv_data = data.to_csv(index=False)
    blob_name = "final_dams_data.csv"  # Name of the blob (file) in Azure Blob Storage
    
    container_client.upload_blob(name=blob_name, data=csv_data, overwrite=True)

# Read the CSV file
data = pd.read_csv(r'C:\Users\nizar\Desktop\Dams_Project\src\pipeline\final_dams_data.csv')

# Upload the file
upload_file(data)
