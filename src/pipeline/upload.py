from config.config import credentials 
from azure.storage.blob import BlobServiceClient
import pandas as pd 
from io import StringIO

"""
def upload_file_to_cloud(data):
  
  blob_service_client = BlobServiceClient.from_connection_string(credentials["connection_string"])
  container_client = blob_service_client.get_container_client(credentials["container_name"])
  csv_data = data.to_csv(index=False)
  blob_name = "final_dams_data.csv"  # Name of the blob (file) in Azure Blob Storage
  container_client.upload_blob(name=blob_name, data=csv_data, overwrite=True)
  """
def upload_file_to_cloud(data):
    # Establish a connection to the Blob service client
    blob_service_client = BlobServiceClient.from_connection_string(credentials["connection_string"])
    
    # Get the container client
    container_client = blob_service_client.get_container_client(credentials["container_name"])
    
    # Name of the blob (file) in Azure Blob Storage
    blob_name = "final_dams_data.csv"
    
    # Check if the blob exists and delete it if it does
    blob_client = container_client.get_blob_client(blob_name)
    if blob_client.exists():
        blob_client.delete_blob()
    
    # Convert the DataFrame to CSV format
    csv_data = data.to_csv(index=False)
    
    # Upload the new file, this will replace the old one
    container_client.upload_blob(name=blob_name, data=csv_data, overwrite=True)


#//////////////////////


def read_file_from_cloud():
    # Establish a connection to the Blob service client
    blob_service_client = BlobServiceClient.from_connection_string(credentials["connection_string"])
    
    # Get the container client
    container_client = blob_service_client.get_container_client(credentials["container_name"])
    
    # Specify the name of the blob (file) you want to read
    blob_name = "final_dams_data.csv"
    
    # Download the blob (file) as a string
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob().readall().decode('utf-8')
    
    # Convert the CSV data to a pandas DataFrame
    data = StringIO(blob_data)
    df = pd.read_csv(data)
    
    return df