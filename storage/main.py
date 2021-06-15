import os
import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


# connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
connect_str = "DefaultEndpointsProtocol=https;AccountName=demorvsv1;AccountKey=08gxrFlQjgclRZ6eyd56JGS8hgS0S0/cjINQRJcmqMS/VhBCpcWIzocBQP5/oiExN7qvE6LKwS4tc9hcbPPK7w==;EndpointSuffix=core.windows.net"


class HelloBlob:

    def __init__(self):
        super().__init__()
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connect_str)
        self.local_path = "data"

    def upload_blob(self, container_name, local_file_name="superfile.csv"):
        upload_file_path = os.path.join(self.local_path, local_file_name)

        # Create a blob client using the local file name as the name for the blob
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=local_file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

    def download_blob(self, container_name, blob_name, local_file_name):
        # Download the blob to a local file
        # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
        blob_client = self.blob_service_client.get_blob_client(
            container=container_name, blob=blob_name)

        download_file_path = os.path.join(self.local_path, local_file_name)
        print("\nDownloading blob to \n\t" + download_file_path)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())


try:
    print("Azure Blob storage v" + __version__ + " - Python quickstart sample")

    blob_storage = HelloBlob()
    # Create a unique name for the container
    container_name = "trunk/2020"
    blob_storage.upload_blob(container_name=container_name)
    blob_storage.download_blob(container_name=container_name,
                               blob_name="superfile.csv", local_file_name="download_data_storage.csv")

    # Quick start code goes here
except Exception as ex:
    print('Exception:')
    print(ex)
