import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

storage_account_name = "demorvs"
storage_account_key = "C02yZ3Agi3a4doN8cC08hEAJceDSwbx1Ie3rAvqmycRrNioKcc4vqoZrj9OD5Bl0+ntv1vsGJJTPxs8tjhJCkQ=="


class HelloDataLake:

    def __init__(self):
        super().__init__()
        self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        self.local_path = "data"

    def upload_blob(self, container_name, blob_name, local_file_name="superfile.csv"):
        try:
            upload_file_path = os.path.join(self.local_path, local_file_name)
            full_path = os.path.abspath(upload_file_path)

            file_system_client = self.service_client.get_file_system_client(file_system=container_name)

            directory_client = file_system_client.get_directory_client("my-directory")

            file_client = directory_client.create_file(blob_name)

            local_file = open(full_path, 'rb')

            file_contents = local_file.read()

            # file_client.upload_data(file_contents, overwrite=True)

            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))

        except Exception as e:
            print(e)

    def download_blob(self, container_name, blob_name, local_file_name):
        try:
            download_file_path = os.path.join(self.local_path, local_file_name)
            full_path = os.path.abspath(download_file_path)

            file_system_client = self.service_client.get_file_system_client(file_system=container_name)

            directory_client = file_system_client.get_directory_client("my-directory")

            local_file = open(full_path, 'wb')

            file_client = directory_client.get_file_client(blob_name)

            download = file_client.download_file()

            downloaded_bytes = download.readall()

            local_file.write(downloaded_bytes)

            local_file.close()

        except Exception as e:
            print(e)


try:

    data_lake = HelloDataLake()
    # Create a unique name for the container
    container_name = "trunk"
    data_lake.upload_blob(container_name=container_name, blob_name="data.csv", local_file_name="superfile_dl.csv")
    data_lake.download_blob(container_name=container_name, blob_name="data.csv", local_file_name="download_data_dl.csv")

    print("Done...")

    # Quick start code goes here
except Exception as ex:
    print('Exception:')
    print(ex)
