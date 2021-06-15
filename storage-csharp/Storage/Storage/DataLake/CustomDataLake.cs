using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.DataLake
{
    public class CustomDataLake
    {
        public DataLakeServiceClient DataLakeServiceClient { get; set; }
        public void GetDataLakeServiceClient(string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient = new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);
        }

        public void GetDataLakeServiceClient(string accountName, string clientID, string clientSecret, string tenantID)
        {

            TokenCredential credential = new ClientSecretCredential(
                tenantID, clientID, clientSecret, new TokenCredentialOptions());

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            DataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), credential);
        }

        public async Task UploadFile(DataLakeFileSystemClient fileSystemClient)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(DateTime.Now.ToString("yyyy/MM/dd/HH/mm"));

            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync("data.csv");

            FileStream fileStream =
                File.OpenRead("data/superfile_dl.csv");

            long fileSize = fileStream.Length;

            await fileClient.AppendAsync(fileStream, offset: 0);

            await fileClient.FlushAsync(position: fileSize);

        }

        public async Task UploadFileBulk(DataLakeFileSystemClient fileSystemClient)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient($"bulk/{DateTime.Now.ToString("yyyy/MM/dd/HH/mm")}");

            DataLakeFileClient fileClient = directoryClient.GetFileClient("data.csv");

            FileStream fileStream =
                File.OpenRead("data/superfile_dl.csv");

            await fileClient.UploadAsync(fileStream);
        }

        public async Task DownloadFile(DataLakeFileSystemClient fileSystemClient)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient($"2021/{DateTime.Now.ToString("MM/dd/HH/mm")}");

            DataLakeFileClient fileClient =
                directoryClient.GetFileClient("data.csv");

            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();

            BinaryReader reader = new(downloadResponse.Value.Content);

            FileStream fileStream =
                File.OpenWrite("data/download_data_dl.csv");

            int bufferSize = 4096;

            byte[] buffer = new byte[bufferSize];

            int count;

            while ((count = reader.Read(buffer, 0, buffer.Length)) != 0)
            {
                fileStream.Write(buffer, 0, count);
            }

            await fileStream.FlushAsync();

            fileStream.Close();
        }
    }
}
