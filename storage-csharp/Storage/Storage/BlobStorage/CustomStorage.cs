using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storage.BlobStorage
{
    public class CustomStorage
    {

        public BlobServiceClient BlobServiceClient { get; set; }

        public CustomStorage(string connectionString)
        {
            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient = new BlobServiceClient(connectionString);
        }

        public async Task UploadBlob(string containerName)
        {
            // Create the container and return a container client object
            BlobContainerClient containerClient = BlobServiceClient.GetBlobContainerClient(containerName);

            // Create a local file in the ./data/ directory for uploading and downloading
            string localPath = "./data/";
            string fileName = "data.csv";
            string localFilePath = Path.Combine(localPath, fileName);

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient("2020/superfile.csv");

            Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);

            // Open the file and upload its data
            using FileStream uploadFileStream = File.OpenRead(localFilePath);
            await blobClient.UploadAsync(uploadFileStream, true);
            uploadFileStream.Close();
        }

        public async Task DownloadBlob(string containerName)
        {
            // Create the container and return a container client object
            BlobContainerClient containerClient = BlobServiceClient.GetBlobContainerClient(containerName);

            // Create a local file in the ./data/ directory for uploading and downloading
            string localPath = "./data/";
            string fileName = "download_storage.csv";
            string localFilePath = Path.Combine(localPath, fileName);

            // Get a reference to a blob
            BlobClient blobClient = containerClient.GetBlobClient("2020/superfile.csv");

            Console.WriteLine("\nDownloading blob to\n\t{0}\n", localFilePath);

            // Download the blob's contents and save it to a file
            BlobDownloadInfo download = await blobClient.DownloadAsync();

            using (FileStream downloadFileStream = File.OpenWrite(localFilePath))
            {
                await download.Content.CopyToAsync(downloadFileStream);
                downloadFileStream.Close();
            }
        }
    }
}
