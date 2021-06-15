using Azure.Storage.Files.DataLake;
using Storage.BlobStorage;
using Storage.DataLake;
using System;
using System.Threading.Tasks;

namespace Storage
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            string accountName = "demorvs";
            string accountKey = "C02yZ3Agi3a4doN8cC08hEAJceDSwbx1Ie3rAvqmycRrNioKcc4vqoZrj9OD5Bl0+ntv1vsGJJTPxs8tjhJCkQ==";
            string storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=demorvsv1;AccountKey=08gxrFlQjgclRZ6eyd56JGS8hgS0S0/cjINQRJcmqMS/VhBCpcWIzocBQP5/oiExN7qvE6LKwS4tc9hcbPPK7w==;EndpointSuffix=core.windows.net";
            Console.Write("1. DataLake\n2. Blob Storage\n > ");
            int option = Convert.ToInt32(Console.ReadLine());

            switch (option)
            {
                case 1:
                    {
                        CustomDataLake dataLakeDemo = new();
                        dataLakeDemo.GetDataLakeServiceClient(accountName, accountKey);
                        DataLakeFileSystemClient filesystem = dataLakeDemo.DataLakeServiceClient.GetFileSystemClient("trunk");
                        await dataLakeDemo.UploadFile(filesystem);
                        Console.WriteLine("Upload done....");
                        Console.WriteLine("Press any key to download...");
                        Console.ReadKey();

                        await dataLakeDemo.DownloadFile(filesystem);
                        Console.WriteLine("Download done....");

                        break;
                    }
                case 2:
                    {
                        CustomStorage customStorage = new(storageAccountConnectionString);
                        await customStorage.UploadBlob("trunk");

                        Console.WriteLine("Upload done....");
                        Console.WriteLine("Press any key to download...");
                        Console.ReadKey();

                        await customStorage.DownloadBlob("trunk");
                        Console.WriteLine("Download done....");

                        break;
                    }
                default:
                    break;
            }

            Console.WriteLine("Example done...");
            Console.WriteLine("Press any key to stop...");
            Console.ReadKey();
        }
    }
}
