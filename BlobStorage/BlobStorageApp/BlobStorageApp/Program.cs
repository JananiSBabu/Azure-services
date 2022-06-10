using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace BlobStorageApp
{
    class Program
    {
        // Copy the connection string from the portal in the variable below.
        private const string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=blobstorageinnovation;AccountKey=ro3vyXRnFdD0cC5uyOLRawUB6S9HgwndLfCCV7orBuxf4R8GRnOW3VtlpkkrTPlQ8SVu3xI6vlNm+AStUHTQFg==;EndpointSuffix=core.windows.net";

        private static BlobServiceClient blobServiceClient;
        private static BlobContainerClient containerClient;

        public static async Task Main()
        {
            Console.WriteLine("Azure Blob Storage exercise\n");

            await CreateContainerAsync();

            // Run the examples asynchronously, wait for the results before proceeding
            // ProcessAsync().GetAwaiter().GetResult();

            BlobUploadOptions options = setoptions();

            // Sequential upload
            await UploadLargeFiles();

            // Parallel upload
            await ParalleUploadLargeFiles(options);
            
            Console.ReadLine();

        }

        private static async Task CreateContainerAsync()
        {
            // Create a client that can authenticate with a connection string
            blobServiceClient = new BlobServiceClient(storageConnectionString);

            // Create the container and return a container client object
            string containerName = "testblobcontainer1";
            containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync();

            Console.WriteLine("A container named '" + containerName + "' has been created. ");
        }

        private static async Task ProcessAsync()
        {
            

            // Create a local file in the ./data/ directory for uploading and downloading
            string fileName = "testfile1.txt";
            string localFilePath = $"C:\\_CodeRepository\\test_data_azure\\testfile1.txt";

            // Get a reference to the blob
            BlobClient blobClient = containerClient.GetBlobClient(fileName);

            Console.WriteLine("Uploading to Blob storage as blob:\n\t {0}\n", blobClient.Uri);

            // Open the file and upload its data
            using FileStream uploadFileStream = File.OpenRead(localFilePath);
            BlobUploadOptions options = setoptions();

            await blobClient.UploadAsync(uploadFileStream, options);
            uploadFileStream.Close();

            Console.WriteLine("\nThe file was uploaded.");

        }

        private static async Task UploadLargeFiles()
        {
            // Path to the directory to upload
            string uploadPath = $"C:\\_CodeRepository\\test_data_azure\\ToUpload";

            // Start a timer to measure how long it takes to upload all the files.
            Stopwatch timer = Stopwatch.StartNew();

            try
            {
                Console.WriteLine($"Iterating in directory: {uploadPath}");
                int count = 0;

                Console.WriteLine($"Found {Directory.GetFiles(uploadPath).Length} file(s)");

                // Create a queue of tasks that will each upload one file.
                var tasks = new Queue<Task<Response<BlobContentInfo>>>();

                // Iterate through the files
                foreach (string filePath in Directory.GetFiles(uploadPath))
                {
                    //BlobContainerClient container = containers[count % 5];
                    string fileName = Path.GetFileName(filePath);
                    Console.WriteLine($"Uploading {fileName} to container {containerClient.Name}");
                    BlobClient blob = containerClient.GetBlobClient(fileName);

                    // Add the upload task to the queue
                    tasks.Enqueue(blob.UploadAsync(filePath));
                    count++;
                }

                // Run all the tasks asynchronously.
                await Task.WhenAll(tasks);

                timer.Stop();
                Console.WriteLine($"Uploaded {count} files in {timer.Elapsed.TotalSeconds} seconds");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Azure request failed: {ex.Message}");
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.WriteLine($"Error parsing files in the directory: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }

        }

        private static async Task ParalleUploadLargeFiles(BlobUploadOptions options)
        {
            // Path to the directory to upload
            string uploadPath = $"C:\\_CodeRepository\\test_data_azure\\ToUploadParallel";

            // Start a timer to measure how long it takes to upload all the files.
            Stopwatch timer = Stopwatch.StartNew();

            try
            {
                Console.WriteLine($"Iterating in directory: {uploadPath}");
                int count = 0;

                Console.WriteLine($"Found {Directory.GetFiles(uploadPath).Length} file(s)");

                // Create a queue of tasks that will each upload one file.
                var tasks = new Queue<Task<Response<BlobContentInfo>>>();

                // Iterate through the files
                foreach (string filePath in Directory.GetFiles(uploadPath))
                {
                    //BlobContainerClient container = containers[count % 5];
                    string fileName = Path.GetFileName(filePath);
                    Console.WriteLine($"Uploading {fileName} to container {containerClient.Name}");
                    BlobClient blob = containerClient.GetBlobClient(fileName);

                    // Add the upload task to the queue
                    if (options != null)
                    {
                        // parallel upload
                        tasks.Enqueue(blob.UploadAsync(filePath, options));
                    }
                    
                    count++;
                }

                // Run all the tasks asynchronously.
                await Task.WhenAll(tasks);

                timer.Stop();
                Console.WriteLine($"Uploaded {count} files in {timer.Elapsed.TotalSeconds} seconds");
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Azure request failed: {ex.Message}");
            }
            catch (DirectoryNotFoundException ex)
            {
                Console.WriteLine($"Error parsing files in the directory: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }

        }


        private static BlobUploadOptions setoptions()
        {
            BlobUploadOptions options = new BlobUploadOptions
            {
                TransferOptions = new StorageTransferOptions
                {
                    // Set the maximum number of workers that
                    // may be used in a parallel transfer.
                    MaximumConcurrency = 8,

                    // Set the maximum length of a transfer to 50MB.
                    MaximumTransferSize = 50 * 1024 * 1024
                }
            };
            return options;
        }
    }
}
