namespace TestApp;

using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

public interface IProcessingBlob
{
    Task<bool> Download(Stream target);
    Task Upload(Stream source);
}

public interface IProcessingBlobs
{
    IProcessingBlob Get(string name);
}

public class ProcessingBlobs : IProcessingBlobs
{
    private readonly BlobContainerClient _container;
    private readonly Task _ensureContainerExists;
    private readonly ILogger<ProcessingBlobs> _logger;

    public ProcessingBlobs(string service, string container, ILogger<ProcessingBlobs> logger)
    {
        _logger = logger;

        var blobService = new BlobServiceClient(new Uri(service), new DefaultAzureCredential());

        _container = blobService.GetBlobContainerClient(container);
        _ensureContainerExists = _container.CreateIfNotExistsAsync();
    }

    public class Blob(BlobClient client, Task ensureContainerExists, ILogger logger) : IProcessingBlob
    {
        public async Task<bool> Download(Stream target)
        {
            if (!await client.ExistsAsync()) 
            {
                logger.LogInformation("Blob {name} does not exist", client.Name);
                return false;
            }

            await logger.Measure($"Downloading {client.Name}", () => client.DownloadToAsync(target));
            return true;
        }

        public async Task Upload(Stream source) 
        {
            await ensureContainerExists;
            await logger.Measure($"Uploading {client.Name}", () => client.UploadAsync(source, true));
        }
    }

    public IProcessingBlob Get(string name) => new Blob(
        _container.GetBlobClient($"{name}.parquet"), 
        _ensureContainerExists,
        _logger);
}
