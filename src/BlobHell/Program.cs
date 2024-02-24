

using System.Text.Json;
using Azure.Storage.Blobs;
using Dapper;
using DuckDB.NET.Data;

var azureStorageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");

// var client = new BlobServiceClient(azureStorageConnectionString);
// var blobContainer = client.GetBlobContainerClient("bits");
// var blob = blobContainer.GetBlobClient("biggo.parquet");
// await blob.UploadAsync("../TestApp/output/invoices-by-uid.parquet");

using var connection = new DuckDBConnection("DataSource=data.db;memory_limit=1GB");
connection.Open();

await connection.ExecuteAsync($@"
    INSTALL azure;
    LOAD azure;
    SET azure_storage_connection_string='{azureStorageConnectionString}';
    SET temp_directory = '/tmp/duckdb';
    CREATE VIEW biggo AS SELECT * FROM 'azure://bits/biggo.parquet';
");

var indented = new JsonSerializerOptions { WriteIndented = true };

string? line;
while ((line = Console.ReadLine()) != null)
{
    try
    {
        var result = connection.Query(line);
        
        if (line.ToLower().StartsWith("explain"))
        {
            Console.WriteLine(result.First().explain_value);
        }
        else
        {
            var json = JsonSerializer.Serialize(result, indented);
            Console.WriteLine(json);
        }
    }
    catch (Exception x)
    {
        Console.WriteLine(x.Message);
    }
}

// EXPLAIN select b1.UniqueId, b2.UniqueId from biggo b1 join biggo b2 on b1.InvoiceAmount = b2.BaseAmount
// select b1.UniqueId, b2.UniqueId from biggo b1 join biggo b2 on b1.InvoiceAmount = b2.BaseAmount limit 10000;
