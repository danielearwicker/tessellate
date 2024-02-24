namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class BucketKey
{
    public byte BucketId { get; set; }

    [ParquetRequired]
    public string Key { get; set; } = string.Empty;

    public int InvoiceIndex { get; set; }
}
