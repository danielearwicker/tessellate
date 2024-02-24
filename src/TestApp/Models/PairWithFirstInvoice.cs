namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class PairWithFirstInvoice
{
    public byte MinBucketId { get; set; }

    [ParquetRequired]
    public InvoiceDeJour First { get; set; } = new();

    public int SecondId { get; set; }
}
