namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class BucketValues
{
    public int InternalId { get; set; }

    [ParquetRequired]
    public string InvoiceNumber { get; set; } = string.Empty;

    [ParquetDecimal(19, 5)]
    public decimal InvoiceAmount { get; set; }

    [ParquetTimestamp]
    public DateTime InvoiceDate { get; set; }

    [ParquetRequired]
    public string SupplierName { get; set; } = string.Empty;
}
