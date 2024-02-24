namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class InvoiceDeJour : BucketValues
{
    [ParquetRequired]
    public string UniqueId { get; set; } = string.Empty;

    [ParquetDecimal(19, 5)]
    public decimal BaseAmount { get; set; }

    [ParquetRequired]
    public string SupplierRef { get; set; } = string.Empty;

    [ParquetRequired]
    public string OrgGroupName { get; set; } = string.Empty;

    [ParquetRequired]
    public string EnteredBy { get; set; } = string.Empty;
}
