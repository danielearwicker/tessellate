namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class UniqueIdByInternalId : InternalIdOnly
{
    [ParquetRequired]
    public string UniqueId { get; set; } = string.Empty;
}
