namespace TestApp.Models;

using Parquet.Serialization.Attributes;

public class PairWithScore
{
    public byte MinBucketId { get; set; }

    [ParquetRequired]
    public string LeaderId { get; set; } = string.Empty;

    [ParquetRequired]
    public string NonLeaderId { get; set; } = string.Empty;

    public long Score { get; set; }
}
