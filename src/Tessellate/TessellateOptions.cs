namespace Tessellate;

public record TessellateOptions(int RecordsPerBatch = 100_000, int BatchesPerPartition = 100)
{
    public int RecordsPerPartition => RecordsPerBatch * BatchesPerPartition;
}
