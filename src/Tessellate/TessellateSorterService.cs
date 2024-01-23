namespace Tessellate;

using Microsoft.Extensions.Logging;

public record TessellateSorterOptions(int RecordsPerBatch = 100_000, int BatchesPerPartition = 100)
{
    public int RecordsPerPartition => RecordsPerBatch * BatchesPerPartition;
}

public interface ITessellateSorterService
{
    ITessellateSorter<T> GetSorter<K, T>(Func<T, K> selectKey, TessellateSorterOptions? options = null) 
        where T : notnull, new();
}

public class TessellateSorterService(ILogger logger) : ITessellateSorterService
{
    public ITessellateSorter<T> GetSorter<K, T>(Func<T, K> selectKey, TessellateSorterOptions? options = null) where T : notnull, new()
        => new TessellateSorter<K, T>(logger, selectKey, options ?? new TessellateSorterOptions());
}
