namespace Tessellate;

using Microsoft.Extensions.Logging;

public interface ITessellateSorter<T> where T : notnull, new()
{
    IAsyncEnumerable<T> Read(ITessellateSource<T> source);

    ITessellateSorterWriter<T> Write(ITessellateTarget<T> target);
}

public class TessellateSorter<K, T>(ILogger logger, Func<T, K> selectKey, TessellateSorterOptions options)
    : ITessellateSorter<T> where T : notnull, new()
{
    public async IAsyncEnumerable<T> Read(ITessellateSource<T> source)
    {
        var partitions = Math.Ceiling(source.BatchCount / (double)options.BatchesPerPartition);

        logger.LogInformation("Reading from {partitions} partitions", partitions);

        var queue = new PriorityQueue<IAsyncEnumerator<T>, K>();

        for (var n = 0; n < partitions; n++)
        {
            var start = n * options.BatchesPerPartition;
            var end = Math.Min(source.BatchCount, start + options.BatchesPerPartition);

            var range = ReadRange(source, start, end).GetAsyncEnumerator();

            if (await range.MoveNextAsync())
            {
                queue.Enqueue(range, selectKey(range.Current));
            }
        }

        while (queue.Count != 0)
        {
            var next = queue.Dequeue();

            yield return next.Current;

            if (await next.MoveNextAsync())
            {
                queue.Enqueue(next, selectKey(next.Current));
            }
            else
            {
                logger.LogInformation("Partition complete, {partitions} partitions remaining", queue.Count);
            }
        }
    }

    private static async IAsyncEnumerable<T> ReadRange(ITessellateSource<T> source, int from, int to)
    {
        for (var n = from; n < to; n++)
        {            
            var rows = await source.Read(n);

            foreach (var row in rows)
            {
                yield return row;
            }
        }
    }

    public ITessellateSorterWriter<T> Write(ITessellateTarget<T> target)
        => new TessellateSorterWriter<K, T>(logger, target, selectKey, options);    
}
