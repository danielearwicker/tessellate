namespace Tessellate;

using Microsoft.Extensions.Logging;

public interface ITessellateTable<T, K> where T : notnull, new()
{
    string Name { get; }

    K GetKey(T row);

    IAsyncEnumerable<T> Read();

    Task<ITessellateWriter<T>> Write();
}

public class TessellateTable<T, K>(
    ITessellateFile file,
    Func<T, K> selectKey,
    TessellateOptions options,
    ITessellateFormat format,
    ILogger logger
) : ITessellateTable<T, K> where T : notnull, new()
{
    public string Name => file.Name;

    public K GetKey(T row) => selectKey(row);

    public async IAsyncEnumerable<T> Read()
    {
        using var stream = file.Read();

        var source = await format.GetSource<T>(stream);

        var partitions = Math.Ceiling(source.BatchCount / (double)options.BatchesPerPartition);

        logger.LogInformation("Reading from {partitions} partitions of {name}", partitions, file.Name);

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

    public async Task<ITessellateWriter<T>> Write()
    {
        logger.LogInformation("Writing to {name}", file.Name);

        return new TessellateWriter<K, T>(logger, 
            await format.GetTarget<T>(file.Write()), selectKey, options);
    }
}
