namespace Tessellate;

using Microsoft.Extensions.Logging;
using Parquet;
using Parquet.Serialization;

public interface ITessellateView<T, K> where T : notnull, new()
{
    string Name { get; }

    K GetKey(T row);

    IAsyncEnumerable<T> Read();
}

public class TessellateView<T, K>(
    ITessellateFile file,
    Func<T, K> selectKey,
    TessellateOptions options,
    ILogger logger
) : ITessellateView<T, K> where T : notnull, new()
{
    protected ITessellateFile file = file;
    protected Func<T, K> selectKey = selectKey;
    protected TessellateOptions options = options;
    protected ILogger logger = logger;

    public string Name => file.Name;

    public K GetKey(T row) => selectKey(row);

    public async IAsyncEnumerable<T> Read()
    {
        using var stream = file.Read();

        if (stream.Length == 0)
        {
            yield break;
        }

        var source = await ParquetReader.CreateAsync(stream);

        var partitions = Math.Ceiling(source.RowGroupCount / (double)options.BatchesPerPartition);

        logger.LogInformation("Reading from {partitions} partitions of {name}", partitions, file.Name);

        var queue = new PriorityQueue<IAsyncEnumerator<T>, K>();

        for (var n = 0; n < partitions; n++)
        {
            var start = n * options.BatchesPerPartition;
            var end = Math.Min(source.RowGroupCount, start + options.BatchesPerPartition);

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

    private static async IAsyncEnumerable<T> ReadRange(ParquetReader source, int from, int to)
    {
        for (var n = from; n < to; n++)
        {            
            using var rowGroupReader = source.OpenRowGroupReader(n);
            var rows = await ParquetSerializer.DeserializeAsync<T>(rowGroupReader, source.Schema);

            foreach (var row in rows)
            {
                yield return row;
            }
        }
    }
}
