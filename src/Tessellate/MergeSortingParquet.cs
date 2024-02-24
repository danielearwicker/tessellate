namespace Tessellate;

using Parquet;
using Parquet.Serialization;

/// <summary>
/// A Parquet file to which records can be written in any order, and when
/// later read the records will be ordered by the provided key. 
/// 
/// During writing, a set of rows are buffered in memory until there are 
/// enough to make a partition. They are sorted by the key and then
/// written as a set of Parquet row groups.
/// 
/// This process continues until all rows are written, and the resulting
/// file consists of one or more partitions (each made up of several row
/// groups), and the rows are correctly sorted within each partition.
/// 
/// When reading the file, a merge sort is used to read row groups from
/// all the partitions, so that the rows are merged into fully sorted 
/// order across the entire file.
/// </summary>
/// <typeparam name="T">The row type</typeparam>
/// <typeparam name="K">The sort key type</typeparam>
/// <param name="Stream">The stream that stores the Parquet data</param>
/// <param name="SelectKey">Function that selects the sort key from a record</param>
/// <param name="RowsPerGroup">Number of records per Parquet row group</param>
/// <param name="RowGroupsPerPartition">Number of row groups per sorted partition</param>
public record MergeSortingParquet<T, K>(
    Stream Stream,
    Func<T, K> SelectKey,
    int RowsPerGroup = 100_000, 
    int RowGroupsPerPartition = 100)
: ISortedTable<T, K> where T : notnull, new()
{
    public int RecordsPerPartition => RowsPerGroup * RowGroupsPerPartition;

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellation)
    {
        if (Stream.Length == 0)
        {
            yield break;
        }

        var source = await ParquetReader.CreateAsync(Stream, cancellationToken: cancellation);

        var partitions = Math.Ceiling(source.RowGroupCount / (double)RowGroupsPerPartition);

        var queue = new PriorityQueue<IAsyncEnumerator<T>, K>();

        for (var n = 0; n < partitions; n++)
        {
            var start = n * RowGroupsPerPartition;
            var end = Math.Min(source.RowGroupCount, start + RowGroupsPerPartition);

            var range = ReadRange(source, start, end).GetAsyncEnumerator(cancellation);

            if (await range.MoveNextAsync())
            {
                queue.Enqueue(range, SelectKey(range.Current));
            }
        }

        while (queue.Count != 0)
        {
            var next = queue.Dequeue();

            yield return next.Current;

            if (await next.MoveNextAsync())
            {
                queue.Enqueue(next, SelectKey(next.Current));
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

    public ITableWriter<T> Write() => new Writer(this);

    private class Writer(MergeSortingParquet<T, K> target)
        : ITableWriter<T> 
    {
        private readonly List<List<(K, T)>> _buffers = [[]];

        private int _recordsAdded = 0;

        private bool _appending = false;
        
        protected async Task Write(IEnumerable<T> rows)
        {
            await ParquetSerializer.SerializeAsync(
                        rows,
                        target.Stream,
                        new ParquetSerializerOptions { Append = _appending });

            _appending = true;
        }

        public async ValueTask Add(T value)
        {        
            if (_recordsAdded == target.RecordsPerPartition)
            {
                await Complete();
            }
            
            var buffer = _buffers[^1];
            buffer.Add((target.SelectKey(value), value));

            if (buffer.Count == target.RecordsPerPartition / 10) 
            {
                _buffers.Add([]);
            }

            _recordsAdded++;
        }

        public async ValueTask Complete()
        {
            if (_buffers[0].Count == 0) return;

            _buffers.AsParallel().ForAll(buffer =>
            {
                buffer.Sort((x, y) => Comparer<K>.Default.Compare(x.Item1, y.Item1));    
            });

            var queue = new PriorityQueue<IEnumerator<(K, T)>, K>();        
        
            foreach (var buffer in _buffers)
            {
                var en = buffer.GetEnumerator();
                if (en.MoveNext())
                {
                    queue.Enqueue(en, en.Current.Item1);
                }
            }

            var batch = new List<T>();

            while (queue.TryDequeue(out var en, out var k))
            {
                batch.Add(en.Current.Item2);

                if (en.MoveNext())
                {
                    queue.Enqueue(en, en.Current.Item1);
                }

                if (batch.Count == target.RowsPerGroup)
                {
                    await Write(batch);
                    batch.Clear();
                }
            }

            if (batch.Count != 0)
            {
                await Write(batch);            
            }

            await target.Stream.FlushAsync();
            target.Stream.Position = 0;

            _buffers.Clear();
            _buffers.Add([]);
            _recordsAdded = 0;
        }
    }   
}
