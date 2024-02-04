namespace Tessellate;

using Parquet;
using Parquet.Serialization;

public record PartiallySortedStream<T, K>(
    Stream Stream,
    Func<T, K> SelectKey,
    int RecordsPerBatch = 100_000, 
    int BatchesPerPartition = 100)
: ISortedStream<T, K> where T : notnull, new()
{
    public int RecordsPerPartition => RecordsPerBatch * BatchesPerPartition;

    public async IAsyncEnumerable<T> Read()
    {
        if (Stream.Length == 0)
        {
            yield break;
        }

        var source = await ParquetReader.CreateAsync(Stream);

        var partitions = Math.Ceiling(source.RowGroupCount / (double)BatchesPerPartition);

        var queue = new PriorityQueue<IAsyncEnumerator<T>, K>();

        for (var n = 0; n < partitions; n++)
        {
            var start = n * BatchesPerPartition;
            var end = Math.Min(source.RowGroupCount, start + BatchesPerPartition);

            var range = ReadRange(source, start, end).GetAsyncEnumerator();

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

    public ITessellateWriter<T> Write() => new Writer(this);

    private class Writer(PartiallySortedStream<T, K> target)
        : ITessellateWriter<T> 
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

                if (batch.Count == target.RecordsPerBatch)
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

            _buffers.Clear();
            _buffers.Add([]);
            _recordsAdded = 0;
        }
    }   
}
