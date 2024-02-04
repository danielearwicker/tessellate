namespace Tessellate;

using Parquet;
using Parquet.Serialization;

public record PreSortedStream<T, K>(
    Stream Stream,
    Func<T, K> SelectKey,
    int RecordsPerBatch = 100_000)
: ISortedStream<T, K> where T : notnull, new()
{    
    public async IAsyncEnumerable<T> Read()
    {
        if (Stream.Length == 0)
        {
            yield break;
        }

        var source = await ParquetReader.CreateAsync(Stream);
        var groups = source.RowGroupCount;

        for (var n = 0; n < groups; n++)
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

    private class Writer(PreSortedStream<T, K> target)
         : ITessellateWriter<T>
    {
        private readonly List<T> _buffer = [];

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
            _buffer.Add(value);

            if (_buffer.Count == target.RecordsPerBatch)
            {
                await Write(_buffer);                
                _buffer.Clear();
            }
        }

        public async ValueTask Complete()
        {
            if (_buffer.Count > 0)
            {
                await Write(_buffer);
            }

            await target.Stream.FlushAsync();
        }
    }
}
