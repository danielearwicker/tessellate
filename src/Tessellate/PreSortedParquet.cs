namespace Tessellate;

using Parquet;
using Parquet.Serialization;

/// <summary>
/// A Parquet file in which the rows are sorted by the specified key
/// across the entire file. It is the responsibility of the writer to
/// ensure that the rows are written in the correct order; this class
/// only uses the <paramref name="SelectKey"/> to validate that rows
/// are not being provided out of order. If they are not, an exception
/// will be thrown during writing.
/// </summary>
/// <typeparam name="T">The row type</typeparam>
/// <typeparam name="K">The sort key type</typeparam>
/// <param name="Stream">The stream that stores the Parquet data</param>
/// <param name="SelectKey">Function that selects the sort key from a record</param>
/// <param name="RowsPerGroup">Number of records per Parquet row group</param>
public record PreSortedParquet<T, K>(
    Stream Stream,
    Func<T, K> SelectKey,
    int RowsPerGroup = 100_000)
: ISortedTable<T, K> where T : notnull, new()
{
    public ISortedView<T2, K> Cast<T2>(Func<T2, K> selectKey) where T2 : notnull, new()
        => new PreSortedParquet<T2, K>(Stream, selectKey, RowsPerGroup);

    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellation)
    {
        if (Stream.Length == 0)
        {
            yield break;
        }

        var source = await ParquetReader.CreateAsync(Stream, cancellationToken: cancellation);
        var groups = source.RowGroupCount;

        for (var n = 0; n < groups; n++)
        {
            using var rowGroupReader = source.OpenRowGroupReader(n);

            var rows = await ParquetSerializer.DeserializeAsync<T>(rowGroupReader, source.Schema, cancellation);

            foreach (var row in rows)
            {
                yield return row;
            }
        }
    }

    public ITableWriter<T> Write() => new Writer(this);

    private class Writer(PreSortedParquet<T, K> target)
         : ITableWriter<T>
    {
        private readonly List<T> _buffer = [];

        private T? _latest;

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
            if (_latest != null)
            {
                if (Comparer<K>.Default.Compare(target.SelectKey(value), target.SelectKey(_latest)) < 0)
                {
                    throw new InvalidOperationException("Records must be pre-sorted by key");
                }
            }

            _latest = value;
            _buffer.Add(value);

            if (_buffer.Count == target.RowsPerGroup)
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
            target.Stream.Position = 0;
        }
    }
}
