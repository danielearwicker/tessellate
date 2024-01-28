using Parquet;
using Parquet.Serialization;

namespace Tessellate;

public class TessellateParquetFormat : ITessellateFormat
{
    public string Extension => ".parquet";

    public async Task<ITessellateSource<T>> GetSource<T>(Stream inputStream) where T : new()
        => inputStream.Length == 0 
            ? new EmptyParquetSource<T>() 
            : new ParquetSource<T>(await ParquetReader.CreateAsync(inputStream));

    public Task<ITessellateTarget<T>> GetTarget<T>(Stream outputStream)
        => Task.FromResult<ITessellateTarget<T>>(new ParquetTarget<T>(outputStream));

    private class EmptyParquetSource<T>() : ITessellateSource<T> where T : new()
    {
        public int BatchCount => 0;

        public Task<IList<T>> Read(int batch) => Task.FromException<IList<T>>(
            new InvalidOperationException("Source is empty"));
    }

    private class ParquetSource<T>(ParquetReader reader) : ITessellateSource<T> where T : new()
    {
        public int BatchCount => reader.RowGroupCount;

        public async Task<IList<T>> Read(int batch)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(batch);
            return await ParquetSerializer.DeserializeAsync<T>(rowGroupReader, reader.Schema);
        }
    }

    private class ParquetTarget<T>(Stream stream) : ITessellateTarget<T>
    {
        private bool _appending = false;

        public async ValueTask DisposeAsync()
        {
            await stream.FlushAsync();
            await stream.DisposeAsync();
        }

        public async Task Write(IEnumerable<T> rows)
        {
            await ParquetSerializer.SerializeAsync(
                        rows,
                        stream,
                        new ParquetSerializerOptions { Append = _appending });

            _appending = true;
        }
    }
}
