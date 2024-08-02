using Microsoft.Extensions.Logging;
using Parquet.MapReduce.Util;
using Parquet.Serialization;

namespace Parquet.MapReduce;

public record ParquetProjectionOptions<SK, TK> : ParquetProjectionBaseOptions
{
    public IComparer<SK?> SourceKeyComparer { get; set; } = Comparer<SK?>.Default;

    public IComparer<TK?> TargetKeyComparer { get; set; } = Comparer<TK?>.Default;
}

public record ParquetProjectionBaseOptions
{
    public ILogger? Logger { get; set; }

    public string LoggingPrefix { get; set; } = "ParquetProjection";

    public Func<string, Stream> CreateTemporaryStream = _ => new TemporaryStream();

    public ParquetOptions? ParquetOptions { get; set; }

    public int RowsPerGroup { get; set; } = 100_000;

    public int GroupsPerBatch { get; set; } = 20;

    public IAsyncEnumerable<T> Read<T>(Stream stream, CancellationToken cancellation) where T : new()
        => stream.Length == 0 
            ? AsyncEnumerable.Empty<T>() 
            : ParquetSerializer.DeserializeAllAsync<T>(stream, ParquetOptions, cancellation);
}

