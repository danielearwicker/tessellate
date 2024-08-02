﻿using Parquet.Serialization;

namespace Parquet.MapReduce.Parquet;

public class BufferedWriter<T>(
    Stream Stream,
    int RowsPerGroup,
    ParquetOptions? parquetOptions = null)
{
    private readonly List<T> _buffer = [];

    private bool _append = false;

    public async ValueTask Add(T record)
    {
        _buffer.Add(record);
        if (_buffer.Count >= RowsPerGroup) await Finish();
    }

    public async ValueTask Finish()
    {
        if (_buffer.Count == 0) return;

        await ParquetSerializer.SerializeAsync(_buffer, Stream, new ParquetSerializerOptions
        {
            Append = _append,
            RowGroupSize = RowsPerGroup,
            ParquetOptions = parquetOptions,
        });

        _buffer.Clear();

        Stream.Position = 0;

        _append = true;
    }

    public async ValueTask AddRange(IAsyncEnumerable<T> source)
    {
        await foreach (var item in source)
        {
            await Add(item);
        }
    }
}

