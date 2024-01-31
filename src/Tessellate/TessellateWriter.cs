namespace Tessellate;

using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Serialization;

public interface ITessellateWriter<V> : IAsyncDisposable
    where V : notnull
{
    ValueTask Add(V value);
}

internal class TessellateWriter<K, T>(
    ILogger logger,
    Stream target, 
    Func<T, K> selectKey,
    TessellateOptions options,
    string targetName
) : ITessellateWriter<T> where T : notnull
{
    private readonly List<List<(K, T)>> _buffers = [[]];

    private int _recordsAdded = 0;
    private bool _appending = false;

    private readonly Stopwatch _timer = new();

    public async ValueTask Add(T value)
    {
        if (!_timer.IsRunning)
        {
            _timer.Start();
        }

        if (_recordsAdded == options.RecordsPerPartition)
        {
            await Flush();
        }
        
        var buffer = _buffers[^1];
        buffer.Add((selectKey(value), value));

        if (buffer.Count == options.RecordsPerPartition / 10) 
        {
            _buffers.Add([]);
        }

        _recordsAdded++;
    }

    private async ValueTask Flush()
    {
        if (_buffers[0].Count == 0) return;

        await LogTiming("Sorting buffers", () => 
        {
            logger.LogInformation("Sorting {count} buffers", _buffers.Count);

            _buffers.AsParallel().ForAll(buffer =>
            {
                buffer.Sort((x, y) => Comparer<K>.Default.Compare(x.Item1, y.Item1));    
            });

            return Task.CompletedTask;
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

            if (batch.Count == options.RecordsPerBatch)
            {
                await LogTiming("Saving batch", async () => 
                {   
                    await Write(batch);
                });

                batch.Clear();
            }
        }

        if (batch.Count != 0)
        {
            await LogTiming("Saving leftover batch", async () => 
            {   
                await Write(batch);
            });
        }

        _buffers.Clear();
        _buffers.Add([]);
        _recordsAdded = 0;
    }

    private async ValueTask LogTiming(string operation, Func<Task> task)
    {
        var timer = new Stopwatch();
        timer.Start();
        await task();
        timer.Stop();

        logger.LogInformation("Timing for {operation}: {seconds} seconds", 
                              operation, timer.Elapsed.TotalSeconds);
    }

    public async ValueTask DisposeAsync()
    {
        await Flush();
        await target.DisposeAsync();

        logger.LogInformation("Write timing for {name}: {seconds} seconds", 
                              targetName, _timer.Elapsed.TotalSeconds);
    }

    public async Task Write(IEnumerable<T> rows)
    {
        await ParquetSerializer.SerializeAsync(
                    rows,
                    target,
                    new ParquetSerializerOptions { Append = _appending });

        _appending = true;
    }
}
