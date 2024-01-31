namespace Tessellate;

using Microsoft.Extensions.Logging;

public interface ITessellateTable<T, K> : ITessellateView<T, K>
    where T : notnull, new()
{
    ITessellateView<V, K> GetView<V>(Func<V, K> selectKey)
        where V : notnull, new();

    ITessellateWriter<T> Write();
}

public class TessellateTable<T, K>(
    ITessellateFile file,
    Func<T, K> selectKey,
    TessellateOptions options,
    ILogger logger
) : TessellateView<T, K>(file, selectKey, options, logger), ITessellateTable<T, K> 
    where T : notnull, new()
{
    public ITessellateView<V, K> GetView<V>(Func<V, K> selectKeyForView) 
        where V : notnull, new() => new TessellateView<V, K>(file, selectKeyForView, options, logger);

    public ITessellateWriter<T> Write()
    {
        logger.LogInformation("Writing to {name}", Name);

        return new TessellateWriter<K, T>(logger, file.Write(), selectKey, options, file.Name);
    }
}
