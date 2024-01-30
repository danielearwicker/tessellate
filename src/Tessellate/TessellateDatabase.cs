namespace Tessellate;

using Microsoft.Extensions.Logging;

public interface ITessellateDatabase
{
    public ITessellateTable<T, K> GetTable<T, K>(string name, Func<T, K> getKey,
        TessellateOptions? options = null) where T : notnull, new();
}

public class TessellateDatabase(
    ITessellateFolder folder, 
    ILogger logger) : ITessellateDatabase
{
    public ITessellateTable<T, K> GetTable<T, K>(
        string name, Func<T, K> getKey, TessellateOptions? options = null) 
            where T : notnull, new()
        => new TessellateTable<T, K>(folder.GetFile($"{name}.parquet"), getKey, 
            options ?? new TessellateOptions(), logger);
}
