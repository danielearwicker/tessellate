namespace Tessellate;

using Microsoft.Extensions.Logging;

public interface ITessellateDatabase
{
    public ITessellateTable<T, K> GetTable<T, K>(string name, Func<T, K> getKey,
        TessellateOptions? options = null) where T : notnull, new();
}

public class TessellateDatabase(
    ITessellateFolder folder, 
    ITessellateFormat format, 
    ILogger logger) : ITessellateDatabase
{
    public ITessellateTable<T, K> GetTable<T, K>(
        string name, Func<T, K> getKey, TessellateOptions? options = null) 
            where T : notnull, new()
        => new TessellateTable<T, K>(folder.GetFile($"{name}{format.Extension}"), getKey, 
            options ?? new TessellateOptions(), format, logger);
}
