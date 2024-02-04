namespace Tessellate;

public interface ISortedStream<T, K> where T : notnull, new()
{
    Func<T, K> SelectKey { get; }

    IAsyncEnumerable<T> Read();
}
