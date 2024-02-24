namespace Tessellate;

public interface ISortedView<T, K> : IAsyncEnumerable<T> where T : notnull, new()
{
    Stream Stream { get; }

    Func<T, K> SelectKey { get; }
}
