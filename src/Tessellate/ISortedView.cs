namespace Tessellate;

public interface ISortedView<T, K> : IAsyncEnumerable<T> where T : notnull, new()
{
    Stream Stream { get; }

    Func<T, K> SelectKey { get; }

    ISortedView<T2, K> Cast<T2>(Func<T2, K> selectKey) where T2 : notnull, new();
}
