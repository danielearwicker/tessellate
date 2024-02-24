namespace Tessellate;

public interface ITempSortedTable<T, K> : ISortedTable<T, K>, IDisposable
    where T : notnull, new() {}
