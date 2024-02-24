namespace Tessellate;

public interface ITableSource
{
    /// <summary>
    /// Creates a <see cref="MergeSortingParquet"/> table backed by  a temporary Parquet file.
    /// </summary>
    /// <typeparam name="T">The row type</typeparam>
    /// <typeparam name="K">The sort key type</typeparam>
    /// <param name="selectKey">Function that selects the sort key from a record</param>
    /// <param name="rowsPerGroup">Number of records per Parquet row group</param>
    /// <param name="rowGroupsPerPartition">Number of row groups per sorted partition</param>
    /// <returns></returns>
    ITempSortedTable<T, K> MergeSorting<T, K>(
        Func<T, K> selectKey,
        int rowsPerGroup = 100_000, 
        int rowGroupsPerPartition = 100) where T : notnull, new();

    /// <summary>
    /// Creates a <see cref="PreSortedParquet"/> table backed by a temporary Parquet file.
    /// </summary>
    /// <typeparam name="T">The row type</typeparam>
    /// <typeparam name="K">The sort key type</typeparam>
    /// <param name="selectKey">Function that selects the sort key from a record</param>
    /// <param name="rowsPerGroup">Number of records per Parquet row group</param>
    /// <returns></returns>
    ITempSortedTable<T, K> PerSorted<T, K>(
        Func<T, K> selectKey,
        int rowsPerGroup = 100_000) where T : notnull, new();
}

public class TableSource(IFileSource files) : ITableSource
{
    public ITempSortedTable<T, K> MergeSorting<T, K>(
        Func<T, K> selectKey,
        int rowsPerGroup = 100_000, 
        int rowGroupsPerPartition = 100) where T : notnull, new()
    {
        var file = files.Create();
        var table = new MergeSortingParquet<T, K>(file.Content, selectKey, rowsPerGroup, rowGroupsPerPartition);
        return new TempSortedTable<T, K>(file, table);
    }

    public ITempSortedTable<T, K> PerSorted<T, K>(
        Func<T, K> selectKey,
        int rowsPerGroup = 100_000) where T : notnull, new()
    {
        var file = files.Create();
        var table = new PreSortedParquet<T, K>(file.Content, selectKey, rowsPerGroup);
        return new TempSortedTable<T, K>(file, table);
    }

    private sealed record TempSortedTable<T, K>(
        IFile File, 
        ISortedTable<T, K> Table) : ITempSortedTable<T, K>
        where T : notnull, new()
    {
        public Func<T, K> SelectKey => Table.SelectKey;

        public Stream Stream => File.Content;

        public void Dispose() => File.Dispose();    

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellation) => Table.GetAsyncEnumerator(cancellation);

        public ITableWriter<T> Write() => Table.Write();
    }
}
