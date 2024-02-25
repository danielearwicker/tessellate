namespace Tessellate;

using Tessellate.AsyncEnumerableExtensions;

public static class TessellateExtensions
{
    public static async Task ToTable<T, K>(this IAsyncEnumerable<T> source, ISortedTable<T, K> target)
        where T : notnull, new()
    {
        var writer = target.Write();

        await foreach (var item in source)
        {
            await writer.Add(item);            
        }

        await writer.Complete();
    }

    public static async Task<ITempSortedTable<T, K>> SortInto<T, K>(this IAsyncEnumerable<T> source, 
        ITableSource tables,
        Func<T, K> selectKey,
        int recordsPerBatch = 100_000,
        int batchesPerPartition = 100,
        string? loggingName = null) where T : notnull, new()
    {
        var table = tables.MergeSorting(selectKey, recordsPerBatch, batchesPerPartition, loggingName);
        await source.ToTable(table);
        return table;
    }
    
    public static async Task<ITempSortedTable<T, K>> Into<T, K>(this IAsyncEnumerable<T> source, 
        ITableSource tables,
        Func<T, K> selectKey,
        int recordsPerBatch = 100_000) where T : notnull, new()
    {
        var table = tables.PreSorted(selectKey, recordsPerBatch);
        await source.ToTable(table);
        return table;
    }

    /// <summary>
    /// Performs an inner join between two tables whose ordering
    /// keys are of the same type and thus can be equated.
    /// </summary>
    /// <typeparam name="Source1">Left table row type</typeparam>
    /// <typeparam name="Source2">Right table row type</typeparam>
    /// <typeparam name="SourceKey">Common key type</typeparam>
    /// <param name="source1">Left table</param>
    /// <param name="source2">Right table</param>
    /// <returns></returns>
    public static IAsyncEnumerable<(Source1 Left, Source2 Right)> 
        InnerJoin<Source1, Source2, SourceKey>(
            this ISortedView<Source1, SourceKey> source1,
            ISortedView<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.InnerJoinAdjacent(
                    source2, source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1 Left, Source2? Right)> 
        LeftJoin<Source1, Source2, SourceKey>(
            this ISortedView<Source1, SourceKey> source1,
            ISortedView<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.LeftJoinAdjacent(source2, source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1? Left, Source2 Right)> 
        RightJoin<Source1, Source2, SourceKey>(
            this ISortedView<Source1, SourceKey> source1,
            ISortedView<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.RightJoinAdjacent(source2, source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1? Left, Source2? Right)> 
        FullJoin<Source1, Source2, SourceKey>(
            this ISortedView<Source1, SourceKey> source1,
            ISortedView<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.FullJoinAdjacent(source2, source1.SelectKey, source2.SelectKey);
}