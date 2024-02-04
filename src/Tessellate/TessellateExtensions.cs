namespace Tessellate;

using Tessellate.AsyncEnumerableExtensions;

public static class TessellateExtensions
{
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
    public static IAsyncEnumerable<(Source1, Source2)> 
        InnerJoin<Source1, Source2, SourceKey>(
            this ISortedStream<Source1, SourceKey> source1,
            ISortedStream<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().InnerJoinAdjacent(
                    source2.Read(), source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1, Source2?)> 
        LeftJoin<Source1, Source2, SourceKey>(
            this ISortedStream<Source1, SourceKey> source1,
            ISortedStream<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().LeftJoinAdjacent(source2.Read(), source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1?, Source2)> 
        RightJoin<Source1, Source2, SourceKey>(
            this ISortedStream<Source1, SourceKey> source1,
            ISortedStream<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().RightJoinAdjacent(source2.Read(), source1.SelectKey, source2.SelectKey);

    public static IAsyncEnumerable<(Source1?, Source2?)> 
        FullJoin<Source1, Source2, SourceKey>(
            this ISortedStream<Source1, SourceKey> source1,
            ISortedStream<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().FullJoinAdjacent(source2.Read(), source1.SelectKey, source2.SelectKey);
}