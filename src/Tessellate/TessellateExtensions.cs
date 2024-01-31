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
            this ITessellateView<Source1, SourceKey> source1,
            ITessellateView<Source2, SourceKey> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().InnerJoinAdjacent(
                    source2.Read(), source1.GetKey, source2.GetKey);

    public static IAsyncEnumerable<((Source1 Item, int Index), Source2)> 
        InnerJoinByIndex<Source1, Source2, Source1Key>(
            this ITessellateView<Source1, Source1Key> source1,
            ITessellateView<Source2, int> source2)
            where Source1 : class, new() 
            where Source2 : class, new()
                => source1.Read().Select((x, i) => (x, i)).InnerJoinAdjacent<(Source1 Item, int Index), Source2, int>(
                    source2.Read(), s1 => s1.Index, source2.GetKey);                    
}