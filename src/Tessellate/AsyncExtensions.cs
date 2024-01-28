namespace Tessellate.AsyncEnumerableExtensions;

public static class AsyncExtensions
{
    public class Group<T, K>(K key) : List<T>, IGrouping<K, T>
    {
        public K Key => key;
    }

    /// <summary>
    /// Returns a sequence of groupings of adjacent elements with the same 
    /// key.
    /// 
    /// Typically the source sequence is already sorted by that key so the
    /// groupings contain all elements with a given key.
    /// </summary>
    /// <typeparam name="T">Element type</typeparam>
    /// <typeparam name="K">Key type</typeparam>
    /// <param name="source">Sequence (usually already sorted by key)</param>
    /// <param name="selectKey">Delegate that obtains the key from an element</param>
    /// <returns></returns>
    public static async IAsyncEnumerable<Group<T, K>> GroupByAdjacent<T, K>(this IAsyncEnumerable<T> source, Func<T, K> selectKey)
    {
        Group<T, K>? group = null;

        await foreach (var item in source)
        {
            var key = selectKey(item);

            if (group == null)
            {
                group = new(key) { item };
            }
            else if (Equals(group.Key, key))
            {
                group.Add(item);
            }
            else
            {
                yield return group;
                group = new(key) { item };
            }
        }

        if (group != null)
        {
            yield return group;
        }
    }

    /// <summary>
    /// Reads two sequences in parallel and returns combinations of adjacent
    /// elements with equal keys.
    /// 
    /// If the input sequences are sorted by their corresponding keys, this
    /// is a true inner join.
    /// 
    /// It is possible for multiple adjacent elements to have the same key,
    /// in which case the resulting sequence will include every combination
    /// of those matching elements.
    /// </summary>
    /// <typeparam name="Source1">Left element type</typeparam>
    /// <typeparam name="Source2">Right element type</typeparam>
    /// <typeparam name="SourceKey">Type of common key</typeparam>
    /// <param name="source1">Left source sequence</param>
    /// <param name="source2">Right source sequence</param>
    /// <param name="selectKey1">Delegate that obtains the key from an element of the left sequence</param>
    /// <param name="selectKey2">Delegate that obtains the key from an element of the right sequence</param>
    /// <returns></returns>
    public static async IAsyncEnumerable<(Source1, Source2)> InnerJoinAdjacent<Source1, Source2, SourceKey>(
        this IAsyncEnumerable<Source1> source1,
        IAsyncEnumerable<Source2> source2,
        Func<Source1, SourceKey> selectKey1,
        Func<Source2, SourceKey> selectKey2)
    {
        var reader1 = source1.GroupByAdjacent(selectKey1).GetAsyncEnumerator();
        var reader2 = source2.GroupByAdjacent(selectKey2).GetAsyncEnumerator();

        var got1 = await reader1.MoveNextAsync();
        var got2 = await reader2.MoveNextAsync();

        while (got1 && got2)
        {
            var group1 = reader1.Current;
            var group2 = reader2.Current;
            var ordering = Comparer<SourceKey>.Default.Compare(group1.Key, group2.Key);
            if (ordering == 0)
            {
                // yield cartesian product of matched groups
                for (var x = 0; x < group1.Count; x++)
                {
                    for (var y = 0; y < group2.Count; y++)
                    {
                        yield return (group1[x], group2[y]);
                    }
                }

                got1 = await reader1.MoveNextAsync();
                got2 = await reader2.MoveNextAsync();
            }
            else if (ordering < 0)
            {
                got1 = await reader1.MoveNextAsync();
            }
            else
            {
                got2 = await reader2.MoveNextAsync();
            }
        }
    }
}
