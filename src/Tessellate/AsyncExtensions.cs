using SuperLinq.Async;

namespace Tessellate.AsyncEnumerableExtensions;

public static class AsyncExtensions
{
/// <summary>
/// Alternative to SuperLinq's GroupAdjacent that protects against needing unlimited 
/// memory to buffer each group by using an <paramref name="aggregate"/> function to 
/// reduce each group member into some type <typeparamref name="G"/> representing
/// the group.
/// </summary>
/// <typeparam name="T">Item to be grouped</typeparam>
/// <typeparam name="K">Key to group by</typeparam>
/// <typeparam name="G">Group of items</typeparam>
/// <param name="source">Source of items to group</param>
/// <param name="selectKey">Function that selects the key to group by</param>
/// <param name="aggregate">Takes the current group and a new member for it, and 
/// returns the updated group</param>
/// <returns></returns>
    public static async IAsyncEnumerable<G> GroupAdjacentAggregated<T, K, G>(
        this IAsyncEnumerable<T> source,
        Func<T, K> selectKey,
        Func<G> newGroup,
        Func<G, T, G> aggregate)
    {
        var e = source.GetAsyncEnumerator();
        if (!await e.MoveNextAsync()) yield break;

        var groupKey = selectKey(e.Current);
        var group = aggregate(newGroup(), e.Current);

        while (await e.MoveNextAsync())
        {
            var newKey = selectKey(e.Current);

            if (Comparer<K>.Default.Compare(groupKey, newKey) != 0)
            {
                yield return group;

                groupKey = newKey;
                group = newGroup();
            }

            group = aggregate(group, e.Current);
        }

        yield return group;
    }

    public static async IAsyncEnumerable<(Source1 Left, Source2 Right)> InnerJoinAdjacent<Source1, Source2, SourceKey>(
        this IAsyncEnumerable<Source1> source1,
        IAsyncEnumerable<Source2> source2,
        Func<Source1, SourceKey> selectKey1,
        Func<Source2, SourceKey> selectKey2)
    {
        await foreach (var (left, right) in source1.FullJoinAdjacent(source2, selectKey1, selectKey2))
        {
            if (left != null && right != null)
            {
                yield return (left, right);
            }
        }
    }

    public static async IAsyncEnumerable<(Source1 Left, Source2? Right)> LeftJoinAdjacent<Source1, Source2, SourceKey>(
        this IAsyncEnumerable<Source1> source1,
        IAsyncEnumerable<Source2> source2,
        Func<Source1, SourceKey> selectKey1,
        Func<Source2, SourceKey> selectKey2)
    {
        await foreach (var (left, right) in source1.FullJoinAdjacent(source2, selectKey1, selectKey2))
        {
            if (left != null)
            {
                yield return (left, right);
            }
        }
    }

    public static async IAsyncEnumerable<(Source1? Left, Source2 Right)> RightJoinAdjacent<Source1, Source2, SourceKey>(
        this IAsyncEnumerable<Source1> source1,
        IAsyncEnumerable<Source2> source2,
        Func<Source1, SourceKey> selectKey1,
        Func<Source2, SourceKey> selectKey2)
    {
        await foreach (var (left, right) in source1.FullJoinAdjacent(source2, selectKey1, selectKey2))
        {
            if (right != null)
            {
                yield return (left, right);
            }
        }
    }

    public static async IAsyncEnumerable<(Source1? Left, Source2? Right)> FullJoinAdjacent<Source1, Source2, SourceKey>(
        this IAsyncEnumerable<Source1> source1,
        IAsyncEnumerable<Source2> source2,
        Func<Source1, SourceKey> selectKey1,
        Func<Source2, SourceKey> selectKey2)
    {
        var reader1 = source1.GroupAdjacent(selectKey1).GetAsyncEnumerator();
        var reader2 = source2.GroupAdjacent(selectKey2).GetAsyncEnumerator();

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
                foreach (var x in group1)
                {
                    foreach (var y in group2)
                    {
                        yield return (x, y);
                    }
                }

                got1 = await reader1.MoveNextAsync();
                got2 = await reader2.MoveNextAsync();
            }
            else if (ordering < 0)
            {
                foreach (var item in reader1.Current)
                {
                    yield return (item, default);
                }

                got1 = await reader1.MoveNextAsync();
            }
            else
            {
                foreach (var item in reader2.Current)
                {
                    yield return (default, item);
                }

                got2 = await reader2.MoveNextAsync();
            }
        }

        while (got1)
        {
            foreach (var item in reader1.Current)
            {
                yield return (item, default);
            }

            got1 = await reader1.MoveNextAsync();
        }

        while (got2)
        {
            foreach (var item in reader2.Current)
            {
                yield return (default, item);
            }

            got2 = await reader2.MoveNextAsync();
        }
    }
}
