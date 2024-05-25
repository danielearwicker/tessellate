namespace Tessellate;

using System.Collections;
using SuperLinq;
using SuperLinq.Async;

public static class SortingExtensions
{
    public static async IAsyncEnumerable<T> MergeSort<T, K>(
        this IEnumerable<IAsyncEnumerable<T>> sources,
        Func<T, K> selectKey,
        IComparer<K>? keyComparer = null)
    {
        var queue = new PriorityQueue<IAsyncEnumerator<T>, K>(keyComparer ?? Comparer<K>.Default);

        foreach (var source in sources) 
        {
            var enumerator = source.GetAsyncEnumerator();
            if (await enumerator.MoveNextAsync())
            {
                queue.Enqueue(enumerator, selectKey(enumerator.Current));
            }
        }

        while (queue.Count != 0)
        {
            var next = queue.Dequeue();

            yield return next.Current;

            if (await next.MoveNextAsync())
            {
                queue.Enqueue(next, selectKey(next.Current));
            }            
        }
    }

    /// <summary>
    /// Sorts up to <paramref name="batchSize"/> elements of the source sequence
    /// at a time, returning the sorted batches as a single concatenated sequence.
    /// 
    /// Effectively the same as:
    /// <code>
    ///     source.Batch(batchSize).SelectMany(x => x.OrderBy(selectKey))
    /// </code>
    /// except a single buffer is reused to reduce memory garbage.
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="K"></typeparam>
    /// <param name="source"></param>
    /// <param name="selectKey"></param>
    /// <param name="batchSize"></param>
    /// <param name="keyComparer"></param>
    /// <returns></returns>
    public static async IAsyncEnumerable<T> PartiallySort<T, K>(
        this IAsyncEnumerable<T> source,        
        int batchSize,
        IComparer<T>? comparer = null)
    {
        var buffer = new T[batchSize];
        var count = 0;

        await foreach (var item in source)
        {
            buffer[count++] = item;

            if (count == batchSize)
            {
                Array.Sort(buffer, comparer);
                foreach (var sorted in buffer) yield return sorted;
                count = 0;
            }
        }

        if (count > 0)
        {
            Array.Sort(buffer, 0, count, comparer);
            foreach (var sorted in buffer) yield return sorted;
        }
    }

    /// <summary>
    /// Let G be the number of rows in a row-group, and S the number of row-groups
    /// in a sorted partition.
    /// 
    /// Rows have been partially sorted (<see cref="PartiallySort"/>) so each sorted 
    /// chunk is of size G*S.
    /// 
    /// The rows are G-chunked into row-groups.
    /// 
    /// To read the rows fully sorted, we S-chunk the row-groups. That means each
    /// chunk of row-groups will cover G*S rows and thus be fully sorted. We then
    /// <see cref="MergeSort"/> across those chunks to get a fully sorted collection
    /// of rows.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="G"></typeparam>
    /// <typeparam name="K"></typeparam>
    /// <param name="rowGroups"></param>
    /// <param name="unpackRowGroup"></param>
    /// <param name="rowGroupsPerSortedBatch"></param>
    /// <param name="selectKey"></param>
    /// <returns></returns>
    public static IAsyncEnumerable<T> SortRowGroups<T, G, K>(
        this IEnumerable<G> rowGroups,        
        Func<G, Task<IEnumerable<T>>> unpackRowGroup,
        int rowGroupsPerSortedBatch,
        Func<T, K> selectKey) => rowGroups
            .Batch(rowGroupsPerSortedBatch)
            .Select(rowGroup => rowGroup
                .ToAsyncEnumerable()
                .SelectManyAwait(
                    async rowGroup => (await unpackRowGroup(rowGroup))
                        .ToAsyncEnumerable()
                )
            )
            .MergeSort(selectKey);

    public static IAsyncEnumerable<T> PartialSortRowGroups<T, G, K>(
        this IEnumerable<G> rowGroups,        
        Func<G, Task<IEnumerable<T>>> unpackRowGroup,
        int batchSize,
        int rowGroupsPerSortedBatch,
        Func<T, K> selectKey) => rowGroups
            .Batch(rowGroupsPerSortedBatch * batchSize)
            .ToAsyncEnumerable()
            .SelectMany(x => x.SortRowGroups(unpackRowGroup, rowGroupsPerSortedBatch, selectKey));
}
