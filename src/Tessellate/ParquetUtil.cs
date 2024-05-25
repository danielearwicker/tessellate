namespace Tessellate;

using System.Collections;
using Parquet;
using SuperLinq;
using System.Linq;

public static class ParquetExtensions
{
    private record ParquetRowGroupList(ParquetReader Reader) : IReadOnlyList<ParquetRowGroupReader>
    {
        public ParquetRowGroupReader this[int index] => Reader.OpenRowGroupReader(index);

        public int Count => Reader.RowGroupCount;

        public IEnumerator<ParquetRowGroupReader> GetEnumerator()
        {
            for (int i = 0; i < Count; i++) yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    public static IReadOnlyList<ParquetRowGroupReader> ToList(this ParquetReader Reader)
        => new ParquetRowGroupList(Reader);

    public static IAsyncEnumerable<T> Pling<T, K>(
        this IEnumerable<ParquetRowGroupReader> rowGroups,        
        Func<ParquetRowGroupReader, Task<IEnumerable<T>>> unpackRowGroup,
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
}
