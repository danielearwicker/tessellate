namespace Tessellate;

public interface ISortedTable<T, K> : ISortedView<T, K> where T : notnull, new()
{
    ITableWriter<T> Write();
}
