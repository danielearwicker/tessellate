namespace Tessellate;

public interface ITableWriter<V> where V : notnull
{
    ValueTask Add(V value);

    ValueTask Complete();
}
