namespace Tessellate;

public interface ITessellateWriter<V> where V : notnull
{
    ValueTask Add(V value);

    ValueTask Complete();
}
