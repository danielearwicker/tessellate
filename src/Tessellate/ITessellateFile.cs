namespace Tessellate;

public interface ITessellateFile
{
    string Name { get; }

    Stream Read();

    Stream Write();
}


