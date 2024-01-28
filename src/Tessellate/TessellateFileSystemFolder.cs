namespace Tessellate;

public class TessellateFileSystemFolder(string rootDir) : ITessellateFolder
{
    private class TessellateFile(string filePath) : ITessellateFile
    {
        public string Name => filePath;

        public Stream Write() => new FileStream(filePath, FileMode.Create, FileAccess.ReadWrite);

        public Stream Read() => !File.Exists(filePath) ? new MemoryStream() : 
                                new FileStream(filePath, FileMode.Open, FileAccess.Read);
    }

    public ITessellateFile GetFile(string name)
        => new TessellateFile(Path.Combine(rootDir, name));
}
