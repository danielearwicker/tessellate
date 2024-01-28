namespace Tessellate;

public class TessellateMemoryFolder() : ITessellateFolder
{
    private class TessellateMemoryFile(string name) : ITessellateFile
    {
        private MemoryStream? _stream;

        public string Name => name;

        public Stream Write()
        {
            _stream = new MemoryStream();
            return _stream;
        }

        public Stream Read()
        {
            var stream = _stream ?? new MemoryStream();
            stream.Position = 0;
            return stream;
        }
    }

    public ITessellateFile GetFile(string name) => new TessellateMemoryFile(name);
}
