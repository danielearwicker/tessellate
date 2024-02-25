namespace Tessellate;

/// <summary>
/// A temporary file. The only way to interact with the file is via 
/// the already open <see cref="IFile.Content"/> stream. Disposing
/// the object deletes the temporary file.
/// </summary>
public interface IFile : IDisposable
{
    Stream Content { get; }
}

/// <summary>
/// A service that creates temporary files, and can itself be
/// disposed to immediately delete all files created from it.
/// </summary>
public interface IFileSource : IDisposable
{
    IFile Create(string name);

    public (int Files, long Bytes) GetStats();
}

/// <summary>
/// Implementation of <see cref="IFileSource"/>.
/// </summary>
public sealed class FileSource(string dirPath) : IFileSource
{
    private sealed class TempFile : IFile
    {
        private readonly string _path;
        private readonly FileSource _source;

        public Stream Content { get; private set; }

        public TempFile(FileSource source, string path)
        {
            _source = source;
            _path = path;

            Content = new FileStream(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite);            
        }

        public void Dispose()
        {
            Content.Dispose();
            if (File.Exists(_path)) File.Delete(_path);
            _source._files.Remove(this);
        }
    }

    private readonly HashSet<TempFile> _files = [];

    public IFile Create(string name)
    {        
        var file = new TempFile(this, Path.Combine(dirPath, $"{DateTime.UtcNow.Ticks}_{name}"));
        _files.Add(file);
        return file;
    }

    public (int Files, long Bytes) GetStats()
    {
        var files = new DirectoryInfo(dirPath).GetFiles();
        return (files.Length, files.Sum(x => x.Length));
    }

    public void Dispose()
    {
        while (_files.Count > 0) _files.First().Dispose();
    }
}
