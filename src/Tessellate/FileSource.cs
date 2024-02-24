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
    IFile Create();
}

/// <summary>
/// Implementation of <see cref="IFileSource"/>.
/// </summary>
public sealed class FileSource : IFileSource
{
    private sealed class TempFile : IFile
    {
        private readonly string _path;
        private readonly FileSource _source;

        public Stream Content { get; private set; }

        public TempFile(FileSource source)
        {
            _source = source;

            Content = new FileStream(_path = Path.GetTempFileName(),
                            FileMode.OpenOrCreate, FileAccess.ReadWrite);            
        }

        public void Dispose()
        {
            Content.Dispose();
            if (File.Exists(_path)) File.Delete(_path);
            _source._files.Remove(this);
        }
    }

    private readonly HashSet<TempFile> _files = [];

    public IFile Create()
    {
        var file = new TempFile(this);
        _files.Add(file);
        return file;
    }

    public void Dispose()
    {
        while (_files.Count > 0) _files.First().Dispose();
    }
}
