﻿namespace Tessellate;

public interface ITessellateSource<T> where T : new()
{
    int BatchCount { get; }

    Task<IList<T>> Read(int batch);
}

public interface ITessellateTarget<T> : IAsyncDisposable
{
    Task Write(IEnumerable<T> rows);
}

public interface ITessellateFormat
{
    string Extension { get; }

    Task<ITessellateSource<T>> GetSource<T>(Stream inputStream) where T : new();

    Task<ITessellateTarget<T>> GetTarget<T>(Stream outputStream);
}
