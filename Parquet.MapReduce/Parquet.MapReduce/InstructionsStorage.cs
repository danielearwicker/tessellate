﻿using Parquet.MapReduce.Types;
using Parquet.MapReduce.Parquet;
using Parquet.MapReduce.Util;

namespace Parquet.MapReduce;


public sealed class InstructionsStorage<SK, TK, TV> : IDisposable
{
    private readonly MergeSorter<ContentInstruction<TK, SK, TV>> _contentSorter;
    private readonly MergeSorter<KeyMappingInstruction<SK, TK>> _keyMappingSorter;
    
    private readonly ParquetProjectionOptions<SK, TK> _options;

    public InstructionsStorage(ParquetProjectionOptions<SK, TK> options)
    {
        var contentComparers = Comparers.Build<ContentInstruction<TK, SK, TV>>();

        _contentSorter = new(
            options with 
            { 
                LoggingPrefix = $"{options.LoggingPrefix}.ContentInstructions"
            },
            contentComparers.By(
                contentComparers.By(x => x.TargetKey, options.TargetKeyComparer),
                contentComparers.By(x => x.SourceKey, options.SourceKeyComparer)));

        var mappingComparers = Comparers.Build<KeyMappingInstruction<SK, TK>>();

        _keyMappingSorter = new(
            options with
            {
                LoggingPrefix = $"{options.LoggingPrefix}.KeyMappingInstructions"
            },
            mappingComparers.By(
                mappingComparers.By(x => x.SourceKey, options.SourceKeyComparer),
                mappingComparers.By(x => x.TargetKey, options.TargetKeyComparer)));

        _options = options;
    }

    public ValueTask Delete(SK? sourceKey, TK? targetKey)
        => AddInternal(sourceKey, targetKey, default, true);

    public ValueTask Add(SK? sourceKey, TK? targetKey, TV? targetValue)
        => AddInternal(sourceKey, targetKey, targetValue, false);

    private async ValueTask AddInternal(SK? sourceKey, TK? targetKey, TV? targetValue, bool deletion)
    {
        await _contentSorter.Add(new ContentInstruction<TK, SK, TV>
        {
            SourceKey = sourceKey,
            TargetKey = targetKey,
            Value = targetValue,
            Deletion = deletion
        });

        await _keyMappingSorter.Add(new KeyMappingInstruction<SK, TK>
        {
            SourceKey = sourceKey,
            TargetKey = targetKey,
            Deletion = deletion,
        });
    }

    public async ValueTask Finish()
    {
        await _contentSorter.Finish();
        await _keyMappingSorter.Finish();
    }

    public IAsyncEnumerable<ContentInstruction<TK, SK, TV>> ReadContentInstructions(CancellationToken cancellation) 
        => _contentSorter.Read(cancellation);

    public IAsyncEnumerable<KeyMappingInstruction<SK, TK>> ReadKeyMappingInstructions(CancellationToken cancellation)
        => _keyMappingSorter.Read(cancellation);

    public void Dispose()
    {
        _contentSorter.Dispose();
        _keyMappingSorter.Dispose();
    }
}

