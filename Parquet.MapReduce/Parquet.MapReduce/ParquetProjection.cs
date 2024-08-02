using SuperLinq.Async;
using Parquet.MapReduce.Types;
using Parquet.MapReduce.Parquet;
using Parquet.MapReduce.Util;
using System.Runtime.CompilerServices;

namespace Parquet.MapReduce;

public delegate IAsyncEnumerable<KeyValuePair<TK, TV>> 
    ProjectKeyValues<SK, SV, TK, TV>(SK key, IAsyncEnumerable<SV> values);

public class ParquetProjection<SK, SV, TK, TV>(ParquetProjectionOptions<SK, TK>? options = null)
{
    private ParquetProjectionOptions<SK, TK> _options = options ?? new ParquetProjectionOptions<SK, TK>();

    /// <summary>
    /// Updates a sorted dataset. It is represented by two Parquet tables:
    /// 
    /// - KeyMapping: order by (SourceKey, TargetKey) and does not include a value
    /// - ContentRecord: order by (TargetKey) and includes a value
    /// 
    /// Separate streams are used for reading the previous and writing the updated
    /// versions of both these datasets, for flexibility in versioning.
    /// 
    /// The content is defined by a projection function <c>ProjectKeyValues</c>
    /// that consumes a source dataset. It is called for each key in the source
    /// and returns a sequence of projected key-value pairs. These results can be
    /// yielded in any order.
    /// 
    /// The <c>sourceUpdates</c> parameter describes the changes to be made. It
    /// must by ordered by <c>Key</c>. There are two kinds of <c>SourceUpdate</c>:
    /// 
    /// - Deletion: only specifies a <c>Key</c> (<c>Value</c> is ignored). Causes 
    /// all records previously projected from that key to be deleted.
    /// - Upsert: has Deletion = false. Causes all records previously projected 
    /// from that key to be replaced with newly projected records.
    /// 
    /// </summary>
    /// <param name="previousKeyMappings"></param>
    /// <param name="updatedKeyMappings"></param>
    /// <param name="previousContent"></param>
    /// <param name="updatedContent"></param>
    /// <param name="sourceUpdates"></param>
    /// <param name="projection"></param>
    /// <param name="targetUpdates"></param>
    /// <param name="cancellation"></param>
    /// <returns></returns>
    public async Task Update(
        Stream previousKeyMappings,
        Stream updatedKeyMappings,
        Stream previousContent,
        Stream updatedContent,
        IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates,
        ProjectKeyValues<SK, SV, TK, TV> projection,
        Stream? targetUpdates = null,
        CancellationToken cancellation = default)
    {
        using var instructions = new InstructionsStorage<SK, TK, TV>(_options);

        var keyMappingsStartPosition = previousKeyMappings.Position;

        var keyMappings = _options.Read<KeyMapping<SK, TK>>(previousKeyMappings, cancellation);

        await GenerateInstructions(keyMappings, sourceUpdates, instructions, projection, cancellation);

        previousKeyMappings.Position = keyMappingsStartPosition;

        await UpdateStream<KeyMapping<SK, TK>, KeyMappingInstruction<SK, TK>>(
            _options.RowsPerGroup, previousKeyMappings, updatedKeyMappings,
            instructions.ReadKeyMappingInstructions(cancellation), 
            ExecuteInstructionsOnMappings, cancellation);

        var updatesWriter = targetUpdates != null
            ? new BufferedWriter<SourceUpdate<TK, TV>>(targetUpdates, _options.RowsPerGroup, _options.ParquetOptions)
            : null;

        await UpdateStream<ContentRecord<TK, SK, TV>, ContentInstruction<TK, SK, TV>>(
            _options.RowsPerGroup, previousContent, updatedContent,
            instructions.ReadContentInstructions(cancellation), 
            (inst, prev) => ExecuteInstructionsOnContent(inst, prev, updatesWriter), 
            cancellation);

        if (updatesWriter != null)
        {
            await updatesWriter.Finish();
        }
    }

    public class KeyOnly
    {
        public SK? Key { get; set; }
    }

    public class ContentRecordFromSource
    {
        public SK? TargetKey { get; set; } // Because the target of the projection is the source of the next projection!

        public SV? Value;
    }

    private async IAsyncEnumerable<SourceUpdate<SK, SV>> ReadKeys(
        Stream updatesStream, 
        Stream contentStream, 
        IAsyncEnumerable<SK> keys,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        updatesStream.Position = 0;
        contentStream.Position = 0;

        var updates = _options.Read<SourceUpdate<SK, SV>>(updatesStream, cancellation);
        var content = _options.Read<ContentRecordFromSource>(contentStream, cancellation);

        await using var updatesEnumerator = updates.GetAsyncEnumerator(cancellation);
        var haveUpdate = await updatesEnumerator.MoveNextAsync();

        await using var contentEnumerator = content.GetAsyncEnumerator(cancellation);
        var haveContent = await contentEnumerator.MoveNextAsync();

        await foreach (var key in keys)
        {
            var hasUpdates = false;

            while (haveUpdate)
            {
                var compared = _options.SourceKeyComparer.Compare(key, updatesEnumerator.Current.Key);

                if (compared == 0)
                {
                    yield return updatesEnumerator.Current;
                    hasUpdates = true;
                }
                else if (compared > 0)
                {
                    break;
                }

                haveUpdate = await updatesEnumerator.MoveNextAsync();
            }

            if (!hasUpdates)
            {
                while (haveContent)
                {
                    var currentContent = contentEnumerator.Current;
                    var compared = _options.SourceKeyComparer.Compare(key, currentContent.TargetKey);

                    if (compared == 0)
                    {
                        // Make it look like an upsert
                        yield return new SourceUpdate<SK, SV>
                        {
                            Key = currentContent.TargetKey,
                            Value = currentContent.Value
                        };
                    }
                    else if (compared > 0)
                    {
                        break;
                    }
                }
            }
        }
    }

    /// <summary>
    /// Reads from one or more pairs of streams. The streams in the pair are:
    /// 
    /// - Updates: containing parquet of <c>SourceUpdate</c> objects in source key order
    /// - Content: containing parquet of <c>ContentRecord</c> objects where the target
    /// types are the same as this projection's source types.
    /// 
    /// The idea is to take the updates and content from a previous projection and use
    /// them as input to this projection.
    /// 
    /// The resulting sequence is suitable for passing to a <c>Update</c> call as the 
    /// <c>sourceUpdates</c> parameter.
    /// 
    /// </summary>
    /// <param name="sources">Streams containing parquet of SourceUpdate objects</param>
    /// <returns>A unified stream suitable for passing to an Update</returns>
    public async IAsyncEnumerable<SourceUpdate<SK, SV>> ReadSources(
        IReadOnlyCollection<(Stream Updates, Stream Content)> sources,
        [EnumeratorCancellation] CancellationToken cancellation)
    {
        foreach (var (updates, _) in sources)
        {
            updates.Position = 0;
        }

        // Generate a temporary stream of all updated keys across all sources
        var sourceKeyReaders = sources.Select(source => _options.Read<KeyOnly>(source.Updates, cancellation).Select(x => x.Key)).ToList();
        var affectedKeys = sourceKeyReaders[0].SortedMerge(_options.SourceKeyComparer, sourceKeyReaders.Skip(1).ToArray());

        using var affectedKeysStream = _options.CreateTemporaryStream(_options.LoggingPrefix + ".AffectedKeys");
        var affectedKeysWriter = new BufferedWriter<KeyOnly>(affectedKeysStream, _options.RowsPerGroup, _options.ParquetOptions);
        await affectedKeysWriter.AddRange(affectedKeys
            .DistinctUntilChanged(_options.SourceKeyComparer.ToEqualityComparer())
            .Select(x => new KeyOnly { Key = x }));
        await affectedKeysWriter.Finish();

        var distinctAffectedKeys = _options.Read<KeyOnly>(affectedKeysStream, cancellation).Select(x => x.Key!);

        var sourceReaders = sources.Select(source => ReadKeys(source.Updates, source.Content, distinctAffectedKeys, cancellation)).ToList();

        var comparer = Comparers.Build<SourceUpdate<SK, SV>>().By(x => x.Key, _options.SourceKeyComparer);

        SourceUpdate<SK, SV>? firstOfGroup = null;
        bool groupIsAllDeletions = false;

        await foreach (var update in sourceReaders[0].SortedMerge(comparer, sourceReaders.Skip(1).ToArray()))
        {
            var compared = firstOfGroup == null ? 1 : comparer.Compare(update, firstOfGroup);
            if (compared < 0)
            {
                throw new InvalidOperationException($"{_options.LoggingPrefix}: Update keys are not correctly ordered");
            }
            if (compared > 0) // New group
            {
                if (groupIsAllDeletions && firstOfGroup != null) // We only saw deletions for previous group
                {
                    yield return firstOfGroup;
                }

                if (update.Deletion)
                {
                    groupIsAllDeletions = true;
                    // Hold off on yielding until we know if there are any non-deletions
                }
                else
                {
                    groupIsAllDeletions = false;
                    yield return update;
                }
            }
            else // 2nd or later update for the same key
            {
                if (!update.Deletion)
                {
                    groupIsAllDeletions = false;
                    yield return update;
                }
            }
        }

        // Handle leftover
        if (groupIsAllDeletions && firstOfGroup != null)
        {
            yield return firstOfGroup;
        }
    }

    private async Task UpdateStream<R, I>(
        int rowsPerGroup,
        Stream previousStream,
        Stream updatedStream,
        IAsyncEnumerable<I> instructions,
        Func<IAsyncEnumerable<I>, IAsyncEnumerable<R>, IAsyncEnumerable<R>> reconcile,
        CancellationToken cancellation) where R : new()
    {
        var previousRecords = _options.Read<R>(previousStream, cancellation);

        var updatedContent = new BufferedWriter<R>(updatedStream, rowsPerGroup, _options.ParquetOptions);

        await updatedContent.AddRange(reconcile(instructions, previousRecords));
        await updatedContent.Finish();

        updatedStream.Position = 0;
    }
    
    private async Task GenerateInstructions(
        IAsyncEnumerable<KeyMapping<SK, TK>> keyMappings,
        IAsyncEnumerable<SourceUpdate<SK, SV>> sourceUpdates,
        InstructionsStorage<SK, TK, TV> instructions,
        ProjectKeyValues<SK, SV, TK, TV> projection,
        CancellationToken cancellation)
    {
        await using var keyMappingsEnumerator = keyMappings.GetAsyncEnumerator(cancellation);
        var haveKeyMapping = await keyMappingsEnumerator.MoveNextAsync();

        await using var sourceUpdatesEnumerator = sourceUpdates.GetAsyncEnumerator(cancellation);
        var haveSourceUpdate = await sourceUpdatesEnumerator.MoveNextAsync();

        while (haveSourceUpdate)
        {
            // fast-forward to relevant keyMappings that tell us what needs to be deleted
            while (haveKeyMapping)
            {
                var compared = _options.SourceKeyComparer.Compare(
                    keyMappingsEnumerator.Current.SourceKey,
                    sourceUpdatesEnumerator.Current.Key);

                if (compared == 0)
                {
                    await instructions.Delete(keyMappingsEnumerator.Current.SourceKey,
                                              keyMappingsEnumerator.Current.TargetKey);
                }
                else if (compared > 0)
                {
                    break;
                }

                haveKeyMapping = await keyMappingsEnumerator.MoveNextAsync();
            }

            var currentKey = sourceUpdatesEnumerator.Current.Key;

            if (sourceUpdatesEnumerator.Current.Deletion)
            {
                haveSourceUpdate = await sourceUpdatesEnumerator.MoveNextAsync();

                if (haveSourceUpdate)
                {
                    var nextKeyCompared = _options.SourceKeyComparer.Compare(sourceUpdatesEnumerator.Current.Key, currentKey);

                    if (nextKeyCompared == 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Deletion for key {currentKey} was followed by more updates for same key");
                    }

                    if (nextKeyCompared < 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Update keys are not correctly ordered");
                    }
                }
            }
            else
            {
                var sequence = new SingleUseSequence<SourceUpdate<SK, SV>, SV>(
                    sourceUpdatesEnumerator,
                    x => x.Value!,
                    x => x.Value != null && _options.SourceKeyComparer.Compare(x.Key!, currentKey) != 0);

                await foreach (var projected in projection(currentKey!, sequence))
                {
                    await instructions.Add(currentKey, projected.Key, projected.Value);
                }

                haveSourceUpdate = sequence.HasCurrent;
                if (haveSourceUpdate) 
                {
                    var nextKeyCompared = _options.SourceKeyComparer.Compare(sourceUpdatesEnumerator.Current.Key, currentKey);

                    if (nextKeyCompared == 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Projection did not consume all values for the same key");
                    }

                    if (nextKeyCompared < 0)
                    {
                        throw new InvalidOperationException($"{_options.LoggingPrefix}: Source update keys are not correctly ordered");
                    }
                }
            }
        }

        await instructions.Finish();
    }

    private async IAsyncEnumerable<R> ExecuteInstructions<R, I>(
        IAsyncEnumerable<I> instructions,
        IAsyncEnumerable<R> previous,
        IComparer<(I, bool)> compareKeys,
        Func<R, I> toInstruction,
        Func<I, R> toRecord,
        Func<I, ValueTask>? addUpdate = null)
        where I : IDeletable
    {
        var comparers = Comparers.Build<(I In, bool Instruction)>();

        var compareForMerge = comparers.By(
                compareKeys,
                comparers.By(x => x.Instruction ? 0 : 1)); // update instructions go first

        var fromInstructions = instructions.Select(x => (In: x, Instruction: true));
        var fromExisting = previous.Select(x => (In: toInstruction(x), Instruction: false));

        (I In, bool Instruction)? firstOfGroup = null;
        var discardExisting = false;

        await foreach (var next in fromInstructions.SortedMerge(compareForMerge, fromExisting))
        {
            var compared = firstOfGroup == null ? 1 : compareKeys.Compare(next, firstOfGroup.Value);
            if (compared < 0)
            {
                throw new InvalidOperationException($"{_options.LoggingPrefix}: Instructions are not correctly ordered");
            }

            if (compared != 0)
            {
                // Starting a new group. If there are instructions, they'll appear first
                // and we will know to discard existing mappings for the same keys
                firstOfGroup = next;
                if (next.Instruction)
                {
                    discardExisting = true;

                    if (!next.In.Deletion)
                    {
                        yield return toRecord(next.In);
                    }

                    if (addUpdate != null)
                    {
                        await addUpdate(next.In);
                    }
                }
                else // First is existing mapping so no instructions to modify it
                {
                    discardExisting = false;
                    yield return toRecord(next.In);
                }
            }
            else if (next.Instruction || !discardExisting)
            {
                yield return toRecord(next.In);

                if (addUpdate != null && next.Instruction)
                {
                    await addUpdate(next.In);
                }
            }
        }
    }

    private IAsyncEnumerable<KeyMapping<SK, TK>> ExecuteInstructionsOnMappings(
        IAsyncEnumerable<KeyMappingInstruction<SK, TK>> instructions,
        IAsyncEnumerable<KeyMapping<SK, TK>> previous)
    {
        var comparers = Comparers.Build<(KeyMappingInstruction<SK, TK> In, bool Instruction)>();

        var compareBothKeys = comparers.By(
                comparers.By(x => x.In.SourceKey, _options.SourceKeyComparer),
                comparers.By(x => x.In.TargetKey, _options.TargetKeyComparer));

        return ExecuteInstructions(instructions, previous, compareBothKeys,
                r => new KeyMappingInstruction<SK, TK> { SourceKey = r.SourceKey, TargetKey = r.TargetKey },
                i => new KeyMapping<SK, TK> { SourceKey = i.SourceKey, TargetKey = i.TargetKey });
    }

    private IAsyncEnumerable<ContentRecord<TK, SK, TV>> ExecuteInstructionsOnContent(
        IAsyncEnumerable<ContentInstruction<TK, SK, TV>> instructions,
        IAsyncEnumerable<ContentRecord<TK, SK, TV>> previous,
        BufferedWriter<SourceUpdate<TK, TV>>? updates = null)
    {
        var comparers = Comparers.Build<(ContentInstruction<TK, SK, TV> In, bool Instruction)>();

        var compareBothKeys = comparers.By(
                comparers.By(x => x.In.TargetKey, _options.TargetKeyComparer),
                comparers.By(x => x.In.SourceKey, _options.SourceKeyComparer));

        return ExecuteInstructions(instructions, previous, compareBothKeys,
                r => new ContentInstruction<TK, SK, TV> { SourceKey = r.SourceKey, TargetKey = r.TargetKey, Value = r.Value },
                i => new ContentRecord<TK, SK, TV> { SourceKey = i.SourceKey, TargetKey = i.TargetKey, Value = i.Value },
                updates != null ? i => updates.Add(new SourceUpdate<TK, TV> { Key = i.TargetKey, Value = i.Value, Deletion = i.Deletion }) : null);
    }
}
