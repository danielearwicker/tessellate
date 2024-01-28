using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Serialization.Attributes;
using Tessellate;

var rand = new Random(12345);

var logged = DateTime.UtcNow;

var logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("Program");

var format = new TessellateParquetFormat();

await ParquetGrouperUtil.LogTiming("End-to-end", async () =>
{
    var sortByUniqueId = new TessellateSorterService(logger)
        .GetSorter<string, InvoiceDeJour>(x => x.UniqueId);

    const string fileByUniqueId = "by-unique-id.parquet";

    {    
        using var outStream = new FileStream(fileByUniqueId, FileMode.Create, FileAccess.ReadWrite);

        var target = await format.GetTarget<InvoiceDeJour>(outStream);

        var writer = sortByUniqueId.Write(target);

        await ParquetGrouperUtil.LogTiming($"Writing {fileByUniqueId}", async () =>
        {
            const int count = 1_000_000;

            var ids = Enumerable.Range(0, count).ToArray();

            for (var n = 0; n < ids.Length - 1; n++)
            {
                var i = rand.Next(n, ids.Length);
                (ids[i], ids[n]) = (ids[n], ids[i]);
            }

            var randRange = Math.Max(5, count / 10);

            for (var n = 0; n < count; n++)
            {
                await writer.Add(new InvoiceDeJour
                {
                    UniqueId = $"u{ids[n]:0000000000}",
                    InvoiceNumber = $"i{rand.Next(0, randRange):0000000000}",
                    InvoiceAmount = ((decimal)rand.Next(0, randRange)) / 100m,
                    InvoiceDate = DateTime.UtcNow.AddSeconds(-n * 5),
                    SupplierName = $"s{rand.Next(0, randRange):0000000000}",
                });
                
                if (TimeToLog())
                {
                    Console.WriteLine(n);
                }
            }

            await writer.Flush();
        });
    }

    const string fileByBucketKey = "by-bucket-key.parquet";

    string GetDateOnly(DateTime dt) => dt.ToString("yyyy-MM-dd");

    // Should be NumericInvoiceNumber also!

    var buckets = new Func<InvoiceDeJour, string>[]
    {
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}",
        i => $"{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}:{i.SupplierName}",
        i => $"{i.InvoiceNumber}:{i.InvoiceAmount}:{i.SupplierName}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.SupplierName}",
        i => $"{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}:{i.SupplierName}",
    };

    var sortByBucketKey = new TessellateSorterService(logger)
        .GetSorter<(byte, string), BucketKey>(x => (x.BucketId, x.Key));

    await Pipe(fileByUniqueId, sortByUniqueId, fileByBucketKey, sortByBucketKey, 
        async (invoice, emit) => 
        {
            for (byte b = 0; b < buckets.Length; b++)
            {
                await emit(new BucketKey 
                { 
                    BucketId = b, 
                    Key = buckets[b](invoice.Item), 
                    UniqueId = invoice.Item.UniqueId,
                });
            }
        });

    const string fileAllPairsByFirstAndSecondId = "all-pairs-by-first-second-id.parquet";

    var sortAllPairsByFirstAndSecondId = new TessellateSorterService(logger)
        .GetSorter<(string FirstId, string SecondId), Pair>(x => (x.FirstId, x.SecondId));

    var group = new List<BucketKey>();
    const int maxGroupSize = 5;

    async ValueTask EmitPairs(Func<Pair, ValueTask> emit)
    {
        if (group.Count >= 2 && group.Count <= maxGroupSize)
        {
            // all pairs in this group have the same bucket ID (and key)
            var bucketId = group[0].BucketId;

            for (var x = 0; x < group.Count - 1; x++)
            {
                for (var y = x + 1; y < group.Count; y++)
                {
                    await emit(new Pair 
                    { 
                        FirstId = group[x].UniqueId, 
                        SecondId = group[y].UniqueId, 
                        BucketId = bucketId 
                    });
                }
            }
        }

        group.Clear();
    }

    await Pipe(fileByBucketKey, sortByBucketKey, fileAllPairsByFirstAndSecondId, sortAllPairsByFirstAndSecondId,
        async (bucketKey, emit) => 
        {
            // starting or continuing a group
            if (group.Count == 0 || group[0].Key == bucketKey.Item.Key)
            {
                // no point keeping more that 1 past max group size
                if (group.Count <= maxGroupSize) 
                {
                    group.Add(bucketKey.Item);
                }
            }
            else
            {
                // reach end of group, so generate pairs if it qualifies
                await EmitPairs(emit);
                // start next group
                group.Add(bucketKey.Item);
            }
        },
        EmitPairs);

    const string fileBestPairsByFirstId = "best-pairs-by-first-id.parquet";

    var sortBestPairsByFirstId = new TessellateSorterService(logger)
        .GetSorter<string, Pair>(x => x.FirstId);

    (string? FirstId, string? SecondId) groupIds = (null, null);
    byte groupMinBucketId = byte.MaxValue;

    async ValueTask EmitGroupPair(Func<Pair, ValueTask> emit)
    {
        if (groupIds.FirstId != null && groupIds.SecondId != null)
        {
            await emit(new Pair { BucketId = groupMinBucketId, FirstId = groupIds.FirstId, SecondId = groupIds.SecondId });
        }
        groupIds = (null, null);
        groupMinBucketId = byte.MaxValue;
    }

    await Pipe(fileAllPairsByFirstAndSecondId, sortAllPairsByFirstAndSecondId, fileBestPairsByFirstId, sortBestPairsByFirstId,
        async (pair, emit) => 
        {                        
            // continuing a group
            if (groupIds == (pair.Item.FirstId, pair.Item.SecondId))
            {
                groupMinBucketId = Math.Min(groupMinBucketId, pair.Item.BucketId);
            }
            else // reach end of group (or starting first one)
            {                
                await EmitGroupPair(emit);                
                // start next group
                groupIds = (pair.Item.FirstId, pair.Item.SecondId);
                groupMinBucketId = pair.Item.BucketId;
            }
        },
        EmitGroupPair);

    const string filePairsBySecondId = "pairs-by-second-id.parquet";

    var sortPairsBySecondId = new TessellateSorterService(logger)
        .GetSorter<string, PairWithFirstInvoice>(x => x.SecondId);

    await InnerJoin(fileBestPairsByFirstId, sortBestPairsByFirstId, x => x.FirstId,
                    fileByUniqueId, sortByUniqueId, x => x.UniqueId,
                    filePairsBySecondId, sortPairsBySecondId,
                    (pair, firstInvoice, emit) => emit(new() 
                    {
                        First = firstInvoice.Item,
                        SecondId = pair.Item.SecondId,
                        MinBucketId = pair.Item.BucketId,
                    }));

    const string fileScoredPairs = "scored-pairs.parquet";

    var sortScoredPairsByLeaderId = new TessellateSorterService(logger)
        .GetSorter<string, PairWithScore>(x => x.LeaderId);

    await InnerJoin(filePairsBySecondId, sortPairsBySecondId, x => x.SecondId,
                    fileByUniqueId, sortByUniqueId, x => x.UniqueId,
                    fileScoredPairs, sortScoredPairsByLeaderId,
                    (pair, secondInvoice, emit) => 
                        emit(ScoreInvoicePair(pair.Item.First, secondInvoice.Item, pair.Item.MinBucketId)));

    static PairWithScore ScoreInvoicePair(InvoiceDeJour first, InvoiceDeJour second, byte minBucketId)
    {
        var (leaderId, nonLeaderId) = (first.UniqueId, second.UniqueId);
        if (leaderId.CompareTo(nonLeaderId) < 0)
        {
            (leaderId, nonLeaderId) = (nonLeaderId, leaderId);
        }

        return new PairWithScore
        { 
            LeaderId = leaderId,
            NonLeaderId = nonLeaderId,
            MinBucketId = minBucketId,
            Score = GetFirstNonZeroDigit(first.SupplierName) * GetFirstNonZeroDigit(second.SupplierName)
        };
    }
});

static int GetFirstNonZeroDigit(string str)
{
    for (var c = 0; c < str.Length; c++)
    {
        var ch = str[c];
        if (char.IsDigit(ch) && ch != '0')
        {
            return ch - '0';
        }
    }

    return 0;
}

bool TimeToLog()
{
    var now = DateTime.UtcNow;
    if ((now - logged).TotalSeconds > 5)
    {
        logged = now;
        return true;
    }

    return false;
}

async Task Pipe<Source, Target>(
    string sourceFile, ITessellateSorter<Source> sourceSorter,
    string targetFile, ITessellateSorter<Target> targetSorter,
    Func<OrdinalItem<Source>, Func<Target, ValueTask>, ValueTask> emit,
    Func<Func<Target, ValueTask>, ValueTask>? flush = null)
    where Source : notnull, new()
    where Target : notnull, new()
{
    await ParquetGrouperUtil.LogTiming($"Reading {sourceFile} to produce {targetFile}", async () => 
    {
        using var inStream = new FileStream(sourceFile, FileMode.Open, FileAccess.Read);
        
        var source = await format.GetSource<Source>(inStream);

        using var outStream = new FileStream(targetFile, FileMode.Create, FileAccess.ReadWrite);

        var target = await format.GetTarget<Target>(outStream);

        var writer = targetSorter.Write(target);

        long sourceIndex = 0;

        await foreach (var row in sourceSorter.Read(source))
        {
            await emit(new (row, sourceIndex++), writer.Add);
            
            if (TimeToLog())
            {
                Console.WriteLine(sourceIndex);
            }
        }

        if (flush != null)
        {
            await flush(writer.Add);
        }

        await writer.Flush();
    });
}

async Task InnerJoin<Source1, Source2, Target, Key>(
    string source1File, ITessellateSorter<Source1> source1Sorter, Func<Source1, Key> getKey1,
    string source2File, ITessellateSorter<Source2> source2Sorter, Func<Source2, Key> getKey2,
    string targetFile, ITessellateSorter<Target> targetSorter,    
    Func<OrdinalItem<Source1>, OrdinalItem<Source2>, Func<Target, ValueTask>, ValueTask> emit,
    Func<Func<Target, ValueTask>, ValueTask>? flush = null)
    where Source1 : class, new()
    where Source2 : class, new()
    where Target : class, new()
{
    await ParquetGrouperUtil.LogTiming($"Reading {source1File} + {source2File} to produce {targetFile}", async () => 
    {
        using var inStreamSource1 = new FileStream(source1File, FileMode.Open, FileAccess.Read);
        
        var source1 = await format.GetSource<Source1>(inStreamSource1);

        using var inStreamSource2 = new FileStream(source2File, FileMode.Open, FileAccess.Read);
        
        var source2 = await format.GetSource<Source2>(inStreamSource2);

        using var outStream = new FileStream(targetFile, FileMode.Create, FileAccess.ReadWrite);

        var target = await format.GetTarget<Target>(outStream);

        var writer = targetSorter.Write(target);

        var reader1 = source1Sorter.Read(source1).GroupByAdjacent(getKey1).GetAsyncEnumerator();        
        var reader2 = source2Sorter.Read(source2).GroupByAdjacent(getKey2).GetAsyncEnumerator();

        var got1 = await reader1.MoveNextAsync();
        var got2 = await reader2.MoveNextAsync();

        var emitted = 0;

        while (got1 && got2)
        {
            var group1 = reader1.Current;
            var group2 = reader2.Current;
            var ordering = Comparer<Key>.Default.Compare(group1.Key, group2.Key);
            if (ordering == 0)
            {
                // emit cartesian product of matched groups
                for (var x = 0; x < group1.Count; x++)
                {
                    for (var y = 0; y < group2.Count; y++)
                    {
                        emitted++;
                        await emit(group1[x], group2[y], writer.Add);
                    }
                }

                got1 = await reader1.MoveNextAsync();
                got2 = await reader2.MoveNextAsync();
            }
            else if (ordering < 0)
            {
                got1 = await reader1.MoveNextAsync();
            }
            else
            {
                got2 = await reader2.MoveNextAsync();
            }
            
            if (TimeToLog())
            {
                Console.WriteLine(emitted);
            }
        }
        
        if (flush != null)
        {
            await flush(writer.Add);
        }

        await writer.Flush();
    });
}

public record struct OrdinalItem<T>(T Item, long Ordinal);

public static class AsyncExtensions
{
    public class Group<T, K>(K key) : List<OrdinalItem<T>>
    {
        public K Key => key;
    }

    public static async IAsyncEnumerable<Group<T, K>> GroupByAdjacent<T, K>(this IAsyncEnumerable<T> source, Func<T, K> getKey)
    {
        Group<T, K>? group = null;

        long sourceIndex = 0;

        await foreach (var item in source)
        {
            var key = getKey(item);

            if (group == null)
            {
                group = new(key) { new(item, sourceIndex) };
            }
            else if (Equals(group.Key, key))
            {
                group.Add(new(item, sourceIndex));
            }
            else
            {
                yield return group;
                group = new(key) { new(item, sourceIndex) };
            }

            sourceIndex++;
        }

        if (group != null)
        {
            yield return group;
        }
    }
}

class ParquetGrouperUtil
{
    public static async Task LogTiming(string label, Func<Task> task)
    {
        Console.WriteLine($"{label}...");
        var timer = new Stopwatch();
        timer.Start();
        await task();
        timer.Stop();
        Console.WriteLine($"{label}: {timer.Elapsed.TotalSeconds}");
    }
}

class InvoiceDeJour
{
    [ParquetRequired] 
    public string UniqueId {get; set; } = string.Empty;

    [ParquetRequired] 
    public string InvoiceNumber {get; set; } = string.Empty;

    [ParquetDecimal(19, 5)] 
    public decimal InvoiceAmount { get; set; }

    [ParquetTimestamp] 
    public DateTime InvoiceDate { get; set; }

    [ParquetTimestamp] 
    public string SupplierName { get; set; } = string.Empty;
}

class BucketKey
{
    public byte BucketId { get; set; }

    [ParquetRequired] 
    public string Key {get; set; } = string.Empty;

    [ParquetRequired] 
    public string UniqueId {get; set; } = string.Empty;
}

class Pair
{
    public byte BucketId { get; set; }

    [ParquetRequired] 
    public string FirstId {get; set; } = string.Empty;

    [ParquetRequired] 
    public string SecondId {get; set; } = string.Empty;
}

class PairWithFirstInvoice
{
    public byte MinBucketId { get; set; }

    [ParquetRequired] 
    public InvoiceDeJour First {get; set; } = new();

    [ParquetRequired] 
    public string SecondId {get; set; } = string.Empty;    
}


class PairWithScore
{
    public byte MinBucketId { get; set; }

    [ParquetRequired] 
    public string LeaderId {get; set; } = string.Empty;

    [ParquetRequired] 
    public string NonLeaderId {get; set; } = string.Empty;

    [ParquetRequired] 
    public long Score {get; set; }
}

