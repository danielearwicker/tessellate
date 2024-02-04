using System.Data.Common;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Serialization.Attributes;
using Tessellate;

int GetSimpleStringHash(string str)
{
    unchecked 
    {
        uint b    = 378551;
        uint a    = 63689;
        uint hash = 0;

        var len = str.Length;
        for (var i = 0; i < len; i++)
        {
            hash = hash * a + str[i];
            a *= b;
        }

        return (int)hash;
    }
}

var rand = new Random(/*12345*/);

var logged = DateTime.UtcNow;

var loggers = LoggerFactory.Create(builder => builder.AddConsole());

Directory.CreateDirectory("output");

var timer = new Stopwatch();
timer.Start();

var receivedInvoicesByUniqueId = new PartiallySortedStream<ReceivedInvoiceDeJour, string>(
    Files.Open("input-invoices-by-uid"),
    x => x.UniqueId
);

{
    var writer = receivedInvoicesByUniqueId.Write();

    const int count = 1_000;

    var randRange = 500_000;

    var ids = Enumerable.Range(0, randRange).ToArray();

    for (var n = 0; n < ids.Length - 1; n++)
    {
        var i = rand.Next(n, ids.Length);
        (ids[i], ids[n]) = (ids[n], ids[i]);
    }

    var baseDateTime = new DateTime(2024, 01, 28, 14, 05, 00);

    for (var n = 0; n < count; n++)
    {
        await writer.Add(new InvoiceDeJour
        {
            UniqueId = $"u{ids[n]:0000000000}",
            InvoiceNumber = $"i{rand.Next(0, randRange):0000000000}",
            InvoiceAmount = rand.Next(0, randRange) / 100m,
            InvoiceDate = baseDateTime.AddSeconds(-n * 5),
            SupplierName = $"sn{rand.Next(0, randRange):0000000000}",
            SupplierRef = $"sr{rand.Next(0, randRange):0000000000}",
            BaseAmount = rand.Next(0, randRange) / 100m,
            OrgGroupName = $"o{rand.Next(0, randRange):0000000000}",
            EnteredBy = $"e{rand.Next(0, randRange):0000000000}",
        });
    }

    await writer.Complete();
}

// Same as receivedInvoicesByUniqueId but with full type
var inputInvoicesByUniqueId = new PartiallySortedStream<InvoiceDeJour, string>(
    receivedInvoicesByUniqueId.Stream,
    receivedInvoicesByUniqueId.SelectKey
);

// As we update invoices-by-uid, we'll generate this temporary set of upserted by InternalID
var inputInvoicesByInternalId = new PartiallySortedStream<InvoiceDeJour, int>(
    Files.Open("input-invoices-by-iid"),
    x => x.InternalId
);

int added = 0;
int updated = 0;
int unchanged = 0;

using (var ids = new IdGenerator(Files.GetFullPath("invoice-next-id.txt")))
await using (var updater = new UpdateStream<InvoiceDeJour, string>("invoices-by-uid", x => x.UniqueId))
{
    var writerByInternalId = inputInvoicesByInternalId.Write();

    await foreach (var (existing, input) in updater.Existing.FullJoin(inputInvoicesByUniqueId))
    {
        if (input == null)
        {
            unchanged++;
            // therefore existing must be non-null
            await updater.Writer.Add(existing!);
        }
        else // we have input, so it replaces existing
        {
            if (existing == null)
            {
                added++;
                // never seen this UniqueId before, so make new InternalId
                input.InternalId = ids.GetNext();
            }
            else // updating, so retain existing InternalId
            {
                updated++;
                input.InternalId = existing.InternalId;
            }

            await updater.Writer.Add(input);
            await writerByInternalId.Add(input);
        }
    }

    await writerByInternalId.Complete();
}

Console.WriteLine($"Added: {added}, Updated: {updated}, Unchanged: {unchanged}");

await using (var updater = new UpdateStream<InvoiceDeJour, int>("invoices-by-iid", x => x.InternalId))
{
    await foreach (var (existing, input) in updater.Existing.FullJoin(inputInvoicesByInternalId))
    {
        if (input == null)
        {
            await updater.Writer.Add(existing!);
        }
        else // we have input, so it replaces existing
        {
            await updater.Writer.Add(input);
        }
    }
}

var invoicesByInternalId = new PartiallySortedStream<InvoiceDeJour, int>(
    Files.Open("invoices-by-iid", true), 
    x => x.InternalId
);

string GetDateOnly(DateTime dt) => dt.ToString("yyyy-MM-dd");

// Should be NumericInvoiceNumber also!

var buckets = new Func<IBucketValues, string>[]
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

var invoicesByBucketKey = new PartiallySortedStream<BucketKey, (byte, string)>(
    Files.Open("invoices-by-bucket-key"),
    x => (x.BucketId, x.Key)
);

var invoicesByInternalIdForBucketing = new PartiallySortedStream<BucketValues, int>(
    invoicesByInternalId.Stream, 
    x => x.InternalId
);

var invoicesByBucketKeyHash = new PartiallySortedStream<BucketKeyHash, (byte, int)>(
    Files.Open("invoices-by-bucket-key-hash"),
    x => (x.BucketId, x.KeyHash),
    1_000_000);

{
    var writer = invoicesByBucketKeyHash.Write();

    var reader = invoicesByInternalIdForBucketing.Read();

    await foreach (var invoice in reader)
    {
        for (byte b = 0; b < buckets.Length; b++)
        {
            await writer.Add(new BucketKeyHash
            { 
                BucketId = b, 
                KeyHash = GetSimpleStringHash(buckets[b](invoice)), 
                InvoiceInternalId = invoice.InternalId,
            });
        }
    }

    await writer.Complete();
}

var potentialBuckets = new PartiallySortedStream<PotentialBucket, int>(
    Files.Open("potential-buckets"), 
    x => x.InvoiceInternalId,
    1_000_000);

{
    var writer = potentialBuckets.Write();

    var group = new List<BucketKeyHash>();

    async ValueTask EmitPotentials()
    {
        if (group.Count > 1)
        {                
            var bucketId = group[0].BucketId;

            for (var x = 0; x < group.Count; x++)
            {
                await writer.Add(new PotentialBucket
                {
                    BucketId = bucketId,
                    InvoiceInternalId = group[x].InvoiceInternalId,
                });
            }
        }

        group.Clear();
    }

    await foreach (var keyHash in invoicesByBucketKeyHash.Read())
    {
        if (group.Count != 0)
        {
            var g = group[0];
            
            if (g.BucketId != keyHash.BucketId ||
                g.KeyHash != keyHash.KeyHash)
            {
                await EmitPotentials();
            }
        }

        group.Add(keyHash);
    }

    await EmitPotentials();

    await writer.Complete();
}

{
    var writer = invoicesByBucketKey.Write();

    await foreach (var (invoice, bucket) in invoicesByInternalIdForBucketing.InnerJoin(potentialBuckets))
    {
        await writer.Add(new BucketKey 
        { 
            BucketId = bucket.BucketId, 
            Key = buckets[bucket.BucketId](invoice), 
            InvoiceIndex = bucket.InvoiceInternalId,
        });
    }

    await writer.Complete();
}


var allPairsByFirstAndSecondId = new PartiallySortedStream<Pair, (int FirstIndex, int SecondIndex)>(
    Files.Open("all-pairs-by-first-second-id"), 
    x => (x.FirstId, x.SecondId));

{
    var writer = allPairsByFirstAndSecondId.Write();

    var group = new List<BucketKey>();
    const int maxGroupSize = 5;

    async ValueTask EmitPairs()
    {
        if (group.Count >= 2 && group.Count <= maxGroupSize)
        {
            // all pairs in this group have the same bucket ID (and key)
            var bucketId = group[0].BucketId;

            for (var x = 0; x < group.Count - 1; x++)
            {
                for (var y = x + 1; y < group.Count; y++)
                {
                    var (FirstId, SecondId) = (group[x].InvoiceIndex, group[y].InvoiceIndex);
                    if (FirstId > SecondId)
                    {
                        (FirstId, SecondId) = (SecondId, FirstId);
                    }

                    await writer.Add(new Pair 
                    { 
                        FirstId = FirstId, 
                        SecondId = SecondId, 
                        BucketId = bucketId 
                    });
                }
            }
        }

        group.Clear();
    }

    await foreach (var bucketKey in invoicesByBucketKey.Read())
    {
        // starting or continuing a group
        if (group.Count == 0 || group[0].Key == bucketKey.Key)
        {
            // no point keeping more that 1 past max group size
            if (group.Count <= maxGroupSize) 
            {
                group.Add(bucketKey);
            }
        }
        else
        {
            // reach end of group, so generate pairs if it qualifies
            await EmitPairs();
            // start next group
            group.Add(bucketKey);
        }
    }

    await EmitPairs();

    await writer.Complete();
}

var bestPairsByFirstId = new PartiallySortedStream<Pair, int>(
    Files.Open("best-pairs-by-first-id"), 
    x => x.FirstId);

{
    var writer = bestPairsByFirstId.Write();

    (int? FirstId, int? SecondId) groupIds = (null, null);
    byte groupMinBucketId = byte.MaxValue;

    async ValueTask EmitGroupPair()
    {
        if (groupIds.FirstId != null && groupIds.SecondId != null)
        {
            await writer.Add(new Pair 
            { 
                BucketId = groupMinBucketId, 
                FirstId = groupIds.FirstId.Value, 
                SecondId = groupIds.SecondId.Value 
            });
        }

        groupIds = (null, null);
        groupMinBucketId = byte.MaxValue;
    }

    await foreach (var pair in allPairsByFirstAndSecondId.Read())
    {                        
        // continuing a group
        if (groupIds == (pair.FirstId, pair.SecondId))
        {
            groupMinBucketId = Math.Min(groupMinBucketId, pair.BucketId);
        }
        else // reach end of group (or starting first one)
        {                
            await EmitGroupPair();

            // start next group
            groupIds = (pair.FirstId, pair.SecondId);
            groupMinBucketId = pair.BucketId;
        }
    }

    await EmitGroupPair();

    await writer.Complete();
}

var pairsBySecondId = new PartiallySortedStream<PairWithFirstInvoice, int>(
    Files.Open("pairs-by-second-id"), 
    x => x.SecondId);

{
    var writer = pairsBySecondId.Write();

    await foreach (var (firstInvoice, pair) in invoicesByInternalId.InnerJoin(bestPairsByFirstId))
    {
        await writer.Add(new() 
        {
            First = firstInvoice,
            SecondId = pair.SecondId,
            MinBucketId = pair.BucketId,
        });
    }

    await writer.Complete();
}

var scoredPairs = new PartiallySortedStream<PairWithScore, string>(
    Files.Open("scored-pairs"), x => x.LeaderId);

{
    var writer = scoredPairs.Write();

    await foreach (var (secondInvoice, pair) in invoicesByInternalId.InnerJoin(pairsBySecondId))
    {
        var first = pair.First;
        var second = secondInvoice;
        var minBucketId = pair.MinBucketId;
    
        var (leaderId, nonLeaderId) = (first.UniqueId, second.UniqueId);
        if (leaderId.CompareTo(nonLeaderId) < 0)
        {
            (leaderId, nonLeaderId) = (nonLeaderId, leaderId);
        }

        await writer.Add(new PairWithScore
        { 
            LeaderId = leaderId,
            NonLeaderId = nonLeaderId,
            MinBucketId = minBucketId,
            Score = GetFirstNonZeroDigit(first.SupplierName) * GetFirstNonZeroDigit(second.SupplierName)
        });
    }

    await writer.Complete();
}

timer.Stop();
Console.WriteLine($"All: {timer.Elapsed.TotalSeconds}");

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

public static class Files
{
    public static string GetFullPath(string name) => $"output/{name}.parquet";

    public static void Replace(string target, string updated)
    {
        var trash = GetFullPath($"{target}-trash");

        File.Move(GetFullPath(target), trash);
        File.Move(GetFullPath(updated), GetFullPath(target));
        File.Delete(trash);
    }

    public static Stream Open(string name, bool retainExisting = false) 
        => new FileStream(GetFullPath(name),
                            retainExisting ? FileMode.OpenOrCreate : FileMode.Create, 
                            FileAccess.ReadWrite);
}

class UpdateStream<T, K> : IAsyncDisposable
    where T : notnull, new()
{
    public PreSortedStream<T, K> Existing { get; }
    public ITessellateWriter<T> Writer { get; }

    private readonly PreSortedStream<T, K> updated;
    private readonly string _baseName;

    private string UpdatedName => $"{_baseName}-updated";

    public UpdateStream(string baseName, Func<T, K> selectKey)
    {
        _baseName = baseName;
        Existing = new(Files.Open(_baseName, true), selectKey);
        updated = new(Files.Open(UpdatedName, false), selectKey);
        Writer = updated.Write();
    }

    public async ValueTask DisposeAsync()
    {
        await Writer.Complete();
        await Existing.Stream.DisposeAsync();
        await updated.Stream.DisposeAsync();

        Files.Replace(_baseName, UpdatedName);
    }
}

public sealed class IdGenerator(string fileName) : IDisposable
{
    private readonly string _fileName = fileName;
    private int _nextId = File.Exists(fileName) ? int.Parse(File.ReadAllText(fileName)) : 1;

    public int GetNext() => _nextId++;

    public void Dispose() => File.WriteAllText(_fileName, $"{_nextId}");
}

interface IBucketValues
{
    public string InvoiceNumber {get; set; }

    public decimal InvoiceAmount { get; set; }

    public DateTime InvoiceDate { get; set; }

    public string SupplierName { get; set; }
}

class ReceivedInvoiceDeJour : IBucketValues
{
    [ParquetRequired] 
    public string UniqueId {get; set; } = string.Empty;

    [ParquetRequired] 
    public string InvoiceNumber {get; set; } = string.Empty;

    [ParquetDecimal(19, 5)] 
    public decimal InvoiceAmount { get; set; }

    [ParquetDecimal(19, 5)] 
    public decimal BaseAmount { get; set; }

    [ParquetTimestamp] 
    public DateTime InvoiceDate { get; set; }

    [ParquetTimestamp] 
    public string SupplierName { get; set; } = string.Empty;

    [ParquetTimestamp] 
    public string SupplierRef { get; set; } = string.Empty;

    [ParquetTimestamp] 
    public string OrgGroupName { get; set; } = string.Empty;

    [ParquetTimestamp] 
    public string EnteredBy { get; set; } = string.Empty;
}

class InvoiceDeJour : ReceivedInvoiceDeJour
{
    [ParquetRequired] 
    public int InternalId {get; set; }
}

class BucketValues : IBucketValues
{
    [ParquetRequired] 
    public int InternalId {get; set; }

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
    public string Key { get; set; } = string.Empty;

    public int InvoiceIndex { get; set; }
}

class BucketKeyHash
{
    public byte BucketId { get; set; }

    [ParquetRequired] 
    public int KeyHash {get; set; }

    public int InvoiceInternalId { get; set; }
}

class PotentialBucket
{
    public byte BucketId { get; set; }

    public int InvoiceInternalId { get; set; }
}

class Pair
{
    public byte BucketId { get; set; }

    public int FirstId { get; set; }

    public int SecondId { get; set; }
}

class PairWithFirstInvoice
{
    public byte MinBucketId { get; set; }

    [ParquetRequired] 
    public InvoiceDeJour First {get; set; } = new();

    public int SecondId { get; set; }
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

