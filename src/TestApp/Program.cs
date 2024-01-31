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

var hashing = args.Contains("--hashing");
var minimal = args.Contains("--minimal");

var rand = new Random(12345);

var logged = DateTime.UtcNow;

var loggers = LoggerFactory.Create(builder => builder.AddConsole());

Directory.CreateDirectory("output");

var db = new TessellateDatabase(
    new TessellateFileSystemFolder("output"),
    loggers.CreateLogger("Program")
);

var timer = new Stopwatch();
timer.Start();

var invoicesByUniqueId = db.GetTable<InvoiceDeJour, string>(
    "invoices-by-uid", x => x.UniqueId);

await using (var writer = invoicesByUniqueId.Write())
{
    const int count = 1_000_000;

    var ids = Enumerable.Range(0, count).ToArray();

    for (var n = 0; n < ids.Length - 1; n++)
    {
        var i = rand.Next(n, ids.Length);
        (ids[i], ids[n]) = (ids[n], ids[i]);
    }

    var randRange = Math.Max(5, count / 10);

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
}

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

var invoicesByBucketKey = db.GetTable<BucketKey, (byte, string)>(
    "invoices-by-bucket-key", x => (x.BucketId, x.Key));

var invoicesByUniqueIdForBucketing = invoicesByUniqueId.GetView<BucketValues>(x => x.UniqueId);

if (hashing)
{
    var invoicesByBucketKeyHash = db.GetTable<BucketKeyHash, (byte, int)>(
        "invoices-by-bucket-key-hash", x => (x.BucketId, x.KeyHash),
        new TessellateOptions(1_000_000));

    await using (var writer = invoicesByBucketKeyHash.Write())
    {
        var invoiceIndex = 0;

        var reader = minimal
            ? invoicesByUniqueIdForBucketing.Read().Cast<IBucketValues>()
            : invoicesByUniqueId.Read();

        await foreach (var invoice in reader)
        {
            for (byte b = 0; b < buckets.Length; b++)
            {
                await writer.Add(new BucketKeyHash
                { 
                    BucketId = b, 
                    KeyHash = GetSimpleStringHash(buckets[b](invoice)), 
                    InvoiceIndex = invoiceIndex,
                });
            }
            invoiceIndex++;
        }
    }

    var potentialBuckets = db.GetTable<PotentialBucket, int>(
        "potential-buckets", x => x.InvoiceIndex,
        new TessellateOptions(1_000_000));

    await using (var writer = potentialBuckets.Write())
    {
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
                        InvoiceIndex = group[x].InvoiceIndex,
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
    }

    await using (var writer = invoicesByBucketKey.Write())
    {
        if (minimal)
        {
            await foreach (var ((invoice, index), bucket) in invoicesByUniqueIdForBucketing.InnerJoinByIndex(potentialBuckets))
            {
                await writer.Add(new BucketKey 
                { 
                    BucketId = bucket.BucketId, 
                    Key = buckets[bucket.BucketId](invoice), 
                    InvoiceIndex = bucket.InvoiceIndex,
                });
            }
        }
        else
        {
            await foreach (var ((invoice, index), bucket) in invoicesByUniqueId.InnerJoinByIndex(potentialBuckets))
            {
                await writer.Add(new BucketKey 
                { 
                    BucketId = bucket.BucketId, 
                    Key = buckets[bucket.BucketId](invoice), 
                    InvoiceIndex = bucket.InvoiceIndex,
                });
            }
        }
    }
}
else
{
    await using (var writer = invoicesByBucketKey.Write())
    {
        var invoiceIndex = 0;

        var reader = minimal
            ? invoicesByUniqueIdForBucketing.Read().Cast<IBucketValues>()
            : invoicesByUniqueId.Read();

        await foreach (var invoice in reader)
        {
            for (byte b = 0; b < buckets.Length; b++)
            {
                await writer.Add(new BucketKey
                { 
                    BucketId = b, 
                    Key = buckets[b](invoice),
                    InvoiceIndex = invoiceIndex,
                });
            }

            invoiceIndex++;
        }
    }
}

var allPairsByFirstAndSecondId = db.GetTable<Pair, (int FirstIndex, int SecondIndex)>(
    "all-pairs-by-first-second-id", x => (x.FirstIndex, x.SecondIndex));

await using (var writer = allPairsByFirstAndSecondId.Write())
{
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
                        FirstIndex = FirstId, 
                        SecondIndex = SecondId, 
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
}

var bestPairsByFirstId = db.GetTable<Pair, int>(
    "best-pairs-by-first-id", x => x.FirstIndex);

await using (var writer = bestPairsByFirstId.Write())
{
    (int? FirstId, int? SecondId) groupIds = (null, null);
    byte groupMinBucketId = byte.MaxValue;

    async ValueTask EmitGroupPair()
    {
        if (groupIds.FirstId != null && groupIds.SecondId != null)
        {
            await writer.Add(new Pair 
            { 
                BucketId = groupMinBucketId, 
                FirstIndex = groupIds.FirstId.Value, 
                SecondIndex = groupIds.SecondId.Value 
            });
        }

        groupIds = (null, null);
        groupMinBucketId = byte.MaxValue;
    }

    await foreach (var pair in allPairsByFirstAndSecondId.Read())
    {                        
        // continuing a group
        if (groupIds == (pair.FirstIndex, pair.SecondIndex))
        {
            groupMinBucketId = Math.Min(groupMinBucketId, pair.BucketId);
        }
        else // reach end of group (or starting first one)
        {                
            await EmitGroupPair();

            // start next group
            groupIds = (pair.FirstIndex, pair.SecondIndex);
            groupMinBucketId = pair.BucketId;
        }
    }

    await EmitGroupPair();
}

var pairsBySecondId = db.GetTable<PairWithFirstInvoice, int>(
    "pairs-by-second-id", x => x.SecondIndex);

await using (var writer = pairsBySecondId.Write())
{
    await foreach (var ((firstInvoice, index), pair) in invoicesByUniqueId.InnerJoinByIndex(bestPairsByFirstId))
    {
        await writer.Add(new() 
        {
            First = firstInvoice,
            SecondIndex = pair.SecondIndex,
            MinBucketId = pair.BucketId,
        });
    }
}

var scoredPairs = db.GetTable<PairWithScore, string>(
    "scored-pairs", x => x.LeaderId);

await using (var writer = scoredPairs.Write())
{
    await foreach (var ((secondInvoice, index), pair) in invoicesByUniqueId.InnerJoinByIndex(pairsBySecondId))
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

interface IBucketValues
{
    public string InvoiceNumber {get; set; }

    public decimal InvoiceAmount { get; set; }

    public DateTime InvoiceDate { get; set; }

    public string SupplierName { get; set; }
}

class InvoiceDeJour : IBucketValues
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

class BucketValues : IBucketValues
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
    public string Key { get; set; } = string.Empty;

    public int InvoiceIndex { get; set; }
}

class BucketKeyHash
{
    public byte BucketId { get; set; }

    [ParquetRequired] 
    public int KeyHash {get; set; }

    public int InvoiceIndex { get; set; }
}

class PotentialBucket
{
    public byte BucketId { get; set; }

    public int InvoiceIndex { get; set; }
}

class Pair
{
    public byte BucketId { get; set; }

    public int FirstIndex { get; set; }

    public int SecondIndex { get; set; }
}

class PairWithFirstInvoice
{
    public byte MinBucketId { get; set; }

    [ParquetRequired] 
    public InvoiceDeJour First {get; set; } = new();

    public int SecondIndex { get; set; }
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

