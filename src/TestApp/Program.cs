using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Serialization.Attributes;
using Tessellate;

var rand = new Random(12345);

var logged = DateTime.UtcNow;

var loggers = LoggerFactory.Create(builder => builder.AddConsole());

Directory.CreateDirectory("output");

var db = new TessellateDatabase(
    new TessellateFileSystemFolder("output"),
    loggers.CreateLogger("Program")
);

await ParquetGrouperUtil.LogTiming("End-to-end", async () =>
{
    var invoicesByUniqueId = db.GetTable<InvoiceDeJour, string>(
        "invoices-by-uid", x => x.UniqueId);

    {
        await using var writer = invoicesByUniqueId.Write();

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
                SupplierName = $"s{rand.Next(0, randRange):0000000000}",
            });                
        }
    }

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

    var invoicesByBucketKey = db.GetTable<BucketKey, (byte, string)>(
        "invoices-by-bucket-key", x => (x.BucketId, x.Key));

    await using (var writer = invoicesByBucketKey.Write())
    {
        await foreach (var invoice in invoicesByUniqueId.Read())
        {
            for (byte b = 0; b < buckets.Length; b++)
            {
                await writer.Add(new BucketKey 
                { 
                    BucketId = b, 
                    Key = buckets[b](invoice), 
                    UniqueId = invoice.UniqueId,
                });
            }
        }
    }

    var allPairsByFirstAndSecondId = db.GetTable<Pair, (string FirstId, string SecondId)>(
        "all-pairs-by-first-second-id", x => (x.FirstId, x.SecondId));
    
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
                        await writer.Add(new Pair 
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
    
    var bestPairsByFirstId = db.GetTable<Pair, string>(
        "best-pairs-by-first-id", x => x.FirstId);

    await using (var writer = bestPairsByFirstId.Write())
    {
        (string? FirstId, string? SecondId) groupIds = (null, null);
        byte groupMinBucketId = byte.MaxValue;

        async ValueTask EmitGroupPair()
        {
            if (groupIds.FirstId != null && groupIds.SecondId != null)
            {
                await writer.Add(new Pair { BucketId = groupMinBucketId, FirstId = groupIds.FirstId, SecondId = groupIds.SecondId });
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
    }

    var pairsBySecondId = db.GetTable<PairWithFirstInvoice, string>(
        "pairs-by-second-id", x => x.SecondId);

    await using (var writer = pairsBySecondId.Write())
    {
        await foreach (var (pair, firstInvoice) in bestPairsByFirstId.InnerJoin(invoicesByUniqueId))
        {
            await writer.Add(new() 
            {
                First = firstInvoice,
                SecondId = pair.SecondId,
                MinBucketId = pair.BucketId,
            });
        }
    }
    
    var scoredPairs = db.GetTable<PairWithScore, string>(
        "scored-pairs", x => x.LeaderId);

    await using (var writer = scoredPairs.Write())
    {
        await foreach (var (pair, secondInvoice) in pairsBySecondId.InnerJoin(invoicesByUniqueId))
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

