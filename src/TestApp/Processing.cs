namespace TestApp;

using System.Diagnostics;
using Parquet;
using Parquet.Serialization;
using Tessellate;
using SuperLinq.Async;
using Tessellate.AsyncEnumerableExtensions;
using TestApp.Models;

public interface IProcessing
{
    public Task<ITempSortedTable<InvoiceDeJour, string>> 
        GetIncomingInvoices(int count, int randRange);

    public Task<ITempSortedTable<InvoiceDeJour, int>> 
        GetIncomingInvoicesByInternalId(
            ISortedTable<InvoiceDeJour, string> inputInvoicesByUniqueId);

    public Task<ITempSortedTable<InvoiceDeJour, int>> 
        GetAllInvoicesByInternalId(
            ITempSortedTable<InvoiceDeJour, int> inputInvoicesByInternalId);

    public Task<ITempSortedTable<BucketKeyHash, (byte BucketId, int InternalId)>> 
        GetInvoiceIdsByBucketKeyHash(
            ISortedTable<InvoiceDeJour, int> invoicesByInternalId);

    public Task<ITempSortedTable<PotentialBucket, int>> 
        GetPotentialBuckets(
            ISortedTable<BucketKeyHash, (byte BucketId, int InternalId)> invoicesByBucketKeyHash);

    public Task<ITempSortedTable<BucketKey, (byte BucketId, string BucketKey)>> 
        GetInvoicesByBucketKey(
            ISortedTable<PotentialBucket, int> potentialBuckets,
            ISortedTable<InvoiceDeJour, int> invoicesByInternalId);

    public Task<ITempSortedTable<Pair, (int FirstId, int SecondId)>> 
        GetPairsByFirstAndSecondId(
            ISortedTable<BucketKey, (byte BucketId, string BucketKey)> invoicesByBucketKey);

    public Task<ITempSortedTable<Pair, int>> 
        GetBestPairsByFirstId(
            ISortedTable<Pair, (int FirstId, int SecondId)> allPairsByFirstAndSecondId);

    public Task<ITempSortedTable<PairWithFirstInvoice, int>> 
        GetPairsBySecondId(
            ISortedTable<Pair, int>  bestPairsByFirstId,
            ITempSortedTable<InvoiceDeJour, int> invoicesByInternalId);

    public Task SavePairScores(
        ISortedTable<PairWithFirstInvoice, int> pairsBySecondId,
        ITempSortedTable<InvoiceDeJour, int> invoicesByInternalId);
}

public class Processing(IProcessingBlobs blobs, ITableSource tables) : IProcessing
{
    public static int GetSimpleStringHash(string str)
    {
        unchecked
        {
            uint b = 378551;
            uint a = 63689;
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

    private readonly Random rand = new(/*12345*/);

    public async Task<ITempSortedTable<InvoiceDeJour, string>> GetIncomingInvoices(int count, int randRange)
    {
        var table = tables.MergeSorting((InvoiceDeJour x) => x.UniqueId, loggingName: "incoming-invoices");

        var writer = table.Write();

        var ids = Enumerable.Range(0, Math.Max(randRange, count)).ToArray();

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
                InternalId = n,
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
        
        return table;
    }

    // To get the maximum internal Id ever generated we can just read the last
    // record of the raw parquet file
    private static async Task<int> GetMaximumInternalId(Stream stream)
    {
        if (stream.Length > 0)
        {
            using var reader = await ParquetReader.CreateAsync(stream);
            if (reader.RowGroupCount > 0)
            {
                using var lastRowGroup = reader.OpenRowGroupReader(reader.RowGroupCount - 1);
                var lastRows = await ParquetSerializer.DeserializeAsync<InternalIdOnly>(lastRowGroup, reader.Schema);
                if (lastRows.Count > 0)
                {
                    return lastRows[lastRows.Count - 1].InternalId;
                }
            }
        }

        return 0;
    }

    public async Task<ITempSortedTable<InvoiceDeJour, int>> GetIncomingInvoicesByInternalId(
        ISortedTable<InvoiceDeJour, string> inputInvoicesByUniqueId)
    {
        // As we update invoices-by-uid, we'll generate this temporary set of upserted by InternalID
        var inputInvoicesByInternalId = tables.MergeSorting((InvoiceDeJour x) => x.InternalId, 
            loggingName: "incoming-invoices-by-iid");

        // Permanent store of invoices by UniqueId
        var invoicesByUid = tables.MergeSorting((InvoiceDeJour x) => x.UniqueId,
            loggingName: "invoices-by-uid-original");

        var invoicesByUidBlob = blobs.Get("invoices-by-uid");
        await invoicesByUidBlob.Download(invoicesByUid.Stream);

        var invoicesByUidUpdated = tables.MergeSorting((InvoiceDeJour x) => x.UniqueId,
            loggingName: "invoices-by-uid-updated");

        // Permanent reverse index, UniqueId by InternalId
        var uidsByInternalId = tables.MergeSorting((UniqueIdByInternalId x) => x.InternalId,
            loggingName: "uid-by-iid-original");
            
        var uidsByInternalIdBlob = blobs.Get("uids-by-iid");
        await uidsByInternalIdBlob.Download(uidsByInternalId.Stream);
        
        var uidsByInternalIdUpdated = tables.MergeSorting((UniqueIdByInternalId x) => x.InternalId,
            loggingName: "uid-by-iid-updated");

        int added = 0;
        int updated = 0;
        int unchanged = 0;

        var writerByInternalId = inputInvoicesByInternalId.Write();
        var writerByUidUpdated = invoicesByUidUpdated.Write();
        var writerByIIdUpdated = uidsByInternalIdUpdated.Write();

        int maxId = await GetMaximumInternalId(uidsByInternalId.Stream);

        await foreach (var (existing, input) in invoicesByUid.FullJoin(inputInvoicesByUniqueId))
        {
            InvoiceDeJour retained;

            if (input == null)
            {
                unchanged++;
                Debug.Assert(existing != null);

                retained = existing;            
            }
            else // we have input, so it replaces existing
            {
                if (existing == null)
                {
                    added++;
                    // never seen this UniqueId before, so make new InternalId
                    input.InternalId = ++maxId;
                }
                else // updating, so retain existing InternalId
                {
                    updated++;
                    input.InternalId = existing.InternalId;
                }

                retained = input;

                await writerByInternalId.Add(input);
            }

            await writerByUidUpdated.Add(retained);
            await writerByIIdUpdated.Add(new UniqueIdByInternalId 
            { 
                UniqueId = retained.UniqueId, 
                InternalId = retained.InternalId 
            });
        }

        await writerByUidUpdated.Complete();
        await writerByInternalId.Complete();
        await writerByIIdUpdated.Complete();

        await uidsByInternalIdBlob.Upload(uidsByInternalIdUpdated.Stream);
        await invoicesByUidBlob.Upload(invoicesByUidUpdated.Stream);

        Console.WriteLine($"Added: {added}, Updated: {updated}, Unchanged: {unchanged}");

        return inputInvoicesByInternalId;
    }

    public async Task<ITempSortedTable<InvoiceDeJour, int>> GetAllInvoicesByInternalId(
        ITempSortedTable<InvoiceDeJour, int> inputInvoicesByInternalId)
    {
        // Permanent store of invoices by InternalId
        var invoicesByInternalId = tables.MergeSorting((InvoiceDeJour x) => x.InternalId,
            loggingName: "invoices-by-iid-original");

        var invoicesByInternalIdBlob = blobs.Get("invoices-by-iid");
        await invoicesByInternalIdBlob.Download(invoicesByInternalId.Stream);
    
        var invoicesByIidUpdated = await invoicesByInternalId.FullJoin(inputInvoicesByInternalId)
            .Select(x => x.Right ?? x.Left!)
            .SortInto(tables, (InvoiceDeJour x) => x.InternalId,
                loggingName: "invoices-by-iid-updated");

        await invoicesByInternalIdBlob.Upload(invoicesByIidUpdated.Stream);
        return invoicesByIidUpdated;
    }

    private static string GetDateOnly(DateTime dt) => dt.ToString("yyyy-MM-dd");

    // Should be NumericInvoiceNumber also!

    private static readonly IReadOnlyList<Func<BucketValues, string>> _buckets =
    [
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}",
        i => $"{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}:{i.SupplierName}",
        i => $"{i.InvoiceNumber}:{i.InvoiceAmount}:{i.SupplierName}",
        i => $"{i.InvoiceNumber}:{GetDateOnly(i.InvoiceDate)}:{i.SupplierName}",
        i => $"{GetDateOnly(i.InvoiceDate)}:{i.InvoiceAmount}:{i.SupplierName}",
    ];

    public async Task<ITempSortedTable<BucketKeyHash, (byte BucketId, int InternalId)>> 
        GetInvoiceIdsByBucketKeyHash(ISortedTable<InvoiceDeJour, int> invoicesByInternalId)
    {
        // Only reading the columns we need for bucket
        var invoicesByInternalIdForBucketing = invoicesByInternalId.Cast<BucketValues>(b => b.InternalId);
        
        return await invoicesByInternalIdForBucketing
            .SelectMany(invoice => _buckets.Select((bucket, bucketId) => new BucketKeyHash
            {
                BucketId = (byte)bucketId, 
                KeyHash = GetSimpleStringHash(bucket(invoice)), 
                InvoiceInternalId = invoice.InternalId,
            }).ToAsyncEnumerable())
            .SortInto(tables, (BucketKeyHash x) => (x.BucketId, x.KeyHash), 1_000_000,
                loggingName: "iid-by-bucketid-keyhash");
    }

    public Task<ITempSortedTable<PotentialBucket, int>> GetPotentialBuckets(
        ISortedTable<BucketKeyHash, (byte BucketId, int InternalId)> invoicesByBucketKeyHash)
        => invoicesByBucketKeyHash            
            .GroupAdjacent(x => (x.BucketId, x.KeyHash))
            .Where(x => x.Count() > 1)
            .SelectMany(g => g.ToAsyncEnumerable())
            .Select(x => new PotentialBucket 
            { 
                BucketId = x.BucketId,
                InvoiceInternalId = x.InvoiceInternalId 
            })
            .SortInto(tables,
                (PotentialBucket x) => x.InvoiceInternalId, 1_000_000,
                loggingName: "potential-buckets");
    

    public Task<ITempSortedTable<BucketKey, (byte BucketId, string BucketKey)>> GetInvoicesByBucketKey(
        ISortedTable<PotentialBucket, int> potentialBuckets,
        ISortedTable<InvoiceDeJour, int> invoicesByInternalId)
        => invoicesByInternalId.InnerJoin(potentialBuckets)
            .Select(item => new BucketKey 
            { 
                BucketId = item.Right.BucketId, 
                Key = _buckets[item.Right.BucketId](item.Left), 
                InvoiceInternalId = item.Right.InvoiceInternalId,
            })
            .SortInto(tables, (BucketKey x) => (x.BucketId, x.Key),
                loggingName: "iid-by-bucketid-key");

    private static IEnumerable<Pair> GeneratePairs(IReadOnlyList<BucketKey> group)
    {        
        // all items in this group have the same bucket ID (and key)       
        var bucketId = group[0].BucketId;
         
        for (var x = 0; x < group.Count - 1; x++)
        {
            for (var y = x + 1; y < group.Count; y++)
            {
                var (FirstId, SecondId) = (group[x].InvoiceInternalId, group[y].InvoiceInternalId);
                if (FirstId > SecondId)
                {
                    (FirstId, SecondId) = (SecondId, FirstId);
                }

                yield return new Pair 
                { 
                    FirstId = FirstId, 
                    SecondId = SecondId, 
                    BucketId = bucketId 
                };
            }
        }
    }

    public Task<ITempSortedTable<Pair, (int FirstId, int SecondId)>> GetPairsByFirstAndSecondId(
        ISortedTable<BucketKey, (byte BucketId, string BucketKey)> invoicesByBucketKey)
        => invoicesByBucketKey
            .GroupAdjacentAggregated(x => x.Key, () => new List<BucketKey>(), (g, x) =>
            {
                if (g.Count < 6) g.Add(x);
                return g;
            })
            .Where(g => g.Count >= 2 && g.Count <= 5)
            .SelectMany(g => GeneratePairs(g).ToAsyncEnumerable())
            .SortInto(tables, (Pair x) => (x.FirstId, x.SecondId),
                loggingName: "pair-by-first-second)");

    public Task<ITempSortedTable<Pair, int>> GetBestPairsByFirstId(
        ISortedTable<Pair, (int FirstId, int SecondId)> allPairsByFirstAndSecondId)    
        => allPairsByFirstAndSecondId
            .GroupAdjacent(p => (p.FirstId, p.SecondId))
            .Select(g => new Pair 
            { 
                BucketId = g.Min(x => x.BucketId),
                FirstId = g.Key.FirstId, 
                SecondId = g.Key.SecondId
            })
            .Into(tables, (Pair x) => x.FirstId); // already sorted

    public Task<ITempSortedTable<PairWithFirstInvoice, int>> GetPairsBySecondId(
        ISortedTable<Pair, int> bestPairsByFirstId,
        ITempSortedTable<InvoiceDeJour, int> invoicesByInternalId)
        => invoicesByInternalId
            .InnerJoin(bestPairsByFirstId)
            .Select(x => new PairWithFirstInvoice
            {
                First = x.Left,
                SecondId = x.Right.SecondId,
                MinBucketId = x.Right.BucketId,
            })
            .SortInto(tables, (PairWithFirstInvoice x) => x.SecondId,
                loggingName: "pairs-with-first-by-second");

    private static PairWithScore ScorePair(InvoiceDeJour first, InvoiceDeJour second, byte minBucketId)
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

    public async Task SavePairScores(
        ISortedTable<PairWithFirstInvoice, int> pairsBySecondId,
        ITempSortedTable<InvoiceDeJour, int> invoicesByInternalId)
    {
        var scoredPairs = await invoicesByInternalId.InnerJoin(pairsBySecondId)
            .Select(x => ScorePair(x.Right.First, x.Left, x.Right.MinBucketId))
            .SortInto(tables, (PairWithScore x) => x.LeaderId,
                loggingName: "pair-scores-by-leader");

        await blobs.Get("scored-pairs-by-leader").Upload(scoredPairs.Stream);
    }

    // Dummy scoring function
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

}
