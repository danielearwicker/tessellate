using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tessellate;
using TestApp;

var logged = DateTime.UtcNow;

var loggers = LoggerFactory.Create(builder => builder.AddConsole());

// When this is disposed, all temporary files created from it are deleted
using var files = new FileSource();

var blobs = new ProcessingBlobs(
    "https://blipvart.blob.core.windows.net", 
    "tessellate", 
    loggers.CreateLogger<ProcessingBlobs>());

var proc = new Processing(blobs, new TableSource(files));

var logger = loggers.CreateLogger("Main");

var quit = new CancellationTokenSource();
var logMemory = Task.Run(() => LogMemory(logger, quit.Token));

await logger.Measure("All", async () =>
{
    // Generate fake incoming invoices
    var incomingByUniqueId = await logger.Measure(
        "GetIncomingInvoices", () => proc.GetIncomingInvoices(100_000, 2_000_000));

    // Assign them each an InternalId, performing an upsert against our store of known invoices
    var incomingByInternalId = await logger.Measure(
        "UpdateInvoicesByUniqueId", () => proc.GetIncomingInvoicesByInternalId(incomingByUniqueId));

    // Update our store of known invoices sorted by InternalId and return the updated version
    var allByInternalId = await logger.Measure(
        "UpdateInvoicesByInternalId", () => proc.GetAllInvoicesByInternalId(incomingByInternalId));

    // Generate the bucket key and then an integer hash of it, sorting all invoices by that. It is
    // important to keep these rows small as we generate I * B of them (I = invoices, B = buckets).
    // This is why we initially produce the integer hash of the bucket key, as bucket keys can be
    // quite long strings.
    var invoicesByBucketKeyHash = await logger.Measure(
        "GetInvoiceIdsByBucketKeyHash", () => proc.GetInvoiceIdsByBucketKeyHash(allByInternalId));

    // Find all integer key hashes that were produced by >= 2 invoices, as they are worth
    // producing the full bucket key for (though they may be just random collisions)
    var potentialBuckets = await logger.Measure(
        "GetPotentialBuckets", () => proc.GetPotentialBuckets(invoicesByBucketKeyHash));

    // Generate the bucket key for every invoice that appears in a potential bucket
    var invoicesByBucketKey = await logger.Measure(
        "GetInvoicesByBucketKey", () => proc.GetInvoicesByBucketKey(potentialBuckets, allByInternalId));

    // Produce all pairs from any bucket with between 2 and 5 invoices
    var allPairs = await logger.Measure(
        "GetPairsByFirstAndSecondId", () => proc.GetPairsByFirstAndSecondId(invoicesByBucketKey));

    // Where pairs have been found by multiple buckets, only keep the lowest bucket ID, and sort
    // the first invoice's InternalId so we're ready to join with allByInternalId
    var bestPairsByFirstId = await logger.Measure(
        "GetBestPairsByFirstId", () => proc.GetBestPairsByFirstId(allPairs));

    // Join the first invoice's data and sort by second invoice's InternalId so we're ready to
    // do another pass to pick up the second invoice's data
    var pairsBySecondId = await logger.Measure(
        "GetPairsBySecondId", () => proc.GetPairsBySecondId(bestPairsByFirstId, allByInternalId));

    // Join the second invoice's data and produce comparison score, and choose which of the pair
    // is the "leader", and sort by the leader's InternalId.
    await logger.Measure(
        "SavePairScores", () => proc.SavePairScores(pairsBySecondId, allByInternalId));
});

quit.Cancel();
await logMemory;

async Task LogMemory(ILogger logger, CancellationToken cancellation)
{    
    try
    {
        while (!cancellation.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellation);

            logger.LogInformation("Private bytes: {bytes}", Process.GetCurrentProcess().PrivateMemorySize64);
        }
    }
    catch (TaskCanceledException) {}
}
