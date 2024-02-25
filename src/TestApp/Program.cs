using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tessellate;
using TestApp;

var logged = DateTime.UtcNow;

var loggers = LoggerFactory.Create(builder => builder.AddConsole());

// When this is disposed, all temporary files created from it are deleted
using var files = new FileSource("/mnt/temp");

var blobs = new ProcessingBlobs(
    "https://blipvart.blob.core.windows.net", 
    "tessellate", 
    loggers.CreateLogger<ProcessingBlobs>());

var proc = new Processing(blobs, new TableSource(files, loggers.CreateLogger<TableSource>()));

var logger = loggers.CreateLogger("Main");

var quit = new CancellationTokenSource();
var logMemory = Task.Run(() => LogMemory(logger, quit.Token));

await logger.Measure("All", async () =>
{
    // Generate fake incoming invoices
    var incomingByUniqueId = await logger.Measure(
        "GetIncomingInvoices", () => proc.GetIncomingInvoices(10_000_000, 9_000_000));

    // Assign them each an InternalId, performing an upsert against our store of known invoices
    var incomingByInternalId = await logger.Measure(
        "UpdateInvoicesByUniqueId", () => proc.GetIncomingInvoicesByInternalId(incomingByUniqueId));

    incomingByUniqueId.Dispose(); // Don't need this anymore, save disk space!

    // Update our store of known invoices sorted by InternalId and return the updated version
    var allByInternalId = await logger.Measure(
        "UpdateInvoicesByInternalId", () => proc.GetAllInvoicesByInternalId(incomingByInternalId));

    incomingByInternalId.Dispose();

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

    invoicesByBucketKeyHash.Dispose();

    // Generate the bucket key for every invoice that appears in a potential bucket
    var invoicesByBucketKey = await logger.Measure(
        "GetInvoicesByBucketKey", () => proc.GetInvoicesByBucketKey(potentialBuckets, allByInternalId));

    potentialBuckets.Dispose();

    // Produce all pairs from any bucket with between 2 and 5 invoices
    var allPairs = await logger.Measure(
        "GetPairsByFirstAndSecondId", () => proc.GetPairsByFirstAndSecondId(invoicesByBucketKey));

    invoicesByBucketKey.Dispose();

    // Where pairs have been found by multiple buckets, only keep the lowest bucket ID, and sort
    // the first invoice's InternalId so we're ready to join with allByInternalId
    var bestPairsByFirstId = await logger.Measure(
        "GetBestPairsByFirstId", () => proc.GetBestPairsByFirstId(allPairs));

    allPairs.Dispose();

    // Join the first invoice's data and sort by second invoice's InternalId so we're ready to
    // do another pass to pick up the second invoice's data
    var pairsBySecondId = await logger.Measure(
        "GetPairsBySecondId", () => proc.GetPairsBySecondId(bestPairsByFirstId, allByInternalId));

    bestPairsByFirstId.Dispose();

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
        var timerAll = new Stopwatch();
        timerAll.Start();
        while (!cancellation.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(2), cancellation);

            var (fileCount, fileBytes) = files.GetStats();

            var pq = 100 * Timers.PriorityQueue.Elapsed.TotalSeconds / timerAll.Elapsed.TotalSeconds;

            logger.LogInformation("Memory {memory} - {files} files totalling {saved}, {pq}%",
                FormatSize(Process.GetCurrentProcess().PrivateMemorySize64), fileCount, FormatSize(fileBytes), pq);
        }
    }
    catch (TaskCanceledException) {}
}

string FormatSize(double value)
{
    var unit = 0;
    while (value > 900)
    {
        unit++;
        value /= 1000;
    }

    var unitName = new[] { "B", "KB", "MB", "GB", "TB", "PB" }[unit];

    return $"{Math.Round(value * 100) / 100} {unitName}";
}
