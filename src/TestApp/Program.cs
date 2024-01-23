
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Parquet.Serialization.Attributes;
using Tessellate;

var rand = new Random();

var logged = DateTime.UtcNow;

var logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger("Program");

var format = new TessellateParquetFormat();

var sorter = new TessellateSorterService(logger)
    .GetSorter<string, InvoiceDeJour>(x => x.InvoiceNumber);

const string localFile = "sid.parquet";

{    
    using var outStream = new FileStream(localFile, FileMode.Create, FileAccess.ReadWrite);

    var target = await format.GetTarget<InvoiceDeJour>(outStream);

    var writer = sorter.Write(target);

    await ParquetGrouperUtil.LogTiming("All", async () =>
    {
        for (var n = 0; n < 100_000_000; n++)
        {
            var inv = new InvoiceDeJour
            {
                InvoiceNumber = $"{rand.Next(0, 10_000_000):00000000}",
                InvoiceAmount = ((decimal)rand.Next(0, 1_000_000)) / 100m,
                InvoiceDate = DateTime.UtcNow.AddSeconds(-n * 5)
            };

            var needsFlush = writer.Add(inv);        
            if (needsFlush)
            {
                await writer.Flush();
            }
            
            if (TimeToLog())
            {
                Console.WriteLine(n);
            }
        }

        await writer.Flush();
    });
}

await ParquetGrouperUtil.LogTiming("Read", async () => 
{
    using var inStream = new FileStream(localFile, FileMode.Open, FileAccess.Read);
    
    var source = await format.GetSource<InvoiceDeJour>(inStream);

    var record = 0;

    await foreach (var invoice in sorter.Read(source))
    {
        record++;

        if (TimeToLog())
        {
            Console.WriteLine($"{record}, {invoice.InvoiceNumber}");
        }
    }

    Console.WriteLine($"Read {record} records");
});

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
    public string InvoiceNumber {get; set; } = string.Empty;

    [ParquetDecimal(19, 5)] 
    public decimal InvoiceAmount { get; set; }

    [ParquetTimestamp] 
    public DateTime InvoiceDate { get; set; }
}
