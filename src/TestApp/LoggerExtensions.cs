namespace TestApp;

using System.Diagnostics;
using Microsoft.Extensions.Logging;

public static class LoggerExtensions
{
    public static async Task<T> Measure<T>(this ILogger logger, string label, Func<Task<T>> operation)
    {
        var timer = new Stopwatch();
        timer.Start();

        var result = await operation();

        timer.Stop();

        logger.LogInformation("{label}: {elapsed}s", label, timer.Elapsed.TotalSeconds);
        return result;
    }

    public static Task Measure(this ILogger logger, string label, Func<Task> operation)
        => logger.Measure(label, async () => { await operation(); return 0; });
}