using FluentAssertions;
using Parquet.MapReduce.Types;
using Parquet.Serialization;

namespace Parquet.MapReduce.Tests;

public class ParquetProjectionTests
{
    private class DataStore<SK, SV, TK, TV>(ProjectKeyValues<SK, SV, TK, TV> projectKeyValues)
    {
        public readonly MemoryStream PreviousMappings = new();
        public readonly MemoryStream UpdatedMappings = new();
        public readonly MemoryStream PreviousContent = new();
        public readonly MemoryStream UpdatedContent = new();
        public readonly MemoryStream TargetUpdates = new();

        public readonly ParquetProjection<SK, SV, TK, TV> Projection = new();

        private static void SwapStreams(Stream previous, Stream updated)
        {
            previous.SetLength(0);
            updated.Position = 0;
            updated.CopyTo(previous);
            updated.SetLength(0);
        }

        public Task Update(IAsyncEnumerable<SourceUpdate<SK, SV>> updates)
        {
            SwapStreams(PreviousMappings, UpdatedMappings);
            SwapStreams(PreviousContent, UpdatedContent);

            return Projection.Update(
                PreviousMappings, UpdatedMappings,
                PreviousContent, UpdatedContent,
                updates, projectKeyValues,
                TargetUpdates);
        }

        public Task Update(params SourceUpdate<SK, SV>[] updates)
            => Update(updates.ToAsyncEnumerable());

        public (Stream Updates, Stream Content) GetResults()
            => (TargetUpdates, UpdatedContent);
    }

    private static async Task AssertContents<T>(Stream stream, params T[] expected) where T : new()
        => (await ParquetSerializer.DeserializeAllAsync<T>(stream).ToListAsync())
            .Should().BeEquivalentTo(expected, o => o.WithStrictOrdering());

    public class StuffIn
    {
        public string FirstName { get; set; } = string.Empty;

        public string LastName { get; set; } = string.Empty;

        public int Copies { get; set; }
    }

    public class StuffOut
    {
        public int Id { get; set; }

        public string FirstFullName { get; set; } = string.Empty;

        public int Copy { get; set; }
    }

    static async IAsyncEnumerable<KeyValuePair<int, StuffOut>> ProjectStuff(int id, IAsyncEnumerable<StuffIn> values)
    {
        var count = 0;
        var copies = 0;
        string firstFullName = string.Empty;
        await foreach (var value in values)
        {
            if (firstFullName == string.Empty)
            {
                firstFullName = $"{value.FirstName} {value.LastName}";
            }

            count++;

            copies = Math.Max(copies, value.Copies);
        }

        for (var i = 1; i <= copies; i++)
        {
            yield return KeyValuePair.Create(count, new StuffOut { Id = id, FirstFullName = firstFullName, Copy = i });
        }
    }

    [Test]
    public async Task ValidFromEmpty()
    {
        var data = new DataStore<int, StuffIn, int, StuffOut>(ProjectStuff);

        await data.Update(
            new() { Key = 1, Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 1 } },
            new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Oldman", Copies = 1 } },
            new() { Key = 2, Value = new StuffIn { FirstName = "Gary", LastName = "Newman", Copies = 1 } },
            new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } });

        // Contents are sorted by (TargetKey, SourceKey)
        await AssertContents<ContentRecord<int, int, StuffOut>>(data.UpdatedContent,
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 } },
            new() { TargetKey = 1, SourceKey = 3, Value = new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 } },
            new() { TargetKey = 2, SourceKey = 2, Value = new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 } });

        // KeyMappings are sorted by SourceKey
        await AssertContents<KeyMapping<int, int>>(data.UpdatedMappings,
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 2, TargetKey = 2 },
            new() { SourceKey = 3, TargetKey = 1 });

        // Generate multiple outputs from one input (replacing source key 1)
        await data.Update(new SourceUpdate<int, StuffIn>
        {
            Key = 1,
            Value = new StuffIn { FirstName = "Randy", LastName = "Newman", Copies = 3 }
        });

        await AssertContents<ContentRecord<int, int, StuffOut>>(data.UpdatedContent,
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 } },
            // Existing 
            new() { TargetKey = 1, SourceKey = 3, Value = new StuffOut { Id = 3, FirstFullName = "Randy Oldman", Copy = 1 } },
            new() { TargetKey = 2, SourceKey = 2, Value = new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 } });

        await AssertContents<KeyMapping<int, int>>(data.UpdatedMappings,
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 2, TargetKey = 2 },
            new() { SourceKey = 3, TargetKey = 1 });

        // Change source key 3 to have 2 records, and thus SK 2 & 3's will contribute to target key 2
        await data.Update(
            new() { Key = 3, Value = new StuffIn { FirstName = "Silly", LastName = "Oldman", Copies = 1 } },
            new() { Key = 3, Value = new StuffIn { FirstName = "Randy", LastName = "Oldman", Copies = 1 } });

        await AssertContents<ContentRecord<int, int, StuffOut>>(data.UpdatedContent,
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 } },
            new() { TargetKey = 2, SourceKey = 2, Value = new StuffOut { Id = 2, FirstFullName = "Gary Oldman", Copy = 1 } },
            new() { TargetKey = 2, SourceKey = 3, Value = new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 } });

        await AssertContents<KeyMapping<int, int>>(data.UpdatedMappings,
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 2, TargetKey = 2 },
            new() { SourceKey = 3, TargetKey = 2 });

        // Delete source key 2
        await data.Update(new SourceUpdate<int, StuffIn>() { Key = 2, Deletion = true });

        await AssertContents<ContentRecord<int, int, StuffOut>>(data.UpdatedContent,
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 1 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 2 } },
            new() { TargetKey = 1, SourceKey = 1, Value = new StuffOut { Id = 1, FirstFullName = "Randy Newman", Copy = 3 } },
            new() { TargetKey = 2, SourceKey = 3, Value = new StuffOut { Id = 3, FirstFullName = "Silly Oldman", Copy = 1 } });

        await AssertContents<KeyMapping<int, int>>(data.UpdatedMappings,
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 1, TargetKey = 1 },
            new() { SourceKey = 3, TargetKey = 2 });
    }

    private static async IAsyncEnumerable<KeyValuePair<int, string>> SimpleText_Identity(int id, IAsyncEnumerable<string> values)
    {
        await foreach (var value in values)
        {
            yield return KeyValuePair.Create(id, value);
        }
    }

    private static async IAsyncEnumerable<KeyValuePair<string, int>> SimpleText_SplitIntoWords(int id, IAsyncEnumerable<string> values)
    {
        await foreach (var value in values)
        {
            foreach (var word in value.Split(' '))
            {
                yield return KeyValuePair.Create(word, id);
            }
        }
    }

    private static async IAsyncEnumerable<KeyValuePair<string, int>> SimpleText_CountWords(string word, IAsyncEnumerable<int> ids)
    {
        yield return KeyValuePair.Create(word, await ids.CountAsync());
    }

    [Test]
    public async Task WordCounting()
    {
        var phrasesById = new DataStore<int, string, int, string>(SimpleText_Identity);
        var booksById = new DataStore<int, string, int, string>(SimpleText_Identity);
        var wordsById = new DataStore<int, string, string, int>(SimpleText_SplitIntoWords);
        var wordCounts = new DataStore<string, int, string, int>(SimpleText_CountWords);

        await phrasesById.Update(
            new() { Key = 1, Value = "the quick brown fox" },
            new() { Key = 2, Value = "jumps over the lazy dog" },
            new() { Key = 3, Value = "sometimes a dog is brown" },
            new() { Key = 4, Value = "brown is my favourite colour" });

        await booksById.Update(
            new() { Key = 1, Value = "the brain police" },
            new() { Key = 2, Value = "sometimes the fox is lazy" },
            new() { Key = 3, Value = "the mystery at dog hall" });

        await wordsById.Update(wordsById.Projection.ReadSources(
            [phrasesById.GetResults(), booksById.GetResults()], default));

        await wordCounts.Update(wordCounts.Projection.ReadSources(
            [wordsById.GetResults()], default));

        await AssertContents<ContentRecord<string, string, int>>(wordCounts.UpdatedContent,
            new ContentRecord<string, string, int>() { TargetKey = "the", SourceKey = "the", Value = 5 });
    }
}
