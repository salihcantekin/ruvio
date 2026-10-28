using System.Reflection;
using System.Text;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.OutputCaching.Tests;

public class StoreEdgeTests
{
    public class StoreClient : DispatchProxy
    {
        public Func<IReadOnlyList<ReadOnlyMemory<byte>>, RespValue> Binary { get; set; } = _ => Resp.Null();
        public Func<IReadOnlyList<IReadOnlyList<string>>, IReadOnlyList<RespValue>> Many { get; set; } = c => [.. c.Select(_ => Resp.Null())];
        public Func<string[], IReadOnlyList<string?>> GetMany { get; set; } = k => [.. k.Select(_ => (string?)null)];
        public List<string[]> ManyCommands { get; } = [];
        public List<string> Operations { get; } = [];

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Operations.Add(targetMethod!.Name);

            switch (targetMethod.Name)
            {
                case nameof(IRuvioClient.ExecuteBinaryAsync):
                    return new ValueTask<RespValue>(Binary((IReadOnlyList<ReadOnlyMemory<byte>>)args![0]!));
                case nameof(IRuvioClient.ExecuteManyAsync):
                    var commands = (IReadOnlyList<IReadOnlyList<string>>)args![0]!;

                    ManyCommands.AddRange(commands.Select(c => c.ToArray()));

                    return new ValueTask<IReadOnlyList<RespValue>>(Many(commands));
                case nameof(IRuvioClient.GetManyAsync):
                    return new ValueTask<IReadOnlyList<string?>>(GetMany((string[])args![0]!));
            }

            throw new NotSupportedException(targetMethod.Name);
        }
    }

    private static (RuvioOutputCacheStore Store, StoreClient Client) Make(Action<RuvioOutputCacheOptions>? configure = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, StoreClient>();
        var options = new RuvioOutputCacheOptions();

        configure?.Invoke(options);

        return (new RuvioOutputCacheStore(client, Options.Create(options)), (StoreClient)(object)client);
    }

    private static readonly string Hash = new('A', 64);
    private static readonly string Version = new('b', 32);

    private static RespValue Record(string magic = "ROC1", string count = "01", string? tags = null, string value = "payload") =>
        Resp.Bulk(Encoding.ASCII.GetBytes(magic + count + (tags ?? Hash + Version) + value));

    [Fact]
    public void OptionsRejectEveryOutOfRangeSetting()
    {
        void Build(Action<RuvioOutputCacheOptions> configure) => Make(configure);

        Assert.Throws<ArgumentException>(() => Build(o => o.InstanceName = " "));
        Assert.Throws<ArgumentException>(() => Build(o => o.InstanceName = null!));
        Assert.Throws<ArgumentException>(() => Build(o => o.InstanceName = new string('i', 257)));
        Build(o => o.InstanceName = new string('i', 256));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxValueBytes = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxValueBytes = 8 * 1024 * 1024 + 1));
        Build(o => o.MaxValueBytes = 8 * 1024 * 1024);
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxKeyBytes = 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxKeyBytes = 64 * 1024 + 1));
        Build(o => o.MaxKeyBytes = 64 * 1024);
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxTagsPerEntry = -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Build(o => o.MaxTagsPerEntry = 33));
        Build(o => o.MaxTagsPerEntry = 0);
        Build(o => o.MaxTagsPerEntry = 32);
        Assert.Throws<ArgumentNullException>(() => new RuvioOutputCacheStore((IRuvioClient)null!, Options.Create(new RuvioOutputCacheOptions())));
        Assert.Throws<ArgumentNullException>(() => new RuvioOutputCacheStore(DispatchProxy.Create<IRuvioClient, StoreClient>(), null!));
        Assert.Throws<ArgumentNullException>(() => ((Microsoft.Extensions.DependencyInjection.IServiceCollection)null!).AddRuvioOutputCache());
    }

    [Fact]
    public void KeysAreStableNamespacedAndDistinguishEntriesFromTags()
    {
        (RuvioOutputCacheStore a, _) = Make(o => o.InstanceName = "a");
        (RuvioOutputCacheStore b, _) = Make(o => o.InstanceName = "b");

        Assert.Equal(a.EntryKey("k"), a.EntryKey("k"));
        Assert.NotEqual(a.EntryKey("k"), a.TagKey("k"));
        Assert.NotEqual(a.EntryKey("k"), b.EntryKey("k"));
        Assert.Matches("^ruvio:output:v1:\\{[0-9A-F]{64}\\}:entry:[0-9A-F]{64}$", a.EntryKey("k"));
        Assert.Matches("^ruvio:output:v1:\\{[0-9A-F]{64}\\}:tag:[0-9A-F]{64}$", a.TagKey("k"));
        Assert.NotEqual(a.EntryKey(new string('x', 2000)), a.EntryKey(new string('x', 2001)));
        Assert.Equal(a.EntryKey(new string('x', 3000)).Length, a.EntryKey("short").Length);
    }

    [Fact(Timeout = 30000)]
    public async Task GetReturnsNullForMissingStaleOrMismatchedEntriesAndRejectsCorruption()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make(o => o.MaxTagsPerEntry = 2);

        client.Binary = _ => Resp.Null();
        Assert.Null(await store.GetAsync("k", default));

        client.Binary = _ => Record(count: "00", tags: "", value: "hello");
        Assert.Equal("hello", Encoding.ASCII.GetString((await store.GetAsync("k", default))!));
        Assert.DoesNotContain(nameof(IRuvioClient.GetManyAsync), client.Operations);

        client.Binary = _ => Record();
        client.GetMany = keys =>
        {
            Assert.Single(keys);
            Assert.EndsWith(":tag:" + Hash, keys[0]);

            return [Version];
        };
        Assert.Equal("payload", Encoding.ASCII.GetString((await store.GetAsync("k", default))!));

        client.GetMany = _ => [new string('c', 32)];
        Assert.Null(await store.GetAsync("k", default));
        client.GetMany = _ => [null];
        Assert.Null(await store.GetAsync("k", default));
        client.GetMany = _ => [Version + "x"];
        Assert.Null(await store.GetAsync("k", default));

        client.Binary = _ => Record(count: "02", tags: Hash + Version + Hash + Version);
        client.GetMany = _ => [Version, "z" + Version[1..]];
        Assert.Null(await store.GetAsync("k", default));

        foreach (RespValue bad in new[]
        {
            Resp.Int(1),
            Resp.Simple("OK"),
            Resp.Bulk("ROC"),
            Record(magic: "XXXX"),
            Record(count: "0x", tags: ""),
            Record(count: "x1", tags: ""),
            Record(count: "03", tags: new string('a', 96 * 3)),
            Record(count: "02", tags: Hash + Version),
            Record(tags: new string('g', 96)),
            Record(tags: Hash + new string('z', 32)),
        })
        {
            client.Binary = _ => bad;
            await Assert.ThrowsAsync<RuvioException>(async () => await store.GetAsync("k", default));
        }

        (RuvioOutputCacheStore small, StoreClient smallClient) = Make(o => o.MaxValueBytes = 4);

        smallClient.Binary = _ => Record(count: "00", tags: "", value: "12345");
        await Assert.ThrowsAsync<RuvioException>(async () => await small.GetAsync("k", default));
        smallClient.Binary = _ => Record(count: "00", tags: "", value: "1234");
        Assert.Equal(4, (await small.GetAsync("k", default))!.Length);

        client.Binary = _ => Resp.Error("ERR down");
        Assert.Contains("down", (await Assert.ThrowsAsync<RuvioException>(async () => await store.GetAsync("k", default))).Message);
        client.Binary = _ => Resp.Error("");
        await Assert.ThrowsAsync<RuvioException>(async () => await store.GetAsync("k", default));
    }

    [Fact(Timeout = 30000)]
    public async Task EvictAcceptsZeroOrOneAndRejectsAnythingElse()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make();

        foreach (long ok in new long[] { 0, 1 })
        {
            client.Binary = args =>
            {
                Assert.Equal("DEL", Encoding.ASCII.GetString(args[0].Span));

                return Resp.Int(ok);
            };
            await store.EvictByTagAsync("t", default);
        }

        foreach (RespValue bad in new[] { Resp.Int(2), Resp.Int(-1), Resp.Simple("OK"), Resp.Null() })
        {
            client.Binary = _ => bad;
            await Assert.ThrowsAsync<RuvioException>(async () => await store.EvictByTagAsync("t", default));
        }
    }

    [Fact(Timeout = 30000)]
    public async Task SetWithoutTagsWritesOneEntryAndChecksTheReply()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make();
        byte[]? record = null;

        client.Binary = args =>
        {
            record = args[2].ToArray();

            return Resp.Ok;
        };
        await store.SetAsync("k", [1, 2, 3], null, TimeSpan.FromSeconds(1.5), default);
        Assert.Equal("ROC100"u8.ToArray().Concat(new byte[] { 1, 2, 3 }).ToArray(), record);
        Assert.DoesNotContain(nameof(IRuvioClient.ExecuteManyAsync), client.Operations);

        foreach (RespValue bad in new[] { Resp.Simple("QUEUED"), Resp.Int(1), Resp.Null() })
        {
            client.Binary = _ => bad;
            await Assert.ThrowsAsync<RuvioException>(async () => await store.SetAsync("k", [], [], TimeSpan.FromSeconds(1), default));
        }

        client.Binary = _ => Resp.Error("ERR no");
        await Assert.ThrowsAsync<RuvioException>(async () => await store.SetAsync("k", [], [], TimeSpan.FromSeconds(1), default));

        client.Binary = _ => Resp.Ok;
        await store.SetAsync("k", new byte[1024 * 1024], [], TimeSpan.FromDays(365), default);
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.SetAsync("k", new byte[1024 * 1024 + 1], [], TimeSpan.FromSeconds(1), default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.SetAsync("k", [], [], TimeSpan.FromDays(365) + TimeSpan.FromMilliseconds(1), default));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await store.SetAsync("k", [], [], TimeSpan.FromTicks(9999), default));
        await store.SetAsync("k", [], [], TimeSpan.FromMilliseconds(1), default);
    }

    [Fact(Timeout = 30000)]
    public async Task SetWithTagsCreatesExtendsOrKeepsTagGenerations()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make();
        string existing = new('d', 32);

        client.Binary = _ => Resp.Ok;
        client.Many = commands =>
        {
            if (commands[0][0] == "GET")
            {
                return
                [
                    Resp.Null(), Resp.Int(-2),
                    Resp.Bulk(existing), Resp.Int(500),
                    Resp.Bulk(existing), Resp.Int(5000),
                ];
            }

            return [.. commands.Select(_ => Resp.Ok)];
        };

        await store.SetAsync("k", [7], ["new", "short", "long"], TimeSpan.FromSeconds(2), default);

        string[][] writes = [.. client.ManyCommands.Where(c => c[0] is "SET" or "PEXPIRE")];

        Assert.Equal(2, writes.Length);
        Assert.Equal("SET", writes[0][0]);
        Assert.Equal(["PX", "2000"], writes[0][^2..]);
        Assert.Equal(32, writes[0][2].Length);
        Assert.Equal("PEXPIRE", writes[1][0]);
        Assert.Equal("2000", writes[1][2]);
    }

    [Fact(Timeout = 30000)]
    public async Task SetWithTagsThatAreAllCurrentWritesOnlyTheEntry()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make();
        string existing = new('0', 32);

        client.Binary = _ => Resp.Ok;
        client.Many = commands => [Resp.Bulk(existing), Resp.Int(60_000)];
        await store.SetAsync("k", [], ["t"], TimeSpan.FromSeconds(1), default);
        Assert.Single(client.ManyCommands, c => c[0] == "GET");
        Assert.DoesNotContain(client.ManyCommands, c => c[0] is "SET" or "PEXPIRE");
    }

    [Fact(Timeout = 30000)]
    public async Task TagStateThatCannotBeTrustedIsAnError()
    {
        (RuvioOutputCacheStore store, StoreClient client) = Make();

        client.Binary = _ => Resp.Ok;

        foreach (RespValue[] reply in new[]
        {
            new[] { Resp.Null() },
            new[] { Resp.Int(1), Resp.Int(1) },
            new[] { Resp.Simple("x"), Resp.Int(1) },
            new[] { Resp.Bulk("short"), Resp.Int(1) },
            new[] { Resp.Bulk(new string('g', 32)), Resp.Int(1) },
            new[] { Resp.Bulk(new string('a', 32)), Resp.Null() },
            new[] { Resp.Bulk(new string('a', 33)), Resp.Int(1) },
        })
        {
            client.Many = _ => reply;
            await Assert.ThrowsAsync<RuvioException>(async () => await store.SetAsync("k", [], ["t"], TimeSpan.FromSeconds(1), default));
        }

        client.Many = commands => commands[0][0] == "GET"
            ? [Resp.Null(), Resp.Int(-2)]
            : [Resp.Error("ERR readonly")];
        Assert.Contains("readonly", (await Assert.ThrowsAsync<RuvioException>(
            async () => await store.SetAsync("k", [], ["t"], TimeSpan.FromSeconds(1), default))).Message);

        client.Many = commands => commands[0][0] == "GET"
            ? [Resp.Null(), Resp.Int(-2)]
            : [Resp.Error("")];
        await Assert.ThrowsAsync<RuvioException>(async () => await store.SetAsync("k", [], ["t"], TimeSpan.FromSeconds(1), default));
    }
}
