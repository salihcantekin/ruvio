using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Idempotency.Tests;

public sealed record Receipt(string Id, long Amount);

[JsonSerializable(typeof(Receipt))]
public sealed partial class TestJson : JsonSerializerContext;

/// <summary>The executor against an in-memory IDEM record store, so every state runs without a server.</summary>
public class ExecutorTests
{
    public class IdemClient : DispatchProxy
    {
        public Dictionary<string, (string Fingerprint, string Owner, byte[]? Result)> Records { get; } = [];
        public List<string> Keys { get; } = [];
        public List<TimeSpan> Retentions { get; } = [];
        public long? CompleteReply { get; set; }
        public int Aborts { get; set; }
        public bool AbortFails { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(IRuvioClient.TryBeginIdempotencyAsync):
                {
                    var key = (string)args![0]!;
                    var fingerprint = (string)args[1]!;
                    var owner = (string)args[2]!;

                    Keys.Add(key);
                    Retentions.Add((TimeSpan)args[3]!);

                    if (!Records.TryGetValue(key, out var record))
                    {
                        Records[key] = (fingerprint, owner, null);

                        return new ValueTask<IdempotencyBegin>(new IdempotencyBegin(IdempotencyState.Acquired, null));
                    }

                    IdempotencyBegin begin = record.Fingerprint != fingerprint ? new(IdempotencyState.Conflict, null)
                        : record.Result is null ? new(IdempotencyState.Pending, null)
                        : new(IdempotencyState.Completed, record.Result);

                    return new ValueTask<IdempotencyBegin>(begin);
                }

                case nameof(IRuvioClient.ExecuteBinaryAsync):
                {
                    var command = (IReadOnlyList<ReadOnlyMemory<byte>>)args![0]!;
                    string Text(int index) => Encoding.UTF8.GetString(command[index].Span);

                    Assert.Equal("IDEM", Text(0));
                    Assert.Equal("COMPLETE", Text(2));
                    string key = Text(1);
                    var record = Records[key];

                    if (CompleteReply is { } forced)
                    {
                        return new ValueTask<RespValue>(Resp.Int(forced));
                    }

                    if (record.Fingerprint != Text(3) || record.Owner != Text(4) || record.Result is not null)
                    {
                        return new ValueTask<RespValue>(Resp.Int(0));
                    }

                    Records[key] = (record.Fingerprint, record.Owner, command[5].ToArray());

                    return new ValueTask<RespValue>(Resp.Int(1));
                }

                case nameof(IRuvioClient.AbortIdempotencyAsync):
                {
                    var key = (string)args![0]!;

                    Aborts++;
                    Assert.Equal(CancellationToken.None, (CancellationToken)args[3]!);

                    if (AbortFails)
                    {
                        return ValueTask.FromException<bool>(new RuvioException("unreachable"));
                    }

                    bool removed = Records.TryGetValue(key, out var record) && record.Result is null
                        && record.Fingerprint == (string)args[1]! && record.Owner == (string)args[2]!;

                    if (removed)
                    {
                        Records.Remove(key);
                    }

                    return new ValueTask<bool>(removed);
                }

                default:
                    throw new NotSupportedException(targetMethod.Name);
            }
        }
    }

    private static (RuvioIdempotentExecutor Executor, IdemClient Fake) New(RuvioIdempotentExecutorOptions? options = null)
    {
        var client = DispatchProxy.Create<IRuvioClient, IdemClient>();

        return (new RuvioIdempotentExecutor(client, options), (IdemClient)(object)client);
    }

    [Fact]
    public async Task TheFirstCallRunsAndRepeatsReplayTheBytes()
    {
        var (executor, fake) = New();
        int runs = 0;
        Func<CancellationToken, Task<byte[]>> work = _ => { runs++; return Task.FromResult(new byte[] { 0, 255, 7 }); };

        IdempotentResult<byte[]> first = await executor.ExecuteOnceAsync("m-1", "sha:1", work);
        IdempotentResult<byte[]> second = await executor.ExecuteOnceAsync("m-1", "sha:1", work);

        Assert.Equal(IdempotentOutcome.Executed, first.Outcome);
        Assert.True(first.Stored);
        Assert.Equal(IdempotentOutcome.Replayed, second.Outcome);
        Assert.True(second.HasValue);
        Assert.Equal(new byte[] { 0, 255, 7 }, second.Value);
        Assert.Equal(1, runs);
        Assert.Equal("idem:m-1", fake.Keys[0]);
        Assert.Equal(TimeSpan.FromDays(1), fake.Retentions[0]);
    }

    [Fact]
    public async Task ARunningOrFailedCallIsPendingAndOtherInputIsAConflict()
    {
        var (executor, _) = New();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await executor.ExecuteOnceAsync("m-2", null, (Func<CancellationToken, Task>)(_ => throw new InvalidOperationException("boom"))));

        int runs = 0;
        IdempotentOutcome pending = await executor.ExecuteOnceAsync("m-2", null, _ => { runs++; return Task.CompletedTask; });
        IdempotentResult<byte[]> conflict = await executor.ExecuteOnceAsync("m-2", "other", _ => { runs++; return Task.FromResult(new byte[1]); });

        Assert.Equal(IdempotentOutcome.Pending, pending);
        Assert.Equal(IdempotentOutcome.Conflict, conflict.Outcome);
        Assert.False(conflict.HasValue);
        Assert.Null(conflict.Value);
        Assert.Equal(0, runs);
    }

    [Fact]
    public async Task AbortOnExceptionFreesTheKeyForEveryOverloadAndKeepsTheException()
    {
        var (executor, fake) = New(new RuvioIdempotentExecutorOptions { AbortOnException = true });
        int runs = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await executor.ExecuteOnceAsync("a-1", null, (Func<CancellationToken, Task>)(_ => throw new InvalidOperationException("boom"))));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await executor.ExecuteOnceAsync("a-2", null, (Func<CancellationToken, Task<byte[]>>)(_ => throw new InvalidOperationException("boom"))));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await executor.ExecuteOnceAsync<Receipt>("a-3", null, _ => throw new InvalidOperationException("boom"), TestJson.Default.Receipt));

        Assert.Equal(3, fake.Aborts);
        Assert.Equal(IdempotentOutcome.Executed, await executor.ExecuteOnceAsync("a-1", null, _ => { runs++; return Task.CompletedTask; }));
        Assert.Equal(IdempotentOutcome.Executed, (await executor.ExecuteOnceAsync("a-2", null, _ => { runs++; return Task.FromResult(new byte[1]); })).Outcome);
        Assert.Equal(2, runs);
        Assert.Equal(3, fake.Aborts);
    }

    [Fact]
    public async Task AFailedAbortKeepsTheWorksExceptionAndThePendingRecord()
    {
        var (executor, fake) = New(new RuvioIdempotentExecutorOptions { AbortOnException = true });

        fake.AbortFails = true;
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await executor.ExecuteOnceAsync("f-1", null, (Func<CancellationToken, Task>)(_ => throw new InvalidOperationException("boom"))));

        Assert.Equal("boom", thrown.Message);
        Assert.Equal(1, fake.Aborts);
        Assert.Equal(IdempotentOutcome.Pending, await executor.ExecuteOnceAsync("f-1", null, _ => Task.CompletedTask));
    }

    [Fact]
    public async Task JsonResultsRoundTrip()
    {
        var (executor, _) = New(new RuvioIdempotentExecutorOptions { KeyPrefix = "pay:" });
        int runs = 0;
        Func<CancellationToken, Task<Receipt>> work = _ => { runs++; return Task.FromResult(new Receipt("r-9", 1250)); };

        IdempotentResult<Receipt> first = await executor.ExecuteOnceAsync("m-3", "sha:3", work, TestJson.Default.Receipt);
        IdempotentResult<Receipt> again = await executor.ExecuteOnceAsync("m-3", "sha:3", work, TestJson.Default.Receipt, TimeSpan.FromHours(1));

        Assert.Equal(new Receipt("r-9", 1250), first.Value);
        Assert.Equal(IdempotentOutcome.Replayed, again.Outcome);
        Assert.Equal(new Receipt("r-9", 1250), again.Value);
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task WorkWithoutAResultReplaysAsReplayed()
    {
        var (executor, fake) = New();
        int runs = 0;

        Assert.Equal(IdempotentOutcome.Executed, await executor.ExecuteOnceAsync("m-4", null, _ => { runs++; return Task.CompletedTask; }));
        Assert.Equal(IdempotentOutcome.Replayed, await executor.ExecuteOnceAsync("m-4", null, _ => { runs++; return Task.CompletedTask; }, TimeSpan.FromMinutes(5)));
        Assert.Equal(1, runs);
        Assert.Empty(fake.Records["idem:m-4"].Result!);
        Assert.Equal(TimeSpan.FromMinutes(5), fake.Retentions[1]);
    }

    [Fact]
    public async Task AnExpiredRecordIsReportedAsNotStored()
    {
        var (executor, fake) = New();

        fake.CompleteReply = 0;
        IdempotentResult<byte[]> result = await executor.ExecuteOnceAsync("m-5", null, _ => Task.FromResult(new byte[] { 1 }));

        Assert.Equal(IdempotentOutcome.Executed, result.Outcome);
        Assert.False(result.Stored);
        Assert.Equal(new byte[] { 1 }, result.Value);

        fake.CompleteReply = 7;
        await Assert.ThrowsAsync<RuvioException>(async () => await executor.ExecuteOnceAsync("m-6", null, _ => Task.FromResult(new byte[] { 1 })));
    }

    [Fact]
    public async Task InputsAreValidatedBeforeAnythingIsSent()
    {
        var (executor, fake) = New();
        Func<CancellationToken, Task<byte[]>> work = _ => Task.FromResult(new byte[1]);

        Assert.Throws<ArgumentNullException>(() => new RuvioIdempotentExecutor(null!));
        Assert.Throws<ArgumentException>(() => New(new RuvioIdempotentExecutorOptions { KeyPrefix = "" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => New(new RuvioIdempotentExecutorOptions { DefaultRetention = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => New(new RuvioIdempotentExecutorOptions { DefaultRetention = TimeSpan.FromDays(8) }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await executor.ExecuteOnceAsync("", null, work));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await executor.ExecuteOnceAsync("k", null, (Func<CancellationToken, Task<byte[]>>)null!));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await executor.ExecuteOnceAsync("k", "", work));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await executor.ExecuteOnceAsync("k", new string('f', 129), work));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await executor.ExecuteOnceAsync("k", null, work, TimeSpan.FromTicks(15000)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await executor.ExecuteOnceAsync("k", null, work, RuvioIdempotentExecutorOptions.MaximumRetention + TimeSpan.FromMilliseconds(1)));
        Assert.Empty(fake.Keys);
        Assert.Equal(IdempotentOutcome.Executed, (await executor.ExecuteOnceAsync("k", new string('f', 128), work, RuvioIdempotentExecutorOptions.MaximumRetention)).Outcome);
    }

    [Fact]
    public async Task RegistrationUsesTheUnkeyedOrTheKeyedClient()
    {
        var keyed = DispatchProxy.Create<IRuvioClient, IdemClient>();
        var services = new ServiceCollection();

        services.AddKeyedSingleton("idem", keyed);
        services.AddRuvioIdempotentExecutor(o => o.ClientKey = "idem");
        await using ServiceProvider provider = services.BuildServiceProvider();

        await provider.GetRequiredService<RuvioIdempotentExecutor>().ExecuteOnceAsync("k", null, _ => Task.CompletedTask);
        Assert.Single(((IdemClient)(object)keyed).Keys);

        var unkeyed = new ServiceCollection();

        unkeyed.AddRuvioIdempotentExecutor();
        await using ServiceProvider missing = unkeyed.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => missing.GetRequiredService<RuvioIdempotentExecutor>());
        Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddRuvioIdempotentExecutor(o => o.DefaultRetention = TimeSpan.Zero));
    }
}
