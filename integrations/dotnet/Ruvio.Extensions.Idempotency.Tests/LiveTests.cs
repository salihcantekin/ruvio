using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Idempotency.Tests;

/// <summary>Run against a real server: set RUVIO_TEST_ADDR to host:port, otherwise each test returns at once.</summary>
public class LiveTests
{
    [Fact]
    public async Task EveryStateAgainstARealServer()
    {
        string? address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");

        if (string.IsNullOrWhiteSpace(address))
        {
            return;
        }

        var uri = new Uri("tcp://" + address);
        await using RuvioConnection connection = await RuvioConnection.ConnectAsync(uri.Host, uri.Port);
        IRuvioClient client = await connection.GetDefaultDatabaseAsync();
        var executor = new RuvioIdempotentExecutor(client, new RuvioIdempotentExecutorOptions
        {
            KeyPrefix = "test:idem:" + Guid.NewGuid().ToString("N") + ":",
            DefaultRetention = TimeSpan.FromSeconds(30),
        });
        int runs = 0;
        byte[] payload = [0, 13, 10, 255, 36];
        Func<CancellationToken, Task<byte[]>> work = _ => { runs++; return Task.FromResult(payload); };

        IdempotentResult<byte[]> first = await executor.ExecuteOnceAsync("m", "sha:a", work);
        IdempotentResult<byte[]> replay = await executor.ExecuteOnceAsync("m", "sha:a", work);
        IdempotentResult<byte[]> conflict = await executor.ExecuteOnceAsync("m", "sha:b", work);

        Assert.Equal(IdempotentOutcome.Executed, first.Outcome);
        Assert.True(first.Stored);
        Assert.Equal(IdempotentOutcome.Replayed, replay.Outcome);
        Assert.Equal(payload, replay.Value);
        Assert.Equal(IdempotentOutcome.Conflict, conflict.Outcome);
        Assert.Equal(1, runs);

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<IdempotentResult<Receipt>> running = executor.ExecuteOnceAsync("json", null, async _ =>
        {
            started.SetResult();
            await finish.Task;

            return new Receipt("r-1", 5);
        }, TestJson.Default.Receipt).AsTask();

        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(IdempotentOutcome.Pending, await executor.ExecuteOnceAsync("json", null, _ => Task.CompletedTask));
        finish.SetResult();
        Assert.Equal(IdempotentOutcome.Executed, (await running.WaitAsync(TimeSpan.FromSeconds(10))).Outcome);

        IdempotentResult<Receipt> stored = await executor.ExecuteOnceAsync("json", null, _ => Task.FromResult(new Receipt("other", 0)), TestJson.Default.Receipt);

        Assert.Equal(IdempotentOutcome.Replayed, stored.Outcome);
        Assert.Equal(new Receipt("r-1", 5), stored.Value);
    }
}
