using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ruvio.Client;

namespace Ruvio.AspNetCore.Idempotency;

internal static class IdempotencyStore
{
    private static readonly Meter Meter = new("Ruvio.AspNetCore.Idempotency");
    private static readonly Counter<long> StorageCalls = Meter.CreateCounter<long>("ruvio.idempotency.storage_calls");
    private static readonly Counter<long> StorageCommands = Meter.CreateCounter<long>("ruvio.idempotency.storage_commands");
    private static readonly Counter<long> States = Meter.CreateCounter<long>("ruvio.idempotency.states");
    private static readonly Histogram<double> StorageDuration = Meter.CreateHistogram<double>(
        "ruvio.idempotency.storage_duration", "ms");

    private static readonly Counter<long> Outages = Meter.CreateCounter<long>("ruvio.idempotency.outage");

    internal static void RecordOutage(RuvioOutageMode mode, string reason, string stage) =>
        Outages.Add(1, new TagList
        {
            { "mode", mode == RuvioOutageMode.FailOpen ? "fail_open" : "fail_closed" },
            { "reason", reason },
            { "stage", stage },
        });

    internal static async Task<(string State, string? Result)> AcquireAsync(
        IRuvioClient client, string key, string fingerprint, string owner, TimeSpan ttl, CancellationToken token, bool count = true)
    {
        long started = Stopwatch.GetTimestamp();
        var reply = await MeasureStorageAsync("begin", started,
            client.BeginIdempotencyAsync(key, fingerprint, owner, (long)ttl.TotalMilliseconds, token))
            .ConfigureAwait(false);

        var items = reply.Items;

        if (reply.Kind != RespKind.Array || items is null || items.Count is < 1 or > 2)
            throw new InvalidOperationException("Invalid idempotency acquisition reply.");

        var state = items[0].AsString();

        if (state == "completed" && items.Count == 2 && items[1].Kind == RespKind.Bulk)
        {
            if (count)
                RecordState(state);

            return (state, items[1].AsString());
        }

        if (items.Count == 1 && state is "acquired" or "pending" or "conflict")
        {
            if (count)
                RecordState(state);

            return (state, null);
        }

        throw new InvalidOperationException("Invalid idempotency acquisition state.");
    }

    internal static async Task CompleteAsync(
        IRuvioClient client, string key, string fingerprint, string owner, string result, CancellationToken token)
    {
        long started = Stopwatch.GetTimestamp();

        // A failed or lost reply does not prove the server did not complete the record, so the error
        // is raised for the caller and never retried here.
        if (!await MeasureStorageAsync("complete", started,
            client.CompleteIdempotencyAsync(key, fingerprint, owner, result, token)).ConfigureAwait(false))
            throw new InvalidOperationException("Idempotency ownership was lost or completion failed; outcome is uncertain.");
    }

    /// <summary>
    /// Gives up the pending record after the handler threw. A failure here is counted and swallowed so the handler's
    /// own exception reaches the caller; the record then stays pending until its retention ends, as without the option.
    /// </summary>
    internal static async Task AbortAsync(IRuvioClient client, string key, string fingerprint, string owner)
    {
        long started = Stopwatch.GetTimestamp();

        try
        {
            if (await MeasureStorageAsync("abort", started,
                client.AbortIdempotencyAsync(key, fingerprint, owner, CancellationToken.None)).ConfigureAwait(false))
                RecordState("aborted");
        }
        catch (Exception)
        {
            // Already counted with outcome "error" by MeasureStorageAsync.
        }
    }

    /// <summary>Records the outcome of a storage call that has already started; no delegate is allocated.</summary>
    private static async ValueTask<T> MeasureStorageAsync<T>(string operation, long started, ValueTask<T> pending)
    {
        string outcome = "error";

        try
        {
            T result = await pending.ConfigureAwait(false);
            outcome = "success";

            return result;
        }
        finally
        {
            var tags = new TagList
            {
                { "operation", operation },
                { "outcome", outcome }
            };
            
            StorageCalls.Add(1, tags);
            StorageCommands.Add(1, tags);
            StorageDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);
        }
    }

    internal static void RecordState(string state)
        => States.Add(1, new TagList { { "state", state } });
}
