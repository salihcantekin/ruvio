namespace Ruvio.Client;

/// <summary>
/// Sorts the exceptions a Ruvio call throws, for a retry or circuit-breaker policy of your own (Polly,
/// <c>Microsoft.Extensions.Resilience</c> or a loop): whether trying again later can succeed, and whether the
/// failed call may already have changed data.
/// </summary>
/// <remarks>
/// The client already follows <c>MOVED</c>/<c>ASK</c>, waits out <c>TRYAGAIN</c> during a slot move and finds a new
/// primary after a failover; these helpers judge what still reaches the caller. Retry a call only when
/// <see cref="IsTransient"/> is true and either the command is safe to repeat (a read, a <c>SET</c> of the same value,
/// a command carrying an idempotency key) or <see cref="MayHaveRun"/> is false.
/// </remarks>
/// <example>
/// <code>
/// var pipeline = new ResiliencePipelineBuilder()
///     .AddRetry(new RetryStrategyOptions
///     {
///         // INCR is not safe to repeat: retry it only when it surely did not run.
///         ShouldHandle = args => ValueTask.FromResult(args.Outcome.Exception is { } e
///             &amp;&amp; RuvioErrors.IsTransient(e) &amp;&amp; !RuvioErrors.MayHaveRun(e)),
///         MaxRetryAttempts = 3,
///     })
///     .Build();
///
/// long visits = await pipeline.ExecuteAsync(async token => await db.IncrementAsync("visits"), cancellationToken);
/// </code>
/// </example>
public static class RuvioErrors
{
    /// <summary>
    /// True when the same call can succeed later without any change on the caller's side: Ruvio could not be reached
    /// or is not serving yet (see <see cref="RuvioOutageOptions.IsOutage"/>), or it answered <c>TRYAGAIN</c> (a slot or
    /// key is moving), <c>READONLY</c> (a failover is in progress) or <c>NOREPLICAS</c> (too few replicas confirmed a
    /// durable write in time). False for every other server reply, such as <c>WRONGTYPE</c>, <c>OOM</c> or a syntax error,
    /// and when <paramref name="cancellationToken"/> was cancelled by the caller.
    /// </summary>
    /// <param name="error">The exception the call threw.</param>
    /// <param name="cancellationToken">The token the call was made with; its own cancellation is never transient.</param>
    /// <returns>Whether retrying later can succeed.</returns>
    public static bool IsTransient(Exception error, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(error);

        return RuvioOutageOptions.IsOutage(error, cancellationToken)
            || error is RuvioException { IsServerReply: true } reply
                && (reply.Message.StartsWith("TRYAGAIN", StringComparison.Ordinal)
                    || reply.Message.StartsWith("READONLY", StringComparison.Ordinal)
                    || reply.Message.StartsWith("NOREPLICAS", StringComparison.Ordinal));
    }

    /// <summary>
    /// True when the failed call may have changed data, so repeating a write that is not safe to repeat could apply it
    /// twice. False only when Ruvio surely did not run it: an open circuit (Ruvio was not called) or a server error
    /// reply, which the server sends instead of running the command. <c>NOREPLICAS</c> is the exception: the write was
    /// applied on the primary and only the replica confirmation timed out, so it counts as run. A dropped connection,
    /// a timeout or a cancellation after sending may have run, including when <see cref="RuvioException.ClosedBeforeReply"/> is true.
    /// </summary>
    /// <param name="error">The exception the call threw.</param>
    /// <returns>Whether the command may have been applied.</returns>
    public static bool MayHaveRun(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return error switch
        {
            RuvioCircuitOpenException => false,
            RuvioException { IsServerReply: true } reply => reply.Message.StartsWith("NOREPLICAS", StringComparison.Ordinal),
            _ => true,
        };
    }
}
