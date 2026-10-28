namespace Ruvio.Extensions.Idempotency;

/// <summary>What <see cref="RuvioIdempotentExecutor"/> did for one call.</summary>
public enum IdempotentOutcome
{
    /// <summary>This call ran the work (and stored its result, see <see cref="IdempotentResult{T}.Stored"/>).</summary>
    Executed,

    /// <summary>An earlier call already finished the work; its stored result is returned and the work did not run.</summary>
    Replayed,

    /// <summary>Another call is running the work right now, or one began it and failed; the work did not run.</summary>
    Pending,

    /// <summary>The key is in use with a different fingerprint (other input under the same key); the work did not run.</summary>
    Conflict,
}

/// <summary>The outcome of <see cref="RuvioIdempotentExecutor.ExecuteOnceAsync(string, string?, Func{CancellationToken, Task{byte[]}}, TimeSpan?, CancellationToken)"/> and its value.</summary>
/// <typeparam name="T">The result type.</typeparam>
/// <param name="Outcome">What happened.</param>
/// <param name="Value">The result for <see cref="IdempotentOutcome.Executed"/> and <see cref="IdempotentOutcome.Replayed"/>; default otherwise.</param>
/// <param name="Stored">
/// False when the work ran but its result could not be stored, because the record expired while the work ran (it
/// took longer than the retention). A later call with the same key then runs the work again.
/// </param>
public readonly record struct IdempotentResult<T>(IdempotentOutcome Outcome, T? Value, bool Stored = true)
{
    /// <summary>True for <see cref="IdempotentOutcome.Executed"/> and <see cref="IdempotentOutcome.Replayed"/>: <see cref="Value"/> holds the result.</summary>
    public bool HasValue => Outcome is IdempotentOutcome.Executed or IdempotentOutcome.Replayed;
}
