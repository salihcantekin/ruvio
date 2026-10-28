namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>A held lock. Dispose it (or call <see cref="ReleaseAsync"/>) to release it.</summary>
public interface IRuvioLock : IAsyncDisposable
{
    /// <summary>The locked resource name, without the key prefix.</summary>
    string Resource { get; }

    /// <summary>False once the lock was released, or a renewal found it expired or taken over.</summary>
    bool IsHeld { get; }

    /// <summary>
    /// Cancelled when the lock is lost while the work is still running. Pass it to the guarded work
    /// so it stops instead of running alongside the new owner.
    /// </summary>
    CancellationToken LockLost { get; }

    /// <summary>
    /// The fencing token of this lock when <see cref="RuvioLockOptions.Fenced"/> is on: a number that only grows for one
    /// resource, also across expiry and release. Pass it to the resource you write and refuse writes carrying a smaller one,
    /// so an owner whose lease ran out cannot overwrite the next owner's work. Null when fencing is off.
    /// </summary>
    long? FencingToken => null;

    /// <summary>
    /// Sets the lease to <paramref name="lease"/> from now (or the original lease when null).
    /// False when the lock is no longer ours.
    /// </summary>
    ValueTask<bool> ExtendAsync(TimeSpan? lease = null, CancellationToken cancellationToken = default);

    /// <summary>Releases the lock only if it is still ours. True when a lock of ours was removed.</summary>
    ValueTask<bool> ReleaseAsync(CancellationToken cancellationToken = default);
}
