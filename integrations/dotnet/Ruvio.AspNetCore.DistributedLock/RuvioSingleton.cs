using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Work that must run on one instance at a time, registered with <c>AddRuvioSingleton</c>. A new instance is
/// resolved (in its own scope) every time this process becomes the one that runs it.
/// </summary>
public interface IRuvioSingletonWorker
{
    /// <summary>
    /// Runs while this instance holds the lock. <paramref name="stoppingToken"/> is cancelled when the host stops or the
    /// lock is lost; stop then, because another instance may already be running. When this returns or throws, the lock is
    /// released and every instance competes for it again after <see cref="RuvioSingletonOptions.RetryInterval"/>.
    /// </summary>
    /// <param name="leadership">The held lock; its <see cref="IRuvioLock.FencingToken"/> is set when fencing is on.</param>
    /// <param name="stoppingToken">Cancelled on host shutdown or when the lock is lost.</param>
    Task ExecuteAsync(IRuvioLock leadership, CancellationToken stoppingToken);
}

/// <summary>Settings for one <c>AddRuvioSingleton</c> registration.</summary>
public sealed class RuvioSingletonOptions
{
    /// <summary>
    /// The lease of the lock. It is renewed every third of its length while the work runs, so this is how long a crashed
    /// instance keeps the others waiting. 30 seconds by default.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long an instance that did not get the lock (or whose work ended) waits before it tries again. 5 seconds by default.
    /// One <c>SET NX</c> (or <c>LEASE</c>) per instance per interval.
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    internal void Validate()
    {
        if (Lease < TimeSpan.FromMilliseconds(3))
        {
            throw new ArgumentOutOfRangeException(nameof(Lease), "Lease must be at least 3 milliseconds.");
        }

        if (RetryInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryInterval), "RetryInterval must be positive.");
        }
    }
}

internal sealed class RuvioSingletonService<TWorker> : BackgroundService
    where TWorker : class, IRuvioSingletonWorker
{
    private readonly string name;
    private readonly RuvioSingletonOptions options;
    private readonly IServiceProvider services;
    private readonly ILogger? logger;

    public RuvioSingletonService(string name, RuvioSingletonOptions options, IServiceProvider services)
    {
        this.name = name;
        this.options = options;
        this.services = services;
        logger = services.GetService<ILoggerFactory>()?.CreateLogger("Ruvio.AspNetCore.DistributedLock.Singleton");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        RuvioLockProvider provider = services.GetRequiredService<RuvioLockProvider>();
        TimeProvider time = provider.Options.TimeProvider;

        if (!provider.Options.AutoRenew)
        {
            throw new InvalidOperationException("AddRuvioSingleton needs AutoRenew on the lock options; without renewal the lock ends after one lease.");
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            IRuvioLock? held = null;

            try
            {
                held = await provider.TryAcquireAsync(name, options.Lease, TimeSpan.Zero, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is Client.RuvioException or IOException or TimeoutException)
            {
                logger?.LogWarning(exception, "Singleton {Name}: could not ask Ruvio for the lock; trying again.", name);
            }

            if (held is not null)
            {
                await RunAsync(held, stoppingToken).ConfigureAwait(false);
            }

            try
            {
                await Task.Delay(options.RetryInterval, time, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task RunAsync(IRuvioLock held, CancellationToken stoppingToken)
    {
        await using (held.ConfigureAwait(false))
        {
            using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, held.LockLost);
            AsyncServiceScope scope = services.CreateAsyncScope();

            await using (scope.ConfigureAwait(false))
            {
                try
                {
                    await scope.ServiceProvider.GetRequiredService<TWorker>().ExecuteAsync(held, linked.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested)
                {
                    // Shutdown or a lost lock: the worker stopped as asked.
                }
                catch (Exception exception)
                {
                    logger?.LogError(exception, "Singleton {Name}: the work failed; the lock is released.", name);
                }
            }

            if (held.LockLost.IsCancellationRequested)
            {
                logger?.LogWarning("Singleton {Name}: the lock was lost; the work was cancelled.", name);
            }
        }
    }
}
