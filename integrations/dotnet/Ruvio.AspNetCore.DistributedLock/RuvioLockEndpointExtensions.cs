using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>
/// Protects minimal API endpoints with a distributed lock in one line.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why use it.</b> A button double-clicked, a client that retries after a timeout, or two app instances behind a
/// load balancer all produce the same request twice at the same time. Without a lock, both run: two charges, two
/// shipments, two rows. An in-process <c>lock</c> or <c>SemaphoreSlim</c> only stops duplicates inside one instance.
/// This filter makes the second request wait or get an immediate 409, across every instance that shares the Ruvio
/// server.
/// </para>
/// <para>
/// <b>What you get.</b> The lock is taken before your handler runs and released when it finishes, even on an exception
/// or a cancelled request. It is owner-checked, so a request whose lease expired can never release someone else's lock.
/// While the handler runs the lease is renewed in the background, so slow work does not lose its lock, but a crashed
/// process frees it within the lease (30 seconds by default).
/// </para>
/// <para>
/// Register once with <c>AddRuvioDistributedLock()</c> (after the Ruvio client), then:
/// <code>
/// app.MapPost("/orders/{orderId}/pay", PayAsync).WithDistributedLock("pay:{orderId}");
/// </code>
/// </para>
/// </remarks>
public static class RuvioLockEndpointExtensions
{
    /// <summary>
    /// Runs the endpoint only while holding the lock named by <paramref name="resource"/>.
    /// </summary>
    /// <typeparam name="TBuilder">The endpoint builder type.</typeparam>
    /// <param name="builder">The endpoint or group to protect. On a group, every endpoint in it is protected.</param>
    /// <param name="resource">
    /// The lock name. Put route values in braces to lock per entity: <c>"pay:{orderId}"</c> locks order 42 and order 43
    /// independently. Without braces it is one lock for the whole endpoint (a job that must never overlap).
    /// </param>
    /// <param name="lease">
    /// How long a crashed process keeps the lock. Null uses <see cref="RuvioLockOptions.DefaultLease"/>. The lease is
    /// renewed while the request runs, so it need not cover the whole request.
    /// </param>
    /// <param name="wait">
    /// How long to wait for a taken lock. Null uses <see cref="RuvioLockOptions.DefaultWait"/> (zero: answer at once).
    /// </param>
    /// <param name="conflictStatusCode">
    /// The status sent when the lock stayed taken. Null uses <see cref="RuvioLockOptions.ConflictStatusCode"/> (409).
    /// </param>
    /// <returns>The same builder, for chaining.</returns>
    public static TBuilder WithDistributedLock<TBuilder>(
        this TBuilder builder,
        string resource,
        TimeSpan? lease = null,
        TimeSpan? wait = null,
        int? conflictStatusCode = null)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        var template = new RuvioLockHttp.Template(resource);

        return builder.WithDistributedLock(template.Resolve, lease, wait, conflictStatusCode);
    }

    /// <summary>
    /// Like <see cref="WithDistributedLock{TBuilder}(TBuilder, string, TimeSpan?, TimeSpan?, int?)"/>, but computes
    /// the lock name from the request itself (a header, the signed-in user, a query value).
    /// </summary>
    /// <param name="builder">The endpoint or group to protect.</param>
    /// <param name="resource">Returns the lock name for a request. Keep it cheap and never return user input unchecked.</param>
    /// <param name="lease">See the string overload.</param>
    /// <param name="wait">See the string overload.</param>
    /// <param name="conflictStatusCode">See the string overload.</param>
    /// <returns>The same builder, for chaining.</returns>
    public static TBuilder WithDistributedLock<TBuilder>(
        this TBuilder builder,
        Func<HttpContext, string> resource,
        TimeSpan? lease = null,
        TimeSpan? wait = null,
        int? conflictStatusCode = null)
        where TBuilder : IEndpointConventionBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(resource);

        builder.AddEndpointFilter(async (invocation, next) =>
        {
            HttpContext context = invocation.HttpContext;
            string name = resource(context);
            var take = await RuvioLockHttp.TryTakeAsync(context, name, lease, wait).ConfigureAwait(false);

            if (take.Unavailable is not null)
            {
                return take.Unavailable;
            }

            if (take.Bypass)
            {
                // Ruvio is away and the application chose availability: no lock, so no exclusion.
                return await next(invocation).ConfigureAwait(false);
            }

            IRuvioLock? held = take.Held;

            if (held is null)
            {
                return RuvioLockHttp.Conflict(context, conflictStatusCode);
            }

            await using (held.ConfigureAwait(false))
            {
                return await next(invocation).ConfigureAwait(false);
            }
        });

        return builder;
    }

    /// <summary>
    /// The lock held for the current request, or null when the endpoint is not protected. Use its
    /// <see cref="IRuvioLock.LockLost"/> token to stop work if the lock is lost mid-request.
    /// </summary>
    /// <param name="context">The current request.</param>
    public static IRuvioLock? GetRuvioLock(this HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return RuvioLockHttp.Get(context);
    }
}
