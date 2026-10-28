using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.AspNetCore.DistributedLock;

/// <summary>Pipeline registration.</summary>
public static class RuvioLockApplicationBuilderExtensions
{
    /// <summary>
    /// Adds the middleware that honours <see cref="RuvioLockAttribute"/>. Call it after
    /// <c>UseRouting</c> and <c>UseAuthentication</c>, and register the services with
    /// <c>AddRuvioDistributedLock</c> and a Ruvio client first.
    /// </summary>
    public static IApplicationBuilder UseRuvioLock(this IApplicationBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);

        // Runs while the pipeline is set up, so a missing AddRuvioDistributedLock fails before the server takes a request.
        if (app.ApplicationServices.GetService<IServiceProviderIsService>() is { } services
            && !services.IsService(typeof(RuvioLockProvider)))
            throw new InvalidOperationException("UseRuvioLock needs services.AddRuvioDistributedLock(...) first.");

        return app.UseMiddleware<RuvioLockMiddleware>();
    }
}
