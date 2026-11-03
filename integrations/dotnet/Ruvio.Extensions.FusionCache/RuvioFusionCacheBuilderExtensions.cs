using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Ruvio.Client;
using Ruvio.Integrations;
using ZiggyCreatures.Caching.Fusion;

namespace Ruvio.Extensions.FusionCache;

/// <summary>FusionCache builder registration.</summary>
public static class RuvioFusionCacheBuilderExtensions
{
    /// <summary>
    /// Adds <see cref="RuvioBackplane"/> to the cache, on the registered <see cref="IRuvioClient"/> (or the keyed one named
    /// by <see cref="RuvioBackplaneOptions.ClientKey"/>). Options are validated here; a missing client fails host start.
    /// </summary>
    /// <param name="builder">The FusionCache builder.</param>
    /// <param name="configure">Channel prefix and client key.</param>
    /// <returns>The same builder.</returns>
    public static IFusionCacheBuilder WithRuvioBackplane(this IFusionCacheBuilder builder, Action<RuvioBackplaneOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new RuvioBackplaneOptions();

        configure?.Invoke(options);
        options.Validate();
        RuvioStartupChecks.RequireAtStart<IRuvioClient>(builder.Services, "Ruvio.Extensions.FusionCache", _ => options.ClientKey);

        return builder.WithBackplane(provider => new RuvioBackplane(
            options.ClientKey is { } key
                ? provider.GetRequiredKeyedService<IRuvioClient>(key)
                : provider.GetRequiredService<IRuvioClient>(),
            options,
            provider.GetService<ILoggerFactory>()?.CreateLogger("Ruvio.Extensions.FusionCache")));
    }
}
