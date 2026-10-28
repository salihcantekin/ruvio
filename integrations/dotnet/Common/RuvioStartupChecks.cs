using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Ruvio.Integrations;

/// <summary>Marker options whose start-time validation checks a Ruvio registration; holds no settings.</summary>
internal sealed class RuvioStartupCheck;

/// <summary>Start-time checks shared by the integration packages, linked into each one as source.</summary>
internal static class RuvioStartupChecks
{
    /// <summary>
    /// Fails host start, before the first request, when <typeparamref name="TService"/> (keyed by the value
    /// <paramref name="clientKey"/> returns, when not null) is not registered. Looks at the registrations only:
    /// nothing is resolved and no connection is opened. Runs where the host validates options on start;
    /// skipped when the container cannot answer the question.
    /// </summary>
    public static void RequireAtStart<TService>(IServiceCollection services, string package, Func<IServiceProvider, string?> clientKey)
        where TService : class
    {
        string name = package + ":" + typeof(TService).Name;

        services.AddSingleton<IValidateOptions<RuvioStartupCheck>>(provider =>
            new RegistrationValidator<TService>(provider, name, package, clientKey));
        services.AddOptions<RuvioStartupCheck>(name).ValidateOnStart();
    }

    /// <summary>The start-failure message when <typeparamref name="TService"/> is not registered; null when it is or cannot be told.</summary>
    public static string? Missing<TService>(IServiceProvider provider, string package, string? clientKey)
    {
        if (clientKey is null)
        {
            return provider.GetService<IServiceProviderIsService>() is { } services && !services.IsService(typeof(TService))
                ? $"{package} needs {typeof(TService).Name}, but it is not registered. Call services.AddRuvioClient(...) "
                    + "or register the client yourself."
                : null;
        }

        return provider.GetService<IServiceProviderIsKeyedService>() is { } keyed && !keyed.IsKeyedService(typeof(TService), clientKey)
            ? $"{package} uses ClientKey \"{clientKey}\", but no {typeof(TService).Name} is registered under that key. "
                + $"Call services.AddKeyedRuvioClient(\"{clientKey}\", ...) or register the keyed client yourself."
            : null;
    }

    private sealed class RegistrationValidator<TService>(
        IServiceProvider provider, string checkName, string package, Func<IServiceProvider, string?> clientKey)
        : IValidateOptions<RuvioStartupCheck>
    {
        public ValidateOptionsResult Validate(string? name, RuvioStartupCheck options)
        {
            if (name != checkName)
                return ValidateOptionsResult.Skip;

            return Missing<TService>(provider, package, clientKey(provider)) is { } error
                ? ValidateOptionsResult.Fail(error)
                : ValidateOptionsResult.Success;
        }
    }
}
