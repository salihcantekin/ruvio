using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Ruvio.Integrations;

/// <summary>
/// Checks the endpoint settings a package reads per request once the request pipeline is built and before
/// the server accepts requests, and fails host start with every bad endpoint listed. Runs as a startup
/// filter, so endpoints mapped on the app and in a Startup class's <c>UseEndpoints</c> are both seen.
/// </summary>
internal sealed class RuvioEndpointCheck(string package, Func<IServiceProvider, Endpoint, string?> check) : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
    {
        next(app);

        if (app.ApplicationServices.GetService<EndpointDataSource>() is not { } source)
            return;

        List<string>? errors = null;

        foreach (Endpoint endpoint in source.Endpoints)
        {
            if (check(app.ApplicationServices, endpoint) is { } error)
                (errors ??= []).Add($"{endpoint.DisplayName ?? "(unnamed endpoint)"}: {error}");
        }

        if (errors is not null)
            throw new InvalidOperationException($"{package} found invalid endpoint settings:{Environment.NewLine}  "
                + string.Join(Environment.NewLine + "  ", errors));
    };

    /// <summary>Adds the check once; the marker type is this package's own, as the file is linked into each package.</summary>
    public static void Add(IServiceCollection services, string package, Func<IServiceProvider, Endpoint, string?> check)
    {
        foreach (ServiceDescriptor descriptor in services)
        {
            if (descriptor.ServiceType == typeof(RuvioEndpointCheckMarker))
                return;
        }

        services.AddSingleton(new RuvioEndpointCheckMarker());
        services.AddSingleton<IStartupFilter>(new RuvioEndpointCheck(package, check));
    }
}

/// <summary>Records that this package's endpoint check is registered, so a repeated Add call does not add it twice.</summary>
internal sealed class RuvioEndpointCheckMarker;
