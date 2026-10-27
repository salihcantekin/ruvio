using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;

namespace Ruvio.AspNetCore.DataProtection;

/// <summary>Registers Ruvio as the Data Protection key-ring store.</summary>
public static class RuvioDataProtectionExtensions
{
    /// <summary>
    /// Persists keys using the registered connection's default database client (the keyed connection named by
    /// <see cref="RuvioDataProtectionOptions.ClientKey"/>, when set) and requires key encryption by default.
    /// </summary>
    public static IDataProtectionBuilder PersistKeysToRuvio(this IDataProtectionBuilder builder,
        Action<RuvioDataProtectionOptions>? configure = null)
        => Persist(builder, configure, static (provider, options) => options.ClientKey is { } key
            ? provider.GetRequiredKeyedService<IRuvioConnection>(key)
            : provider.GetRequiredService<IRuvioConnection>());

    /// <summary>Uses a caller-owned connection, for example a keyed service targeting durable, non-evicting storage.</summary>
    public static IDataProtectionBuilder PersistKeysToRuvio(this IDataProtectionBuilder builder,
        Func<IServiceProvider, IRuvioConnection> connectionFactory, Action<RuvioDataProtectionOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);

        return Persist(builder, configure, (provider, _) => connectionFactory(provider));
    }

    private static IDataProtectionBuilder Persist(IDataProtectionBuilder builder, Action<RuvioDataProtectionOptions>? configure,
        Func<IServiceProvider, RuvioDataProtectionOptions, IRuvioConnection> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = new RuvioDataProtectionOptions();

        configure?.Invoke(options);
        options.Validate();

        builder.Services.AddOptions<KeyManagementOptions>()
            .Configure<IServiceProvider>((keys, provider) =>
            {
                IRuvioConnection connection = connectionFactory(provider, options);
                IRuvioClient client = connection.GetDefaultDatabaseAsync().GetAwaiter().GetResult();
                keys.XmlRepository = new RuvioXmlRepository(client, Options.Create(options));
            })
            .PostConfigure(keys =>
            {
                if (options.RequireEncryption && keys.XmlEncryptor is null)
                    throw new InvalidOperationException(
                        "Ruvio Data Protection requires an XML encryptor. Configure ProtectKeysWithCertificate "
                        + "or another key encryptor; disable RequireEncryption only for isolated development.");
            });

        return builder;
    }
}
