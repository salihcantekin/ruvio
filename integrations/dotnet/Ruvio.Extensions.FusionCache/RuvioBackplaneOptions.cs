namespace Ruvio.Extensions.FusionCache;

/// <summary>Settings of <see cref="RuvioBackplane"/>.</summary>
public sealed class RuvioBackplaneOptions
{
    /// <summary>
    /// Text put before FusionCache's channel name. Caches that share a prefix and a channel name hear each other.
    /// Default <c>ruvio:fusioncache:</c>.
    /// </summary>
    public string ChannelPrefix { get; set; } = "ruvio:fusioncache:";

    /// <summary>
    /// Name of a connection registered with <c>AddKeyedRuvioClient</c>. Null (the default) uses the unkeyed
    /// <see cref="Ruvio.Client.IRuvioClient"/>.
    /// </summary>
    public string? ClientKey { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ChannelPrefix))
            throw new ArgumentException("ChannelPrefix must not be empty.", nameof(ChannelPrefix));

        if (ClientKey is { Length: 0 })
            throw new ArgumentException("ClientKey must be null or a non-empty name.", nameof(ClientKey));
    }
}
