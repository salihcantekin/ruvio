using Microsoft.Extensions.Logging;
using Ruvio.Client;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane;

namespace Ruvio.Extensions.FusionCache;

/// <summary>
/// A FusionCache backplane over Ruvio's sharded Pub/Sub (<c>SPUBLISH</c> / <c>SSUBSCRIBE</c>). A channel belongs to one
/// slot, so every app node reaches the same owner, also in a multi-node cluster.
/// </summary>
/// <remarks>
/// A message is FusionCache's own <see cref="BackplaneMessage"/> bytes, base64 encoded. Messages are not stored: a node
/// that is disconnected misses them, and its in-memory copies then live until they expire.
/// </remarks>
public sealed class RuvioBackplane : IFusionCacheBackplane
{
    private readonly IRuvioClient _client;
    private readonly RuvioBackplaneOptions _options;
    private readonly ILogger? _logger;
    private readonly object _gate = new();
    private string? _channel;
    private BackplaneSubscriptionOptions? _subscription;

    /// <summary>Creates a backplane on <paramref name="client"/>.</summary>
    public RuvioBackplane(IRuvioClient client, RuvioBackplaneOptions? options = null, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        _options = options ?? new RuvioBackplaneOptions();
        _options.Validate();
        _client = client;
        _logger = logger;
    }

    /// <inheritdoc />
    public void Subscribe(BackplaneSubscriptionOptions options)
        => Task.Run(() => SubscribeAsync(options).AsTask()).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async ValueTask SubscribeAsync(BackplaneSubscriptionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        string channel = _options.ChannelPrefix + (options.ChannelName ?? options.CacheName);

        lock (_gate)
        {
            if (_channel is not null)
                return;

            _channel = channel;
            _subscription = options;
        }

        try
        {
            await _client.SubscribeShardedAsync(channel, OnMessageAsync).ConfigureAwait(false);
        }
        catch
        {
            lock (_gate)
            {
                _channel = null;
                _subscription = null;
            }

            throw;
        }

        if (options.ConnectHandlerAsync is { } handler)
            await handler(new BackplaneConnectionInfo(false)).ConfigureAwait(false);
        else
            options.ConnectHandler?.Invoke(new BackplaneConnectionInfo(false));
    }

    /// <inheritdoc />
    public void Unsubscribe() => Task.Run(() => UnsubscribeAsync().AsTask()).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async ValueTask UnsubscribeAsync()
    {
        string? channel;

        lock (_gate)
        {
            channel = _channel;
            _channel = null;
            _subscription = null;
        }

        if (channel is not null)
            await _client.UnsubscribeShardedAsync(channel).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Publish(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
        => Task.Run(() => PublishAsync(message, options, token).AsTask(), token).GetAwaiter().GetResult();

    /// <inheritdoc />
    public async ValueTask PublishAsync(BackplaneMessage message, FusionCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        string channel;

        lock (_gate)
        {
            channel = _channel ?? throw new InvalidOperationException("The backplane is not subscribed yet; FusionCache subscribes it when the cache starts.");
        }

        await _client.PublishShardedAsync(channel, Convert.ToBase64String(BackplaneMessage.ToByteArray(message)), token).ConfigureAwait(false);
    }

    private async Task OnMessageAsync(PubSubMessage received)
    {
        BackplaneSubscriptionOptions? subscription;

        lock (_gate)
            subscription = _subscription;

        if (subscription is null)
            return;

        BackplaneMessage? message;

        try
        {
            message = BackplaneMessage.FromByteArray(Convert.FromBase64String(received.Payload));
        }
        catch (Exception exception) when (exception is FormatException or ArgumentException or InvalidOperationException)
        {
            _logger?.LogWarning(exception, "Ruvio backplane: ignored a message on {Channel} that is not a FusionCache message.", received.Channel);

            return;
        }

        if (message is null)
            return;

        if (subscription.IncomingMessageHandlerAsync is { } handler)
            await handler(message).ConfigureAwait(false);
        else
            subscription.IncomingMessageHandler?.Invoke(message);
    }
}
