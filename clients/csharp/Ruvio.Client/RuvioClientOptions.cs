namespace Ruvio.Client;

/// <summary>How <c>SET</c> treats a key that already exists or is missing.</summary>
public enum SetMode
{
    /// <summary>Write the key either way.</summary>
    Always = 0,

    /// <summary>Write only when the key is absent (<c>NX</c>).</summary>
    IfNotExists = 1,

    /// <summary>Write only when the key exists (<c>XX</c>).</summary>
    IfExists = 2,
}

/// <summary>
/// Where a cluster client sends read-only commands (those the server flags <c>readonly</c>, such as
/// <c>GET</c>, <c>HGETALL</c> or <c>ZRANGE</c>). Replicas are found in <c>CLUSTER SLOTS</c>, which lists each
/// shard's replicas after its primary. Replication is asynchronous: a read from a replica may miss recent
/// writes, including the caller's own. Writes, transactions, blocking commands, Pub/Sub and any pipeline that
/// holds a write always go to the primaries.
/// </summary>
public enum ReadPolicy
{
    /// <summary>Every command goes to the slot's primary, as before replicas were listed.</summary>
    Primary = 0,

    /// <summary>
    /// Reads go to one replica of the slot's shard when one is known and reachable, otherwise to the primary.
    /// A replica that cannot be opened, or answers <c>LOADING</c>, is skipped until the slot map is reloaded.
    /// </summary>
    PreferReplica = 1,

    /// <summary>
    /// Reads go to a replica of the slot's shard; with none known or reachable they fail with a
    /// <see cref="RuvioException"/>.
    /// </summary>
    Replica = 2,
}

/// <summary>Where to connect, and an optional password sent as <c>AUTH</c>.</summary>
public sealed class RuvioClientOptions
{
    /// <summary>Server host. Defaults to loopback.</summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>RESP port. Defaults to 6379.</summary>
    public int Port { get; set; } = 6379;

    /// <summary>
    /// Seed addresses as <c>host:port</c> strings (an IPv6 host in brackets, <c>[::1]:6379</c>). Empty by
    /// default. When it is not empty it replaces <see cref="Host"/> and <see cref="Port"/>: connect tries the
    /// seeds in order, one attempt each, and keeps the first that answers; a seed that cannot be reached is
    /// skipped, while a refused <c>AUTH</c> or <c>SELECT</c> stops at once. A standalone reconnect tries every
    /// seed once per attempt, starting with the one after the address that dropped. In a cluster the seeds are
    /// asked for the slot map when no known node answers.
    /// </summary>
    public List<string> Seeds { get; set; } = [];

    /// <summary>ACL user. When set together with <see cref="Password"/>, the client sends <c>AUTH user password</c>.</summary>
    public string? Username { get; set; }

    /// <summary>When set, the client sends <c>AUTH</c> before <see cref="RuvioConnection.ConnectAsync(RuvioClientOptions, CancellationToken)"/> returns.</summary>
    public string? Password { get; set; }

    /// <summary>
    /// Logical database for a standalone server. Its database-scoped client sends
    /// <c>SELECT</c> after <c>AUTH</c>. Cluster mode supports database 0 only.
    /// </summary>
    public int Database { get; set; }

    /// <summary>
    /// When above zero, every connection the client opens (reconnects, pooled and per-node connections
    /// included) sends <c>CLIENT DURABILITY REPLICAS n</c> after <c>AUTH</c>, so the server holds its replies until
    /// <c>n</c> replicas have the writes on disk. A refused command fails the connect. Zero (the default) sends nothing.
    /// </summary>
    public int DurabilityReplicas { get; set; }

    /// <summary>
    /// How long the server waits for the replicas before it answers <c>NOREPLICAS</c>, sent as <c>TIMEOUT</c> in
    /// milliseconds. Null (the default) leaves the server's own limit. Used only with <see cref="DurabilityReplicas"/>.
    /// </summary>
    public TimeSpan? DurabilityTimeout { get; set; }

    /// <summary>
    /// Serializer settings the typed JSON methods (<c>JsonSetValueAsync</c>, <c>JsonGetValueAsync</c>, ...)
    /// use when a call passes none. Null means <see cref="System.Text.Json.JsonSerializerOptions.Default"/>.
    /// </summary>
    public System.Text.Json.JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>How long TCP connect may take.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long one command may wait for its reply. On expiry the call throws <see cref="TimeoutException"/>;
    /// the connection stays in use and the late reply is discarded (a socket write that takes no data for the
    /// whole limit closes the connection instead). One timer per connection enforces it, so it costs
    /// nothing per command. Default 30 seconds; zero (or
    /// <see cref="Timeout.InfiniteTimeSpan"/>) means no limit, so a half-open peer blocks a call until the socket
    /// drops or its token is cancelled. A blocking command gets its own wait on top (<c>BLPOP k 5</c> may take
    /// 5 s plus this), and one that waits forever (timeout <c>0</c>) has no limit. Commands on the multiplexed
    /// path (every typed command) are covered; Pub/Sub and the initial <c>AUTH</c>/<c>SELECT</c> are not.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum admitted command groups per socket, including queued writes and pending replies.
    /// Each ExecuteManyAsync pipeline is one group. Defaults to 256; must be positive.
    /// </summary>
    public int MaxInFlightRequests { get; set; } = 256;

    /// <summary>
    /// When true, each command socket keeps blocking I/O and gets its own reader thread: callers
    /// write their frames synchronously and the thread reads the replies, instead of going through
    /// the runtime's async socket engine and thread pool on every round trip. It costs one thread
    /// per socket (one per shard in a cluster) and cuts CPU per round trip, which matters most for
    /// small pipelines and for pipelines split over many shards. Pub/Sub sockets are not affected.
    /// Off by default.
    /// </summary>
    public bool DedicatedReaderThread { get; set; }

    /// <summary>
    /// When true (the default), connect sends <c>CLUSTER SLOTS</c>. A map with two or
    /// more shards turns the client into a router: later commands go to the shard that
    /// owns the key. A disabled-cluster error, or a single shard, keeps one socket.
    /// </summary>
    public bool DiscoverCluster { get; set; } = true;

    /// <summary>
    /// When true (the default), a broken command or shard socket is opened again before the
    /// next command. The command that discovers the drop fails and is never sent again, even
    /// a read whose reply had not started. Set this to false to reconnect only from <c>ReconnectAsync</c>.
    /// </summary>
    public bool Reconnect { get; set; } = true;

    /// <summary>How many times a reconnect tries <c>connect</c> before giving up. One means a single try.</summary>
    public int MaxReconnectAttempts { get; set; } = 8;

    /// <summary>First automatic wait after a failed reconnect attempt.</summary>
    public TimeSpan ReconnectBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Ceiling for the automatic exponential wait.</summary>
    public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Replaces the built-in delay. The attempt argument starts at 1 after the first failure.
    /// Return null to stop. When this is set, <see cref="MaxReconnectAttempts"/> is not applied.
    /// </summary>
    public Func<int, TimeSpan?>? ReconnectDelay { get; set; }

    /// <summary>
    /// Where a cluster client sends read-only commands; see <see cref="ReadPolicy"/>. A standalone connection
    /// ignores it. Defaults to <see cref="ReadPolicy.Primary"/>.
    /// </summary>
    public ReadPolicy ReadPolicy { get; set; } = ReadPolicy.Primary;

    /// <summary>
    /// When true (the default), a standalone client finds the primary again after a failover. With two or more
    /// <see cref="Seeds"/>, or a <see cref="SentinelMaster"/> name, a reconnect asks the seeds <c>ROLE</c> and
    /// connects to the one that answers <c>master</c> (a replica's reported primary is followed too), and a command
    /// answered <c>READONLY</c> does the same and is sent once more to the new primary, since <c>READONLY</c>
    /// means it was not applied. A command inside a transaction the client opened is not sent again, and neither
    /// is a pipeline's. Nothing is sent while the connection is healthy. A cluster client ignores it. Set it to
    /// false to get the <c>READONLY</c> error and the plain seed order back.
    /// </summary>
    public bool FollowPrimary { get; set; } = true;

    /// <summary>
    /// When set, <see cref="Seeds"/> (or <see cref="Host"/> and <see cref="Port"/>) are coordinator or Sentinel
    /// addresses, not data nodes: the primary of this master name is asked with
    /// <c>SENTINEL get-master-addr-by-name</c>, checked with <c>ROLE</c> and connected to, at connect time, on every
    /// reconnect and after a <c>READONLY</c> reply. The same <c>AUTH</c> credentials go to the coordinators and the
    /// data node. Connect throws a <see cref="RuvioException"/> naming the master when no coordinator names a
    /// primary. Null (the default) leaves it off.
    /// </summary>
    public string? SentinelMaster { get; set; }
}

/// <summary>Reads and checks the <c>host:port</c> entries of <see cref="RuvioClientOptions.Seeds"/>.</summary>
internal static class SeedAddresses
{
    /// <summary>The parsed seeds; empty when none are configured.</summary>
    public static (string Host, int Port)[] Parse(RuvioClientOptions options)
    {
        List<string>? seeds = options.Seeds;

        if (seeds is not { Count: > 0 })
        {
            return [];
        }

        var parsed = new (string Host, int Port)[seeds.Count];

        for (int index = 0; index < parsed.Length; index++)
        {
            parsed[index] = ParseOne(seeds[index]);
        }

        return parsed;
    }

    private static (string Host, int Port) ParseOne(string? entry)
    {
        string text = entry?.Trim() ?? "";
        int colon = text.LastIndexOf(':');

        if (colon < 0)
        {
            throw new ArgumentException($"Seed \"{text}\" must be host:port.", nameof(RuvioClientOptions.Seeds));
        }

        string host = text[..colon];

        if (host.Length > 1 && host[0] == '[' && host[^1] == ']')
        {
            host = host[1..^1];
        }

        if (string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"Seed \"{text}\" has an empty host.", nameof(RuvioClientOptions.Seeds));
        }

        if (!int.TryParse(text[(colon + 1)..], System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out int port) || port is < 1 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(RuvioClientOptions.Seeds), $"Seed \"{text}\" must have a port between 1 and 65535.");
        }

        return (host, port);
    }
}

/// <summary>A command or subscriber socket dropped, or came back.</summary>
public sealed class RuvioConnectionNotice : EventArgs
{
    internal RuvioConnectionNotice(string host, int port, bool subscriber, Exception? error)
    {
        Host = host;
        Port = port;
        Subscriber = subscriber;
        Error = error;
    }

    /// <summary>Host the socket was using.</summary>
    public string Host { get; }

    /// <summary>Port the socket was using.</summary>
    public int Port { get; }

    /// <summary>True when the socket is the dedicated Pub/Sub connection.</summary>
    public bool Subscriber { get; }

    /// <summary>Present for a drop. Null when the socket was restored.</summary>
    public Exception? Error { get; }
}
