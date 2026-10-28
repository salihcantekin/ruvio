using System.Buffers;
using System.Runtime.CompilerServices;
using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>
/// Routes commands to the shard that owns their slot. Built from <c>CLUSTER SLOTS</c>;
/// a <c>MOVED</c> reply points that slot at the named node and retries, and an <c>ASK</c> reply sends the command once
/// (after <c>ASKING</c>) to the named node without changing the map; both count against one budget of three redirects.
/// <c>TRYAGAIN</c> sends the command again after a wait doubling from 5 ms, at most seven times (635 ms in all: a slot
/// range moving between nodes is frozen for a few hundred milliseconds).
/// </summary>
internal sealed class ClusterRouter : IDisposable
{
    private const int MaxRedirects = 3;
    private const int MaxTryAgains = 7;
    private const int FirstTryAgainDelayMs = 5;

    /// <summary>
    /// The one <c>TRYAGAIN</c> wait of a call. The 5, 10, ... 320 ms schedule runs from the first <c>TRYAGAIN</c> the call
    /// sees and is shared by every command of a pipeline, so N refused commands wait for one budget (635 ms), not N.
    /// </summary>
    private sealed class TryAgainClock
    {
        private long _started;

        /// <summary>
        /// How long to wait before retry number <paramref name="retries"/> (from 0), or null when the budget is spent. A command
        /// that has not been retried yet always gets one more try, so a freeze that ended while earlier commands waited does not
        /// fail the later ones.
        /// </summary>
        public TimeSpan? Pause(int retries)
        {
            long now = System.Diagnostics.Stopwatch.GetTimestamp();

            if (_started == 0)
            {
                _started = now;
            }

            TimeSpan elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(_started, now);
            int at = 0;

            for (int step = 0; step < MaxTryAgains; step++)
            {
                at += FirstTryAgainDelayMs << step;

                if (TimeSpan.FromMilliseconds(at) > elapsed)
                {
                    return TimeSpan.FromMilliseconds(at) - elapsed;
                }
            }

            return retries == 0 ? TimeSpan.Zero : null;
        }
    }

    private static readonly string[] AskingCommand = ["ASKING"];
    private static readonly byte[] AskingFrame = Encoding.ASCII.GetBytes("*1\r\n$6\r\nASKING\r\n");

    internal static readonly HashSet<string> SplitNames = new(StringComparer.Ordinal)
    {
        "DEL", "EXISTS", "MGET", "MSET", "TOUCH", "UNLINK",
    };

    internal static readonly HashSet<string> TransactionNames = new(StringComparer.Ordinal)
    {
        "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH",
    };

    private readonly RuvioClientOptions _options;
    private readonly RuvioSocket _seed;
    private readonly Dictionary<string, RuvioSocket> _connections = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _connectGate = new(1, 1);
    private readonly SemaphoreSlim _refreshGate = new(1, 1);
    private readonly List<IReadOnlyList<string>> _queued = [];
    private readonly (string Host, int Port)[] _seeds;
    private ClusterTopology _topology;
    private Tuple<string, int> _home;
    private RuvioSocket? _pinned;
    private bool _multiPending;
    private bool _inMulti;
    private bool _disposed;
    private Func<string, int, CancellationToken, Task<RuvioSocket>>? _open;
    private Action<string, int, Exception>? _lost;
    private Action<string, int>? _restored;

    private ClusterRouter(RuvioSocket seed, RuvioClientOptions options, ClusterTopology topology)
    {
        _seed = seed;
        _options = options;
        PickReplicas(topology);
        _topology = topology;
        _seeds = SeedAddresses.Parse(options);
        _home = Tuple.Create(seed.Host, seed.Port);
        _connections[Endpoint(seed.Host, seed.Port)] = seed;
    }

    public int NodeCount => _topology.Nodes.Count;

    /// <summary>
    /// Makes <paramref name="topology"/> current. Under a replica read policy each shard's reads go to
    /// one of its replicas, picked at random so many clients spread over them, and kept until the next load.
    /// </summary>
    private void Install(ClusterTopology topology)
    {
        PickReplicas(topology);
        _topology = topology;
    }

    private void PickReplicas(ClusterTopology topology)
    {
        if (_options.ReadPolicy != ReadPolicy.Primary)
        {
            topology.PickReplicas(Random.Shared.Next());
        }
    }

    /// <summary>How many node endpoints have a socket entry, open or broken.</summary>
    internal int EndpointCount
    {
        get
        {
            lock (_connections)
            {
                return _connections.Count;
            }
        }
    }

    internal bool HasLiveConnection()
    {
        lock (_connections)
        {
            return _connections.Values.Any(connection => !connection.IsBroken);
        }
    }

    internal void Bind(
        Func<string, int, CancellationToken, Task<RuvioSocket>> open,
        Action<string, int, Exception> lost,
        Action<string, int> restored)
    {
        _open = open;
        _lost = lost;
        _restored = restored;
    }

    internal async Task ReconnectBrokenAsync(CancellationToken cancellationToken)
    {
        (string Host, int Port)[] nodes = [.. _topology.Nodes];

        foreach ((string host, int port) in nodes)
        {
            await ConnectionAsync(host, port, cancellationToken, force: true);
        }
    }

    /// <summary>
    /// Asks the seed for <c>CLUSTER SLOTS</c>. Null when the server is not a cluster
    /// or has one node, so the caller keeps the plain single-socket path.
    /// </summary>
    public static async Task<ClusterRouter?> DiscoverAsync(
        RuvioSocket seed,
        RuvioClientOptions options,
        CancellationToken cancellationToken)
    {
        RespValue reply = await seed.SendAsync(["CLUSTER", "SLOTS"], cancellationToken);
        ClusterTopology? topology = ClusterTopology.Parse(reply);

        if (topology is null)
        {
            return null;
        }

        return new ClusterRouter(seed, options, topology);
    }

    /// <summary>
    /// Routes one command. <paramref name="redirects"/> counts the <c>MOVED</c> replies the
    /// caller already followed for it.
    /// </summary>
    public async Task<RespValue> ExecuteAsync(
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken,
        int redirects = 0)
    {
        string name = ClusterKeys.Name(arguments);

        if (_pinned is not null || _multiPending)
        {
            return await TransactionAsync(name, arguments, cancellationToken);
        }

        switch (name)
        {
            case "MULTI":
                _multiPending = true;

                return RespValue.Simple("OK");
            case "WATCH":
                return await WatchAsync(arguments, cancellationToken);
            case "DBSIZE":
            case "FLUSHDB":
            case "FLUSHALL":
                return await EveryNodeAsync(name, arguments, cancellationToken);
            case "SCRIPT" or "FUNCTION" when SpreadsToEveryNode(arguments):
                return await EveryNodeAsync(name, arguments, cancellationToken);
            case "SCAN":
                return await ScanAsync(arguments, cancellationToken);
            case "PUBLISH":
                return await SendRoutedAsync(await ForSlotAsync(0, cancellationToken), arguments, cancellationToken, slot: 0);
        }

        IReadOnlyList<string> keys = ClusterKeys.Of(name, arguments);

        if (SpansSlots(keys) && !UnpairedMset(name, arguments))
        {
            // Only the split commands can be answered per slot; anything else (RENAME, COPY,
            // SMOVE, EVAL, ...) is refused here instead of reaching a shard.
            return SplitNames.Contains(name)
                ? await SplitAsync(name, arguments, keys, cancellationToken)
                : CrossSlot();
        }

        if (_options.ReadPolicy != ReadPolicy.Primary && keys.Count > 0 && ClusterKeys.Reads(name))
        {
            return await SendReadAsync(ClusterKeys.Slot(keys[0]), arguments, cancellationToken, redirects);
        }

        RuvioSocket connection = await TargetAsync(keys, cancellationToken);

        return await SendRoutedAsync(connection, arguments, cancellationToken, redirects);
    }

    /// <summary>
    /// Routes an ordinary single-key command without building a string list. Everything
    /// else (transactions, multi-key, fan-out) takes the list path.
    /// </summary>
    internal ValueTask<RespValue> ExecuteAsync(in RespCommand command, CancellationToken cancellationToken, int redirects = 0)
    {
        if (_pinned is null && !_multiPending && command.Count >= 2
            && command.Name is { } raw && command.TextAt(1) is { } key)
        {
            string name = raw.ToUpperInvariant();

            if (ClusterKeys.IsSingleKey(name))
            {
                int slot = ClusterKeys.Slot(key);

                return _options.ReadPolicy != ReadPolicy.Primary && ClusterKeys.Reads(name)
                    ? SendReadToSlotAsync(command, slot, redirects, cancellationToken)
                    : SendToSlotAsync(command, slot, redirects, cancellationToken);
            }
        }

        return new ValueTask<RespValue>(ExecuteAsync(command.AsList(), cancellationToken, redirects));
    }

    /// <summary>The error for keys in several slots, returned before anything is sent.</summary>
    internal static RespValue CrossSlot()
    {
        return RespValue.Error("CROSSSLOT Keys in request do not hash to the same slot (checked by the client, nothing was sent)");
    }

    // Pooled builder: each routed call reuses a state machine box instead of allocating a new one.
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> SendToSlotAsync(
        RespCommand command, int slot, int redirects, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);
        RespValue reply = await connection.SendAsync(command, cancellationToken).ConfigureAwait(false);

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(command, slot, redirects, reply, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    /// <summary>The slow path of <see cref="SendToSlotAsync"/>: only an error reply that is a redirect or <c>TRYAGAIN</c> gets here.</summary>
    private Task<RespValue> FollowUpAsync(
        RespCommand command, int slot, int redirects, RespValue reply, CancellationToken cancellationToken)
    {
        IReadOnlyList<string> arguments = command.AsList();

        return ResolveAsync(
            reply,
            redirects,
            token => ForSlotAsync(slot, token),
            (node, asking, token) => asking ? AskAsync(node, arguments, token) : node.SendAsync(arguments, token).AsTask(),
            cancellationToken);
    }

    /// <summary>
    /// A read for <paramref name="slot"/>: sent to the slot's replica, or to the primary when it has none.
    /// A replica that answers <c>LOADING</c> is skipped until the next load and, under
    /// <see cref="ReadPolicy.PreferReplica"/>, the read is sent again to the primary.
    /// </summary>
    private async ValueTask<RespValue> SendReadToSlotAsync(
        RespCommand command, int slot, int redirects, CancellationToken cancellationToken)
    {
        ReadTarget target = await ReadSocketAsync(slot, cancellationToken).ConfigureAwait(false);
        RespValue reply = await target.Socket.SendAsync(command, cancellationToken).ConfigureAwait(false);

        if (target.Replica && IsLoading(reply))
        {
            target.Skip();

            return _options.ReadPolicy == ReadPolicy.Replica
                ? reply
                : await SendToSlotAsync(command, slot, redirects, cancellationToken).ConfigureAwait(false);
        }

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(command, slot, redirects, reply, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    /// <summary>The string-list form of <see cref="SendReadToSlotAsync"/>.</summary>
    private async Task<RespValue> SendReadAsync(
        int slot, IReadOnlyList<string> arguments, CancellationToken cancellationToken, int redirects)
    {
        ReadTarget target = await ReadSocketAsync(slot, cancellationToken).ConfigureAwait(false);
        RespValue reply = await target.Socket.SendAsync(arguments, cancellationToken).ConfigureAwait(false);

        if (target.Replica && IsLoading(reply))
        {
            target.Skip();

            return _options.ReadPolicy == ReadPolicy.Replica
                ? reply
                : await SendRoutedAsync(await ForSlotAsync(slot, cancellationToken), arguments, cancellationToken, redirects);
        }

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(arguments, slot, redirects, reply, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    /// <summary>The socket a read goes to; <c>Topology</c> and <c>Node</c> name the replica when it is one.</summary>
    private readonly record struct ReadTarget(RuvioSocket Socket, ClusterTopology? Topology = null, int Node = -1)
    {
        public bool Replica => Topology is not null;

        /// <summary>Stops using this replica until the next slot map load.</summary>
        public void Skip() => Topology?.SkipReplica(Node);
    }

    private static bool IsLoading(RespValue reply) =>
        reply is { Kind: RespKind.Error, Text: { } text } && (text.StartsWith("LOADING", StringComparison.Ordinal) || text.StartsWith("MASTERDOWN", StringComparison.Ordinal));

    /// <summary>
    /// The replica <paramref name="slot"/>'s reads go to, opened; the primary when it has none or it cannot be
    /// opened under <see cref="ReadPolicy.PreferReplica"/>, an error under <see cref="ReadPolicy.Replica"/>.
    /// </summary>
    private ValueTask<ReadTarget> ReadSocketAsync(int slot, CancellationToken cancellationToken)
    {
        ClusterTopology topology = _topology;
        int node = topology.NodeOf(slot);

        if (node >= 0 && topology.ReadReplica(node) is not null)
        {
            if (topology.ReplicaSockets[node] is { IsBroken: false } cached)
            {
                return new ValueTask<ReadTarget>(new ReadTarget(cached, topology, node));
            }

            return OpenReplicaAsync(slot, topology, node, cancellationToken);
        }

        if (_options.ReadPolicy == ReadPolicy.Replica)
        {
            throw RuvioException.Client($"no replica is known for slot {slot} (read policy Replica)");
        }

        return PrimaryReadAsync(slot, cancellationToken);
    }

    private async ValueTask<ReadTarget> PrimaryReadAsync(int slot, CancellationToken cancellationToken) =>
        new(await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false));

    private async ValueTask<ReadTarget> OpenReplicaAsync(
        int slot, ClusterTopology topology, int node, CancellationToken cancellationToken)
    {
        (string host, int port) = topology.ReadReplica(node)!.Value;

        try
        {
            RuvioSocket opened = await ConnectionAsync(host, port, cancellationToken, replica: true).ConfigureAwait(false);

            topology.ReplicaSockets[node] = opened;

            return new ReadTarget(opened, topology, node);
        }

        catch (Exception error) when (CanReload(error, cancellationToken))
        {
            topology.SkipReplica(node);

            if (_options.ReadPolicy == ReadPolicy.Replica)
            {
                throw;
            }
        }

        return new ReadTarget(await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false));
    }

    internal async Task<RespValue> ExecuteBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken, int redirects = 0)
    {
        if (_pinned is not null || _multiPending)
        {
            throw new NotSupportedException("Binary commands cannot be mixed with cluster transactions.");
        }

        string name = Encoding.ASCII.GetString(arguments[0].Span).ToUpperInvariant();
        int first = 1;
        int count;

        switch (name)
        {
            case "EVAL":
            case "EVALSHA":
                if (arguments.Count < 3 ||
                    !int.TryParse(Encoding.ASCII.GetString(arguments[2].Span), NumberStyles.None,
                        CultureInfo.InvariantCulture, out count) ||
                    count < 0 || count > arguments.Count - 3)
                {
                    throw new ArgumentException("Invalid script key count.", nameof(arguments));
                }

                first = 3;
                break;
            case "DEL": case "UNLINK": case "EXISTS":
                count = arguments.Count - 1;
                break;
            case "GET": case "SET": case "HGET": case "HSET": case "HMGET": case "HMSET":
            case "HDEL": case "PEXPIRE": case "PEXPIREAT": case "PTTL": case "TTL": case "PERSIST":
            case "IDEM":
                count = Math.Min(1, arguments.Count - 1);
                break;
            case "PING": case "ECHO":
                count = 0;
                break;
            default:
                throw new NotSupportedException($"Binary cluster routing is not supported for {name}.");
        }

        int slot = count == 0 ? 0 : ClusterKeys.Slot(arguments[first].Span);
        bool read = count > 0 && _options.ReadPolicy != ReadPolicy.Primary && ClusterKeys.Reads(name);

        for (int index = 1; index < count; index++)
        {
            if (ClusterKeys.Slot(arguments[first + index].Span) != slot)
            {
                // DEL, UNLINK and EXISTS are split like their string form; a script cannot be.
                return first == 1
                    ? await SplitBinaryAsync(arguments, read, cancellationToken).ConfigureAwait(false)
                    : CrossSlot();
            }
        }

        return read
            ? await SendBinaryReadAsync(arguments, slot, redirects, cancellationToken).ConfigureAwait(false)
            : await SendBinaryToSlotAsync(arguments, slot, redirects, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The binary form of <see cref="SendReadToSlotAsync"/>.</summary>
    private async Task<RespValue> SendBinaryReadAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, int slot, int redirects, CancellationToken cancellationToken)
    {
        ReadTarget target = await ReadSocketAsync(slot, cancellationToken).ConfigureAwait(false);
        RespValue reply = await target.Socket.SendBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

        if (target.Replica && IsLoading(reply))
        {
            target.Skip();

            return _options.ReadPolicy == ReadPolicy.Replica
                ? reply
                : await SendBinaryToSlotAsync(arguments, slot, redirects, cancellationToken).ConfigureAwait(false);
        }

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(arguments, slot, redirects, reply, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    private async Task<RespValue> SendBinaryToSlotAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, int slot, int redirects, CancellationToken cancellationToken)
    {
        RuvioSocket connection = await ForSlotAsync(slot, cancellationToken).ConfigureAwait(false);
        RespValue reply = await connection.SendBinaryAsync(arguments, cancellationToken).ConfigureAwait(false);

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(arguments, slot, redirects, reply, cancellationToken).ConfigureAwait(false)
            : reply;
    }

    /// <summary>The binary form of the slow path of <see cref="SendToSlotAsync"/>.</summary>
    private Task<RespValue> FollowUpAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, int slot, int redirects, RespValue reply, CancellationToken cancellationToken)
    {
        return ResolveAsync(
            reply,
            redirects,
            token => ForSlotAsync(slot, token),
            (node, asking, token) => asking ? AskBinaryAsync(node, arguments, token) : node.SendBinaryAsync(arguments, token).AsTask(),
            cancellationToken);
    }

    /// <summary>Sends a binary <c>DEL</c>, <c>UNLINK</c> or <c>EXISTS</c> once per slot and sums the counts; <c>EXISTS</c> parts are reads.</summary>
    private async Task<RespValue> SplitBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, bool read, CancellationToken cancellationToken)
    {
        var bySlot = new Dictionary<int, List<ReadOnlyMemory<byte>>>();

        for (int index = 1; index < arguments.Count; index++)
        {
            int slot = ClusterKeys.Slot(arguments[index].Span);

            if (!bySlot.TryGetValue(slot, out List<ReadOnlyMemory<byte>>? part))
            {
                part = [arguments[0]];
                bySlot[slot] = part;
            }

            part.Add(arguments[index]);
        }

        var sends = new Task<RespValue>[bySlot.Count];
        int next = 0;

        foreach ((int slot, List<ReadOnlyMemory<byte>> part) in bySlot)
        {
            sends[next++] = read
                ? SendBinaryReadAsync(part, slot, 0, cancellationToken)
                : SendBinaryToSlotAsync(part, slot, 0, cancellationToken);
        }

        long total = 0;

        foreach (RespValue reply in await Task.WhenAll(sends).ConfigureAwait(false))
        {
            if (reply.Kind is RespKind.Error)
            {
                return reply;
            }

            total += reply.Integer;
        }

        return RespValue.FromInteger(total);
    }

    /// <summary>
    /// Groups a pipeline by shard and sends each group as one write. Replies come back in
    /// command order. A split command whose keys span slots is sent per slot inside the
    /// groups and its replies merged at its own index. A pipeline that holds a transaction
    /// goes to one shard unchanged.
    /// </summary>
    public async Task<IReadOnlyList<RespValue>> ExecuteManyAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        if (_pinned is not null || _multiPending)
        {
            return await ExecuteManyGroupedAsync(commands, cancellationToken);
        }

        // A pipeline goes to replicas only when every command in it is a read, so it never
        // reads its own writes from a replica that has not received them.
        if (_options.ReadPolicy != ReadPolicy.Primary && AllReads(commands))
        {
            return await ExecuteManyGroupedAsync(commands, cancellationToken, reads: true);
        }

        ClusterTopology topology = _topology;
        int count = commands.Count;
        int nodes = topology.Nodes.Count;

        // One int per command (its node index) followed by one counter per node; the seed
        // connection stands in for the last node, for commands without a key.
        int[] scratch = ArrayPool<int>.Shared.Rent(count + nodes + 1);

        try
        {
            Array.Clear(scratch, count, nodes + 1);

            if (!AssignNodes(commands, topology, scratch))
            {
                return await ExecuteManyGroupedAsync(commands, cancellationToken);
            }

            return await SendOnNodesAsync(commands, topology, scratch, cancellationToken);
        }

        finally
        {
            ArrayPool<int>.Shared.Return(scratch);
        }
    }

    /// <summary>
    /// Stores the node index of every command in <paramref name="scratch"/> in one pass. False
    /// when the pipeline holds a transaction command or a split command spanning slots, which
    /// take the grouped path.
    /// </summary>
    private static bool AssignNodes(
        IReadOnlyList<IReadOnlyList<string>> commands, ClusterTopology topology, int[] scratch)
    {
        int count = commands.Count;
        int seed = topology.Nodes.Count;

        for (int index = 0; index < count; index++)
        {
            IReadOnlyList<string> command = commands[index];
            ClusterKeys.CommandInfo? info = ClusterKeys.Info(command[0]);

            if (info is not null
                && (info.Transaction || (info.Split && SplitSpansSlots(info.Name, command))))
            {
                return false;
            }

            int node = seed;

            if (ClusterKeys.FirstKey(info, command) is { } key)
            {
                node = topology.NodeOf(ClusterKeys.Slot(key));

                if (node < 0)
                {
                    node = seed;
                }
            }

            scratch[index] = node;
            scratch[count + node]++;
        }

        return true;
    }

    private async Task<IReadOnlyList<RespValue>> SendOnNodesAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        ClusterTopology topology,
        int[] scratch,
        CancellationToken cancellationToken)
    {
        int count = commands.Count;
        int nodes = topology.Nodes.Count;
        var sockets = new RuvioSocket?[nodes + 1];
        int groups = 0;
        int only = 0;

        for (int node = 0; node <= nodes; node++)
        {
            if (scratch[count + node] == 0)
            {
                continue;
            }

            sockets[node] = await NodeSocketAsync(topology, node, cancellationToken);

            // Keyless commands share the seed's connection with the node it serves.
            if (node == nodes)
            {
                for (int other = 0; other < nodes; other++)
                {
                    if (ReferenceEquals(sockets[other], sockets[node]))
                    {
                        for (int index = 0; index < count; index++)
                        {
                            if (scratch[index] == node)
                            {
                                scratch[index] = other;
                            }
                        }

                        scratch[count + other] += scratch[count + node];
                        scratch[count + node] = 0;
                        sockets[node] = null;

                        break;
                    }
                }
            }

            if (scratch[count + node] > 0)
            {
                groups++;
                only = node;
            }
        }

        if (groups == 1)
        {
            // Every command is for one node: the plain pipeline, unchanged.
            IReadOnlyList<RespValue> values = await sockets[only]!.SendManyAsync(commands, cancellationToken);

            return HasFollowUp(values) ? await ResendFollowUpAsync(values, commands, cancellationToken) : values;
        }

        var sends = new ValueTask<IReadOnlyList<RespValue>>[nodes + 1];
        var started = new bool[nodes + 1];
        var frames = new PooledFrame[nodes + 1];
        var replies = new RespValue[count];

        try
        {
            // Every node's write is queued before any reply is awaited.
            for (int node = 0; node <= nodes; node++)
            {
                if (scratch[count + node] > 0)
                {
                    frames[node] = RespCodec.EncodeSelectedPooled(commands, scratch, node);
                    started[node] = true;
                    sends[node] = sockets[node]!.SendFrameAsync(
                        frames[node].Buffer.AsMemory(0, frames[node].Length), scratch[count + node], cancellationToken);
                }
            }

            for (int node = 0; node <= nodes; node++)
            {
                if (started[node])
                {
                    started[node] = false;
                    Scatter(scratch, count, node, await sends[node], replies);
                }
            }
        }

        finally
        {
            // A failed send leaves the others running; wait for them before their buffers are reused.
            for (int node = 0; node <= nodes; node++)
            {
                if (started[node])
                {
                    try
                    {
                        await sends[node];
                    }
                    catch
                    {
                        // The first failure is already being thrown; this wait only keeps the buffer alive.
                    }
                }
            }

            foreach (PooledFrame frame in frames)
            {
                if (frame.Buffer is not null)
                {
                    frame.Dispose();
                }
            }
        }

        return HasFollowUp(replies) ? await ResendFollowUpAsync(replies, commands, cancellationToken) : replies;
    }

    private static void Scatter(int[] scratch, int count, int node, IReadOnlyList<RespValue> values, RespValue[] replies)
    {
        int next = 0;

        for (int index = 0; index < count; index++)
        {
            if (scratch[index] == node)
            {
                replies[index] = values[next++];
            }
        }
    }

    private static bool HasFollowUp(IReadOnlyList<RespValue> replies)
    {
        for (int index = 0; index < replies.Count; index++)
        {
            if (NeedsFollowUp(replies[index]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Sends each command that got a <c>MOVED</c>, <c>ASK</c> or <c>TRYAGAIN</c> again, straight from the caller's arguments.</summary>
    private async Task<IReadOnlyList<RespValue>> ResendFollowUpAsync(
        IReadOnlyList<RespValue> values,
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        RespValue[] replies = values as RespValue[] ?? [.. values];
        var clock = new TryAgainClock();

        for (int index = 0; index < replies.Length; index++)
        {
            if (NeedsFollowUp(replies[index]))
            {
                replies[index] = await FollowUpAsync(commands[index], -1, 0, replies[index], cancellationToken, clock);
            }
        }

        return replies;
    }

    private ValueTask<RuvioSocket> NodeSocketAsync(ClusterTopology topology, int node, CancellationToken cancellationToken)
    {
        if (node == topology.Nodes.Count)
        {
            return TargetKeyAsync(null, cancellationToken);
        }

        if (topology.Sockets[node] is { IsBroken: false } cached)
        {
            return new ValueTask<RuvioSocket>(cached);
        }

        return OpenForNodeAsync(topology, node, cancellationToken);
    }

    /// <summary>The grouped path: split commands and transactions, one dictionary entry per connection.</summary>
    private async Task<IReadOnlyList<RespValue>> ExecuteManyGroupedAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken,
        bool reads = false)
    {
        if (_pinned is not null || _multiPending || HasTransactionCommand(commands))
        {
            RuvioSocket single = _pinned ?? await FirstKeyedTargetAsync(commands, cancellationToken);

            return await single.SendManyAsync(commands, cancellationToken);
        }

        var groups = new Dictionary<RuvioSocket, List<int>>();

        // With reads, the replica sockets in use: a LOADING answer sends that group's commands to the primaries.
        Dictionary<RuvioSocket, ReadTarget>? replicas = reads ? [] : null;

        // Per-slot parts of split commands get positions after the pipeline's own commands.
        List<IReadOnlyList<string>>? extra = null;
        List<(int Index, string Name, int Keys, List<SplitPart> Parts, int Offset)>? splits = null;

        for (int index = 0; index < commands.Count; index++)
        {
            IReadOnlyList<string> command = commands[index];
            string name = ClusterKeys.Name(command);

            if (SplitNames.Contains(name) && SplitSpansSlots(name, command))
            {
                IReadOnlyList<string> keys = ClusterKeys.Of(name, command);
                List<SplitPart> parts = SplitParts(name, command, keys);

                extra ??= [];
                splits ??= [];
                splits.Add((index, name, keys.Count, parts, commands.Count + extra.Count));

                foreach (SplitPart part in parts)
                {
                    AddToGroup(
                        groups,
                        reads ? await GroupReadSocketAsync(part.Slot, replicas!, cancellationToken) : await ForSlotAsync(part.Slot, cancellationToken),
                        commands.Count + extra.Count);
                    extra.Add(part.Command);
                }

                continue;
            }

            string? key = ClusterKeys.First(name, command);

            AddToGroup(
                groups,
                reads && key is not null
                    ? await GroupReadSocketAsync(ClusterKeys.Slot(key), replicas!, cancellationToken)
                    : await TargetKeyAsync(key, cancellationToken),
                index);
        }

        var replies = new RespValue[commands.Count + (extra?.Count ?? 0)];

        if (groups.Count == 1 && extra is null)
        {
            foreach ((RuvioSocket only, List<int> _) in groups)
            {
                IReadOnlyList<RespValue> values = await only.SendManyAsync(commands, cancellationToken);

                for (int index = 0; index < values.Count; index++)
                {
                    replies[index] = values[index];
                }
            }
        }
        else
        {
            var sends = new Task[groups.Count];
            int next = 0;

            foreach ((RuvioSocket connection, List<int> positions) in groups)
            {
                var batch = new IReadOnlyList<string>[positions.Count];

                for (int index = 0; index < batch.Length; index++)
                {
                    int position = positions[index];

                    batch[index] = position < commands.Count ? commands[position] : extra![position - commands.Count];
                }

                sends[next++] = SendGroupAsync(connection, batch, positions, replies, cancellationToken);
            }

            await Task.WhenAll(sends);
        }

        if (replicas is { Count: > 0 })
        {
            await ReadAgainFromPrimariesAsync(groups, replicas, commands, extra, replies, cancellationToken);
        }

        var clock = new TryAgainClock();

        for (int index = 0; index < replies.Length; index++)
        {
            // A split command's own index stays empty until its parts are merged below.
            if (replies[index] is { } reply && NeedsFollowUp(reply))
            {
                IReadOnlyList<string> command = index < commands.Count ? commands[index] : extra![index - commands.Count];

                replies[index] = await FollowUpAsync(command, -1, 0, reply, cancellationToken, clock);
            }
        }

        if (splits is null)
        {
            return replies;
        }

        foreach ((int index, string name, int keys, List<SplitPart> parts, int offset) in splits)
        {
            replies[index] = MergeSplit(name, keys, parts, replies, offset);
        }

        return replies[..commands.Count];
    }

    private static bool AllReads(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        for (int index = 0; index < commands.Count; index++)
        {
            if (!ClusterKeys.Reads(ClusterKeys.Name(commands[index])))
            {
                return false;
            }
        }

        return true;
    }

    private async ValueTask<RuvioSocket> GroupReadSocketAsync(
        int slot, Dictionary<RuvioSocket, ReadTarget> replicas, CancellationToken cancellationToken)
    {
        ReadTarget target = await ReadSocketAsync(slot, cancellationToken);

        if (target.Replica)
        {
            replicas[target.Socket] = target;
        }

        return target.Socket;
    }

    /// <summary>
    /// A replica that answered <c>LOADING</c> to a pipelined read is skipped until the next load, and under
    /// <see cref="ReadPolicy.PreferReplica"/> those commands are sent again to their primaries.
    /// </summary>
    private async Task ReadAgainFromPrimariesAsync(
        Dictionary<RuvioSocket, List<int>> groups,
        Dictionary<RuvioSocket, ReadTarget> replicas,
        IReadOnlyList<IReadOnlyList<string>> commands,
        List<IReadOnlyList<string>>? extra,
        RespValue[] replies,
        CancellationToken cancellationToken)
    {
        foreach ((RuvioSocket connection, List<int> positions) in groups)
        {
            if (!replicas.TryGetValue(connection, out ReadTarget target))
            {
                continue;
            }

            foreach (int position in positions)
            {
                if (replies[position] is not { } reply || !IsLoading(reply))
                {
                    continue;
                }

                target.Skip();

                if (_options.ReadPolicy == ReadPolicy.Replica)
                {
                    continue;
                }

                IReadOnlyList<string> command = position < commands.Count ? commands[position] : extra![position - commands.Count];
                int slot = ClusterKeys.Slot(ClusterKeys.First(ClusterKeys.Name(command), command)!);

                replies[position] = await SendRoutedAsync(await ForSlotAsync(slot, cancellationToken), command, cancellationToken);
            }
        }
    }

    private static void AddToGroup(Dictionary<RuvioSocket, List<int>> groups, RuvioSocket connection, int position)
    {
        if (!groups.TryGetValue(connection, out List<int>? positions))
        {
            positions = [];
            groups[connection] = positions;
        }

        positions.Add(position);
    }

    /// <summary>
    /// Picks the socket for a subscribe command. Sharded channels go to their slot owner;
    /// classic channels and patterns go to the owner of slot 0 so every client meets there.
    /// </summary>
    public Task<RuvioSocket> SubscriptionTargetAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        (string host, int port) = SubscriptionEndpoint(name, arguments);

        return ConnectionAsync(host, port, cancellationToken);
    }

    /// <summary>
    /// Host and port for a dedicated subscriber socket. Classic channels meet on
    /// slot 0; sharded channels go to the slot owner. This is not a command socket.
    /// </summary>
    public (string Host, int Port) SubscriptionEndpoint(string name, IReadOnlyList<string> arguments)
    {
        int slot = name is "SSUBSCRIBE" or "SUNSUBSCRIBE" && arguments.Count > 1
            ? ClusterKeys.Slot(arguments[1])
            : 0;

        Tuple<string, int> home = _home;

        return _topology.Owner(slot) ?? (home.Item1, home.Item2);
    }

    private async Task<RespValue> TransactionAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        if (_pinned is { } pinned)
        {
            RespValue reply = await pinned.SendAsync(arguments, cancellationToken);

            if (name is "EXEC" or "DISCARD" || (name == "UNWATCH" && !_inMulti))
            {
                _pinned = null;
                _inMulti = false;
            }
            else if (name == "MULTI" && reply.Kind is not RespKind.Error)
            {
                _inMulti = true;
            }

            return reply;
        }

        switch (name)
        {
            case "MULTI":
                return RespValue.Error("ERR MULTI calls can not be nested");
            case "WATCH":
                return RespValue.Error("ERR WATCH inside MULTI is not allowed");
            case "DISCARD":
                ResetTransaction();

                return RespValue.Simple("OK");
            case "EXEC":
                IReadOnlyList<RespValue> flushed = await StartTransactionAsync(
                    await TargetAsync([], cancellationToken),
                    [.. _queued, arguments],
                    cancellationToken);

                ResetTransaction();

                return flushed[^1];
        }

        IReadOnlyList<string> keys = ClusterKeys.Of(name, arguments);

        if (keys.Count == 0)
        {
            _queued.Add([.. arguments]);

            return RespValue.Simple("QUEUED");
        }

        RuvioSocket owner = await ForSlotAsync(ClusterKeys.Slot(keys[0]), cancellationToken);
        IReadOnlyList<RespValue> replies = await StartTransactionAsync(owner, [.. _queued, arguments], cancellationToken);

        _queued.Clear();
        _multiPending = false;

        if (replies[0].Kind is RespKind.Error)
        {
            return replies[0];
        }

        _pinned = owner;
        _inMulti = true;

        return replies[^1];
    }

    private static Task<IReadOnlyList<RespValue>> StartTransactionAsync(
        RuvioSocket connection,
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        return connection.SendManyAsync([["MULTI"], .. commands], cancellationToken);
    }

    private void ResetTransaction()
    {
        _queued.Clear();
        _multiPending = false;
        _inMulti = false;
        _pinned = null;
    }

    private async Task<RespValue> WatchAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        RuvioSocket owner = await TargetAsync(ClusterKeys.Of("WATCH", arguments), cancellationToken);
        RespValue reply = await owner.SendAsync(arguments, cancellationToken);

        if (reply.Kind is not RespKind.Error)
        {
            _pinned = owner;
        }

        return reply;
    }

    private static bool SpreadsToEveryNode(IReadOnlyList<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return false;
        }

        string subcommand = arguments[1].ToUpperInvariant();

        return subcommand is "LOAD" or "FLUSH" or "DELETE";
    }

    private async Task<RespValue> EveryNodeAsync(
        string name,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken)
    {
        RuvioSocket[] connections = await EveryConnectionAsync(cancellationToken);
        var sends = new Task<RespValue>[connections.Length];

        for (int index = 0; index < connections.Length; index++)
        {
            sends[index] = connections[index].SendAsync(arguments, cancellationToken).AsTask();
        }

        RespValue[] replies = await Task.WhenAll(sends);
        long total = 0;

        foreach (RespValue reply in replies)
        {
            if (reply.Kind is RespKind.Error)
            {
                return reply;
            }

            total += reply.Integer;
        }

        return name == "DBSIZE" ? RespValue.FromInteger(total) : replies[0];
    }

    /// <summary>
    /// Walks each shard in turn. The client cursor is <c>serverCursor * nodes + node</c>,
    /// so 0 starts on the first shard and 0 comes back only after the last one.
    /// </summary>
    private async Task<RespValue> ScanAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        ulong nodes = (ulong)_topology.Nodes.Count;

        if (arguments.Count < 2
            || !ulong.TryParse(arguments[1], NumberStyles.None, CultureInfo.InvariantCulture, out ulong cursor))
        {
            return RespValue.Error("ERR invalid cursor");
        }

        int node = (int)(cursor % nodes);
        string[] forwarded = [.. arguments];

        forwarded[1] = (cursor / nodes).ToString(CultureInfo.InvariantCulture);
        (string host, int port) = _topology.Nodes[node];

        RuvioSocket connection = await ConnectionAsync(host, port, cancellationToken);
        RespValue reply = await connection.SendAsync(forwarded, cancellationToken);

        if (reply.Kind is RespKind.Error || reply.Items is not [var next, var keys])
        {
            return reply;
        }

        ulong serverNext = ulong.Parse(next.AsString() ?? "0", NumberStyles.None, CultureInfo.InvariantCulture);
        ulong clientNext = serverNext != 0
            ? checked((serverNext * nodes) + (ulong)node)
            : (ulong)node + 1 < nodes ? (ulong)node + 1 : 0;

        byte[] encoded = Encoding.ASCII.GetBytes(clientNext.ToString(CultureInfo.InvariantCulture));

        return RespValue.Array([RespValue.Bulk(encoded), keys]);
    }

    /// <summary>Sends one sub-command per slot, pipelined per shard, and merges the replies.</summary>
    private async Task<RespValue> SplitAsync(
        string name,
        IReadOnlyList<string> arguments,
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        List<SplitPart> parts = SplitParts(name, arguments, keys);
        var byConnection = new Dictionary<RuvioSocket, List<int>>();
        bool read = _options.ReadPolicy != ReadPolicy.Primary && ClusterKeys.Reads(name);
        ReadTarget[]? targets = read ? new ReadTarget[parts.Count] : null;

        for (int part = 0; part < parts.Count; part++)
        {
            if (read)
            {
                targets![part] = await ReadSocketAsync(parts[part].Slot, cancellationToken);
                AddToGroup(byConnection, targets[part].Socket, part);
            }
            else
            {
                AddToGroup(byConnection, await ForSlotAsync(parts[part].Slot, cancellationToken), part);
            }
        }

        var replies = new RespValue[parts.Count];

        var sends = new Task[byConnection.Count];
        int sent = 0;

        foreach ((RuvioSocket connection, List<int> indexes) in byConnection)
        {
            var batch = new IReadOnlyList<string>[indexes.Count];

            for (int index = 0; index < batch.Length; index++)
            {
                batch[index] = parts[indexes[index]].Command;
            }

            sends[sent++] = SendGroupAsync(connection, batch, indexes, replies, cancellationToken);
        }

        await Task.WhenAll(sends);

        var clock = new TryAgainClock();

        for (int part = 0; part < parts.Count; part++)
        {
            if (targets is not null && targets[part].Replica && IsLoading(replies[part]))
            {
                targets[part].Skip();

                if (_options.ReadPolicy != ReadPolicy.Replica)
                {
                    replies[part] = await SendRoutedAsync(
                        await ForSlotAsync(parts[part].Slot, cancellationToken), parts[part].Command, cancellationToken);
                }
            }

            if (NeedsFollowUp(replies[part]))
            {
                replies[part] = await FollowUpAsync((IReadOnlyList<string>)parts[part].Command, parts[part].Slot, 0, replies[part], cancellationToken, clock);
            }
        }

        return MergeSplit(name, keys.Count, parts, replies, 0);
    }

    /// <summary>One slot's share of a split command: its key positions and the command to send.</summary>
    private readonly record struct SplitPart(int Slot, List<int> Positions, string[] Command);

    /// <summary>Breaks a split command into one command per slot, keys in their original order.</summary>
    private static List<SplitPart> SplitParts(string name, IReadOnlyList<string> arguments, IReadOnlyList<string> keys)
    {
        bool pairs = name == "MSET";
        var bySlot = new Dictionary<int, List<int>>();

        for (int index = 0; index < keys.Count; index++)
        {
            int slot = ClusterKeys.Slot(keys[index]);

            if (!bySlot.TryGetValue(slot, out List<int>? positions))
            {
                positions = [];
                bySlot[slot] = positions;
            }

            positions.Add(index);
        }

        var parts = new List<SplitPart>(bySlot.Count);

        foreach ((int slot, List<int> positions) in bySlot)
        {
            var command = new string[1 + (positions.Count * (pairs ? 2 : 1))];
            int written = 0;

            command[written++] = arguments[0];

            foreach (int position in positions)
            {
                if (pairs)
                {
                    command[written++] = arguments[1 + (position * 2)];
                    command[written++] = arguments[2 + (position * 2)];
                }
                else
                {
                    command[written++] = keys[position];
                }
            }

            parts.Add(new SplitPart(slot, positions, command));
        }

        return parts;
    }

    /// <summary>
    /// Merges the part replies at <paramref name="offset"/>: <c>MGET</c> in key order, <c>MSET</c>
    /// as OK, counts summed. The first error reply wins.
    /// </summary>
    private static RespValue MergeSplit(string name, int keyCount, List<SplitPart> parts, RespValue[] replies, int offset)
    {
        for (int part = 0; part < parts.Count; part++)
        {
            if (replies[offset + part].Kind is RespKind.Error)
            {
                return replies[offset + part];
            }
        }

        switch (name)
        {
            case "MSET":
                return RespValue.Simple("OK");

            case "MGET":
                var values = new RespValue[keyCount];

                for (int part = 0; part < parts.Count; part++)
                {
                    IReadOnlyList<RespValue> items = replies[offset + part].Items ?? [];

                    for (int index = 0; index < items.Count; index++)
                    {
                        values[parts[part].Positions[index]] = items[index];
                    }
                }

                return RespValue.Array(values);

            default:
                long total = 0;

                for (int part = 0; part < parts.Count; part++)
                {
                    total += replies[offset + part].Integer;
                }

                return RespValue.FromInteger(total);
        }
    }

    /// <summary>
    /// True when a split command's keys span slots, read straight from the arguments so a
    /// one-slot <c>MGET</c> in a pipeline builds no key list.
    /// </summary>
    private static bool SplitSpansSlots(string name, IReadOnlyList<string> command)
    {
        int step = name == "MSET" ? 2 : 1;

        if (command.Count < 2 + step || UnpairedMset(name, command))
        {
            return false;
        }

        int first = ClusterKeys.Slot(command[1]);

        for (int index = 1 + step; index < command.Count; index += step)
        {
            if (ClusterKeys.Slot(command[index]) != first)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>An <c>MSET</c> with a key and no value is not split; the shard answers with its arity error.</summary>
    private static bool UnpairedMset(string name, IReadOnlyList<string> arguments) =>
        name == "MSET" && arguments.Count % 2 == 0;

    private static bool HasTransactionCommand(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        for (int index = 0; index < commands.Count; index++)
        {
            if (TransactionNames.Contains(ClusterKeys.Name(commands[index])))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task SendGroupAsync(
        RuvioSocket connection,
        IReadOnlyList<IReadOnlyList<string>> batch,
        List<int> positions,
        RespValue[] replies,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<RespValue> values = await connection.SendManyAsync(batch, cancellationToken);

        for (int index = 0; index < values.Count; index++)
        {
            replies[positions[index]] = values[index];
        }
    }

    private static bool SpansSlots(IReadOnlyList<string> keys)
    {
        if (keys.Count < 2)
        {
            return false;
        }

        int first = ClusterKeys.Slot(keys[0]);

        for (int index = 1; index < keys.Count; index++)
        {
            if (ClusterKeys.Slot(keys[index]) != first)
            {
                return true;
            }
        }

        return false;
    }

    private async Task<RespValue> SendRoutedAsync(
        RuvioSocket connection,
        IReadOnlyList<string> arguments,
        CancellationToken cancellationToken,
        int redirects = 0,
        int slot = -1)
    {
        RespValue reply = await connection.SendAsync(arguments, cancellationToken);

        return NeedsFollowUp(reply)
            ? await FollowUpAsync(arguments, slot, redirects, reply, cancellationToken)
            : reply;
    }

    /// <summary>
    /// The slow path of the string-list sends. A retry goes to the owner of <paramref name="slot"/>, or of the
    /// command's keys when the slot is -1.
    /// </summary>
    private Task<RespValue> FollowUpAsync(
        IReadOnlyList<string> arguments, int slot, int redirects, RespValue reply, CancellationToken cancellationToken,
        TryAgainClock? clock = null)
    {
        return ResolveAsync(
            reply,
            redirects,
            token => slot >= 0
                ? ForSlotAsync(slot, token)
                : TargetAsync(ClusterKeys.Of(ClusterKeys.Name(arguments), arguments), token),
            (node, asking, token) => asking ? AskAsync(node, arguments, token) : node.SendAsync(arguments, token).AsTask(),
            cancellationToken,
            clock);
    }

    /// <summary>
    /// Follows the redirects and <c>TRYAGAIN</c> replies <paramref name="reply"/> starts, until a reply that is neither
    /// or a budget runs out (3 redirects shared by <c>MOVED</c> and <c>ASK</c>, 3 retries of <c>TRYAGAIN</c>); that reply is returned.
    /// <paramref name="redirects"/> counts the <c>MOVED</c> replies already followed. <paramref name="route"/> picks the
    /// node for a retry from the current slot map; <paramref name="send"/> sends the command to a node, after <c>ASKING</c> when asked.
    /// </summary>
    /// <summary>The wait before a <c>TRYAGAIN</c> retry: the fixed schedule for one command, the shared clock for a pipeline.</summary>
    private static TimeSpan? NextTryAgainPause(TryAgainClock? clock, int retries) =>
        clock is null ? TimeSpan.FromMilliseconds(FirstTryAgainDelayMs << retries) : clock.Pause(retries);

    private async Task<RespValue> ResolveAsync(
        RespValue reply,
        int redirects,
        Func<CancellationToken, ValueTask<RuvioSocket>> route,
        Func<RuvioSocket, bool, CancellationToken, Task<RespValue>> send,
        CancellationToken cancellationToken,
        TryAgainClock? clock = null)
    {
        int moves = redirects;
        int retries = 0;

        while (true)
        {
            if (MovedTarget(reply) is { } moved)
            {
                if (redirects >= MaxRedirects)
                {
                    return reply;
                }

                redirects++;

                RuvioSocket owner = await FollowMovedAsync(moved, moves++, cancellationToken).ConfigureAwait(false);

                reply = await send(owner, false, cancellationToken).ConfigureAwait(false);
            }
            else if (AskTarget(reply) is { } ask)
            {
                if (redirects >= MaxRedirects)
                {
                    return reply;
                }

                redirects++;

                RuvioSocket node = await ConnectionAsync(ask.Host, ask.Port, cancellationToken).ConfigureAwait(false);

                reply = await send(node, true, cancellationToken).ConfigureAwait(false);
            }
            else if (IsTryAgain(reply) && retries < MaxTryAgains && NextTryAgainPause(clock, retries) is { } pause)
            {
                retries++;

                if (pause > TimeSpan.Zero)
                {
                    await Task.Delay(pause, cancellationToken).ConfigureAwait(false);
                }

                RuvioSocket owner = await route(cancellationToken).ConfigureAwait(false);

                reply = await send(owner, false, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                return reply;
            }
        }
    }

    /// <summary>Sends <c>ASKING</c> and the command in one write; the <c>ASKING</c> reply is dropped, an error in it is thrown.</summary>
    private static async Task<RespValue> AskAsync(RuvioSocket node, IReadOnlyList<string> arguments, CancellationToken cancellationToken)
    {
        IReadOnlyList<RespValue> replies = await node.SendManyAsync([AskingCommand, arguments], cancellationToken).ConfigureAwait(false);

        RuvioSocket.ThrowOnError(replies[0]);

        return replies[1];
    }

    /// <summary>The binary form of <see cref="AskAsync"/>.</summary>
    private static async Task<RespValue> AskBinaryAsync(
        RuvioSocket node, IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken)
    {
        byte[] command = RespCodec.Encode(arguments);
        byte[] frame = new byte[AskingFrame.Length + command.Length];

        AskingFrame.CopyTo(frame, 0);
        command.CopyTo(frame, AskingFrame.Length);

        IReadOnlyList<RespValue> replies = await node.SendManyAsync(frame, 2, cancellationToken).ConfigureAwait(false);

        RuvioSocket.ThrowOnError(replies[0]);

        return replies[1];
    }

    /// <summary>True for an error reply that starts <c>MOVED</c>, <c>ASK</c> or <c>TRYAGAIN</c>; checked by prefix, only on errors.</summary>
    private static bool NeedsFollowUp(RespValue reply) =>
        reply is { Kind: RespKind.Error, Text: { } text }
        && (text.StartsWith("MOVED ", StringComparison.Ordinal)
            || text.StartsWith("ASK ", StringComparison.Ordinal)
            || text.StartsWith("TRYAGAIN", StringComparison.Ordinal));

    private static bool IsTryAgain(RespValue reply) =>
        reply is { Kind: RespKind.Error, Text: { } text } && text.StartsWith("TRYAGAIN", StringComparison.Ordinal);

    /// <summary>
    /// Points the slot a <c>MOVED</c> names at its node and returns that node's socket; the
    /// retry always goes to the named node. <paramref name="redirect"/> counts the earlier
    /// <c>MOVED</c> replies for the same command. The whole slot map is read again when the
    /// node is not in it, and on the command's second and later <c>MOVED</c>.
    /// </summary>
    private async Task<RuvioSocket> FollowMovedAsync(
        (int Slot, string Host, int Port) moved, int redirect, CancellationToken cancellationToken)
    {
        if (redirect > 0 || !_topology.Assign(moved.Slot, moved.Host, moved.Port))
        {
            await RefreshAsync(cancellationToken).ConfigureAwait(false);
            _topology.Assign(moved.Slot, moved.Host, moved.Port);
        }

        return await ConnectionAsync(moved.Host, moved.Port, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Points one slot at the node a <c>MOVED</c> named, when that node is in the map.</summary>
    internal void PointSlot((int Slot, string Host, int Port) moved)
    {
        _topology.Assign(moved.Slot, moved.Host, moved.Port);
    }

    /// <summary>The slot and node of a <c>MOVED slot host:port</c> reply; null for any other reply.</summary>
    internal static (int Slot, string Host, int Port)? MovedTarget(RespValue reply) => RedirectTarget(reply, "MOVED ");

    /// <summary>The slot and node of an <c>ASK slot host:port</c> reply; null for any other reply.</summary>
    private static (int Slot, string Host, int Port)? AskTarget(RespValue reply) => RedirectTarget(reply, "ASK ");

    private static (int Slot, string Host, int Port)? RedirectTarget(RespValue reply, string prefix)
    {
        if (reply.Kind is not RespKind.Error || reply.Text is not { } text || !text.StartsWith(prefix, StringComparison.Ordinal))
        {
            return null;
        }

        string[] parts = text.Split(' ');

        if (parts.Length < 3
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int slot))
        {
            return null;
        }

        int colon = parts[2].LastIndexOf(':');

        if (colon <= 0
            || !int.TryParse(parts[2][(colon + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            return null;
        }

        return (slot, parts[2][..colon], port);
    }

    /// <summary>
    /// Reloads the slot map. Nodes with an open socket are asked first; when none of them answers
    /// (the usual case after a node went away), every other known node and then every seed is opened
    /// in turn, one connect attempt each, until one returns a slot map. False when none did.
    /// </summary>
    private async Task<bool> RefreshAsync(CancellationToken cancellationToken)
    {
        await _refreshGate.WaitAsync(cancellationToken);

        try
        {
            RuvioSocket[] live;

            lock (_connections)
            {
                live = [.. _connections.Values.Where(connection => !connection.IsBroken)];
            }

            foreach (RuvioSocket connection in live)
            {
                if (await AskSlotsAsync(connection, cancellationToken) is { } topology)
                {
                    Install(topology);

                    return true;
                }
            }

            return await ReloadFromUnopenedAsync(cancellationToken);
        }

        finally
        {
            _refreshGate.Release();
        }
    }

    private static async Task<ClusterTopology?> AskSlotsAsync(RuvioSocket connection, CancellationToken cancellationToken)
    {
        try
        {
            return ClusterTopology.Parse(await connection.SendAsync(["CLUSTER", "SLOTS"], cancellationToken));
        }

        catch (Exception error) when (error is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// Opens the known nodes without a live socket and then the seeds, one connect attempt each, and
    /// asks each for the slot map. A seed that could not be opened, or did not answer, is not kept.
    /// </summary>
    private async Task<bool> ReloadFromUnopenedAsync(CancellationToken cancellationToken)
    {
        var tried = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var candidates = new List<(string Host, int Port)>(_topology.Nodes);

        candidates.AddRange(_seeds);

        foreach ((string host, int port) in candidates)
        {
            string endpoint = Endpoint(host, port);

            if (!tried.Add(endpoint))
            {
                continue;
            }

            lock (_connections)
            {
                if (_connections.TryGetValue(endpoint, out RuvioSocket? existing) && !existing.IsBroken)
                {
                    continue;
                }
            }

            RuvioSocket opened;

            try
            {
                opened = await RuvioSocket.OpenAsync(host, port, _options, cancellationToken);
            }

            catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                continue;
            }

            ClusterTopology? topology = await AskSlotsAsync(opened, cancellationToken);

            if (topology is null)
            {
                opened.Dispose();

                continue;
            }

            RuvioSocket? stale;

            lock (_connections)
            {
                _connections.TryGetValue(endpoint, out stale);
                _connections[endpoint] = opened;
            }

            if (stale is not null)
            {
                stale.Dispose();
            }

            Install(topology);

            return true;
        }

        return false;
    }

    private ValueTask<RuvioSocket> TargetAsync(IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        return TargetKeyAsync(keys.Count > 0 ? keys[0] : null, cancellationToken);
    }

    private ValueTask<RuvioSocket> TargetKeyAsync(string? key, CancellationToken cancellationToken)
    {
        if (key is not null)
        {
            return ForSlotAsync(ClusterKeys.Slot(key), cancellationToken);
        }

        return new ValueTask<RuvioSocket>(KeylessAsync(cancellationToken));
    }

    /// <summary>
    /// The socket for a command without a key: the home node. When that node is gone, the slot map is
    /// reloaded and the first node of the new map that opens becomes the home node.
    /// </summary>
    private async Task<RuvioSocket> KeylessAsync(CancellationToken cancellationToken)
    {
        Tuple<string, int> home = _home;

        try
        {
            return await ConnectionAsync(home.Item1, home.Item2, cancellationToken);
        }

        catch (Exception error) when (CanReload(error, cancellationToken))
        {
            if (!await RefreshAsync(cancellationToken))
            {
                throw;
            }

            foreach ((string host, int port) in _topology.Nodes)
            {
                try
                {
                    RuvioSocket opened = await ConnectionAsync(host, port, cancellationToken);

                    _home = Tuple.Create(host, port);

                    return opened;
                }

                catch (Exception next) when (CanReload(next, cancellationToken))
                {
                }
            }

            throw;
        }
    }

    private bool CanReload(Exception error, CancellationToken cancellationToken) =>
        !_disposed && error is not ObjectDisposedException
        && (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested);

    private async Task<RuvioSocket> FirstKeyedTargetAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        foreach (IReadOnlyList<string> command in commands)
        {
            if (ClusterKeys.First(ClusterKeys.Name(command), command) is { } key)
            {
                return await ForSlotAsync(ClusterKeys.Slot(key), cancellationToken);
            }
        }

        return await TargetKeyAsync(null, cancellationToken);
    }

    private ValueTask<RuvioSocket> ForSlotAsync(int slot, CancellationToken cancellationToken)
    {
        ClusterTopology topology = _topology;
        int node = topology.NodeOf(slot);

        if (node < 0)
        {
            return TargetKeyAsync(null, cancellationToken);
        }

        if (topology.Sockets[node] is { IsBroken: false } cached)
        {
            return new ValueTask<RuvioSocket>(cached);
        }

        return OpenSlotAsync(slot, topology, node, cancellationToken);
    }

    /// <summary>
    /// Opens the owner of a slot. A node that cannot be opened (after the reconnect attempts)
    /// triggers a slot map reload, and the command goes to the owner the new map names.
    /// </summary>
    private async ValueTask<RuvioSocket> OpenSlotAsync(
        int slot, ClusterTopology topology, int node, CancellationToken cancellationToken)
    {
        try
        {
            return await OpenForNodeAsync(topology, node, cancellationToken);
        }

        catch (Exception error) when (CanReload(error, cancellationToken))
        {
            if (!await RefreshAsync(cancellationToken))
            {
                throw;
            }

            ClusterTopology reloaded = _topology;
            int owner = reloaded.NodeOf(slot);

            if (owner < 0)
            {
                throw;
            }

            if (reloaded.Sockets[owner] is { IsBroken: false } cached)
            {
                return cached;
            }

            return await OpenForNodeAsync(reloaded, owner, cancellationToken);
        }
    }

    private async ValueTask<RuvioSocket> OpenForNodeAsync(
        ClusterTopology topology, int node, CancellationToken cancellationToken)
    {
        (string host, int port) = topology.Nodes[node];
        RuvioSocket connection = await ConnectionAsync(host, port, cancellationToken);

        topology.Sockets[node] = connection;

        return connection;
    }

    private async Task<RuvioSocket[]> EveryConnectionAsync(CancellationToken cancellationToken)
    {
        var connections = new List<RuvioSocket>();

        foreach ((string host, int port) in _topology.Nodes)
        {
            connections.Add(await ConnectionAsync(host, port, cancellationToken));
        }

        return [.. connections];
    }

    private async Task<RuvioSocket> ConnectionAsync(
        string host,
        int port,
        CancellationToken cancellationToken,
        bool force = false,
        bool replica = false)
    {
        string endpoint = Endpoint(host, port);

        lock (_connections)
        {
            if (_connections.TryGetValue(endpoint, out RuvioSocket? existing) && !existing.IsBroken)
            {
                return existing;
            }
        }

        await _connectGate.WaitAsync(cancellationToken);

        try
        {
            RuvioSocket? stale;

            lock (_connections)
            {
                if (_connections.TryGetValue(endpoint, out stale) && !stale.IsBroken)
                {
                    return stale;
                }
            }

            ObjectDisposedException.ThrowIf(_disposed, this);

            if (stale is { IsBroken: true } && !_options.Reconnect && !force)
            {
                throw new RuvioException("connection is closed");
            }

            if (stale is { IsBroken: true })
            {
                _lost?.Invoke(host, port, new RuvioException("connection is closed"));
            }

            RuvioSocket opened = _open is null
                ? await RuvioSocket.OpenAsync(host, port, _options, cancellationToken)
                : await _open(host, port, cancellationToken);

            if (replica)
            {
                // Redis replicas serve reads only to connections that sent READONLY; Ruvio replicas
                // serve them anyway and answer OK.
                try
                {
                    await opened.SendAsync(["READONLY"], cancellationToken);
                }

                catch
                {
                    opened.Dispose();

                    throw;
                }
            }

            if (stale is { IsBroken: true })
            {
                _restored?.Invoke(host, port);
            }

            lock (_connections)
            {
                _connections[endpoint] = opened;
            }

            if (stale is not null && !ReferenceEquals(stale, _seed))
            {
                stale.Dispose();
            }

            return opened;
        }

        finally
        {
            _connectGate.Release();
        }
    }

    private static string Endpoint(string host, int port)
    {
        return $"{host}:{port.ToString(CultureInfo.InvariantCulture)}";
    }

    public void Dispose()
    {
        _disposed = true;

        lock (_connections)
        {
            foreach (RuvioSocket connection in _connections.Values)
            {
                connection.Dispose();
            }

            _connections.Clear();
        }
    }
}

/// <summary>Which node owns each slot, and the nodes in slot order.</summary>
internal sealed class ClusterTopology
{
    private readonly short[] _owners;

    private (string Host, int Port)?[] _reads = [];

    private ClusterTopology(
        short[] owners, IReadOnlyList<(string Host, int Port)> nodes, IReadOnlyList<List<(string Host, int Port)>> replicas)
    {
        _owners = owners;
        Nodes = nodes;
        Replicas = replicas;
        ReplicaSockets = new RuvioSocket?[nodes.Count];
    }

    /// <summary>Per node of <see cref="Nodes"/>: the replicas <c>CLUSTER SLOTS</c> lists after it.</summary>
    public IReadOnlyList<List<(string Host, int Port)>> Replicas { get; }

    /// <summary>Open replica sockets by node index, for the replica each node's reads go to.</summary>
    internal RuvioSocket?[] ReplicaSockets { get; }

    /// <summary>The replica a node's reads go to; null when it has none, the policy is primary, or it was skipped.</summary>
    internal (string Host, int Port)? ReadReplica(int node) => (uint)node < (uint)_reads.Length ? _reads[node] : null;

    /// <summary>Picks one replica per node, <paramref name="pick"/> modulo the replica count, kept until the next load.</summary>
    internal void PickReplicas(int pick)
    {
        var reads = new (string Host, int Port)?[Nodes.Count];

        for (int node = 0; node < reads.Length; node++)
        {
            List<(string Host, int Port)> replicas = Replicas[node];

            if (replicas.Count > 0)
            {
                reads[node] = replicas[pick % replicas.Count];
            }
        }

        _reads = reads;
    }

    /// <summary>Stops sending reads to the replica <paramref name="node"/> uses, and to the same replica of any other node.</summary>
    internal void SkipReplica(int node)
    {
        (string Host, int Port)? skipped = ReadReplica(node);

        for (int other = 0; other < _reads.Length; other++)
        {
            if (_reads[other] == skipped)
            {
                _reads[other] = null;
            }
        }
    }

    public IReadOnlyList<(string Host, int Port)> Nodes { get; }

    /// <summary>Open sockets by node index, so a warm route needs no endpoint string or lookup lock.</summary>
    internal RuvioSocket?[] Sockets { get; private init; } = [];

    public (string Host, int Port)? Owner(int slot)
    {
        short node = _owners[slot];

        return node < 0 ? null : Nodes[node];
    }

    public int NodeOf(int slot) => _owners[slot];

    /// <summary>
    /// Points one slot at a node already in the map, after a <c>MOVED</c>. False when the slot
    /// is out of range or the node is unknown, so the caller reads the whole map instead.
    /// </summary>
    public bool Assign(int slot, string host, int port)
    {
        if ((uint)slot >= ClusterKeys.SlotCount)
        {
            return false;
        }

        for (int node = 0; node < Nodes.Count; node++)
        {
            if (Nodes[node].Port == port && string.Equals(Nodes[node].Host, host, StringComparison.OrdinalIgnoreCase))
            {
                _owners[slot] = (short)node;

                return true;
            }
        }

        return false;
    }

    /// <summary>Reads a <c>CLUSTER SLOTS</c> reply. Null for an error or an empty map.</summary>
    public static ClusterTopology? Parse(RespValue reply)
    {
        if (reply.Kind is not RespKind.Array || reply.Items is not { Count: > 0 } ranges)
        {
            return null;
        }

        var owners = new short[ClusterKeys.SlotCount];

        Array.Fill(owners, (short)-1);

        var nodes = new List<(string Host, int Port)>();
        var replicas = new List<List<(string Host, int Port)>>();

        foreach (RespValue range in ranges.OrderBy(range => range.Items?[0].Integer ?? 0))
        {
            if (range.Items is not { Count: >= 3 } fields || fields[2].Items is not { Count: >= 2 } primary)
            {
                return null;
            }

            string host = primary[0].AsString() ?? "";
            int port = (int)primary[1].Integer;
            int node = nodes.FindIndex(existing =>
                existing.Port == port && string.Equals(existing.Host, host, StringComparison.OrdinalIgnoreCase));

            if (node < 0)
            {
                node = nodes.Count;
                nodes.Add((host, port));
                replicas.Add([]);
            }

            // Replicas follow the primary; one that cannot be read is skipped, not fatal.
            for (int index = 3; index < fields.Count; index++)
            {
                if (fields[index].Items is { Count: >= 2 } replica
                    && replica[0].AsString() is { Length: > 0 } replicaHost
                    && replica[1].Kind is RespKind.Integer && replica[1].Integer is > 0 and <= 65535
                    && !replicas[node].Contains((replicaHost, (int)replica[1].Integer)))
                {
                    replicas[node].Add((replicaHost, (int)replica[1].Integer));
                }
            }

            long first = Math.Max(0, fields[0].Integer);
            long last = Math.Min(ClusterKeys.SlotCount - 1, fields[1].Integer);

            for (long slot = first; slot <= last; slot++)
            {
                owners[slot] = (short)node;
            }
        }

        return new ClusterTopology(owners, nodes, replicas) { Sockets = new RuvioSocket?[nodes.Count] };
    }
}
