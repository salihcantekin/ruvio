using System.Net.Sockets;
using System.Runtime.CompilerServices;

namespace Ruvio.Client;

/// <summary>
/// Finds the node that is the primary: short-lived probe sockets that ask <c>ROLE</c> (or, with a coordinator
/// name, <c>SENTINEL get-master-addr-by-name</c>). A probe sends <c>AUTH</c> as configured and one command, and
/// keeps no state of the connection.
/// </summary>
internal static class PrimaryProbe
{
    /// <summary>What <c>ROLE</c> said: a primary, or a replica with the address it reports for its primary.</summary>
    private readonly record struct Role(bool Primary, (string Host, int Port)? Reported);

    /// <summary>
    /// One round: the address of a node that answers <c>ROLE</c> with <c>master</c>, asking <paramref name="seeds"/>
    /// in order from <paramref name="start"/>, or null. A replica's reported primary is asked after the seeds,
    /// once per address. With <see cref="RuvioClientOptions.SentinelMaster"/> the seeds are coordinators: each
    /// names a primary and the first that <c>ROLE</c> confirms wins. The address in <paramref name="avoid"/> (the
    /// one that just answered <c>READONLY</c>) is not returned while it still says <c>master</c>: a replaced primary
    /// does. It is cleared once it answers <c>ROLE</c> as a replica, so a later round takes it when it is promoted.
    /// Every question is bounded; a refused <c>AUTH</c> is thrown.
    /// </summary>
    public static async Task<(string Host, int Port)?> FindAsync(
        IReadOnlyList<(string Host, int Port)> seeds,
        int start,
        StrongBox<(string Host, int Port)?>? avoid,
        RuvioClientOptions options,
        CancellationToken cancellationToken)
    {
        RuvioClientOptions probe = ProbeOptions(options);
        var queue = new Queue<(string Host, int Port)>(seeds.Count);

        for (int step = 0; step < seeds.Count; step++)
        {
            queue.Enqueue(seeds[(start + step) % seeds.Count]);
        }

        if (options.SentinelMaster is { } name)
        {
            foreach ((string Host, int Port) coordinator in queue)
            {
                if (await AskSentinelAsync(coordinator, name, probe, cancellationToken).ConfigureAwait(false) is not { } named)
                {
                    continue;
                }

                Role? namedRole = await AskRoleAsync(named, probe, cancellationToken).ConfigureAwait(false);

                if (namedRole is { Primary: true } && !Same(named, avoid?.Value))
                {
                    return named;
                }

                Forget(avoid, named, namedRole);
            }

            return null;
        }

        List<(string Host, int Port)> seen = [.. queue];

        while (queue.TryDequeue(out (string Host, int Port) address))
        {
            Role? role = await AskRoleAsync(address, probe, cancellationToken).ConfigureAwait(false);

            if (role is { Primary: true } && !Same(address, avoid?.Value))
            {
                return address;
            }

            Forget(avoid, address, role);

            if (role is { Primary: false, Reported: { } reported } && !seen.Exists(known => Same(known, reported)))
            {
                seen.Add(reported);
                queue.Enqueue(reported);
            }
        }

        return null;
    }

    /// <summary>The avoided address said it is a replica: from now on it may be taken once it says <c>master</c>.</summary>
    private static void Forget(StrongBox<(string Host, int Port)?>? avoid, (string Host, int Port) address, Role? role)
    {
        if (avoid is not null && role is { Primary: false } && Same(address, avoid.Value))
        {
            avoid.Value = null;
        }
    }

    /// <summary>
    /// The options of a probe socket: <c>AUTH</c> only (no durability), and a reply wait of at most the connect
    /// timeout, so a hung seed costs one bounded wait and never the 30 s command timeout.
    /// </summary>
    private static RuvioClientOptions ProbeOptions(RuvioClientOptions options) => new()
    {
        Username = options.Username,
        Password = options.Password,
        ConnectTimeout = options.ConnectTimeout,
        CommandTimeout = ReplyWait(options),
        MaxInFlightRequests = options.MaxInFlightRequests,
    };

    private static TimeSpan ReplyWait(RuvioClientOptions options) =>
        options.CommandTimeout <= TimeSpan.Zero || options.CommandTimeout == Timeout.InfiniteTimeSpan
            ? options.ConnectTimeout
            : options.CommandTimeout < options.ConnectTimeout ? options.CommandTimeout : options.ConnectTimeout;

    private static bool Same((string Host, int Port) left, (string Host, int Port)? right) =>
        right is { } other
        && left.Port == other.Port
        && string.Equals(left.Host, other.Host, StringComparison.OrdinalIgnoreCase);

    private static async Task<Role?> AskRoleAsync(
        (string Host, int Port) address,
        RuvioClientOptions probe,
        CancellationToken cancellationToken) =>
        ReadRole(await AskAsync(address, probe, ["ROLE"], cancellationToken).ConfigureAwait(false));

    /// <summary>The address <c>SENTINEL get-master-addr-by-name</c> names, when the coordinator answers one.</summary>
    private static async Task<(string Host, int Port)?> AskSentinelAsync(
        (string Host, int Port) coordinator,
        string name,
        RuvioClientOptions probe,
        CancellationToken cancellationToken)
    {
        RespValue? reply = await AskAsync(coordinator, probe, ["SENTINEL", "get-master-addr-by-name", name], cancellationToken)
            .ConfigureAwait(false);

        if (reply is not { Kind: RespKind.Array, Items: { Count: 2 } items })
        {
            return null;
        }

        string? host = items[0].AsString();

        return !string.IsNullOrEmpty(host)
            && items[1].AsString() is { } text
            && int.TryParse(text, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out int port)
            && port is >= 1 and <= 65535
            ? (host, port)
            : null;
    }

    /// <summary>The first element names the role; a replica's second and third are the host and port it reports.</summary>
    private static Role? ReadRole(RespValue? reply)
    {
        if (reply is not { Kind: RespKind.Array, Items: { Count: > 0 } items })
        {
            return null;
        }

        string? kind = items[0].AsString();

        if (string.Equals(kind, "master", StringComparison.OrdinalIgnoreCase))
        {
            return new Role(true, null);
        }

        if (!string.Equals(kind, "slave", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        string? host = items.Count > 1 ? items[1].AsString() : null;
        long port = 0;

        if (items.Count > 2)
        {
            RespValue field = items[2];

            if (field.Kind is RespKind.Integer)
            {
                port = field.Integer;
            }
            else if (field.AsString() is { } text)
            {
                long.TryParse(text, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out port);
            }
        }

        return new Role(false, !string.IsNullOrEmpty(host) && port is >= 1 and <= 65535 ? (host, (int)port) : null);
    }

    /// <summary>
    /// One bounded question to one address. Null when it cannot be reached, times out, drops or answers an
    /// error; a refused <c>AUTH</c> is thrown, as connecting does.
    /// </summary>
    private static async Task<RespValue?> AskAsync(
        (string Host, int Port) address,
        RuvioClientOptions probe,
        string[] arguments,
        CancellationToken cancellationToken)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        RuvioSocket? socket = null;

        // The connect and the reply wait each have their own limit; this one also covers AUTH.
        bound.CancelAfter(probe.ConnectTimeout + probe.CommandTimeout);

        try
        {
            socket = await RuvioSocket.OpenAsync(address.Host, address.Port, probe, bound.Token).ConfigureAwait(false);

            RespValue reply = await socket.SendAsync(arguments, bound.Token).ConfigureAwait(false);

            return reply.Kind is RespKind.Error ? null : reply;
        }
        catch (Exception error) when (NoAnswer(error, cancellationToken))
        {
            return null;
        }
        finally
        {
            socket?.Dispose();
        }
    }

    private static bool NoAnswer(Exception error, CancellationToken cancellationToken) =>
        error is SocketException or IOException or TimeoutException or RuvioException { IsServerReply: false }
        || (error is OperationCanceledException && !cancellationToken.IsCancellationRequested);
}
