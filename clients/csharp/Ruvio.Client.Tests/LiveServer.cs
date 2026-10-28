using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>A test that needs a real Ruvio server at <c>RUVIO_TEST_ADDR=host:port</c>; skipped when it is not set.</summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (LiveServer.Address is null)
        {
            Skip = "RUVIO_TEST_ADDR is not set";
        }
    }
}

/// <summary>Live tests share one server and FLUSHALL it, so they never run in parallel with each other.</summary>
[CollectionDefinition("Live", DisableParallelization = true)]
public sealed class LiveCollection;

internal static class LiveServer
{
    public static (string Host, int Port)? Address
    {
        get
        {
            string? value = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR");
            int colon = value?.LastIndexOf(':') ?? -1;

            return colon > 0 && int.TryParse(value![(colon + 1)..], out int port) ? (value[..colon], port) : null;
        }
    }

    /// <summary>Opens a client on <paramref name="database"/> through <paramref name="proxy"/> (or directly) and flushes that database.</summary>
    public static async Task<RuvioConnection> ConnectAsync(int database, RecordingProxy? proxy = null, bool flush = true)
    {
        (string host, int port) = Address ?? throw new InvalidOperationException("RUVIO_TEST_ADDR is not set");
        RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = proxy is null ? host : "127.0.0.1",
            Port = proxy?.Port ?? port,
            Database = database,
            DiscoverCluster = false,
        });

        if (flush)
        {
            await (await connection.GetDefaultDatabaseAsync()).FlushDatabaseAsync();
        }

        return connection;
    }
}

/// <summary>
/// A TCP pass-through between the client and the live server that records the name of every command the client
/// sends (with the subcommand for container commands such as <c>ACL</c> or <c>XGROUP</c>). Used to prove which
/// matrix commands a live run really exercised.
/// </summary>
internal sealed class RecordingProxy : IAsyncDisposable
{
    private static readonly HashSet<string> Containers = new(StringComparer.Ordinal)
    {
        "ACL", "CLIENT", "CLUSTER", "CONFIG", "FUNCTION", "RUVIO", "SCRIPT", "XGROUP", "XINFO",
    };

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentBag<TcpClient> _sockets = [];
    private readonly Task _accepting;

    public RecordingProxy()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accepting = AcceptLoop();
    }

    public int Port { get; }

    public ConcurrentDictionary<string, int> Seen { get; } = new(StringComparer.Ordinal);

    private async Task AcceptLoop()
    {
        (string host, int port) = LiveServer.Address!.Value;

        while (!_stop.IsCancellationRequested)
        {
            TcpClient inbound;

            try
            {
                inbound = await _listener.AcceptTcpClientAsync(_stop.Token);
            }
            catch (Exception) when (_stop.IsCancellationRequested)
            {
                return;
            }

            var outbound = new TcpClient { NoDelay = true };
            await outbound.ConnectAsync(host, port);
            inbound.NoDelay = true;
            _sockets.Add(inbound);
            _sockets.Add(outbound);
            _ = Pump(inbound.GetStream(), outbound.GetStream(), record: true);
            _ = Pump(outbound.GetStream(), inbound.GetStream(), record: false);
        }
    }

    private async Task Pump(NetworkStream from, NetworkStream to, bool record)
    {
        byte[] buffer = new byte[64 * 1024];
        var pending = new List<byte>();

        try
        {
            while (true)
            {
                int read = await from.ReadAsync(buffer, _stop.Token);

                if (read == 0)
                {
                    break;
                }

                if (record)
                {
                    pending.AddRange(buffer.AsSpan(0, read));
                    Parse(pending);
                }

                await to.WriteAsync(buffer.AsMemory(0, read), _stop.Token);
            }
        }
        catch (Exception)
        {
            // The other side closed; tear down both directions.
        }

        try
        {
            to.Socket.Shutdown(SocketShutdown.Send);
        }
        catch (Exception)
        {
        }
    }

    private void Parse(List<byte> pending)
    {
        byte[] data = [.. pending];
        int offset = 0;

        while (TryCommand(data, ref offset, out string? name))
        {
            if (name is not null)
            {
                Seen.AddOrUpdate(name, 1, (_, count) => count + 1);
            }
        }

        pending.RemoveRange(0, offset);
    }

    private static bool TryCommand(byte[] data, ref int offset, out string? name)
    {
        name = null;
        int cursor = offset;

        if (!TryLine(data, ref cursor, out string header) || header.Length == 0 || header[0] != '*')
        {
            return false;
        }

        int count = int.Parse(header.AsSpan(1));
        string? first = null;
        string? second = null;

        for (int index = 0; index < count; index++)
        {
            if (!TryLine(data, ref cursor, out string length) || length[0] != '$')
            {
                return false;
            }

            int size = int.Parse(length.AsSpan(1));

            if (cursor + size + 2 > data.Length)
            {
                return false;
            }

            if (index < 2)
            {
                string word = Encoding.UTF8.GetString(data, cursor, Math.Min(size, 64)).ToUpperInvariant();

                if (index == 0)
                    first = word;
                else
                    second = word;
            }

            cursor += size + 2;
        }

        offset = cursor;
        name = first is not null && second is not null && Containers.Contains(first) ? first + " " + second : first;

        return true;
    }

    private static bool TryLine(byte[] data, ref int cursor, out string line)
    {
        for (int index = cursor; index + 1 < data.Length; index++)
        {
            if (data[index] == '\r' && data[index + 1] == '\n')
            {
                line = Encoding.ASCII.GetString(data, cursor, index - cursor);
                cursor = index + 2;

                return true;
            }
        }

        line = "";

        return false;
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();

        foreach (TcpClient socket in _sockets)
        {
            socket.Dispose();
        }

        try
        {
            await _accepting;
        }
        catch (Exception)
        {
        }

        _stop.Dispose();
    }
}
