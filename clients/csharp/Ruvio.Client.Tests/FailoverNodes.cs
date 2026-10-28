using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Ruvio.Client.Tests;

/// <summary>What a scripted node says it is.</summary>
internal enum NodeKind
{
    /// <summary>Answers <c>ROLE</c> with <c>master</c> and takes writes.</summary>
    Master,

    /// <summary>Answers <c>ROLE</c> with <c>slave</c> (naming <see cref="NodeMode.Primary"/> when set) and refuses writes with <c>READONLY</c>.</summary>
    Replica,

    /// <summary>Says <c>master</c> but refuses writes anyway.</summary>
    Liar,

    /// <summary>A coordinator that names <see cref="NodeMode.Primary"/> for <c>SENTINEL get-master-addr-by-name</c>.</summary>
    Names,

    /// <summary>Accepts connections and reads commands, but never answers.</summary>
    Silent,

    /// <summary>A replica (naming no primary) that is promoted to <see cref="Master"/> right after it answers its first <c>ROLE</c>.</summary>
    Promoted,
}

internal sealed record NodeMode(NodeKind Kind, int Primary = 0);

/// <summary>One command a scripted node received, with the connection it came on.</summary>
internal sealed record SeenCommand(int Connection, string[] Arguments);

/// <summary>
/// A scripted server on loopback: answers <c>ROLE</c> and <c>SENTINEL</c> by its <see cref="NodeMode"/>, refuses writes
/// with <c>READONLY</c> when it is not a primary, and records every command. Every wait is bounded by the
/// node's lifetime.
/// </summary>
internal sealed class FailoverNode : IDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<SeenCommand> _seen = new();
    private readonly ConcurrentBag<TcpClient> _sockets = [];
    private volatile NodeMode _mode;
    private int _connections;

    public FailoverNode(NodeMode mode)
    {
        _mode = mode;
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _ = AcceptAsync();
    }

    public int Port { get; }

    public string Address => $"127.0.0.1:{Port}";

    public IReadOnlyCollection<SeenCommand> Commands => _seen;

    public void BecomeMode(NodeMode mode) => _mode = mode;

    /// <summary>How many commands named <paramref name="name"/> arrived, on any connection.</summary>
    public int Seen(string name) =>
        _seen.Count(command => string.Equals(command.Arguments[0], name, StringComparison.OrdinalIgnoreCase));

    /// <summary>Closes every connection the node accepted; it keeps listening.</summary>
    public void DropConnections()
    {
        while (_sockets.TryTake(out TcpClient? socket))
        {
            socket.Dispose();
        }
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        DropConnections();
        _stop.Dispose();
    }

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                TcpClient socket = await _listener.AcceptTcpClientAsync(_stop.Token);

                _sockets.Add(socket);
                _ = ServeAsync(socket, Interlocked.Increment(ref _connections));
            }
        }
        catch (Exception error) when (error is OperationCanceledException or ObjectDisposedException or SocketException)
        {
        }
    }

    private async Task ServeAsync(TcpClient socket, int connection)
    {
        try
        {
            NetworkStream stream = socket.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8);

            while (await ReadCommandAsync(reader) is { } arguments)
            {
                _seen.Enqueue(new SeenCommand(connection, arguments));

                if (_mode.Kind is NodeKind.Silent)
                {
                    continue;
                }

                NodeMode mode = _mode;

                if (mode.Kind is NodeKind.Promoted && arguments[0].Equals("ROLE", StringComparison.OrdinalIgnoreCase))
                {
                    _mode = new NodeMode(NodeKind.Master);
                }

                await stream.WriteAsync(Encoding.UTF8.GetBytes(Answer(arguments, mode)), _stop.Token);
            }
        }
        catch (Exception error) when (error is IOException or OperationCanceledException or ObjectDisposedException or SocketException)
        {
        }
    }

    private async Task<string[]?> ReadCommandAsync(StreamReader reader)
    {
        string? header = await reader.ReadLineAsync(_stop.Token);

        if (header is null || header.Length < 2 || header[0] != '*')
        {
            return null;
        }

        var arguments = new string[int.Parse(header[1..])];

        for (int index = 0; index < arguments.Length; index++)
        {
            await reader.ReadLineAsync(_stop.Token);
            arguments[index] = await reader.ReadLineAsync(_stop.Token) ?? "";
        }

        return arguments;
    }

    private string Answer(string[] arguments, NodeMode mode)
    {
        switch (arguments[0].ToUpperInvariant())
        {
            case "ROLE":
                return mode.Kind switch
                {
                    NodeKind.Promoted => "*3\r\n$5\r\nslave\r\n$0\r\n\r\n:0\r\n",
                    NodeKind.Replica => mode.Primary == 0
                        ? "*3\r\n$5\r\nslave\r\n$0\r\n\r\n:0\r\n"
                        : $"*3\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:{mode.Primary}\r\n",
                    _ => "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n",
                };
            case "SENTINEL":
                return mode.Kind is NodeKind.Names
                    ? $"*2\r\n$9\r\n127.0.0.1\r\n${mode.Primary.ToString().Length}\r\n{mode.Primary}\r\n"
                    : "*-1\r\n";
            case "SET" or "INCR" or "DEL":
                return mode.Kind is NodeKind.Replica or NodeKind.Liar or NodeKind.Promoted
                    ? "-READONLY You can't write against a read only replica.\r\n"
                    : "+OK\r\n";
            case "NOPE":
                return "-ERR unknown command 'NOPE'\r\n";
            case "PING":
                return "+PONG\r\n";
            case "EXEC":
                return "*0\r\n";
            default:
                return "+OK\r\n";
        }
    }
}
