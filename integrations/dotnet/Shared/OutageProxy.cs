using System.Net;
using System.Net.Sockets;

namespace Ruvio.Integrations.Tests;

/// <summary>
/// A TCP forwarder in front of the live test server that can be switched off and on, so a test takes Ruvio away
/// from a running application without touching the shared server. While it is down every open connection is
/// closed and each new one is accepted and closed at once, which a client sees as a server that went away.
/// </summary>
internal sealed class OutageProxy : IDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly string upstreamHost;
    private readonly int upstreamPort;
    private readonly object gate = new();
    private readonly List<Socket> open = [];
    private readonly CancellationTokenSource stop = new();
    private volatile bool down;
    private long bytesToServer;
    private long connections;

    public OutageProxy(string upstreamHost, int upstreamPort)
    {
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
        listener.Start();
        _ = AcceptLoop();
    }

    /// <summary>The proxy's own port; point the client here.</summary>
    public int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

    /// <summary>Bytes the clients have sent towards the server so far; unchanged while nothing calls Ruvio.</summary>
    public long BytesToServer => Interlocked.Read(ref bytesToServer);

    /// <summary>Connections accepted so far while the proxy was up.</summary>
    public long Connections => Interlocked.Read(ref connections);

    public static OutageProxy ForLiveServer()
    {
        string address = Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")!;
        int colon = address.LastIndexOf(':');

        return new OutageProxy(address[..colon], int.Parse(address[(colon + 1)..]));
    }

    /// <summary>Takes Ruvio away: closes every open connection and refuses the next ones.</summary>
    public void Cut()
    {
        down = true;

        lock (gate)
        {
            foreach (Socket socket in open)
                Close(socket);

            open.Clear();
        }
    }

    /// <summary>Brings Ruvio back for new connections.</summary>
    public void Restore() => down = false;

    public void Dispose()
    {
        stop.Cancel();
        listener.Stop();
        Cut();
    }

    private async Task AcceptLoop()
    {
        while (!stop.IsCancellationRequested)
        {
            Socket client;

            try
            {
                client = await listener.AcceptSocketAsync(stop.Token);
            }
            catch (Exception)
            {
                return;
            }

            if (down)
            {
                Close(client);

                continue;
            }

            _ = Pipe(client);
        }
    }

    private async Task Pipe(Socket client)
    {
        var upstream = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };

        client.NoDelay = true;

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);

            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            await upstream.ConnectAsync(upstreamHost, upstreamPort, timeout.Token);
        }
        catch (Exception)
        {
            Close(client);
            Close(upstream);

            return;
        }

        lock (gate)
        {
            if (down)
            {
                Close(client);
                Close(upstream);

                return;
            }

            open.Add(client);
            open.Add(upstream);
        }

        Interlocked.Increment(ref connections);
        await Task.WhenAny(Copy(client, upstream, counted: true), Copy(upstream, client, counted: false));
        Close(client);
        Close(upstream);
    }

    private async Task Copy(Socket from, Socket to, bool counted)
    {
        byte[] buffer = new byte[16 * 1024];

        try
        {
            while (true)
            {
                int read = await from.ReceiveAsync(buffer, SocketFlags.None, stop.Token);

                if (read == 0)
                    return;

                if (counted)
                    Interlocked.Add(ref bytesToServer, read);

                int sent = 0;

                while (sent < read)
                    sent += await to.SendAsync(buffer.AsMemory(sent, read - sent), SocketFlags.None, stop.Token);
            }
        }
        catch (Exception)
        {
            // A closed or cut socket ends the copy; the other direction is closed by the caller.
        }
    }

    private static void Close(Socket socket)
    {
        try
        {
            socket.Close(0);
        }
        catch (Exception)
        {
            // Already closed.
        }
    }
}

/// <summary>A clock that moves only when a test says so.</summary>
internal sealed class ManualTime : TimeProvider
{
    private long ticks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => Interlocked.Read(ref ticks);

    public void Advance(TimeSpan by) => Interlocked.Add(ref ticks, by.Ticks);
}
