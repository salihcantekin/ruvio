using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Client.Tests;

/// <summary>An in-process RESP server that parses whole commands so tests can script exact replies.</summary>
internal sealed class FakeServer : IDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private Socket? _closed;

    public FakeServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    public int Port { get; }

    public async Task<FakeConn> AcceptAsync() => new(await TestDeadlines.AcceptAsync(_listener));

    public void StopListening() => _closed ??= ClosedPort.Stop(_listener);

    public void Dispose()
    {
        _listener.Stop();
        _closed?.Dispose();
    }

    public RuvioClientOptions Options(Action<RuvioClientOptions>? configure = null)
    {
        var options = new RuvioClientOptions { Host = "127.0.0.1", Port = Port, DiscoverCluster = false };

        configure?.Invoke(options);

        return options;
    }
}

internal sealed class FakeConn(TcpClient client) : IDisposable
{
    private readonly NetworkStream _stream = client.GetStream();

    public async Task<string[]> ReadCommand()
    {
        string header = await Line();

        Assert.Equal('*', header[0]);
        var parts = new string[int.Parse(header[1..])];

        for (int i = 0; i < parts.Length; i++)
        {
            string size = await Line();

            Assert.Equal('$', size[0]);
            var buffer = new byte[int.Parse(size[1..]) + 2];

            await TestDeadlines.ReadExactlyAsync(_stream, buffer);
            parts[i] = Encoding.UTF8.GetString(buffer, 0, buffer.Length - 2);
        }

        return parts;
    }

    public async Task Expect(params string[] arguments) => Assert.Equal(arguments, await ReadCommand());

    public Task Reply(string resp) => _stream.WriteAsync(Encoding.UTF8.GetBytes(resp)).AsTask();

    public Task ReplyRaw(byte[] bytes) => _stream.WriteAsync(bytes).AsTask();

    public void Close() => client.Dispose();

    public void Dispose() => client.Dispose();

    private async Task<string> Line()
    {
        var text = new StringBuilder();
        var one = new byte[1];

        while (true)
        {
            await TestDeadlines.ReadExactlyAsync(_stream, one);

            if (one[0] == '\n')
            {
                return text.ToString().TrimEnd('\r');
            }

            text.Append((char)one[0]);
        }
    }
}

public class TransportEdgeTests
{
    [Fact(Timeout = 15000)]
    public async Task OptionsAreValidatedBeforeAnySocketOpens()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await RuvioConnection.ConnectAsync((RuvioClientOptions)null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await RuvioConnection.ConnectAsync((IPAddress)null!, 1));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await RuvioConnection.ConnectAsync((IPEndPoint)null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => await RuvioConnection.ConnectAsync(new RuvioClientOptions { Host = " " }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await RuvioConnection.ConnectAsync(new RuvioClientOptions { Host = "" }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await RuvioConnection.ConnectAsync(new RuvioClientOptions { Port = 0 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await RuvioConnection.ConnectAsync(new RuvioClientOptions { Port = 65536 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await RuvioConnection.ConnectAsync(new RuvioClientOptions { Database = -1 }));

        using var server = new FakeServer();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await RuvioConnection.ConnectAsync(server.Options(o => o.MaxInFlightRequests = 0)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await RuvioConnection.ConnectAsync(server.Options(o => o.MaxInFlightRequests = -3)));
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectingToAClosedPortOrWithACancelledTokenFails()
    {
        int dead;

        using (var probe = new FakeServer())
        {
            dead = probe.Port;
        }

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await RuvioConnection.ConnectAsync(new RuvioClientOptions { Port = dead, DiscoverCluster = false }));

        using var server = new FakeServer();
        using var cancelled = new CancellationTokenSource();

        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await RuvioConnection.ConnectAsync(server.Options(), cancelled.Token));
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectOverloadsForwardHostPortDatabaseAndEndpoint()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> viaEndpoint = RuvioConnection.ConnectAsync(new IPEndPoint(IPAddress.Loopback, server.Port), database: 3);
        using FakeConn conn = await server.AcceptAsync();

        await conn.Expect("CLUSTER", "SLOTS");
        await conn.Reply("-ERR This instance has cluster support disabled\r\n");
        await conn.Expect("SELECT", "3");
        await conn.Reply("+OK\r\n");
        await using RuvioConnection connection = await viaEndpoint;

        Assert.Equal(3, connection.DefaultDatabase);
        Assert.Equal("127.0.0.1", connection.Host);
        Assert.Equal(server.Port, connection.Port);
        Assert.False(connection.IsCluster);
        Assert.Equal(1, connection.ShardCount);
        Assert.True(connection.IsConnected);
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        Assert.Equal(3, client.Database);
        Assert.Same(client, await connection.GetDatabaseAsync(3));
        Assert.Equal("127.0.0.1", client.Host);
        Assert.Equal(server.Port, client.Port);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await connection.GetDatabaseAsync(-1));
    }

    [Fact(Timeout = 15000)]
    public async Task AuthFailureClosesTheSocketAndSurfacesTheServerError()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o => o.Password = "bad"));
        using FakeConn conn = await server.AcceptAsync();

        await conn.Expect("AUTH", "bad");
        await conn.Reply("-WRONGPASS invalid password\r\n");
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await connecting);

        Assert.Contains("WRONGPASS", error.Message);
        Assert.True(error.IsServerReply);
    }

    [Fact(Timeout = 15000)]
    public async Task AuthWithAUserNameSendsBothAndEmitsPhaseTelemetry()
    {
        var activities = new List<string>();
        var measurements = new List<string>();
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Ruvio.Client",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => { lock (activities) { activities.Add(activity.OperationName); } },
        };

        ActivitySource.AddActivityListener(activityListener);
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Ruvio.Client")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<double>((instrument, _, tags, _) =>
        {
            lock (measurements)
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key == "phase")
                    {
                        measurements.Add((string)tag.Value!);
                    }
                }
            }
        });
        // On .NET 8, MeterListener.Dispose unsubscribes from the instruments only when this callback is set. Without it
        // the listener kept recording every later command of the test run into the list above, and the allocation tests
        // saw that list grow.
        meterListener.MeasurementsCompleted = (_, _) => { };
        meterListener.Start();

        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o =>
        {
            o.Username = "alice";
            o.Password = "secret";
        }));
        using FakeConn conn = await server.AcceptAsync();

        await conn.Expect("AUTH", "alice", "secret");
        await conn.Reply("+OK\r\n");
        await using RuvioConnection connection = await connecting;
        meterListener.RecordObservableInstruments();

        lock (activities)
        {
            Assert.Contains("socket.queue", activities);
            Assert.Contains("socket.write", activities);
            Assert.Contains("socket.reply", activities);
        }

        lock (measurements)
        {
            Assert.Contains("queue", measurements);
            Assert.Contains("write", measurements);
            Assert.Contains("reply", measurements);
        }

        // No other test listens to these instruments, so they must be off once this listener is gone.
        meterListener.Dispose();
        Assert.False(RuvioSocket.PhaseMetricsEnabled, "the disposed listener still records socket phases");
    }

    [Fact(Timeout = 15000)]
    public async Task ConnectionLossFailsPendingAndLaterCommandsAndReportsEvents()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o => o.Reconnect = false));
        using FakeConn conn = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        var failures = new List<Exception>();
        var lost = new List<RuvioConnectionNotice>();

        connection.ConnectionFailed += (_, e) => failures.Add(e);
        connection.ConnectionLost += (_, e) => lost.Add(e);

        ValueTask<string> ping = client.PingAsync();

        await conn.Expect("PING");
        conn.Close();
        await Assert.ThrowsAsync<RuvioException>(async () => await ping);
        Assert.False(connection.IsConnected);
        RuvioException again = await Assert.ThrowsAsync<RuvioException>(async () => await client.PingAsync());

        Assert.Contains("closed", again.Message);
        await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync("GET", "k"));
        await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteManyAsync([["PING"], ["PING"]]));
        await Assert.ThrowsAsync<RuvioException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray(), "k"u8.ToArray() }));
    }

    [Fact(Timeout = 15000)]
    public async Task GarbageFromTheServerBreaksTheConnectionAndManualReconnectRestoresIt()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o => o.Reconnect = false));
        using FakeConn first = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        var restored = new List<RuvioConnectionNotice>();

        connection.ConnectionRestored += (_, e) => restored.Add(e);
        ValueTask<string> ping = client.PingAsync();

        await first.Expect("PING");
        await first.Reply("!garbage\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await ping);
        Assert.False(connection.IsConnected);

        Task reconnect = connection.ReconnectAsync();
        using FakeConn second = await server.AcceptAsync();

        await reconnect;
        Assert.True(connection.IsConnected);
        ValueTask<string> pong = client.PingAsync();

        await second.Expect("PING");
        await second.Reply("+PONG\r\n");
        Assert.Equal("PONG", await pong);

        // Reconnect with nothing broken is a no-op.
        await connection.ReconnectAsync();
    }

    [Fact(Timeout = 15000)]
    public async Task ReconnectGivesUpAfterTheConfiguredAttemptsAndHonoursACustomDelay()
    {
        using var server = new FakeServer();
        int attempts = 0;
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o =>
        {
            o.MaxReconnectAttempts = 3;
            o.ReconnectBaseDelay = TimeSpan.FromMilliseconds(1);
            o.ReconnectMaxDelay = TimeSpan.FromMilliseconds(2);
        }));
        FakeConn first = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        first.Close();
        server.StopListening();
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        Assert.False(connection.IsConnected);
        await Assert.ThrowsAnyAsync<Exception>(async () => await client.PingAsync());
        await Assert.ThrowsAnyAsync<Exception>(async () => await connection.ReconnectAsync());

        // A custom delay that returns null stops at once.
        using var other = new FakeServer();
        Task<RuvioConnection> second = RuvioConnection.ConnectAsync(other.Options(o => o.ReconnectDelay = _ =>
        {
            attempts++;

            return null;
        }));
        FakeConn conn = await other.AcceptAsync();
        await using RuvioConnection secondConnection = await second;
        RuvioClient secondClient = await secondConnection.GetDefaultDatabaseAsync();

        conn.Close();
        other.StopListening();
        await Assert.ThrowsAnyAsync<Exception>(async () => await secondClient.PingAsync());
        await Assert.ThrowsAnyAsync<Exception>(async () => await secondClient.PingAsync());
        Assert.True(attempts >= 1);
    }

    [Fact(Timeout = 15000)]
    public async Task CancellationAndSaturationAreReportedWithoutBreakingTheConnection()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options(o => o.MaxInFlightRequests = 1));
        using FakeConn conn = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        using var already = new CancellationTokenSource();

        already.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.PingAsync(already.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.GetStringAsync("k", already.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray(), "k"u8.ToArray() }, already.Token));

        // The only slot is taken by an unanswered command, so a second caller waits and its cancellation frees it.
        ValueTask<string> first = client.PingAsync();

        await conn.Expect("PING");
        using var cancel = new CancellationTokenSource(100);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await client.PingAsync(cancel.Token));
        await conn.Reply("+PONG\r\n");
        Assert.Equal("PONG", await first);

        ValueTask<string> after = client.PingAsync();

        await conn.Expect("PING");
        await conn.Reply("+PONG\r\n");
        Assert.Equal("PONG", await after);
    }

    [Fact(Timeout = 15000)]
    public async Task DisposedClientsAndConnectionsRefuseWork()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options());
        using FakeConn conn = await server.AcceptAsync();
        RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        await connection.DisposeAsync();
        connection.Dispose();
        Assert.False(connection.IsConnected);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await client.PingAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.GetDatabaseAsync(0));
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await connection.ReconnectAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "GET"u8.ToArray() }));
    }

    [Fact(Timeout = 15000)]
    public async Task ClientArgumentValidationRejectsBadInput()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options());
        using FakeConn conn = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await client.ExpireAsync("k", TimeSpan.FromSeconds(-1)));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.DeleteAsync(Array.Empty<string>()));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.ExecuteAsync(Array.Empty<string>()));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ExecuteBinaryAsync(null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.ExecuteBinaryAsync(Array.Empty<ReadOnlyMemory<byte>>()));
        await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync("SELECT", "1"));
        await Assert.ThrowsAsync<RuvioException>(async () => await client.ExecuteAsync("select", "1"));
        await Assert.ThrowsAsync<RuvioException>(async () =>
            await client.ExecuteBinaryAsync(new ReadOnlyMemory<byte>[] { "SELECT"u8.ToArray(), "1"u8.ToArray() }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await client.SetAsync("k", "v", TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentNullException>(() => RuvioClient.GetHashSlot(null!));
    }

    [Fact(Timeout = 15000)]
    public async Task ExpireChoosesSecondsOrMillisecondsAndMapsReplies()
    {
        using var server = new FakeServer();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(server.Options());
        using FakeConn conn = await server.AcceptAsync();
        await using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();

        ValueTask<bool> whole = client.ExpireAsync("k", TimeSpan.FromSeconds(5));
        await conn.Expect("EXPIRE", "k", "5");
        await conn.Reply(":1\r\n");
        Assert.True(await whole);

        ValueTask<bool> fraction = client.ExpireAsync("k", TimeSpan.FromMilliseconds(1500.2));
        await conn.Expect("PEXPIRE", "k", "1501");
        await conn.Reply(":0\r\n");
        Assert.False(await fraction);

        ValueTask<bool> weird = client.ExpireAsync("k", TimeSpan.FromSeconds(1));
        await conn.Expect("EXPIRE", "k", "1");
        await conn.Reply("+OK\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await weird);

        ValueTask<long> del = client.DeleteAsync("k");
        await conn.Expect("DEL", "k");
        await conn.Reply("+OK\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await del);

        ValueTask<long> incr = client.IncrementAsync("n");
        await conn.Expect("INCR", "n");
        await conn.Reply("$1\r\n1\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await incr);

        ValueTask<string> ping = client.PingAsync();
        await conn.Expect("PING");
        await conn.Reply("*2\r\n$4\r\npong\r\n$2\r\nhi\r\n");
        Assert.Equal("hi", await ping);

        ValueTask<string> nullPing = client.PingAsync();
        await conn.Expect("PING");
        await conn.Reply("$-1\r\n");
        await Assert.ThrowsAsync<RuvioException>(async () => await nullPing);
    }
}
