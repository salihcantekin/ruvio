using Ruvio.Client;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Ruvio.Client.Tests;

public class MultiplexingTests
{
    [Theory(Timeout = 15000)]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    public async Task ReadyQueueBatchesPreserveFifoSkipCancellationAndFaultAllOnWriteFailure(
        bool fail, bool large, bool coalesce)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using var stream = new GatedWriteStream(tcp.GetStream()) { FailSecondWrite = fail };
        var mux = coalesce
            ? new CommandMultiplexer(stream, new RespReader(stream), 32, coalesceWrites: true)
            : new CommandMultiplexer(stream, new RespReader(stream), 32, coalesceWrites: false);
        try
        {
            Task<RespValue> first = mux.SendCommandAsync(new RespCommand("ECHO", "first"), CancellationToken.None).AsTask();
            await stream.Started.Task;
            using var cancellation = new CancellationTokenSource();
            Task<RespValue> cancelled = mux.SendCommandAsync(new RespCommand("ECHO", "skip"), cancellation.Token).AsTask();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
            Task<RespValue>? invalid = fail ? null : mux.SendCommandAsync(
                new RespCommand("ECHO", "\ud800"), CancellationToken.None).AsTask();
            string Value(int i) => large && i == 0 ? new string('x', 70 * 1024) : i.ToString();
            Task<RespValue>[] queued = Enumerable.Range(0, 20).Select(i =>
                mux.SendCommandAsync(new RespCommand("ECHO", Value(i)), CancellationToken.None).AsTask()).ToArray();
            stream.Resume.TrySetResult();

            if (fail)
            {
                foreach (Task<RespValue> task in queued.Prepend(first))
                    await Assert.ThrowsAsync<RuvioException>(() => task);

                Assert.Equal(2, stream.Writes);
            }
            else
            {
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                using var reader = new RespReader(server.GetStream());

                for (int i = -1; i < 20; i++)
                {
                    RespValue frame = await reader.ReadAsync(deadline.Token);
                    string expected = i < 0 ? "first" : Value(i);
                    Assert.Equal(expected, frame.Items![1].AsString());
                    await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes($"+{expected}\r\n"), deadline.Token);
                }

                Assert.Equal("first", (await first).AsString());
                await Assert.ThrowsAsync<EncoderFallbackException>(() => invalid!);

                for (int i = 0; i < queued.Length; i++)
                    Assert.Equal(Value(i), (await queued[i]).AsString());

                Assert.Equal(coalesce ? (large ? 4 : 3) : 21, stream.Writes);
            }
        }
        finally
        {
            stream.Resume.TrySetResult();
            await mux.StopAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task WriterHandoffsWithConcurrentProducersKeepFramesAndRepliesIntact()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Task<RuvioConnection> connecting = RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Port = ((IPEndPoint)listener.LocalEndpoint).Port,
            DiscoverCluster = false,
            Reconnect = false,
            MaxInFlightRequests = 8,
        });
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        using RuvioConnection connection = await connecting;
        RuvioClient client = await connection.GetDefaultDatabaseAsync();
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var reader = new RespReader(server.GetStream());
        Task responding = RespondAsync();
        async Task RespondAsync()
        {
            for (int i = 0; i < 1024; i++)
            {
                RespValue command = await reader.ReadAsync(deadline.Token);
                Assert.Equal("ECHO", command.Items![0].AsString());
                string value = command.Items[1].AsString()!;
                await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes($"+{value}\r\n"), deadline.Token);
            }
        }

        for (int wave = 0; wave < 32; wave++)
        {
            int batch = wave;
            await Task.WhenAll(Enumerable.Range(0, 32).Select(worker => Task.Run(async () =>
            {
                string value = $"{batch}:{worker}";
                Assert.Equal(value, (await client.ExecuteAsync(["ECHO", value], deadline.Token)).AsString());
            })));
        }

        await responding;
    }

    [Fact(Timeout = 15000)]
    public async Task CancellationDuringWriteDoesNotReuseCallersBinaryMemory()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using var stream = new GatedWriteStream(tcp.GetStream());
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 2);
        try
        {
            byte[] value = "original"u8.ToArray();
            byte[] expected = RespCodec.Encode(new ReadOnlyMemory<byte>[] { "ECHO"u8.ToArray(), value });
            using var cancel = new CancellationTokenSource();
            ValueTask<RespValue> first = mux.SendAsync(
                new ReadOnlyMemory<byte>[] { "ECHO"u8.ToArray(), value }, cancel.Token);
            Assert.True(stream.Started.Task.IsCompleted, "Idle writer should start on the caller's path.");
            await stream.Started.Task;
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await first);
            value.AsSpan().Fill((byte)'x');
            ValueTask<IReadOnlyList<RespValue>> next = mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "ECHO", "next" } }, CancellationToken.None);
            stream.Resume.TrySetResult();
            var received = new byte[expected.Length];
            await TestDeadlines.ReadExactlyAsync(server.GetStream(), received);
            Assert.Equal(expected, received);
            byte[] nextExpected = RespCodec.Encode(["ECHO", "next"]);
            received = new byte[nextExpected.Length];
            await TestDeadlines.ReadExactlyAsync(server.GetStream(), received);
            Assert.Equal(nextExpected, received);
            await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes("+original\r\n+next\r\n"));
            Assert.Equal("next", (await next)[0].AsString());
        }
        finally
        {
            stream.Resume.TrySetResult();
            await mux.StopAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task CommandTimeoutFailsACallWhoseReplyNeverComes()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using NetworkStream stream = tcp.GetStream();
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 2, commandTimeout: TimeSpan.FromMilliseconds(200));
        try
        {
            await Assert.ThrowsAsync<TimeoutException>(async () => await mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "GET", "k" } }, CancellationToken.None));
        }
        finally
        {
            await mux.StopAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task CommandTimeoutDropsTheLateReplyAndKeepsTheConnection()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using NetworkStream stream = tcp.GetStream();
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 4, commandTimeout: TimeSpan.FromMilliseconds(200));
        try
        {
            // The first caller reads the socket itself; the second waits behind it. Both run out.
            ValueTask<RespValue> first = mux.SendCommandAsync(new RespCommand("GET", "a"), CancellationToken.None);
            ValueTask<IReadOnlyList<RespValue>> second = mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "GET", "b" }, new[] { "GET", "c" } }, CancellationToken.None);
            await Assert.ThrowsAsync<TimeoutException>(async () => await first);
            await Assert.ThrowsAsync<TimeoutException>(async () => await second);
            Assert.False(mux.IsBroken);

            // The late replies are read and dropped; the next command gets its own reply.
            await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes("+late-a\r\n+late-b\r\n+late-c\r\n"));
            ValueTask<RespValue> third = mux.SendCommandAsync(new RespCommand("PING"), CancellationToken.None);
            await Task.Delay(50);
            await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
            Assert.Equal("PONG", (await third).AsString());
        }
        finally
        {
            await mux.StopAsync();
        }
    }

    [Fact]
    public void CommandTimeoutDefaultsToThirtySeconds()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), new RuvioClientOptions().CommandTimeout);
    }

    [Fact(Timeout = 15000)]
    public async Task CommandTimeoutAddsABlockingCommandsOwnWait()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using NetworkStream stream = tcp.GetStream();
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 2, commandTimeout: TimeSpan.FromMilliseconds(200));
        try
        {
            // An empty list: the server answers nil when BLPOP's 1 s runs out, well past the 200 ms limit.
            Task answer = Task.Run(async () =>
            {
                await Task.Delay(1000);
                await server.GetStream().WriteAsync(Encoding.ASCII.GetBytes("*-1\r\n"));
            });
            var started = System.Diagnostics.Stopwatch.StartNew();
            IReadOnlyList<RespValue> replies = await mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "BLPOP", "k", "1" } }, CancellationToken.None);
            Assert.Equal(RespKind.Null, replies[0].Kind);
            Assert.InRange(started.ElapsedMilliseconds, 900L, 3000L);
            await answer;

            // The plain limit still holds for the next command.
            await Assert.ThrowsAsync<TimeoutException>(async () => await mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "PING" } }, CancellationToken.None));
        }
        finally
        {
            await mux.StopAsync();
        }
    }

    [Fact]
    public void BlockingWaitTable()
    {
        const long forever = -1;
        var cases = new (string[] Args, long Millis)[]
        {
            (new[] { "GET", "k" }, 0),
            (new[] { "SET", "k", "v" }, 0),
            (new[] { "blpop", "a", "b", "1" }, 1000),
            (new[] { "BLPOP", "k", "0" }, forever),
            (new[] { "BLPOP", "k", "0.5" }, 500),
            (new[] { "BLPOP", "k", "soon" }, forever),
            (new[] { "BLPOP", "k", "-1" }, forever),
            (new[] { "BRPOP", "k", "2" }, 2000),
            (new[] { "BZPOPMIN", "k", "1.5" }, 1500),
            (new[] { "BZPOPMAX", "k", "3" }, 3000),
            (new[] { "BRPOPLPUSH", "a", "b", "4" }, 4000),
            (new[] { "BLMOVE", "a", "b", "LEFT", "RIGHT", "0.25" }, 250),
            (new[] { "BLMPOP", "2", "2", "a", "b", "LEFT" }, 2000),
            (new[] { "BZMPOP", "0", "1", "z", "MIN" }, forever),
            (new[] { "XREAD", "STREAMS", "s", "$" }, 0),
            (new[] { "XREAD", "COUNT", "1", "BLOCK", "700", "STREAMS", "s", "$" }, 700),
            (new[] { "XREAD", "BLOCK", "0", "STREAMS", "s", "$" }, forever),
            (new[] { "XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">" }, 0),
            (new[] { "XREADGROUP", "GROUP", "BLOCK", "c", "STREAMS", "s", ">" }, 0),
            (new[] { "XREADGROUP", "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">" }, 50),
            (new[] { "WAIT", "1", "100" }, 100),
            (new[] { "WAIT", "1", "0" }, forever),
            (new[] { "WAITAOF", "1", "0", "200" }, 200),
            (new[] { "FILLGET", "k", "1000" }, 0),
            (new[] { "FILLGET", "k", "1000", "WAIT", "300" }, 300),
            (new[] { "FILLGET", "k", "1000", "WAIT", "0" }, forever),
            (new[] { "MONITOR" }, forever),
            (new[] { "SUBSCRIBE", "news" }, forever),
            (new[] { "PSUBSCRIBE", "n*" }, forever),
            (new[] { "SSUBSCRIBE", "news" }, forever),
            (new[] { "PUBLISH", "news", "x" }, 0),
            (new[] { "BLPOP" }, forever),
        };

        var failures = new List<string>();

        foreach ((string[] args, long millis) in cases)
        {
            var raw = new ReadOnlyMemory<byte>[args.Length];

            for (int index = 0; index < args.Length; index++)
            {
                raw[index] = Encoding.UTF8.GetBytes(args[index]);
            }

            long[] got =
            [
                (long)BlockingWait.OfParts(args).TotalMilliseconds,
                (long)BlockingWait.OfCommand(new RespCommand(args)).TotalMilliseconds,
                (long)BlockingWait.OfPayload(RespCodec.Encode((IReadOnlyList<string>)args)).TotalMilliseconds,
                (long)BlockingWait.OfArguments(raw).TotalMilliseconds,
            ];

            foreach (long value in got)
            {
                if (value != millis)
                    failures.Add($"{string.Join(" ", args)}: got {value}, want {millis}");
            }
        }

        Assert.Empty(failures);

        // A pipeline takes its widest wait; any wait-forever wins.
        var pipeline = new IReadOnlyList<string>[] { new[] { "GET", "k" }, new[] { "BLPOP", "k", "2" }, new[] { "XREAD", "BLOCK", "100", "STREAMS", "s", "$" } };
        Assert.Equal(TimeSpan.FromSeconds(2), BlockingWait.OfPipeline(pipeline));
        Assert.Equal(BlockingWait.Forever, BlockingWait.OfPipeline(new IReadOnlyList<string>[] { new[] { "BLPOP", "k", "2" }, new[] { "BLPOP", "k", "0" } }));
        byte[] joined = RespCodec.Encode((IReadOnlyList<string>)new[] { "GET", "k" }).Concat(RespCodec.Encode((IReadOnlyList<string>)new[] { "BLPOP", "k", "2" })).ToArray();
        Assert.Equal(TimeSpan.FromSeconds(2), BlockingWait.OfPayload(joined));
        Assert.Equal(TimeSpan.Zero, BlockingWait.OfPayload(Encoding.ASCII.GetBytes("garbage")));
    }

    [Fact(Timeout = 15000)]
    public async Task CancelsQueuedWriteWithoutSendingOrEncodingIt()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using var stream = new GatedWriteStream(tcp.GetStream());
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 2);
        try
        {
            ValueTask<IReadOnlyList<RespValue>> first = mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "ECHO", "first" } }, CancellationToken.None);
            await stream.Started.Task;
            using var cancel = new CancellationTokenSource();
            ValueTask<RespValue> skipped = mux.SendCommandAsync(new RespCommand("ECHO", "never"), cancel.Token);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await skipped);
            ValueTask<IReadOnlyList<RespValue>> next = mux.SendAsync(
                new IReadOnlyList<string>[] { new[] { "ECHO", "next" } }, CancellationToken.None);
            stream.Resume.TrySetResult();
            byte[] expected = RespCodec.EncodeMany([["ECHO", "first"], ["ECHO", "next"]]);
            var received = new byte[expected.Length];
            await TestDeadlines.ReadExactlyAsync(server.GetStream(), received);
            Assert.Equal(expected, received);
            await server.GetStream().WriteAsync("+first\r\n+next\r\n"u8.ToArray());
            Assert.Equal("first", (await first)[0].AsString());
            Assert.Equal("next", (await next)[0].AsString());
        }
        finally
        {
            stream.Resume.TrySetResult();
            await mux.StopAsync();
        }
    }

    [Fact(Timeout = 15000)]
    public async Task StopDuringDirectWriteFaultsQueueAndCapacityWaiters()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var tcp = new TcpClient { NoDelay = true };
        Task connecting = tcp.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port);
        using TcpClient server = await TestDeadlines.AcceptAsync(listener);
        await connecting;
        using var stream = new GatedWriteStream(tcp.GetStream());
        var mux = new CommandMultiplexer(stream, new RespReader(stream), 2);
        ValueTask<IReadOnlyList<RespValue>>[] calls = Enumerable.Range(0, 32).Select(i =>
            mux.SendAsync(new IReadOnlyList<string>[] { new[] { "ECHO", i.ToString() } },
                CancellationToken.None)).ToArray();
        Assert.True(stream.Started.Task.IsCompleted);
        await mux.StopAsync();

        foreach (ValueTask<IReadOnlyList<RespValue>> call in calls)
            await Assert.ThrowsAsync<RuvioException>(async () => await call);
    }

    [Fact(Timeout = 15000)]
    public async Task SendsConcurrentCommandsBeforeAnyReplyAndMatchesFifo()
    {
        await using var session = await ScriptedSession.Open();
        Task<RespValue>[] calls = Enumerable.Range(0, 64)
            .Select(i => session.Client.ExecuteAsync("ECHO", i.ToString()).AsTask()).ToArray();

        for (int i = 0; i < calls.Length; i++)
            await session.Expect("ECHO", i.ToString());

        await session.Reply(string.Concat(Enumerable.Range(0, 64).Select(i => $"+{i}\r\n")));
        RespValue[] replies = await Task.WhenAll(calls);

        for (int i = 0; i < replies.Length; i++)
            Assert.Equal(i.ToString(), replies[i].AsString());
    }

    [Fact(Timeout = 15000)]
    public async Task PipelineRepliesAndServerErrorsDoNotShiftNextCall()
    {
        await using var session = await ScriptedSession.Open();
        ValueTask<IReadOnlyList<RespValue>> pipeline = session.Client.ExecuteManyAsync(
            [["ECHO", "a"], ["BAD"], ["ECHO", "b"]]);
        ValueTask<RespValue> next = session.Client.ExecuteAsync("ECHO", "c");
        await session.Expect("ECHO", "a");
        await session.Expect("BAD");
        await session.Expect("ECHO", "b");
        await session.Expect("ECHO", "c");
        await session.Reply("+a\r\n-ERR bad\r\n+b\r\n+c\r\n");
        IReadOnlyList<RespValue> replies = await pipeline;
        Assert.Equal("a", replies[0].AsString());
        Assert.Equal(RespKind.Error, replies[1].Kind);
        Assert.Equal("b", replies[2].AsString());
        Assert.Equal("c", (await next).AsString());
    }

    [Fact(Timeout = 15000)]
    public async Task CancellationAfterSendDrainsReplyAndKeepsSocketUsable()
    {
        await using var session = await ScriptedSession.Open(maxInFlight: 1);
        using var cancel = new CancellationTokenSource();
        ValueTask<RespValue> first = session.Client.ExecuteAsync(["ECHO", "a"], cancel.Token);
        await session.Expect("ECHO", "a");
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await first);
        ValueTask<RespValue> next = session.Client.ExecuteAsync("ECHO", "b");
        await session.Reply("+a\r\n");
        await session.Expect("ECHO", "b");
        await session.Reply("+b\r\n");
        Assert.Equal("b", (await next).AsString());
        Assert.True(session.Connection.IsConnected);
    }

    [Fact(Timeout = 15000)]
    public async Task CapacityWaitCanBeCancelledWithoutSendingAndOptionsAreCopied()
    {
        await using var session = await ScriptedSession.Open(maxInFlight: 1);
        ValueTask<RespValue> first = session.Client.ExecuteAsync("ECHO", "a");
        await session.Expect("ECHO", "a");
        using var cancel = new CancellationTokenSource();
        ValueTask<RespValue> cancelled = session.Client.ExecuteAsync(["ECHO", "never"], cancel.Token);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await cancelled);
        ValueTask<RespValue> next = session.Client.ExecuteAsync("ECHO", "b");
        await session.Reply("+a\r\n");
        await session.Expect("ECHO", "b");
        await session.Reply("+b\r\n");
        Assert.Equal("a", (await first).AsString());
        Assert.Equal("b", (await next).AsString());
    }

    [Fact(Timeout = 15000)]
    public async Task DisconnectFaultsAllPendingGroups()
    {
        await using var session = await ScriptedSession.Open();
        ValueTask<RespValue>[] calls = Enumerable.Range(0, 16)
            .Select(i => session.Client.ExecuteAsync("ECHO", i.ToString())).ToArray();

        for (int i = 0; i < calls.Length; i++)
            await session.Expect("ECHO", i.ToString());

        session.CloseCommandSocket();

        foreach (ValueTask<RespValue> call in calls)
            await Assert.ThrowsAsync<RuvioException>(async () => await call);
    }

    [Fact(Timeout = 15000)]
    public async Task ClosedBeforeReplyIsPerRequestNotSharedWithAPartialPreviousReply()
    {
        await using var session = await ScriptedSession.Open();
        ValueTask<string?> partial = session.Client.GetStringAsync("a");
        ValueTask<string?> unread = session.Client.GetStringAsync("b");
        await session.Expect("GET", "a");
        await session.Expect("GET", "b");
        await session.Reply("$3\r\nx");
        session.CloseCommandSocket();
        RuvioException error = await Assert.ThrowsAsync<RuvioException>(async () => await partial);
        Assert.False(error.ClosedBeforeReply);

        // The read whose reply never started fails too; it is not sent again.
        RuvioException closed = await Assert.ThrowsAsync<RuvioException>(async () => await unread);
        Assert.True(closed.ClosedBeforeReply);

        ValueTask<string?> next = session.Client.GetStringAsync("c");
        SubscriberPipe replacement = await session.AcceptDatabaseAsync();
        await replacement.Expect("GET", "c");
        await replacement.Reply("$-1\r\n");
        Assert.Null(await next);
    }

    [Fact(Timeout = 15000)]
    public async Task DisposalDoesNotReconnectPendingReads()
    {
        await using var session = await ScriptedSession.Open();
        ValueTask<string?> read = session.Client.GetStringAsync("a");
        await session.Expect("GET", "a");
        session.Connection.Dispose();
        await Assert.ThrowsAsync<RuvioException>(async () => await read);
    }

    [Fact(Timeout = 15000)]
    public async Task DisposalUnblocksPendingCallsAndAdmissionWaiters()
    {
        await using var session = await ScriptedSession.Open(maxInFlight: 1);
        ValueTask<RespValue> first = session.Client.ExecuteAsync("ECHO", "a");
        await session.Expect("ECHO", "a");
        ValueTask<RespValue> waiting = session.Client.ExecuteAsync("ECHO", "b");
        session.Connection.Dispose();
        await Assert.ThrowsAsync<RuvioException>(async () => await first);
        await Assert.ThrowsAsync<RuvioException>(async () => await waiting);
    }

    [Fact(Timeout = 15000)]
    public async Task InvalidUnsentCommandDoesNotBreakTransport()
    {
        await using var session = await ScriptedSession.Open();
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await session.Client.ExecuteAsync(Array.Empty<string>(), CancellationToken.None));
        ValueTask<string> ping = session.Client.PingAsync();
        await session.Expect("PING");
        await session.Reply("+PONG\r\n");
        Assert.Equal("PONG", await ping);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task InvalidCapacityIsRejectedBeforeConnect(int capacity)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await RuvioConnection.ConnectAsync(new RuvioClientOptions { MaxInFlightRequests = capacity }));
    }

    private sealed class GatedWriteStream(NetworkStream inner) : Stream
    {
        internal readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Writes;
        internal bool FailSecondWrite;
        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken token = default) =>
            inner.ReadAsync(buffer, token);
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken token = default)
        {
            int writes = Interlocked.Increment(ref Writes);
            Started.TrySetResult();
            await Resume.Task.WaitAsync(token);

            if (FailSecondWrite && writes == 2)
                throw new IOException("Injected batch write failure.");

            await inner.WriteAsync(buffer, token);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Resume.TrySetResult();
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
