using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;

namespace Ruvio.Client;

/// <summary>One TCP socket with its reader. Replies are returned as values; errors do not throw.</summary>
internal sealed class RuvioSocket : IDisposable
{
    private static readonly ActivitySource Activities = new("Ruvio.Client");
    private static readonly Meter Meter = new("Ruvio.Client");
    private static readonly Histogram<double> SocketPhaseDuration = Meter.CreateHistogram<double>(
        "ruvio_client_socket_phase_duration_milliseconds", "ms");
    private static readonly Histogram<double> CommandDuration = Meter.CreateHistogram<double>(
        "ruvio_client_command_duration_milliseconds", "ms");
    private static readonly Counter<long> SocketWrites = Meter.CreateCounter<long>(
        "ruvio_client_socket_write_calls");
    private static readonly Counter<long> SocketWrittenBytes = Meter.CreateCounter<long>(
        "ruvio_client_socket_written_bytes", "By");
    private readonly TcpClient _tcp;
    private readonly Stream _stream;
    private readonly bool _dedicatedReader;
    private readonly RespReader _reader;
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly SemaphoreSlim _read = new(1, 1);
    private bool _independentIo;
    private readonly object _multiplexerGate = new();
    private readonly int _maxInFlight;
    private readonly TimeSpan _commandTimeout;
    private CommandMultiplexer? _multiplexer;
    private bool _initialized;

    private volatile bool _broken;
    private volatile bool _disposed;

    private RuvioSocket(TcpClient tcp, Stream? secure, string host, int port, int maxInFlight, bool dedicatedReader, TimeSpan commandTimeout)
    {
        _commandTimeout = commandTimeout;
        _tcp = tcp;
        _dedicatedReader = dedicatedReader;
        _stream = secure ?? (dedicatedReader ? new BlockingSocketStream(tcp.Client) : tcp.GetStream());
        _reader = new RespReader(_stream);
        Host = host;
        Port = port;
        _maxInFlight = maxInFlight;
    }

    public string Host { get; }

    public int Port { get; }

    public bool IsBroken => _broken || _disposed || _multiplexer?.IsBroken == true;

    /// <summary>Connects, then sends <c>AUTH</c> and <c>CLIENT DURABILITY</c> when the options ask for them. <c>SELECT</c> is left to the caller.</summary>
    // subscriber: a Pub/Sub socket, which keeps async I/O whatever the options say.
    public static async Task<RuvioSocket> OpenAsync(
        string host,
        int port,
        RuvioClientOptions options,
        CancellationToken cancellationToken,
        bool subscriber = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxInFlightRequests);
        
        var tcp = new TcpClient { NoDelay = true };
        bool dedicatedReader = options.DedicatedReaderThread && !subscriber && options.Tls is null;
        using var connect = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connect.CancelAfter(options.ConnectTimeout);

        Stream? secure = null;

        try
        {
            if (dedicatedReader)
            {
                // A blocking connect keeps the socket in blocking mode; it runs on a pool thread so
                // the timeout and the caller's token still apply (disposing the client aborts it).
                using CancellationTokenRegistration abort = connect.Token.Register(tcp.Dispose);
                await Task.Run(() => tcp.Connect(host, port), CancellationToken.None).ConfigureAwait(false);
                connect.Token.ThrowIfCancellationRequested();
            }
            else
            {
                await tcp.ConnectAsync(host, port, connect.Token);
            }

            if (options.Tls is { } tls)
            {
                secure = await AuthenticateAsync(tcp, host, tls, connect.Token);
            }
        }
        catch (Exception error) when (dedicatedReader && connect.Token.IsCancellationRequested && error is not OperationCanceledException)
        {
            tcp.Dispose();
            throw new OperationCanceledException(connect.Token);
        }
        catch
        {
            secure?.Dispose();
            tcp.Dispose();
            throw;
        }

        var connection = new RuvioSocket(tcp, secure, host, port, options.MaxInFlightRequests, dedicatedReader, options.CommandTimeout);

        if (options.Password is null && options.DurabilityReplicas <= 0)
        {
            connection._initialized = true;

            return connection;
        }

        try
        {
            if (options.Password is not null)
            {
                IReadOnlyList<string> auth = options.Username is null
                    ? ["AUTH", options.Password]
                    : ["AUTH", options.Username, options.Password];

                await HandshakeAsync(connection, auth, dedicatedReader, cancellationToken);
            }

            if (options.DurabilityReplicas > 0)
            {
                await HandshakeAsync(connection, DurabilityCommand(options), dedicatedReader, cancellationToken);
            }
        }
        catch (Exception) when (dedicatedReader && cancellationToken.IsCancellationRequested)
        {
            connection.Dispose();
            throw new OperationCanceledException(cancellationToken);
        }
        catch
        {
            connection.Dispose();
            throw;
        }

        connection._initialized = true;

        return connection;
    }

    /// <summary>Wraps the connected socket in TLS and finishes the handshake, so a refused certificate fails the connect.</summary>
    private static async Task<SslStream> AuthenticateAsync(TcpClient tcp, string host, RuvioTlsOptions tls, CancellationToken cancellationToken)
    {
        var secure = new SslStream(tcp.GetStream(), leaveInnerStreamOpen: false);
        var authentication = new SslClientAuthenticationOptions
        {
            TargetHost = tls.ServerName ?? host,
            ClientCertificates = tls.ClientCertificates,
        };

        if (tls.InsecureSkipVerify)
        {
            authentication.RemoteCertificateValidationCallback = static (_, _, _, _) => true;
        }
        else if (tls.TrustedCertificates is { Count: > 0 } roots)
        {
            var policy = new X509ChainPolicy
            {
                TrustMode = X509ChainTrustMode.CustomRootTrust,
                RevocationMode = X509RevocationMode.NoCheck,
            };

            policy.CustomTrustStore.AddRange(roots);
            authentication.CertificateChainPolicy = policy;
        }

        try
        {
            await secure.AuthenticateAsClientAsync(authentication, cancellationToken);
        }
        catch
        {
            secure.Dispose();
            throw;
        }

        return secure;
    }

    /// <summary>The <c>CLIENT DURABILITY</c> command the options ask for, with <c>TIMEOUT</c> only when one is set.</summary>
    private static IReadOnlyList<string> DurabilityCommand(RuvioClientOptions options)
    {
        string replicas = options.DurabilityReplicas.ToString(CultureInfo.InvariantCulture);

        if (options.DurabilityTimeout is not { } timeout)
        {
            return ["CLIENT", "DURABILITY", "REPLICAS", replicas];
        }

        return ["CLIENT", "DURABILITY", "REPLICAS", replicas, "TIMEOUT", ((long)timeout.TotalMilliseconds).ToString(CultureInfo.InvariantCulture)];
    }

    private static async Task HandshakeAsync(
        RuvioSocket connection,
        IReadOnlyList<string> command,
        bool dedicatedReader,
        CancellationToken cancellationToken)
    {
        if (dedicatedReader)
        {
            // The blocking stream ignores tokens: run the command off the caller's thread, and let the
            // token close the socket so a server that never answers cannot hang the connect.
            using CancellationTokenRegistration abort = cancellationToken.Register(connection.Dispose);
            ThrowOnError(await Task.Run(() => connection.SendAsync(command, CancellationToken.None).AsTask(), CancellationToken.None).ConfigureAwait(false));
            cancellationToken.ThrowIfCancellationRequested();
        }
        else
        {
            ThrowOnError(await connection.SendAsync(command, cancellationToken));
        }
    }

    /// <summary>
    /// Lets a subscriber pump read while another thread writes <c>SUBSCRIBE</c>.
    /// Command sockets must not enable this: replies would interleave.
    /// </summary>
    public void EnableIndependentIo()
    {
        _independentIo = true;
    }

    public async Task WriteAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        await _write.WaitAsync(cancellationToken);

        try
        {
            ThrowIfClosed();

            await _stream.WriteAsync(payload, cancellationToken);
            RecordSocketWrite(payload.Length);
            await _stream.FlushAsync(cancellationToken);
        }
        catch (Exception exception) when (exception is not RuvioException and not OperationCanceledException)
        {
            _broken = true;

            throw new RuvioException(exception.Message, isServerReply: false, exception, closedBeforeReply: !_reader.ReplyStarted);
        }
        catch
        {
            _broken = true;

            throw;
        }
        finally
        {
            _write.Release();
        }
    }

    public ValueTask<RespValue> SendAsync(IReadOnlyList<string> arguments, CancellationToken cancellationToken) =>
        SendAsync(new RespCommand(arguments), cancellationToken);

    internal ValueTask<RespValue> SendAsync(in RespCommand command, CancellationToken cancellationToken)
    {
        if (_initialized && !_independentIo)
        {
            CommandMultiplexer? multiplexer = TryMultiplexer(out Exception? error);

            return multiplexer is null
                ? ValueTask.FromException<RespValue>(error!)
                : multiplexer.SendCommandAsync(command, cancellationToken);
        }

        return First(SendManyAsync(command.EncodePooled(), 1, cancellationToken));
    }

    private static async ValueTask<RespValue> First(Task<IReadOnlyList<RespValue>> replies) =>
        (await replies.ConfigureAwait(false))[0];

    public Task<IReadOnlyList<RespValue>> SendManyAsync(
        IReadOnlyList<IReadOnlyList<string>> commands,
        CancellationToken cancellationToken)
    {
        return _initialized && !_independentIo
            ? Multiplexer.SendAsync(commands, cancellationToken).AsTask()
            : SendManyAsync(RespCodec.EncodeManyPooled(commands), commands.Count, cancellationToken);
    }

    internal ValueTask<RespValue> SendBinaryAsync(
        IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken cancellationToken)
    {
        if (_initialized && !_independentIo)
        {
            CommandMultiplexer? multiplexer = TryMultiplexer(out Exception? error);

            return multiplexer is null
                ? ValueTask.FromException<RespValue>(error!)
                : multiplexer.SendAsync(arguments, cancellationToken);
        }

        return First(SendManyAsync(RespCodec.Encode(arguments), 1, cancellationToken));
    }

    public async Task<IReadOnlyList<RespValue>> SendManyAsync(
        byte[] payload,
        int replies,
        CancellationToken cancellationToken)
    {
        return await SendManyAsync(payload.AsMemory(), replies, cancellationToken);
    }

    private async Task<IReadOnlyList<RespValue>> SendManyAsync(
        PooledFrame frame,
        int replies,
        CancellationToken cancellationToken)
    {
        try
        {
            return await SendManyAsync(frame.Buffer.AsMemory(0, frame.Length), replies, cancellationToken);
        }
        finally
        {
            frame.Dispose();
        }
    }

    /// <summary>Queues an encoded pipeline; on the multiplexed path no task is allocated for the wait.</summary>
    internal ValueTask<IReadOnlyList<RespValue>> SendFrameAsync(
        ReadOnlyMemory<byte> payload,
        int replies,
        CancellationToken cancellationToken)
    {
        return _initialized && !_independentIo
            ? Multiplexer.SendAsync(payload, replies, cancellationToken)
            : new ValueTask<IReadOnlyList<RespValue>>(SendManyAsync(payload, replies, cancellationToken));
    }

    internal async Task<IReadOnlyList<RespValue>> SendManyAsync(
        ReadOnlyMemory<byte> payload,
        int replies,
        CancellationToken cancellationToken)
    {
        if (_initialized && !_independentIo)
            return await Multiplexer.SendAsync(payload, replies, cancellationToken).ConfigureAwait(false);

        _reader.PrepareReply();

        if (_independentIo)
        {
            await WriteAsync(payload, cancellationToken);
            var independent = new RespValue[replies];

            for (int index = 0; index < replies; index++)
            {
                independent[index] = await ReadAsync(cancellationToken);
            }

            return independent;
        }

        long queueStarted = Stopwatch.GetTimestamp();

        try
        {
            using var queueActivity = Activities.StartActivity("socket.queue");
            queueActivity?.SetTag("db.system", "ruvio");
            await _write.WaitAsync(cancellationToken);
        }
        finally
        {
            RecordSocketPhase("queue", queueStarted);
        }

        try
        {
            ThrowIfClosed();

            long writeStarted = Stopwatch.GetTimestamp();
            using (var writeActivity = Activities.StartActivity("socket.write"))
            {
                writeActivity?.SetTag("db.system", "ruvio");
                try
                {
                    await _stream.WriteAsync(payload, cancellationToken);
                    await _stream.FlushAsync(cancellationToken);
                }
                finally
                {
                    RecordSocketPhase("write", writeStarted);
                }
            }

            var values = new RespValue[replies];

            long replyStarted = Stopwatch.GetTimestamp();
            using (var replyActivity = Activities.StartActivity("socket.reply"))
            {
                replyActivity?.SetTag("db.system", "ruvio");

                try
                {
                    for (int index = 0; index < replies; index++)
                    {
                        values[index] = await _reader.ReadAsync(cancellationToken);
                    }
                }
                finally
                {
                    RecordSocketPhase("reply", replyStarted);
                }
            }

            return values;
        }

        catch (Exception exception) when (exception is not RuvioException and not OperationCanceledException)
        {
            _broken = true;

            throw new RuvioException(exception.Message, isServerReply: false, exception, closedBeforeReply: !_reader.ReplyStarted);
        }
        catch
        {
            _broken = true;

            throw;
        }
        finally
        {
            _write.Release();
        }
    }

    private CommandMultiplexer? TryMultiplexer(out Exception? error)
    {
        try
        {
            error = null;

            return Multiplexer;
        }
        catch (Exception exception)
        {
            error = exception;

            return null;
        }
    }

    private CommandMultiplexer Multiplexer
    {
        get
        {
            ThrowIfClosed();
            CommandMultiplexer? existing = Volatile.Read(ref _multiplexer);

            if (existing is not null)
                return existing;

            lock (_multiplexerGate)
            {
                ThrowIfClosed();
                existing = _multiplexer;

                if (existing is not null)
                    return existing;

                existing = new CommandMultiplexer(_stream, _reader, _maxInFlight, coalesceWrites: true, dedicatedReader: _dedicatedReader, commandTimeout: _commandTimeout);
                Volatile.Write(ref _multiplexer, existing);

                return existing;
            }
        }
    }

    /// <summary>Reads one value without writing, for subscription pushes.</summary>
    public async Task<RespValue> ReadAsync(CancellationToken cancellationToken)
    {
        SemaphoreSlim gate = _independentIo ? _read : _write;

        await gate.WaitAsync(cancellationToken);

        try
        {
            ThrowIfClosed();

            return await _reader.ReadAsync(cancellationToken);
        }
        catch (Exception exception) when (exception is not RuvioException and not OperationCanceledException)
        {
            _broken = true;

            throw new RuvioException(exception.Message, isServerReply: false, exception);
        }
        catch
        {
            _broken = true;

            throw;
        }
        finally
        {
            gate.Release();
        }
    }

    public static RespValue ThrowOnError(RespValue reply)
    {
        if (reply.Kind is RespKind.Error)
        {
            throw new RuvioException(reply.Text ?? "ERR", isServerReply: true);
        }

        return reply;
    }

    private void ThrowIfClosed()
    {
        if (_disposed || _broken)
        {
            throw new RuvioException("connection is closed");
        }
    }

    internal static bool TracingEnabled => Activities.HasListeners();
    internal static bool PhaseMetricsEnabled => SocketPhaseDuration.Enabled;

    /// <summary>True when a listener wants command spans or the command duration; otherwise commands skip both.</summary>
    internal static bool CommandTelemetryEnabled => Activities.HasListeners() || CommandDuration.Enabled;

    /// <summary>A client span for one command, named after it. Keys and values are never recorded.</summary>
    internal static Activity? StartCommand(string? name, int database)
    {
        Activity? activity = Activities.StartActivity(name ?? "ruvio", ActivityKind.Client);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("db.system", "ruvio");
            activity.SetTag("db.operation.name", name);
            activity.SetTag("db.namespace", database.ToString(CultureInfo.InvariantCulture));
        }

        return activity;
    }

    internal static void EndCommand(Activity? activity, string? name, long started, Exception? failure)
    {
        if (failure is not null && activity is not null)
        {
            activity.SetStatus(ActivityStatusCode.Error, failure.Message);
            activity.SetTag("error.type", failure.GetType().FullName);
        }

        if (!CommandDuration.Enabled)
            return;

        var tags = new TagList
        {
            { "db.system", "ruvio" },
            { "db.operation.name", name },
            { "outcome", failure is null ? "ok" : "error" },
        };

        CommandDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);
    }

    internal static void RecordSocketWrite(int bytes)
    {
        SocketWrites.Add(1);
        SocketWrittenBytes.Add(bytes);
    }

    internal static Activity? StartPhase(string phase, Activity? parentActivity, long startedTimestamp = 0)
    {
        if (!Activities.HasListeners())
            return null;

        ActivityContext parent = parentActivity?.Context ?? default;
        DateTimeOffset start = startedTimestamp == 0
            ? default
            : DateTimeOffset.UtcNow - Stopwatch.GetElapsedTime(startedTimestamp);
        string name = phase switch
        {
            "queue" => "socket.queue",
            "write" => "socket.write",
            "reply" => "socket.reply",
            _ => throw new ArgumentOutOfRangeException(nameof(phase)),
        };

        Activity? activity = Activities.StartActivity(name, ActivityKind.Internal, parent, startTime: start);
        activity?.SetTag("db.system", "ruvio");

        return activity;
    }

    internal static void RecordSocketPhase(string phase, long started)
    {
        if (!SocketPhaseDuration.Enabled) 
            return;

        var tags = new TagList
        {
            { "backend", "ruvio" },
            { "phase", phase }
        };

        SocketPhaseDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);
    }

    public void Dispose()
    {
        lock (_multiplexerGate)
        {
            if (_disposed) 
                return;
            
            _disposed = true;
            _broken = true;
            
            if (_multiplexer is not null)
                _ = _multiplexer.StopAsync();
            else
                _reader.Dispose();
            
            _stream.Dispose();
            _tcp.Dispose();
        }
    }
}
