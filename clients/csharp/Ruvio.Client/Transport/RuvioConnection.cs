using System.Collections.Concurrent;
using System.Net;

namespace Ruvio.Client;

/// <summary>
/// Owns the server connection and all database-scoped clients created from it.
/// Disposing this connection closes every child client and its sockets.
/// </summary>
/// <remarks>
/// Create one with <c>ConnectAsync</c> and keep it for the life of the application: it is thread-safe and
/// multiplexes all calls over its sockets. Commands live on the <see cref="IRuvioClient"/> that
/// <see cref="GetDefaultDatabaseAsync(CancellationToken)"/> or <see cref="GetDatabaseAsync(int, CancellationToken)"/> returns.
/// On connect the client sends <c>AUTH</c> and <c>SELECT</c> when configured and, unless
/// <see cref="RuvioClientOptions.DiscoverCluster"/> is off, <c>CLUSTER SLOTS</c> to find the shards.
/// </remarks>
/// <example>
/// <code>
/// await using RuvioConnection connection = await RuvioConnection.ConnectAsync("localhost", 6379);
/// IRuvioClient db = await connection.GetDefaultDatabaseAsync();
/// await db.SetAsync("hello", "world");
/// </code>
/// </example>
public sealed class RuvioConnection : IRuvioConnection
{
    private readonly RuvioClientOptions _options;
    private readonly SemaphoreSlim _databaseGate = new(1, 1);
    private readonly ConcurrentDictionary<int, RuvioClient> _clients = new();
    private int _disposed;

    private RuvioConnection(RuvioClientOptions options, RuvioClient initialClient)
    {
        _options = options;
        AddClient(initialClient);
    }

    /// <summary>The host the seed connection was opened to.</summary>
    public string Host => _options.Host;

    /// <summary>The RESP port the seed connection was opened to.</summary>
    public int Port => _options.Port;

    /// <summary>The logical database set in <see cref="RuvioClientOptions.Database"/> when the connection was opened.</summary>
    public int DefaultDatabase => _options.Database;

    /// <summary>
    /// True when the connection is not disposed and every database client created so far has a live command socket.
    /// A socket that dropped makes this false until it is reopened (automatically when
    /// <see cref="RuvioClientOptions.Reconnect"/> is on, or by <see cref="ReconnectAsync"/>).
    /// </summary>
    public bool IsConnected =>
        Volatile.Read(ref _disposed) == 0 && _clients.Values.All(client => client.IsConnected);

    /// <summary>True when <c>CLUSTER SLOTS</c> showed two or more shards, so the client routes each key by hash slot.</summary>
    public bool IsCluster => _clients.Values.FirstOrDefault()?.IsCluster ?? false;

    /// <summary>The number of shards the client routes to: 1 for a standalone server (0 once every client is disposed).</summary>
    public int ShardCount => _clients.Values.FirstOrDefault()?.ShardCount ?? 0;

    /// <summary>
    /// Raised when a command or subscriber socket fails and will not be retried, or the retry budget
    /// (<see cref="RuvioClientOptions.MaxReconnectAttempts"/>) ran out. The argument is the failure.
    /// </summary>
    public event EventHandler<Exception>? ConnectionFailed;

    /// <summary>Raised when a command or subscriber socket closes. The notice names the host, the port and whether it was the subscriber socket.</summary>
    public event EventHandler<RuvioConnectionNotice>? ConnectionLost;

    /// <summary>Raised after a replacement socket for a dropped one is open.</summary>
    public event EventHandler<RuvioConnectionNotice>? ConnectionRestored;

    /// <summary>Opens the connection and its initial database client with default options (database 0, no authentication).</summary>
    /// <param name="host">The server host name or address.</param>
    /// <param name="port">The RESP port, usually 6379.</param>
    /// <param name="cancellationToken">Abandons the connect attempt with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The open connection. Dispose it when finished.</returns>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled, or the connect took longer than <see cref="RuvioClientOptions.ConnectTimeout"/>.</exception>
    /// <example>
    /// <code>
    /// await using var connection = await RuvioConnection.ConnectAsync("localhost", 6379);
    /// </code>
    /// </example>
    public static Task<RuvioConnection> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default) =>
        ConnectAsync(new RuvioClientOptions { Host = host, Port = port }, cancellationToken);

    /// <summary>Opens the connection and its initial database client on a chosen logical database.</summary>
    /// <param name="host">The server host name or address.</param>
    /// <param name="port">The RESP port, usually 6379.</param>
    /// <param name="database">The logical database to <c>SELECT</c>. Cluster mode supports database 0 only.</param>
    /// <param name="cancellationToken">Abandons the connect attempt with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The open connection. Dispose it when finished.</returns>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    /// <exception cref="RuvioException">The server refuses <c>SELECT</c> for that database.</exception>
    /// <example>
    /// <code>
    /// await using var connection = await RuvioConnection.ConnectAsync("localhost", 6379, database: 2);
    /// </code>
    /// </example>
    public static Task<RuvioConnection> ConnectAsync(
        string host,
        int port,
        int database,
        CancellationToken cancellationToken = default) =>
        ConnectAsync(new RuvioClientOptions { Host = host, Port = port, Database = database }, cancellationToken);

    /// <summary>Opens the connection to an IP address.</summary>
    /// <param name="address">The server address.</param>
    /// <param name="port">The RESP port, usually 6379.</param>
    /// <param name="database">The logical database to <c>SELECT</c>. Default 0.</param>
    /// <param name="cancellationToken">Abandons the connect attempt with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The open connection. Dispose it when finished.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="address"/> is null.</exception>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    public static Task<RuvioConnection> ConnectAsync(
        IPAddress address,
        int port,
        int database = 0,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);

        return ConnectAsync(address.ToString(), port, database, cancellationToken);
    }

    /// <summary>Opens the connection to an endpoint.</summary>
    /// <param name="endPoint">The server address and port.</param>
    /// <param name="database">The logical database to <c>SELECT</c>. Default 0.</param>
    /// <param name="cancellationToken">Abandons the connect attempt with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The open connection. Dispose it when finished.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endPoint"/> is null.</exception>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    public static Task<RuvioConnection> ConnectAsync(
        IPEndPoint endPoint,
        int database = 0,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);

        return ConnectAsync(endPoint.Address, endPoint.Port, database, cancellationToken);
    }

    /// <summary>Opens the connection with the supplied authentication, routing, and retry options.</summary>
    /// <param name="options">The settings. They are copied, so changing the object afterwards has no effect.</param>
    /// <param name="cancellationToken">Abandons the connect attempt with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The open connection. Dispose it when finished.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    /// <exception cref="RuvioException">The server refuses <c>AUTH</c> or <c>SELECT</c>.</exception>
    /// <example>
    /// <code>
    /// await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
    /// {
    ///     Host = "ruvio.internal",
    ///     Port = 6379,
    ///     Username = "app",
    ///     Password = secret,
    ///     ConnectTimeout = TimeSpan.FromSeconds(3),
    /// });
    /// </code>
    /// </example>
    public static async Task<RuvioConnection> ConnectAsync(
        RuvioClientOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);

        RuvioClientOptions ownedOptions = CopyOptions(options, options.Database);
        RuvioClient initialClient = await RuvioClient.ConnectClientAsync(ownedOptions, cancellationToken)
            .ConfigureAwait(false);

        return new RuvioConnection(ownedOptions, initialClient);
    }

    /// <summary>
    /// Gets the one shared command client for a logical database. Standalone clients use
    /// separate command sockets so concurrent database clients cannot race through SELECT.
    /// Cluster mode supports database 0 only.
    /// </summary>
    /// <param name="index">The logical database number, zero or more. Cluster mode supports 0 only.</param>
    /// <param name="cancellationToken">Abandons the wait with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The same client every time for one index; the first call for an index opens its sockets.</returns>
    /// <exception cref="ObjectDisposedException">The connection was disposed.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is negative.</exception>
    /// <exception cref="RuvioException">The server is a cluster and <paramref name="index"/> is not 0, or the server refuses <c>SELECT</c>.</exception>
    /// <example>
    /// <code>
    /// IRuvioClient cache = await connection.GetDatabaseAsync(1);
    /// </code>
    /// </example>
    public async Task<RuvioClient> GetDatabaseAsync(
        int index,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);

        await _databaseGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (_clients.TryGetValue(index, out RuvioClient? existing))
            {
                return existing;
            }

            if (IsCluster && index != 0)
            {
                throw new RuvioException("SELECT is not supported in cluster mode");
            }

            RuvioClientOptions databaseOptions = CopyOptions(_options, index);
            RuvioClient client = await RuvioClient.ConnectClientAsync(databaseOptions, cancellationToken)
                                                  .ConfigureAwait(false);
                
            AddClient(client);

            return client;
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    /// <summary>Gets the database client selected by <see cref="RuvioClientOptions.Database"/>.</summary>
    /// <param name="cancellationToken">Abandons the wait with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>The shared client for <see cref="DefaultDatabase"/>.</returns>
    /// <exception cref="ObjectDisposedException">The connection was disposed.</exception>
    public Task<RuvioClient> GetDefaultDatabaseAsync(CancellationToken cancellationToken = default) =>
        GetDatabaseAsync(DefaultDatabase, cancellationToken);

    async Task<IRuvioClient> IRuvioConnection.GetDefaultDatabaseAsync(CancellationToken cancellationToken) =>
        await GetDefaultDatabaseAsync(cancellationToken).ConfigureAwait(false);

    async Task<IRuvioClient> IRuvioConnection.GetDatabaseAsync(
        int index,
        CancellationToken cancellationToken) =>
        await GetDatabaseAsync(index, cancellationToken).ConfigureAwait(false);

    /// <summary>Reconnects every database client whose command connections are broken.</summary>
    /// <param name="cancellationToken">Abandons the wait with an <see cref="OperationCanceledException"/>.</param>
    /// <returns>A task that completes when the sockets are open again.</returns>
    /// <exception cref="ObjectDisposedException">The connection was disposed.</exception>
    /// <exception cref="System.Net.Sockets.SocketException">The server cannot be reached.</exception>
    /// <remarks>Needed only when <see cref="RuvioClientOptions.Reconnect"/> is off or its attempts ran out; with it on, a broken socket is reopened before the next command.</remarks>
    public async Task ReconnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        
        await _databaseGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            await Task.WhenAll(_clients.Values.Select(client => client.ReconnectAsync(cancellationToken)))
                .ConfigureAwait(false);
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    private void AddClient(RuvioClient client)
    {
        if (!_clients.TryAdd(client.Database, client))
        {
            client.Dispose();
            throw new InvalidOperationException($"A client for database {client.Database} already exists.");
        }

        client.ConnectionFailed += OnConnectionFailed;
        client.ConnectionLost += OnConnectionLost;
        client.ConnectionRestored += OnConnectionRestored;
    }

    private void OnConnectionFailed(object? sender, Exception exception) =>
        ConnectionFailed?.Invoke(this, exception);

    private void OnConnectionLost(object? sender, RuvioConnectionNotice notice) =>
        ConnectionLost?.Invoke(this, notice);

    private void OnConnectionRestored(object? sender, RuvioConnectionNotice notice) =>
        ConnectionRestored?.Invoke(this, notice);

    /// <summary>Closes the connection and disposes every database client it created. Calling it again does nothing.</summary>
    public void Dispose()
    {
        _databaseGate.Wait();
        
        try
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            foreach (RuvioClient client in _clients.Values)
            {
                client.ConnectionFailed -= OnConnectionFailed;
                client.ConnectionLost -= OnConnectionLost;
                client.ConnectionRestored -= OnConnectionRestored;
                client.Dispose();
            }

            _clients.Clear();
        }
        finally
        {
            _databaseGate.Release();
        }
    }

    /// <summary>Disposes the connection and all its database clients. Same as <see cref="Dispose"/>.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();

        return ValueTask.CompletedTask;
    }

    private static RuvioClientOptions CopyOptions(RuvioClientOptions options, int database) => new()
    {
        Host = options.Host,
        Port = options.Port,
        Seeds = options.Seeds is { Count: > 0 } ? [.. options.Seeds] : [],
        Username = options.Username,
        Password = options.Password,
        Database = database,
        DurabilityReplicas = options.DurabilityReplicas,
        DurabilityTimeout = options.DurabilityTimeout,
        ConnectTimeout = options.ConnectTimeout,
        CommandTimeout = options.CommandTimeout,
        MaxInFlightRequests = options.MaxInFlightRequests,
        DedicatedReaderThread = options.DedicatedReaderThread,
        DiscoverCluster = options.DiscoverCluster,
        Reconnect = options.Reconnect,
        MaxReconnectAttempts = options.MaxReconnectAttempts,
        ReconnectBaseDelay = options.ReconnectBaseDelay,
        ReconnectMaxDelay = options.ReconnectMaxDelay,
        ReconnectDelay = options.ReconnectDelay,
        ReadPolicy = options.ReadPolicy,
        JsonSerializerOptions = options.JsonSerializerOptions,
    };
}

/// <summary>
/// Extension methods for <see cref="IRuvioConnection"/>.
/// </summary>
public static class RuvioConnectionExtensions
{
    /// <summary>
    /// Gets the default database client from an asynchronous connection task.
    /// </summary>
    /// <param name="connectionTask">The task representing the asynchronous connection.</param>
    /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
    /// <returns>The default database client.</returns>
    public static async Task<IRuvioClient> GetDefaultDatabaseAsync(this Task<IRuvioConnection> connectionTask, CancellationToken cancellationToken = default)
    {
        var connection = await connectionTask.ConfigureAwait(false);

        return await connection.GetDefaultDatabaseAsync(cancellationToken).ConfigureAwait(false);
    }
}