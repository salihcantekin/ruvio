using Ruvio.Client;
using Xunit;

namespace Ruvio.Integrations.Tests;

/// <summary>A test that needs the live server at <c>RUVIO_TEST_ADDR</c>; skipped when it is not set.</summary>
public sealed class OutageLiveFactAttribute : FactAttribute
{
    public OutageLiveFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RUVIO_TEST_ADDR")))
            Skip = "Set RUVIO_TEST_ADDR to run against live Ruvio.";
    }
}

internal static class OutageClient
{
    /// <summary>
    /// A client that reaches the live server through <paramref name="proxy"/>, with short timeouts so a test
    /// that takes Ruvio away fails fast instead of waiting out the defaults.
    /// </summary>
    public static async Task<(RuvioConnection Connection, IRuvioClient Client)> ConnectAsync(OutageProxy proxy)
    {
        var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
        {
            Host = "127.0.0.1",
            Port = proxy.Port,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            CommandTimeout = TimeSpan.FromSeconds(3),
            MaxReconnectAttempts = 1,
            ReconnectBaseDelay = TimeSpan.FromMilliseconds(5),
            ReconnectMaxDelay = TimeSpan.FromMilliseconds(5),
        });

        return (connection, await connection.GetDefaultDatabaseAsync());
    }

    /// <summary>Waits (bounded) until the client reaches Ruvio again after <see cref="OutageProxy.Restore"/>.</summary>
    public static async Task WaitUntilServingAsync(IRuvioClient client)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(10);

        while (true)
        {
            try
            {
                await client.PingAsync();

                return;
            }
            catch (Exception) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(20);
            }
        }
    }
}
