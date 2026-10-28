// Probe for scripts/seed_failover.py: connects with the seeds in RUVIO_SEEDS, reads seed:name,
// waits for a line on stdin (the first server is stopped by then) and reads it again.
//
//   dotnet build clients/csharp/Ruvio.Client.SeedProbe
//   scripts/seed_failover.py target/debug/ruvio -- dotnet clients/csharp/Ruvio.Client.SeedProbe/bin/Debug/net8.0/Ruvio.Client.SeedProbe.dll
using Ruvio.Client;

string seeds = Environment.GetEnvironmentVariable("RUVIO_SEEDS")
    ?? throw new InvalidOperationException("RUVIO_SEEDS is set by the script");

await using RuvioConnection connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Seeds = [.. seeds.Split(',')],
    ReconnectBaseDelay = TimeSpan.FromMilliseconds(50),
});
RuvioClient client = await connection.GetDefaultDatabaseAsync();

Console.WriteLine($"first {await client.GetStringAsync("seed:name")}");
Console.Out.Flush();
Console.In.ReadLine();

for (int attempt = 0; attempt < 50; attempt++)
{
    try
    {
        Console.WriteLine($"after {await client.GetStringAsync("seed:name")}");

        return 0;
    }
    catch (Exception)
    {
        await Task.Delay(100);
    }
}

return 1;
