// Probe for scripts/client_faults.py: writes fault:<name>:<i> = i with durability replicas 1, reads
// each key back, and keeps going through a failover until a line arrives on stdin. Environment:
// RUVIO_SEEDS, RUVIO_PROBE_NAME and, for TLS, RUVIO_PROBE_CA (PEM path). Output, one line each:
// ready, ack <i>, err <i> <text>, bad <i> <value>, done.
//
//   dotnet build clients/csharp/Ruvio.Client.FaultProbe
//   python3 scripts/client_faults.py target/debug/ruvio
using System.Security.Cryptography.X509Certificates;
using Ruvio.Client;

string seeds = Environment.GetEnvironmentVariable("RUVIO_SEEDS")
    ?? throw new InvalidOperationException("RUVIO_SEEDS is set by the script");
string name = Environment.GetEnvironmentVariable("RUVIO_PROBE_NAME") ?? "";
string? ca = Environment.GetEnvironmentVariable("RUVIO_PROBE_CA");

RuvioClientOptions options = new()
{
    Seeds = [.. seeds.Split(',')],
    DurabilityReplicas = 1,
};

if (!string.IsNullOrEmpty(ca))
{
    options.Tls = new RuvioTlsOptions
    {
        TrustedCertificates = new X509Certificate2Collection(X509Certificate2.CreateFromPem(File.ReadAllText(ca))),
    };
}

await using RuvioConnection connection = await RuvioConnection.ConnectAsync(options);
RuvioClient client = await connection.GetDefaultDatabaseAsync();

bool stop = false;
_ = Task.Run(() =>
{
    Console.In.ReadLine();
    stop = true;
});

Console.WriteLine("ready");

for (int i = 1; !Volatile.Read(ref stop);)
{
    string key = $"fault:{name}:{i}";
    try
    {
        await client.SetAsync(key, i.ToString());
        string? got = await client.GetStringAsync(key);

        if (got != i.ToString())
        {
            Console.WriteLine($"bad {i} {got}");
        }

        Console.WriteLine($"ack {i}");
        i++;
    }
    catch (Exception error)
    {
        Console.WriteLine($"err {i} {error.GetType().Name}");
        await Task.Delay(20);
    }
}

Console.WriteLine("done");

return 0;
