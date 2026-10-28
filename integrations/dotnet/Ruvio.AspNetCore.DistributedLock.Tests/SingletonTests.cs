using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.DistributedLock.Tests;

/// <summary>One-instance hosted services and once-per-period work, against a scripted lock server.</summary>
public class SingletonTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(10);

    public sealed class Probe
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Stopped { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Runs;
        public long? Token;
        public bool Fail;
    }

    public sealed class LeaderWorker(Probe probe) : IRuvioSingletonWorker
    {
        public async Task ExecuteAsync(IRuvioLock leadership, CancellationToken stoppingToken)
        {
            Interlocked.Increment(ref probe.Runs);
            probe.Token = leadership.FencingToken;
            probe.Started.TrySetResult();

            if (probe.Fail)
            {
                throw new InvalidOperationException("work failed");
            }

            try
            {
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            finally
            {
                probe.Stopped.TrySetResult();
            }
        }
    }

    // One lock key shared by every instance, with SET NX / IFEQ / DELEX semantics.
    private sealed class LockServer
    {
        private readonly object gate = new();
        private string? holder;

        public IRuvioClient Client()
        {
            var client = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();

            ((OfflineLockTests.ScriptClient)(object)client).Handler = (command, _) =>
            {
                lock (gate)
                {
                    string token = command[0] == "DELEX" ? command[3] : command[2];

                    switch (command[0])
                    {
                        case "SET" when command[3] == "NX":
                            if (holder is not null)
                            {
                                return Resp.Null();
                            }

                            holder = token;

                            return Resp.Ok;
                        case "SET":
                            return holder == token ? Resp.Ok : Resp.Null();
                        case "DELEX":
                            if (holder != token)
                            {
                                return Resp.Int(0);
                            }

                            holder = null;

                            return Resp.Int(1);
                        default:
                            throw new InvalidOperationException(command[0]);
                    }
                }
            };

            return client;
        }
    }

    private static (ServiceProvider Services, IHostedService Service) Instance(IRuvioClient client, Probe probe, Action<RuvioLockOptions>? locks = null)
    {
        var services = new ServiceCollection();

        services.AddSingleton(client);
        services.AddSingleton(probe);
        services.AddRuvioDistributedLock(o => { o.KeyPrefix = "p:"; locks?.Invoke(o); });
        services.AddRuvioSingleton<LeaderWorker>("leader", o =>
        {
            o.Lease = TimeSpan.FromSeconds(1);
            o.RetryInterval = TimeSpan.FromMilliseconds(20);
        });
        ServiceProvider provider = services.BuildServiceProvider();

        return (provider, provider.GetServices<IHostedService>().OfType<RuvioSingletonService<LeaderWorker>>().Single());
    }

    [Fact(Timeout = 30000)]
    public async Task OnlyOneInstanceRunsAndAnotherTakesOverWhenItStops()
    {
        var server = new LockServer();
        var probeA = new Probe();
        var probeB = new Probe();
        var (servicesA, a) = Instance(server.Client(), probeA);
        var (servicesB, b) = Instance(server.Client(), probeB);

        await using (servicesA)
        await using (servicesB)
        {
            await a.StartAsync(default);
            await probeA.Started.Task.WaitAsync(Patience);
            await b.StartAsync(default);
            // B asks every 20 ms; while A holds and renews the lock none of those asks wins.
            await Task.Delay(200);
            Assert.False(probeB.Started.Task.IsCompleted);

            await a.StopAsync(default);
            await probeA.Stopped.Task.WaitAsync(Patience);
            await probeB.Started.Task.WaitAsync(Patience);
            Assert.Equal(1, probeA.Runs);
            Assert.Null(probeB.Token);
            await b.StopAsync(default);
            await probeB.Stopped.Task.WaitAsync(Patience);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task FailedWorkReleasesTheLockAndRunsAgain()
    {
        var probe = new Probe { Fail = true };
        var (services, service) = Instance(new LockServer().Client(), probe);

        await using (services)
        {
            await service.StartAsync(default);

            for (int attempt = 0; attempt < 500 && Volatile.Read(ref probe.Runs) < 3; attempt++)
            {
                await Task.Delay(20);
            }

            await service.StopAsync(default);
            Assert.True(probe.Runs >= 3);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task WithoutAutoRenewTheServiceRefusesToStart()
    {
        var (services, service) = Instance(new LockServer().Client(), new Probe(), o => o.AutoRenew = false);

        await using (services)
        {
            // The check runs before the first await, so the host fails at start.
            await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(default));
        }
    }

    [Fact]
    public void RegistrationValidatesAndKeepsEveryName()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddRuvioSingleton<LeaderWorker>(""));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioSingleton<LeaderWorker>("a", o => o.Lease = TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioSingleton<LeaderWorker>("a", o => o.RetryInterval = TimeSpan.Zero));
        services.AddRuvioSingleton<LeaderWorker>("first");
        services.AddRuvioSingleton<LeaderWorker>("second");

        Assert.Equal(2, services.Count(d => d.ServiceType == typeof(IHostedService)));
        Assert.Single(services, d => d.ServiceType == typeof(LeaderWorker));
    }

    [Fact(Timeout = 30000)]
    public async Task RunOncePerPeriodRunsOnlyForTheFirstCaller()
    {
        long reply = 1;
        var client = DispatchProxy.Create<IRuvioClient, OfflineLockTests.ScriptClient>();
        var script = (OfflineLockTests.ScriptClient)(object)client;

        script.Handler = (_, _) => Resp.Int(reply);
        var provider = new RuvioLockProvider(client, new RuvioLockOptions { KeyPrefix = "p:" });
        int runs = 0;

        Assert.True(await provider.RunOncePerPeriodAsync("report", TimeSpan.FromHours(1), _ => { runs++; return Task.CompletedTask; }));
        reply = 0;
        Assert.False(await provider.RunOncePerPeriodAsync("report", TimeSpan.FromHours(1), _ => { runs++; return Task.CompletedTask; }));
        Assert.Equal(1, runs);
        Assert.Equal(["ONCE", "p:once:report", "3600000"], script.Sent[0]);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await provider.RunOncePerPeriodAsync("report", TimeSpan.Zero, _ => Task.CompletedTask));
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await provider.RunOncePerPeriodAsync("report", TimeSpan.FromSeconds(1), null!));
    }

    [Fact(Timeout = 30000)]
    public async Task LiveRunOncePerPeriodRunsOnce()
    {
        if (await LiveTests.TryConnectAsync() is not { } connected)
        {
            return;
        }

        await using RuvioConnection connection = connected.Connection;
        var provider = new RuvioLockProvider(connected.Client, new RuvioLockOptions { KeyPrefix = "test:once:" + Guid.NewGuid().ToString("N") + ":" });
        int runs = 0;

        Assert.True(await provider.RunOncePerPeriodAsync("job", TimeSpan.FromSeconds(5), _ => { runs++; return Task.CompletedTask; }));
        Assert.False(await provider.RunOncePerPeriodAsync("job", TimeSpan.FromSeconds(5), _ => { runs++; return Task.CompletedTask; }));
        Assert.Equal(1, runs);
    }
}
