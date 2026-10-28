using System.Reflection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public class ValidationTests
{
    [Fact]
    public void RegistrationValidatesAndPreservesExistingCache()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddRuvioDistributedCache(o => o.InstanceName = ""));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedCache(o => o.MaxValueBytes = -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedCache(o => o.MaxKeyBytes = 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedCache(o => o.FillTimeout = TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedCache(o => o.FillWait = TimeSpan.FromDays(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddRuvioDistributedCache(o => o.MaxValueBytes = 64 * 1024 * 1024));
        IRuvioClient client = DispatchProxy.Create<IRuvioClient, RejectingClient>();

        services.AddSingleton(client);
        services.AddRuvioDistributedCache(o => o.InstanceName = "unit");
        using ServiceProvider provider = services.BuildServiceProvider();
        IDistributedCache cache = provider.GetRequiredService<IDistributedCache>();

        Assert.IsType<RuvioDistributedCache>(cache);
        Assert.Same(cache, provider.GetRequiredService<IDistributedCache>());

        var existing = new ServiceCollection();

        existing.AddSingleton(cache);
        existing.AddRuvioDistributedCache();
        using ServiceProvider preserved = existing.BuildServiceProvider();

        Assert.Same(cache, preserved.GetRequiredService<IDistributedCache>());
    }

    [Fact]
    public void ConstructorValidatesRuntimeOptions()
    {
        IRuvioClient client = DispatchProxy.Create<IRuvioClient, RejectingClient>();

        Assert.Throws<ArgumentException>(() =>
            new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions { InstanceName = " " })));
    }

    [Fact(Timeout = 30000)]
    public async Task NullSizeUnicodeAndExpirationValidationPrecedesIo()
    {
        var cache = Cache(new RuvioCacheOptions { MaxValueBytes = 1, MaxKeyBytes = 30 });

        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.RefreshAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.RemoveAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("k", null!, new()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.SetAsync("k", [], null!));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync("k", [1, 2], new()));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.GetAsync(new string('é', 20)));
        await Assert.ThrowsAsync<System.Text.EncoderFallbackException>(() => cache.GetAsync("\ud800"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cache.SetAsync("k", [], new() { AbsoluteExpiration = DateTimeOffset.UtcNow.AddSeconds(-1) }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cache.SetAsync("k", [], new() { SlidingExpiration = TimeSpan.FromTicks(1) }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cache.SetAsync("k", [], new() { AbsoluteExpirationRelativeToNow = TimeSpan.FromTicks(1) }));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            cache.SetAsync("k", [], new() { AbsoluteExpirationRelativeToNow = TimeSpan.MaxValue }));
    }

    [Fact(Timeout = 30000)]
    public async Task CancellationPreventsDispatchForEveryOperation()
    {
        var cache = Cache();
        using var canceled = new CancellationTokenSource();

        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.GetAsync("", canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.SetAsync("", [], new(), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.RefreshAsync("", canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cache.RemoveAsync("", canceled.Token));
    }

    [Fact(Timeout = 30000)]
    public async Task FailureIsPropagatedOnceAndCancellationIsForwarded()
    {
        IRuvioClient client = DispatchProxy.Create<IRuvioClient, RejectingClient>();
        var fake = (RejectingClient)client;
        var cache = new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions()));
        using var source = new CancellationTokenSource();

        await Assert.ThrowsAsync<IOException>(() => cache.SetAsync("", [], new(), source.Token));
        Assert.Equal(1, fake.Calls);
        Assert.Equal(source.Token, fake.Token);
    }

    [Fact]
    public void SyncCallsDoNotRunTheClientOnTheCallersSynchronizationContext()
    {
        IRuvioClient client = DispatchProxy.Create<IRuvioClient, RejectingClient>();
        var fake = (RejectingClient)client;
        var cache = new RuvioDistributedCache(client, Options.Create(new RuvioCacheOptions()));
        SynchronizationContext? previous = SynchronizationContext.Current;

        try
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            Assert.Throws<IOException>(() => cache.Get("k"));
            Assert.Null(fake.Context);
            Assert.Throws<IOException>(() => cache.Set("k", [], new()));
            Assert.Null(fake.Context);
            Assert.Throws<IOException>(() => cache.Refresh("k"));
            Assert.Null(fake.Context);
            Assert.Throws<IOException>(() => cache.Remove("k"));
            Assert.Null(fake.Context);
            Assert.Equal(4, fake.Calls);
        }

        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }
    }

    private static RuvioDistributedCache Cache(RuvioCacheOptions? options = null)
        => new(DispatchProxy.Create<IRuvioClient, RejectingClient>(), Options.Create(options ?? new()));

    public class RejectingClient : DispatchProxy
    {
        public int Calls { get; private set; }
        public CancellationToken Token { get; private set; }
        public SynchronizationContext? Context { get; private set; }
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            Assert.Equal(nameof(IRuvioClient.ExecuteBinaryAsync), targetMethod!.Name);
            Calls++;
            Token = (CancellationToken)args![1]!;
            Context = SynchronizationContext.Current;

            return ValueTask.FromException<RespValue>(new IOException("Ambiguous write; do not retry."));
        }
    }
}
