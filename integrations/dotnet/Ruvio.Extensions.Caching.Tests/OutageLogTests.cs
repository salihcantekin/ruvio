using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.Extensions.Caching.Tests;

public sealed class OutageLogTests
{
    [Fact]
    public void BreakerTransitionsAreLoggedUnderThePackageName()
    {
        var logs = new CapturingLoggers();
        var services = new ServiceCollection();

        services.AddSingleton<ILoggerFactory>(logs);
        services.AddSingleton<IRuvioClient>(static _ => System.Reflection.DispatchProxy.Create<IRuvioClient, NoCalls>());
        services.AddRuvioDistributedCache(static o =>
            o.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 1, BreakDuration = TimeSpan.FromSeconds(7) });
        using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetRequiredService<IDistributedCache>();
        provider.GetRequiredService<IOptions<RuvioCacheOptions>>().Value.Outage.Breaker!.RecordFailure();

        (string category, LogLevel level, string message) = Assert.Single(logs.Entries);
        Assert.Equal("Ruvio.Extensions.Caching", category);
        Assert.Equal(LogLevel.Warning, level);
        Assert.Contains("7 s", message);
    }

    [Fact]
    public void NoBreakerMeansNoListener()
    {
        var logs = new CapturingLoggers();
        var services = new ServiceCollection();

        services.AddSingleton<ILoggerFactory>(logs);
        services.AddSingleton<IRuvioClient>(static _ => System.Reflection.DispatchProxy.Create<IRuvioClient, NoCalls>());
        services.AddRuvioDistributedCache();
        using ServiceProvider provider = services.BuildServiceProvider();

        provider.GetRequiredService<IDistributedCache>();

        Assert.Null(provider.GetRequiredService<IOptions<RuvioCacheOptions>>().Value.Outage.Breaker);
        Assert.Empty(logs.Entries);
    }

    public class NoCalls : System.Reflection.DispatchProxy
    {
        protected override object? Invoke(System.Reflection.MethodInfo? targetMethod, object?[]? args)
            => throw new NotSupportedException();
    }

    private sealed class CapturingLoggers : ILoggerFactory
    {
        public List<(string Category, LogLevel Level, string Message)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }

        private sealed class Logger(CapturingLoggers owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (owner.Entries)
                    owner.Entries.Add((category, logLevel, formatter(state, exception)));
            }
        }
    }
}
