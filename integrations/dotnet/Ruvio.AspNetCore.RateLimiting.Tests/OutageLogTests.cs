using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ruvio.Client;
using Xunit;

namespace Ruvio.AspNetCore.RateLimiting.Tests;

public sealed class OutageLogTests
{
    [Fact]
    public void BreakerTransitionsAreLoggedOnceUnderThePackageName()
    {
        var sink = new List<(string Category, LogLevel Level)>();
        var services = new ServiceCollection();

        services.AddLogging(logging => logging.AddProvider(new Sink(sink)));
        services.AddRuvioRateLimiting(options =>
        {
            options.PartitionKey = static _ => "p";
            options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions { FailureThreshold = 1 };
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        RuvioCircuitBreaker breaker = provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>().Value.Outage.Breaker!;

        breaker.RecordFailure();
        provider.GetRequiredService<IOptions<RuvioRateLimitingOptions>>();     // cached: not attached a second time

        Assert.Equal([("Ruvio.AspNetCore.RateLimiting", LogLevel.Warning)], sink);
    }

    private sealed class Sink(List<(string, LogLevel)> entries) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new Logger(entries, categoryName);

        public void Dispose()
        {
        }

        private sealed class Logger(List<(string, LogLevel)> entries, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (category.StartsWith("Ruvio", StringComparison.Ordinal))
                    lock (entries)
                        entries.Add((category, logLevel));
            }
        }
    }
}
