using Microsoft.Extensions.Logging;
using Ruvio.Client;

namespace Ruvio.Integrations;

/// <summary>Logs a package's circuit-breaker transitions, so an open circuit (fast failures or fallbacks) is visible.</summary>
internal static class RuvioOutageLog
{
    private static readonly Action<ILogger, double, Exception?> Opened = LoggerMessage.Define<double>(
        LogLevel.Warning, new EventId(1, "RuvioCircuitOpened"),
        "Ruvio circuit opened after repeated outage failures; Ruvio is not called for {BreakSeconds} s.");

    private static readonly Action<ILogger, Exception?> Probing = LoggerMessage.Define(
        LogLevel.Information, new EventId(2, "RuvioCircuitHalfOpen"),
        "Ruvio circuit half-open: one trial call goes to Ruvio.");

    private static readonly Action<ILogger, Exception?> Closed = LoggerMessage.Define(
        LogLevel.Information, new EventId(3, "RuvioCircuitClosed"),
        "Ruvio circuit closed: Ruvio answers again.");

    /// <summary>
    /// Subscribes to the breaker of <paramref name="outage"/> when one is configured and a logger factory exists.
    /// Called once per options instance; does nothing for invalid options, which their own validation reports.
    /// </summary>
    public static void Attach(RuvioOutageOptions outage, ILoggerFactory? loggers, string package)
    {
        if (loggers is null || outage.CircuitBreaker is null || !outage.IsValid() || outage.Breaker is not { } breaker)
            return;

        ILogger logger = loggers.CreateLogger(package);

        breaker.StateChanged += (_, change) =>
        {
            switch (change.Current)
            {
                case RuvioCircuitState.Open:
                    Opened(logger, change.RetryAfter.TotalSeconds, null);

                    break;

                case RuvioCircuitState.HalfOpen:
                    Probing(logger, null);

                    break;

                default:
                    Closed(logger, null);

                    break;
            }
        };
    }
}
