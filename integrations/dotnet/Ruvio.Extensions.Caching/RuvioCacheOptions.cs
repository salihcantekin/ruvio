using System.Text;
using Ruvio.Client;

namespace Ruvio.Extensions.Caching;

/// <summary>Settings for a Ruvio cache. Captured and validated when the cache is constructed.</summary>
public sealed class RuvioCacheOptions
{
    /// <summary>Application namespace, prepended to the reserved <c>:ruvio:cache:v1:</c> marker.</summary>
    public string InstanceName { get; set; } = "default";

    /// <summary>Maximum payload size, excluding the storage header. Defaults to one MiB.</summary>
    public int MaxValueBytes { get; set; } = 1024 * 1024;

    /// <summary>Maximum UTF-8 size of the full namespaced key. Defaults to 1024 bytes.</summary>
    public int MaxKeyBytes { get; set; } = 1024;

    /// <summary>How long a caller that is building a value in <c>GetOrCreateAsync</c> keeps the fill marker. Defaults to 30 seconds.</summary>
    public TimeSpan FillTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long other callers of <c>GetOrCreateAsync</c> wait for the builder before building the value themselves. Defaults to 10 seconds.</summary>
    public TimeSpan FillWait { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// What happens when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, lets the storage
    /// exception reach the caller (<see cref="RuvioOutageMode.FailClosed"/> does the same, a cache has no response to
    /// refuse). With <see cref="RuvioOutageMode.FailOpen"/> a read is a miss, a write or removal is dropped and
    /// <c>GetOrCreate</c> runs its factory without caching; a dropped removal can leave a stale entry to be read once
    /// Ruvio is back. Add a circuit breaker with <see cref="RuvioOutageOptions.CircuitBreaker"/> to stop waiting
    /// on a Ruvio that is down.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    internal const string VersionPrefix = ":ruvio:cache:v1:";
    internal static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(InstanceName))
            throw new ArgumentException("InstanceName must not be empty.", nameof(InstanceName));

        Outage.Validate();

        if (MaxValueBytes is < 0 or > 64 * 1024 * 1024 - 64)
            throw new ArgumentOutOfRangeException(nameof(MaxValueBytes));

        if (FillTimeout < TimeSpan.FromMilliseconds(1) || FillTimeout > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(FillTimeout), "FillTimeout must be between 1 ms and one hour.");

        if (FillWait < TimeSpan.FromMilliseconds(1) || FillWait > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(FillWait), "FillWait must be between 1 ms and one hour.");

        if (MaxKeyBytes < 1 || Utf8.GetByteCount(InstanceName + VersionPrefix) > MaxKeyBytes)
            throw new ArgumentOutOfRangeException(nameof(MaxKeyBytes), "The key limit must accommodate the namespace.");
    }
}
