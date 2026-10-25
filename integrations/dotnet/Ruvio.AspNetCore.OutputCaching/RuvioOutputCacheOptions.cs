using System.Text;
using Ruvio.Client;

namespace Ruvio.AspNetCore.OutputCaching;

/// <summary>
/// Represents the configuration options for the Ruvio output-cache store in ASP.NET Core applications.
/// </summary>
public sealed class RuvioOutputCacheOptions
{
    /// <summary>
    /// Gets or sets the instance name for the Ruvio output-cache store.
    /// </summary>
    public string InstanceName { get; set; } = "default";

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of a cache entry's value.
    /// </summary>
    public int MaxValueBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of a cache entry's key.
    /// </summary>
    public int MaxKeyBytes { get; set; } = 4096;

    /// <summary>
    /// Gets or sets the maximum number of tags allowed per cache entry.
    /// </summary>
    public int MaxTagsPerEntry { get; set; } = 32;

    /// <summary>
    /// What happens when Ruvio cannot be reached. The default, <see cref="RuvioOutageMode.Throw"/>, lets the storage
    /// exception reach the output-cache middleware (<see cref="RuvioOutageMode.FailClosed"/> does the same). With
    /// <see cref="RuvioOutageMode.FailOpen"/> a read is a miss and writes are dropped, so the endpoint runs and answers;
    /// an eviction by tag that was dropped can leave stale entries to be served once Ruvio is back. Add a circuit breaker
    /// with <see cref="RuvioOutageOptions.CircuitBreaker"/> to stop waiting on a Ruvio that is down.
    /// </summary>
    public RuvioOutageOptions Outage { get; } = new();

    internal static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(InstanceName) || Utf8.GetByteCount(InstanceName) > 256)
            throw new ArgumentException("InstanceName must contain 1-256 UTF-8 bytes.");

        if (MaxValueBytes is < 1 or > 8 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxValueBytes));

        if (MaxKeyBytes is < 1 or > 64 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxKeyBytes));

        Outage.Validate();

        if (MaxTagsPerEntry is < 0 or > 32)
            throw new ArgumentOutOfRangeException(nameof(MaxTagsPerEntry));
    }
}
