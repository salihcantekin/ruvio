using Microsoft.Extensions.Caching.Distributed;

namespace Ruvio.Extensions.Caching;

/// <summary>
/// <see cref="DistributedCacheEntryOptions"/> with Ruvio's extras: a stale period for fail-safe and
/// stale-while-revalidate, and tags for <see cref="RuvioDistributedCache.RemoveByTagAsync"/>. Pass it wherever a
/// <see cref="DistributedCacheEntryOptions"/> is taken. Other caches ignore the extras; entries that use none of them are
/// stored exactly as before.
/// </summary>
/// <remarks>
/// An entry written with these options uses a newer stored format that package versions without them cannot read
/// (they throw). Upgrade every instance that shares an <see cref="RuvioCacheOptions.InstanceName"/> first.
/// </remarks>
public sealed class RuvioCacheEntryOptions : DistributedCacheEntryOptions
{
    /// <summary>
    /// How long the entry is kept after it stops being fresh. In that time <c>GetOrCreate*</c> lets one caller refresh
    /// it while the others get the stale value at once, and returns the stale value when the factory throws. A plain
    /// <c>Get</c> treats a stale entry as a miss. Needs an absolute or relative expiration (the end of the fresh period);
    /// cannot be combined with <see cref="DistributedCacheEntryOptions.SlidingExpiration"/>. Null (the default) keeps
    /// no stale period.
    /// </summary>
    public TimeSpan? StaleFor { get; set; }

    /// <summary>
    /// Tags of the entry. <see cref="RuvioDistributedCache.RemoveByTagAsync"/> with any of them removes it. A tagged
    /// read costs a second round trip (the tags' current generations). At most
    /// <see cref="RuvioCacheOptions.MaxTagsPerEntry"/>.
    /// </summary>
    public IReadOnlyList<string>? Tags { get; set; }
}
