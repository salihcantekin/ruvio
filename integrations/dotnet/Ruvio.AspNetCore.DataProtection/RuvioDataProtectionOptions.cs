using System.Text;

namespace Ruvio.AspNetCore.DataProtection;

/// <summary>Settings for storing the ASP.NET Core Data Protection key ring in Ruvio.</summary>
public sealed class RuvioDataProtectionOptions
{
    /// <summary>The Ruvio key that holds the key ring.</summary>
    public string Key { get; set; } = "ruvio:data-protection:v1:keys";

    /// <summary>
    /// The key of a connection registered with <c>AddKeyedRuvioClient</c>. Null (the default) uses the unkeyed
    /// <c>IRuvioClient</c> (its connection's default database); ignored by the connection-factory overload of <c>PersistKeysToRuvio</c>.
    /// </summary>
    public string? ClientKey { get; set; }

    /// <summary>The longest a key-ring read or write may take, from 1 ms to 5 minutes.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>The largest accepted size of one key-ring element, in bytes.</summary>
    public int MaxElementBytes { get; set; } = 64 * 1024;

    /// <summary>The most key-ring elements that are stored.</summary>
    public int MaxElements { get; set; } = 128;

    /// <summary>Refuses to store keys unless a key encryptor is configured.</summary>
    public bool RequireEncryption { get; set; } = true;

    /// <summary>
    /// Refuses to read or write the key ring when the server keeps data in memory only (durability profile
    /// <c>memory</c>, lost on restart) or does not say which profile it runs. Off by default: the first key-ring
    /// access logs a warning instead.
    /// </summary>
    public bool RequireDurableStorage { get; set; }

    internal static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Key) || Utf8.GetByteCount(Key) > 1024)
            throw new ArgumentException("Key must contain 1-1024 UTF-8 bytes.");

        if (OperationTimeout < TimeSpan.FromMilliseconds(1) || OperationTimeout > TimeSpan.FromMinutes(5))
            throw new ArgumentOutOfRangeException(nameof(OperationTimeout));

        if (MaxElementBytes is < 1 or > 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxElementBytes));

        if (MaxElements is < 1 or > 4096 || (long)MaxElements * MaxElementBytes > 8 * 1024 * 1024)
            throw new ArgumentOutOfRangeException(nameof(MaxElements), "The key-ring payload budget must not exceed 8 MiB.");
    }
}
