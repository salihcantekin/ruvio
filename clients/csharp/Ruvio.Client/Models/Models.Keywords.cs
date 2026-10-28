namespace Ruvio.Client;

/// <summary>The distance unit of the <c>GEO*</c> commands. The enum overloads send a pre-encoded token, with no string encoding per call.</summary>
/// <example>
/// <code>
/// double? km = await client.GeoDistanceAsync("cities", "ankara", "izmir", GeoUnit.Kilometers);
/// </code>
/// </example>
public enum GeoUnit
{
    /// <summary>Meters (<c>m</c>).</summary>
    Meters,

    /// <summary>Kilometers (<c>km</c>).</summary>
    Kilometers,

    /// <summary>Feet (<c>ft</c>).</summary>
    Feet,

    /// <summary>Miles (<c>mi</c>).</summary>
    Miles,
}

/// <summary>The result order of <c>GEOSEARCH</c>, <c>GEORADIUS</c> and <c>GEORADIUSBYMEMBER</c>.</summary>
/// <example>
/// <code>
/// var farthest = await client.GeoRadiusAsync("cities", 32.8, 39.9, 500, GeoUnit.Kilometers, GeoOrder.Descending);
/// </code>
/// </example>
public enum GeoOrder
{
    /// <summary>Nearest first (<c>ASC</c>).</summary>
    Ascending,

    /// <summary>Farthest first (<c>DESC</c>).</summary>
    Descending,
}

/// <summary>Where <c>LINSERT</c> puts the new element relative to the pivot.</summary>
/// <example>
/// <code>
/// long length = await client.InsertListAsync("queue", InsertPosition.After, "b", "c");
/// </code>
/// </example>
public enum InsertPosition
{
    /// <summary>Before the pivot (<c>BEFORE</c>).</summary>
    Before,

    /// <summary>After the pivot (<c>AFTER</c>).</summary>
    After,
}

/// <summary>An end of a list, used by <c>LMOVE</c>, <c>BLMOVE</c>, <c>LMPOP</c> and <c>BLMPOP</c>.</summary>
/// <example>
/// <code>
/// string? moved = await client.MoveListItemAsync("source", "target", ListSide.Right, ListSide.Left);
/// </code>
/// </example>
public enum ListSide
{
    /// <summary>The head (<c>LEFT</c>).</summary>
    Left,

    /// <summary>The tail (<c>RIGHT</c>).</summary>
    Right,
}

/// <summary>The condition of <c>EXPIRE</c>, <c>PEXPIRE</c>, <c>EXPIREAT</c> and <c>PEXPIREAT</c>. A key without a deadline counts as an infinite deadline for <see cref="Gt"/> and <see cref="Lt"/>.</summary>
/// <example>
/// <code>
/// bool set = await client.ExpireAsync("session:ada", TimeSpan.FromMinutes(30), ExpireCondition.Gt);
/// </code>
/// </example>
public enum ExpireCondition
{
    /// <summary>Only when the key has no deadline (<c>NX</c>).</summary>
    Nx,

    /// <summary>Only when the key already has a deadline (<c>XX</c>).</summary>
    Xx,

    /// <summary>Only when the new deadline is later than the current one (<c>GT</c>).</summary>
    Gt,

    /// <summary>Only when the new deadline is earlier than the current one (<c>LT</c>).</summary>
    Lt,
}

/// <summary>The value type a <c>SCAN ... TYPE</c> filter accepts.</summary>
/// <example>
/// <code>
/// ScanPage page = await client.ScanAsync(0, "user:*", 100, KeyType.Hash);
/// </code>
/// </example>
public enum KeyType
{
    /// <summary><c>string</c>.</summary>
    String,

    /// <summary><c>json</c>.</summary>
    Json,

    /// <summary><c>list</c>.</summary>
    List,

    /// <summary><c>set</c>.</summary>
    Set,

    /// <summary><c>hash</c>.</summary>
    Hash,

    /// <summary><c>zset</c>.</summary>
    SortedSet,

    /// <summary><c>bloomfilter</c>.</summary>
    BloomFilter,

    /// <summary><c>stream</c>.</summary>
    Stream,

    /// <summary><c>lease</c>.</summary>
    Lease,

    /// <summary><c>limit</c>.</summary>
    Limit,

    /// <summary><c>semaphore</c>.</summary>
    Semaphore,

    /// <summary><c>once</c>.</summary>
    Once,
}

/// <summary>What a <see cref="BitFieldStep"/> does.</summary>
public enum BitFieldOp
{
    /// <summary><c>GET type offset</c>.</summary>
    Get,

    /// <summary><c>SET type offset value</c>.</summary>
    Set,

    /// <summary><c>INCRBY type offset increment</c>.</summary>
    IncrBy,
}

/// <summary>What <c>BITFIELD</c> does when a <c>SET</c> or <c>INCRBY</c> leaves the range of the field type.</summary>
public enum BitFieldOverflow
{
    /// <summary>Wrap around (<c>WRAP</c>, the default).</summary>
    Wrap,

    /// <summary>Clamp to the smallest or largest value (<c>SAT</c>).</summary>
    Saturate,

    /// <summary>Refuse the write and return null for that step (<c>FAIL</c>).</summary>
    Fail,
}

/// <summary>
/// Field types for <c>BITFIELD</c>: <c>i1</c> to <c>i64</c> (signed) and <c>u1</c> to <c>u63</c> (unsigned). The server
/// requires the lowercase prefix. The returned strings are shared and never allocate.
/// </summary>
/// <example>
/// <code>
/// string type = BitFieldType.Unsigned(8); // "u8"
/// </code>
/// </example>
public static class BitFieldType
{
    private static readonly string[] SignedNames = Build('i', 64);
    private static readonly string[] UnsignedNames = Build('u', 63);

    /// <summary>The signed type of <paramref name="bits"/> bits, for example <c>i16</c>. Valid widths are 1 to 64.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1 to 64.</exception>
    public static string Signed(int bits)
    {
        if (bits < 1 || bits > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "A signed field is 1 to 64 bits wide.");
        }

        return SignedNames[bits];
    }

    /// <summary>The unsigned type of <paramref name="bits"/> bits, for example <c>u8</c>. Valid widths are 1 to 63.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1 to 63.</exception>
    public static string Unsigned(int bits)
    {
        if (bits < 1 || bits > 63)
        {
            throw new ArgumentOutOfRangeException(nameof(bits), bits, "An unsigned field is 1 to 63 bits wide.");
        }

        return UnsignedNames[bits];
    }

    private static string[] Build(char prefix, int max)
    {
        var names = new string[max + 1];

        for (int bits = 1; bits <= max; bits++)
        {
            names[bits] = prefix + bits.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return names;
    }
}

/// <summary>
/// One <c>BITFIELD</c> subcommand: a <c>GET</c>, <c>SET</c> or <c>INCRBY</c>, or an <c>OVERFLOW</c> mode that applies to the
/// later <c>SET</c> and <c>INCRBY</c> steps. Build steps with the factory methods and pass them to
/// <c>BitFieldAsync</c>.
/// </summary>
/// <example>
/// <code>
/// var results = await client.BitFieldAsync(
///     "counters",
///     BitFieldStep.Overflow(BitFieldOverflow.Saturate),
///     BitFieldStep.IncrBy(BitFieldType.Unsigned(8), 0, 10),
///     BitFieldStep.Get(BitFieldType.Unsigned(8), 0));
/// </code>
/// </example>
public readonly struct BitFieldStep
{
    private readonly string? _type;
    private readonly long _offset;
    private readonly long _value;
    private readonly byte _kind;

    private BitFieldStep(byte kind, string? type, long offset, long value)
    {
        _kind = kind;
        _type = type;
        _offset = offset;
        _value = value;
    }

    /// <summary><c>GET type offset</c>. <paramref name="type"/> comes from <see cref="BitFieldType"/>; <paramref name="offset"/> is a bit offset.</summary>
    public static BitFieldStep Get(string type, long offset) => new(0, type, offset, 0);

    /// <summary><c>SET type offset value</c>. Returns the old value.</summary>
    public static BitFieldStep Set(string type, long offset, long value) => new(1, type, offset, value);

    /// <summary><c>INCRBY type offset increment</c>. Returns the new value.</summary>
    public static BitFieldStep IncrBy(string type, long offset, long increment) => new(2, type, offset, increment);

    /// <summary><c>OVERFLOW mode</c>. Applies to the <c>SET</c> and <c>INCRBY</c> steps after it.</summary>
    public static BitFieldStep Overflow(BitFieldOverflow mode) => new((byte)(3 + (int)mode), null, 0, 0);

    internal static string[] Words(BitFieldStep first, BitFieldStep[] rest) => Command(null, null, first, rest);

    internal static string[] Command(string? name, string? key, BitFieldStep first, BitFieldStep[] rest)
    {
        ArgumentNullException.ThrowIfNull(rest);

        var words = new List<string>((rest.Length + 1) * 4 + 2);

        if (name is not null)
        {
            words.Add(name);
            words.Add(key!);
        }

        first.AppendTo(words);

        foreach (BitFieldStep step in rest)
        {
            step.AppendTo(words);
        }

        return words.ToArray();
    }

    internal void AppendTo(List<string> arguments)
    {
        if (_kind >= 3)
        {
            arguments.Add("OVERFLOW");
            arguments.Add(Keywords.Word((BitFieldOverflow)(_kind - 3)));

            return;
        }

        ArgumentNullException.ThrowIfNull(_type, "type");
        arguments.Add(Keywords.Word((BitFieldOp)_kind));
        arguments.Add(_type);
        arguments.Add(Keywords.Number(_offset));

        if (_kind != 0)
        {
            arguments.Add(Keywords.Number(_value));
        }
    }
}

/// <summary>What a <see cref="StreamTrim"/> measures: the number of entries or the lowest id to keep.</summary>
public enum StreamTrimStrategy
{
    /// <summary>Keep at most a number of the newest entries (<c>MAXLEN</c>).</summary>
    MaxLen = 1,

    /// <summary>Remove entries with an id below a threshold (<c>MINID</c>).</summary>
    MinId = 2,
}

/// <summary>How exactly a <see cref="StreamTrim"/> must hit its threshold.</summary>
public enum StreamTrimMode
{
    /// <summary>Trim to exactly the threshold (<c>=</c>, the default).</summary>
    Exact,

    /// <summary>Trim at least to the threshold, which is allowed to keep a few more entries (<c>~</c>). Ruvio trims exactly in both modes.</summary>
    Approximate,
}

/// <summary>
/// One trim rule for <c>XADD</c> and <c>XTRIM</c>: a <see cref="StreamTrimStrategy"/> (<c>MAXLEN</c> or <c>MINID</c>), a
/// <see cref="StreamTrimMode"/> (<c>=</c> or <c>~</c>) and a threshold. The server does not accept <c>LIMIT</c>.
/// </summary>
/// <example>
/// <code>
/// await client.AddStreamEntryAsync("events", "*", StreamTrim.MaxLen(10_000, StreamTrimMode.Approximate), ("type", "login"));
/// long removed = await client.TrimStreamAsync("events", StreamTrim.MinId("1760000000000-0"));
/// </code>
/// </example>
public readonly struct StreamTrim
{
    private StreamTrim(StreamTrimStrategy strategy, StreamTrimMode mode, long length, string? id)
    {
        Strategy = strategy;
        Mode = mode;
        Length = length;
        Id = id;
    }

    /// <summary>Whether the rule counts entries or compares ids. A default-constructed value has neither and is refused when sent.</summary>
    public StreamTrimStrategy Strategy { get; }

    /// <summary>Whether the threshold is applied exactly or approximately.</summary>
    public StreamTrimMode Mode { get; }

    /// <summary>The most entries to keep; used when <see cref="Strategy"/> is <see cref="StreamTrimStrategy.MaxLen"/>.</summary>
    public long Length { get; }

    /// <summary>The lowest id to keep; used when <see cref="Strategy"/> is <see cref="StreamTrimStrategy.MinId"/>.</summary>
    public string? Id { get; }

    /// <summary>Keeps at most <paramref name="maxLength"/> of the newest entries (<c>MAXLEN [=|~] n</c>).</summary>
    /// <param name="maxLength">The most entries to keep. Must not be negative.</param>
    /// <param name="mode"><see cref="StreamTrimMode.Exact"/> (default) or <see cref="StreamTrimMode.Approximate"/>.</param>
    /// <returns>The trim rule.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxLength"/> is negative.</exception>
    /// <example>
    /// <code>
    /// StreamTrim trim = StreamTrim.MaxLen(1000, StreamTrimMode.Approximate);   // MAXLEN ~ 1000
    /// </code>
    /// </example>
    public static StreamTrim MaxLen(long maxLength, StreamTrimMode mode = StreamTrimMode.Exact)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxLength);

        return new StreamTrim(StreamTrimStrategy.MaxLen, mode, maxLength, null);
    }

    /// <summary>Removes entries with an id below <paramref name="id"/> (<c>MINID [=|~] id</c>).</summary>
    /// <param name="id">The lowest id to keep, as <c>ms-seq</c> or <c>ms</c>.</param>
    /// <param name="mode"><see cref="StreamTrimMode.Exact"/> (default) or <see cref="StreamTrimMode.Approximate"/>.</param>
    /// <returns>The trim rule.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is <see langword="null"/>.</exception>
    /// <example>
    /// <code>
    /// StreamTrim trim = StreamTrim.MinId("1760000000000-0");   // MINID = 1760000000000-0
    /// </code>
    /// </example>
    public static StreamTrim MinId(string id, StreamTrimMode mode = StreamTrimMode.Exact)
    {
        ArgumentNullException.ThrowIfNull(id);

        return new StreamTrim(StreamTrimStrategy.MinId, mode, 0, id);
    }
}

/// <summary>
/// The sections <c>INFO</c> can return. Unknown names make the server answer with an empty text, so the enum avoids typos.
/// </summary>
/// <example>
/// <code>
/// string keyspace = await client.GetInfoAsync(InfoSection.Keyspace);
/// string metrics = await client.GetInfoAsync(InfoSection.Prometheus);
/// </code>
/// </example>
public enum InfoSection
{
    /// <summary>Version, backend, health and persistence state (<c>server</c>).</summary>
    Server,

    /// <summary>Connection and network counters (<c>stats</c>).</summary>
    Stats,

    /// <summary>Input, output and Pub/Sub buffer peaks (<c>buffers</c>).</summary>
    Buffers,

    /// <summary>Charged memory (<c>memory</c>).</summary>
    Memory,

    /// <summary>Memory per kind: keys, values, collections, index (<c>memorybreakdown</c>).</summary>
    MemoryBreakdown,

    /// <summary>Key and expiry counts (<c>keyspace</c>).</summary>
    Keyspace,

    /// <summary>Snapshot state (<c>snapshot</c>).</summary>
    Snapshot,

    /// <summary>Whether cluster mode is on (<c>cluster</c>).</summary>
    Cluster,

    /// <summary>Replication role and sequences (<c>replication</c>).</summary>
    Replication,

    /// <summary>Command counters (<c>commandcounters</c>).</summary>
    CommandCounters,

    /// <summary>Command latency statistics (<c>latencystats</c>).</summary>
    LatencyStats,

    /// <summary>Log storage counters (<c>logs</c>).</summary>
    Logs,

    /// <summary>Slab allocator counters (<c>slabs</c>).</summary>
    Slabs,

    /// <summary>Every section (<c>all</c>).</summary>
    All,

    /// <summary>The default sections, the same as <c>INFO</c> without an argument (<c>default</c>).</summary>
    Default,

    /// <summary>The metrics in Prometheus text format instead of <c>INFO</c> lines (<c>prometheus</c>).</summary>
    Prometheus,
}

/// <summary>The connection kind a <c>CLIENT LIST TYPE</c> filter accepts.</summary>
/// <example>
/// <code>
/// string pubsubClients = await client.GetClientListAsync(ClientType.PubSub);
/// </code>
/// </example>
public enum ClientType
{
    /// <summary>Ordinary client connections (<c>normal</c>).</summary>
    Normal,

    /// <summary>Connections to a master (<c>master</c>).</summary>
    Master,

    /// <summary>Replica connections (<c>replica</c>).</summary>
    Replica,

    /// <summary>Connections subscribed to channels (<c>pubsub</c>).</summary>
    PubSub,
}

/// <summary>
/// Stream ids with a special meaning, as the exact strings the stream methods accept.
/// </summary>
/// <example>
/// <code>
/// var all = await client.GetStreamRangeAsync("events", StreamIds.Min, StreamIds.Max);
/// string id = await client.AddStreamEntryAsync("events", StreamIds.New, ("type", "login"));
/// </code>
/// </example>
public static class StreamIds
{
    /// <summary>Let the server pick the id of a new entry (<c>*</c>), for <c>XADD</c>.</summary>
    public const string New = "*";

    /// <summary>Only entries added after the group was created or last delivered (<c>$</c>), for <c>XGROUP CREATE</c> and <c>XREAD</c>.</summary>
    public const string LastDelivered = "$";

    /// <summary>Only entries never delivered to another consumer (<c>&gt;</c>), for <c>XREADGROUP</c>.</summary>
    public const string NewMessages = ">";

    /// <summary>The smallest possible id (<c>-</c>), for range methods.</summary>
    public const string Min = "-";

    /// <summary>The largest possible id (<c>+</c>), for range methods.</summary>
    public const string Max = "+";
}

/// <summary>
/// Score bounds for the sorted-set range methods (<c>min</c> and <c>max</c> strings), as the exact text the server parses.
/// </summary>
/// <example>
/// <code>
/// var top = await client.GetSortedSetRangeByScoreAsync("board", ScoreBound.Exclusive(10), ScoreBound.PositiveInfinity);
/// </code>
/// </example>
public static class ScoreBound
{
    /// <summary>Below every score (<c>-inf</c>).</summary>
    public const string NegativeInfinity = "-inf";

    /// <summary>Above every score (<c>+inf</c>).</summary>
    public const string PositiveInfinity = "+inf";

    /// <summary>Includes <paramref name="score"/> itself (plain number).</summary>
    /// <param name="score">The score.</param>
    /// <returns>The bound text, for example <c>1.5</c>.</returns>
    public static string Inclusive(double score) => RespRead.Number(score);

    /// <summary>Excludes <paramref name="score"/> itself (<c>(score</c>).</summary>
    /// <param name="score">The score.</param>
    /// <returns>The bound text, for example <c>(1.5</c>.</returns>
    public static string Exclusive(double score) => string.Concat("(", RespRead.Number(score));
}

/// <summary>
/// Lexicographic bounds for the sorted-set <c>BYLEX</c> methods (<c>min</c> and <c>max</c> strings).
/// </summary>
/// <example>
/// <code>
/// var names = await client.GetSortedSetRangeByLexAsync("names", LexBound.Inclusive("a"), LexBound.Exclusive("n"));
/// </code>
/// </example>
public static class LexBound
{
    /// <summary>Before every member (<c>-</c>).</summary>
    public const string Min = "-";

    /// <summary>After every member (<c>+</c>).</summary>
    public const string Max = "+";

    /// <summary>Includes <paramref name="member"/> (<c>[member</c>).</summary>
    /// <param name="member">The member.</param>
    /// <returns>The bound text, for example <c>[apple</c>.</returns>
    public static string Inclusive(string member) => string.Concat("[", member);

    /// <summary>Excludes <paramref name="member"/> (<c>(member</c>).</summary>
    /// <param name="member">The member.</param>
    /// <returns>The bound text, for example <c>(apple</c>.</returns>
    public static string Exclusive(string member) => string.Concat("(", member);
}
