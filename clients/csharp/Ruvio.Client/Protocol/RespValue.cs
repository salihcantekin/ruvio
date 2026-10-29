using System.Text;

namespace Ruvio.Client;

/// <summary>RESP2 value kinds this client reads.</summary>
public enum RespKind
{
    /// <summary>A simple string, such as <c>+OK</c>.</summary>
    Simple,

    /// <summary>An error reply. Typed commands throw instead of returning this.</summary>
    Error,

    /// <summary>A signed integer.</summary>
    Integer,

    /// <summary>A bulk string, including the empty bulk.</summary>
    Bulk,

    /// <summary>An array of values.</summary>
    Array,

    /// <summary>A null bulk or a null array.</summary>
    Null,
}

/// <summary>One RESP2 value.</summary>
public sealed class RespValue
{
    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private RespValue(
        RespKind kind,
        string? text,
        long integer,
        byte[]? bytes,
        IReadOnlyList<RespValue>? items)
    {
        Kind = kind;
        Text = text;
        Integer = integer;
        Bytes = bytes;
        Items = items;
    }

    /// <summary>The RESP kind.</summary>
    public RespKind Kind { get; }

    /// <summary>Simple-string or error text. Null for bulk, array, integer, and null values.</summary>
    public string? Text { get; }

    /// <summary>The integer when <see cref="Kind"/> is <see cref="RespKind.Integer"/>.</summary>
    public long Integer { get; }

    /// <summary>Raw bulk bytes when <see cref="Kind"/> is <see cref="RespKind.Bulk"/>.</summary>
    public byte[]? Bytes { get; }

    /// <summary>Array elements when <see cref="Kind"/> is <see cref="RespKind.Array"/>.</summary>
    public IReadOnlyList<RespValue>? Items { get; }

    // Values are immutable, so frequent replies are shared instead of allocated per reply.
    private const long MinCachedInteger = -2;
    private const long MaxCachedInteger = 255;

    private static readonly RespValue NullValue = new(RespKind.Null, null, 0, null, null);
    private static readonly RespValue EmptyBulk = new(RespKind.Bulk, null, 0, System.Array.Empty<byte>(), null);
    private static readonly RespValue Ok = new(RespKind.Simple, "OK", 0, null, null);
    private static readonly RespValue Pong = new(RespKind.Simple, "PONG", 0, null, null);
    private static readonly RespValue Queued = new(RespKind.Simple, "QUEUED", 0, null, null);
    private static readonly RespValue[] Integers = CreateIntegers();

    private static RespValue[] CreateIntegers()
    {
        var values = new RespValue[MaxCachedInteger - MinCachedInteger + 1];

        for (int i = 0; i < values.Length; i++)
        {
            values[i] = new RespValue(RespKind.Integer, null, i + MinCachedInteger, null, null);
        }

        return values;
    }

    internal static RespValue Simple(string text) => text switch
    {
        "OK" => Ok,
        "PONG" => Pong,
        "QUEUED" => Queued,
        _ => new(RespKind.Simple, text, 0, null, null),
    };

    internal static RespValue Simple(ReadOnlySpan<byte> line)
    {
        if (line.SequenceEqual("OK"u8))
            return Ok;

        if (line.SequenceEqual("PONG"u8))
            return Pong;

        if (line.SequenceEqual("QUEUED"u8))
            return Queued;

        return new(RespKind.Simple, Encoding.UTF8.GetString(line), 0, null, null);
    }

    internal static RespValue Error(string text) => new(RespKind.Error, text, 0, null, null);

    internal static RespValue FromInteger(long value) =>
        value is >= MinCachedInteger and <= MaxCachedInteger
            ? Integers[value - MinCachedInteger]
            : new(RespKind.Integer, null, value, null, null);

    internal static RespValue Bulk(byte[] bytes) =>
        bytes.Length == 0 ? EmptyBulk : new(RespKind.Bulk, null, 0, bytes, null);

    internal static RespValue Null() => NullValue;

    internal static RespValue Array(IReadOnlyList<RespValue> items) =>
        new(RespKind.Array, null, 0, null, items);

    /// <summary>
    /// UTF-8 text of a simple string, error, or bulk string. An integer is its
    /// decimal form. Null when the value is null or an array (including a Pub/Sub
    /// push). Invalid UTF-8 in a bulk still throws.
    /// </summary>
    public string? AsString()
    {
        if (Kind is RespKind.Null or RespKind.Array)
        {
            return null;
        }

        if (Kind is RespKind.Integer)
        {
            return Integer.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (Kind is RespKind.Simple or RespKind.Error)
        {
            return Text;
        }

        if (Kind is RespKind.Bulk)
        {
            try
            {
                return Utf8.GetString(Bytes!);
            }

            catch (DecoderFallbackException)
            {
                throw RuvioException.Client("bulk string is not UTF-8");
            }
        }

        return null;
    }
}
