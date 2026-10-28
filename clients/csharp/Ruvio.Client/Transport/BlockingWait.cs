using System.Globalization;
using System.Text;

namespace Ruvio.Client;

/// <summary>
/// How long a blocking command may legitimately wait, so the command timeout can add it. The rule is the
/// same in every Ruvio client: a command that carries its own wait gets <c>wait + CommandTimeout</c>; a wait
/// of <c>0</c> (block forever) or an argument that does not parse means no deadline for that call.
/// <list type="bullet">
/// <item><c>BLPOP</c>, <c>BRPOP</c>, <c>BZPOPMIN</c>, <c>BZPOPMAX</c>, <c>BRPOPLPUSH</c>, <c>BLMOVE</c>: last argument, seconds.</item>
/// <item><c>BLMPOP</c>, <c>BZMPOP</c>: first argument, seconds.</item>
/// <item><c>XREAD</c>, <c>XREADGROUP</c>: <c>BLOCK ms</c> (absent: not blocking).</item>
/// <item><c>WAIT</c>, <c>WAITAOF</c>: last argument, milliseconds.</item>
/// <item><c>FILLGET key fillMs WAIT waitMs</c>: <c>waitMs</c>.</item>
/// <item><c>SUBSCRIBE</c>, <c>PSUBSCRIBE</c>, <c>SSUBSCRIBE</c>, <c>MONITOR</c>: push mode, no deadline.</item>
/// </list>
/// A plain command costs a length and first-letter check and allocates nothing.
/// </summary>
internal static class BlockingWait
{
    /// <summary>The wait of a command that blocks with no bound: no deadline.</summary>
    internal static readonly TimeSpan Forever = Timeout.InfiniteTimeSpan;

    /// <summary>The wider of two waits (<see cref="TimeSpan.Zero"/>: not blocking).</summary>
    internal static TimeSpan Max(TimeSpan a, TimeSpan b) =>
        a == Forever || b == Forever ? Forever : a > b ? a : b;

    /// <summary>The wait of one typed command.</summary>
    internal static TimeSpan OfCommand(in RespCommand command)
    {
        string? name = command.Name;

        return name is not null && Candidate(name.Length, name[0]) ? Classify(command.AsList()) : TimeSpan.Zero;
    }

    /// <summary>The wait of one command given as strings, name first.</summary>
    internal static TimeSpan OfParts(IReadOnlyList<string> parts) =>
        parts.Count > 0 && parts[0] is { Length: > 0 } name && Candidate(name.Length, name[0])
            ? Classify(parts)
            : TimeSpan.Zero;

    /// <summary>The widest wait of a pipeline of commands given as strings.</summary>
    internal static TimeSpan OfPipeline(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        TimeSpan wait = TimeSpan.Zero;

        for (int index = 0; index < commands.Count && wait != Forever; index++)
        {
            wait = Max(wait, OfParts(commands[index]));
        }

        return wait;
    }

    /// <summary>The wait of one command given as raw arguments, name first.</summary>
    internal static TimeSpan OfArguments(IReadOnlyList<ReadOnlyMemory<byte>> arguments)
    {
        if (arguments.Count == 0 || arguments[0].Length == 0 || !Candidate(arguments[0].Length, (char)arguments[0].Span[0]))
            return TimeSpan.Zero;

        var parts = new string[arguments.Count];

        for (int index = 0; index < parts.Length; index++)
        {
            parts[index] = Encoding.UTF8.GetString(arguments[index].Span);
        }

        return Classify(parts);
    }

    /// <summary>The widest wait of the RESP-encoded commands in <paramref name="payload"/>.</summary>
    internal static TimeSpan OfPayload(ReadOnlySpan<byte> payload)
    {
        TimeSpan wait = TimeSpan.Zero;
        int position = 0;

        while (position < payload.Length && wait != Forever)
        {
            if (!Header(payload, ref position, (byte)'*', out int count) || count < 1)
                return wait;

            int nameStart = position;

            if (!Bulk(payload, ref position, out int nameOffset, out int nameLength))
                return wait;

            if (nameLength == 0 || !Candidate(nameLength, (char)payload[nameOffset]))
            {
                for (int index = 1; index < count; index++)
                {
                    if (!Bulk(payload, ref position, out _, out _))
                        return wait;
                }

                continue;
            }

            var parts = new string[count];
            position = nameStart;

            for (int index = 0; index < count; index++)
            {
                if (!Bulk(payload, ref position, out int offset, out int length))
                    return wait;

                parts[index] = Encoding.UTF8.GetString(payload.Slice(offset, length));
            }

            wait = Max(wait, Classify(parts));
        }

        return wait;
    }

    // Length and first letter of every name Classify knows; GET, SET, PING, ... stop here.
    private static bool Candidate(int length, char first)
    {
        int letter = first | 0x20;

        return length switch
        {
            4 => letter == 'w',
            5 => letter is 'b' or 'x',
            6 or 8 => letter == 'b',
            7 => letter is 'w' or 'f' or 'm',
            9 => letter == 's',
            10 => letter is 'b' or 'x' or 'p' or 's',
            _ => false,
        };
    }

    private static TimeSpan Classify(IReadOnlyList<string> parts)
    {
        string name = parts[0].ToUpperInvariant();
        int count = parts.Count - 1;

        switch (name)
        {
            case "BLPOP" or "BRPOP" or "BZPOPMIN" or "BZPOPMAX" or "BRPOPLPUSH" or "BLMOVE":
                return count == 0 ? Forever : Seconds(parts[^1]);
            case "BLMPOP" or "BZMPOP":
                return count == 0 ? Forever : Seconds(parts[1]);
            case "WAIT" or "WAITAOF":
                return count == 0 ? Forever : Milliseconds(parts[^1]);
            case "XREAD" or "XREADGROUP":
                // XREADGROUP GROUP <group> <consumer>: names that may spell BLOCK
                for (int index = name == "XREAD" ? 1 : 4; index < parts.Count; index++)
                {
                    if (string.Equals(parts[index], "STREAMS", StringComparison.OrdinalIgnoreCase))
                        return TimeSpan.Zero;

                    if (string.Equals(parts[index], "BLOCK", StringComparison.OrdinalIgnoreCase))
                        return index + 1 < parts.Count ? Milliseconds(parts[index + 1]) : Forever;
                }

                return TimeSpan.Zero;
            case "FILLGET":
                return parts.Count >= 5 && string.Equals(parts[3], "WAIT", StringComparison.OrdinalIgnoreCase)
                    ? Milliseconds(parts[4])
                    : TimeSpan.Zero;
            case "MONITOR" or "SUBSCRIBE" or "PSUBSCRIBE" or "SSUBSCRIBE":
                return Forever;
            default:
                return TimeSpan.Zero;
        }
    }

    private static TimeSpan Seconds(string text) =>
        double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double seconds)
        && seconds > 0
        && seconds < TimeSpan.MaxValue.TotalSeconds / 2
            ? TimeSpan.FromSeconds(seconds)
            : Forever;

    private static TimeSpan Milliseconds(string text) =>
        long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long milliseconds)
        && milliseconds > 0
        && milliseconds < (long)(TimeSpan.MaxValue.TotalMilliseconds / 2)
            ? TimeSpan.FromMilliseconds(milliseconds)
            : Forever;

    private static bool Header(ReadOnlySpan<byte> data, ref int position, byte prefix, out int value)
    {
        value = 0;

        if (position >= data.Length || data[position] != prefix)
            return false;

        int index = position + 1;

        while (index < data.Length && data[index] is >= (byte)'0' and <= (byte)'9')
        {
            value = value * 10 + (data[index] - '0');

            if (value > 1 << 30)
                return false;

            index++;
        }

        if (index == position + 1 || index + 1 >= data.Length || data[index] != '\r' || data[index + 1] != '\n')
            return false;

        position = index + 2;

        return true;
    }

    private static bool Bulk(ReadOnlySpan<byte> data, ref int position, out int offset, out int length)
    {
        offset = 0;

        if (!Header(data, ref position, (byte)'$', out length) || position + length + 2 > data.Length)
            return false;

        offset = position;
        position += length + 2;

        return true;
    }
}
