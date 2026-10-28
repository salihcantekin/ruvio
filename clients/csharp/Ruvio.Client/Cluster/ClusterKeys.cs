using System.Collections.Frozen;
using System.Globalization;
using System.Text;
using Ruvio.Client.Cluster;

namespace Ruvio.Client;

/// <summary>Hash slots and the key positions the cluster router needs.</summary>
internal static class ClusterKeys
{
    public const int SlotCount = 16384;

    /// <summary>What the router needs to know about a command name, found without allocating.</summary>
    internal sealed record CommandInfo(string Name, bool SingleKey, bool Split, bool Transaction);

    private static readonly FrozenDictionary<string, CommandInfo> Infos = CreateInfos();

    private static FrozenDictionary<string, CommandInfo> CreateInfos()
    {
        var infos = new Dictionary<string, CommandInfo>(StringComparer.OrdinalIgnoreCase);

        foreach (string name in CommandKeyRules.Rules.Keys
            .Concat(ClusterRouter.SplitNames).Concat(ClusterRouter.TransactionNames).Append("SPUBLISH"))
        {
            infos[name] = new CommandInfo(
                name,
                IsSingleKey(name),
                ClusterRouter.SplitNames.Contains(name),
                ClusterRouter.TransactionNames.Contains(name));
        }

        return infos.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>The router's facts about a command name in any letter case; null for a name the tables do not know.</summary>
    internal static CommandInfo? Info(string name) => Infos.GetValueOrDefault(name);

    /// <summary>
    /// The first key without allocating: the second argument for a command the table does not
    /// know and for the ordinary "first argument is the key" rule, the general lookup otherwise.
    /// </summary>
    internal static string? FirstKey(CommandInfo? info, IReadOnlyList<string> arguments)
    {
        if (info is null || info.SingleKey)
        {
            return arguments.Count >= 2 ? arguments[1] : null;
        }

        return First(info.Name, arguments);
    }

    public static string Name(IReadOnlyList<string> arguments)
    {
        return arguments[0].ToUpperInvariant();
    }

    /// <summary>True when the only key is the first argument, the common case for routing.</summary>
    public static bool IsSingleKey(string name) =>
        !CommandKeyRules.Rules.TryGetValue(name, out CommandKeyRules.Rule? rule)
            || rule is { Kind: CommandKeyRules.Range, A: 1, B: 1 }
            || name == "SPUBLISH";

    /// <summary>Whether a command only reads and may go to a replica: flagged readonly (2) and not blocking (16) in the command table.</summary>
    public static bool Reads(string name) =>
        CommandKeyRules.Rules.TryGetValue(name, out CommandKeyRules.Rule? rule)
        && (rule.Flags & 2) != 0 && (rule.Flags & 16) == 0;

    /// <summary>The first key of a command without building the key list; null for keyless commands.</summary>
    public static string? First(string name, IReadOnlyList<string> arguments)
    {
        int count = arguments.Count;

        if (count < 2)
        {
            return null;
        }

        if (!CommandKeyRules.Rules.TryGetValue(name, out CommandKeyRules.Rule? rule))
        {
            return arguments[1];
        }

        switch (rule.Kind)
        {
            case CommandKeyRules.Range:
                return rule.A <= Last(rule, count) && rule.A < count ? arguments[rule.A] : null;
            case CommandKeyRules.KeyAndStore:
                return rule.A < count ? arguments[rule.A] : null;
            case CommandKeyRules.None:
                return IsShardChannel(name) ? arguments[1] : null;
        }

        IReadOnlyList<string> keys = Apply(name, rule, arguments);

        return keys.Count > 0 ? keys[0] : null;
    }

    /// <summary>
    /// The keys of a command, in argument order, from the server's command table. Empty for
    /// keyless commands; a command the table does not know is keyed by its first argument.
    /// </summary>
    public static IReadOnlyList<string> Of(string name, IReadOnlyList<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return [];
        }

        if (!CommandKeyRules.Rules.TryGetValue(name, out CommandKeyRules.Rule? rule))
        {
            return [arguments[1]];
        }

        return Apply(name, rule, arguments);
    }

    private static IReadOnlyList<string> Apply(string name, CommandKeyRules.Rule rule, IReadOnlyList<string> arguments)
    {
        return rule.Kind switch
        {
            CommandKeyRules.Range => Every(arguments, rule.A, Last(rule, arguments.Count), rule.C),
            CommandKeyRules.NumKeys => Counted(arguments, rule.A, rule.B),
            CommandKeyRules.KeywordHalf => AfterKeyword(arguments, rule.Keywords[0]),
            CommandKeyRules.KeyAndStore => KeyAndStores(arguments, rule.A, rule.Keywords),
            // Sharded Pub/Sub has no keys but goes to the slot of its channels.
            _ when name == "SPUBLISH" => [arguments[1]],
            _ when IsShardChannel(name) => Slice(arguments, 1, arguments.Count - 1),
            _ => [],
        };
    }

    private static bool IsShardChannel(string name) => name is "SPUBLISH" or "SSUBSCRIBE" or "SUNSUBSCRIBE";

    private static int Last(CommandKeyRules.Rule rule, int count) => rule.B < 0 ? count + rule.B : rule.B;

    /// <summary>CRC16-XMODEM of the key, or of the first non-empty <c>{tag}</c> in it.</summary>
    public static int Slot(string key)
    {
        int bytes = Encoding.UTF8.GetByteCount(key);
        Span<byte> utf8 = stackalloc byte[256];
        byte[]? rented = null;
       
        if (bytes > utf8.Length)
        {
            rented = System.Buffers.ArrayPool<byte>.Shared.Rent(bytes);
            utf8 = rented;
        }

        try
        {
            Encoding.UTF8.GetBytes(key, utf8[..bytes]);

            return Slot(utf8[..bytes]);
        }

        finally
        {
            if (rented is not null)
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    internal static int Slot(ReadOnlySpan<byte> key)
    {
        int open = ByteScan.IndexOf(key, (byte)'{');

        if (open >= 0)
        {
            int close = ByteScan.IndexOf(key[(open + 1)..], (byte)'}');

            if (close > 0)
            {
                key = key.Slice(open + 1, close);
            }
        }

        return Crc16(key) % SlotCount;
    }

    private static readonly ushort[] CrcTable = CreateCrcTable();

    private static ushort[] CreateCrcTable()
    {
        var table = new ushort[256];

        for (int value = 0; value < table.Length; value++)
        {
            int crc = value << 8;

            for (int bit = 0; bit < 8; bit++)
            {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            }

            table[value] = (ushort)(crc & 0xFFFF);
        }

        return table;
    }

    private static int Crc16(ReadOnlySpan<byte> bytes)
    {
        int crc = 0;

        foreach (byte value in bytes)
        {
            crc = ((crc << 8) ^ CrcTable[((crc >> 8) ^ value) & 0xFF]) & 0xFFFF;
        }

        return crc;
    }

    private static string[] Slice(IReadOnlyList<string> arguments, int start, int length)
    {
        if (length <= 0)
        {
            return [];
        }

        var keys = new string[length];
        
        for (int index = 0; index < length; index++)
        {
            keys[index] = arguments[start + index];
        }

        return keys;
    }

    private static string[] Every(IReadOnlyList<string> arguments, int first, int last, int step)
    {
        last = Math.Min(last, arguments.Count - 1);

        if (last < first)
        {
            return [];
        }

        var keys = new string[(last - first) / step + 1];

        for (int index = 0; index < keys.Length; index++)
        {
            keys[index] = arguments[first + index * step];
        }

        return keys;
    }

    private static string[] Counted(IReadOnlyList<string> arguments, int countIndex, int before)
    {
        int count = arguments.Count;
        int keys = countIndex < count
            && ulong.TryParse(arguments[countIndex], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out ulong number)
                ? (int)Math.Min(number, (ulong)(count - countIndex - 1))
                : 0;

        int head = before > 0 && before < count ? 1 : 0;

        if (head + keys == 0)
        {
            return [];
        }

        var all = new string[head + keys];

        if (head == 1)
        {
            all[0] = arguments[before];
        }

        for (int index = 0; index < keys; index++)
        {
            all[head + index] = arguments[countIndex + 1 + index];
        }

        return all;
    }

    private static string[] AfterKeyword(IReadOnlyList<string> arguments, string keyword)
    {
        for (int index = 1; index < arguments.Count; index++)
        {
            if (arguments[index].Equals(keyword, StringComparison.OrdinalIgnoreCase))
            {
                int rest = arguments.Count - index - 1;

                return Slice(arguments, index + 1, rest / 2);
            }
        }

        return [];
    }

    private static List<string> KeyAndStores(IReadOnlyList<string> arguments, int first, string[] keywords)
    {
        var keys = new List<string>(2);

        if (first < arguments.Count)
        {
            keys.Add(arguments[first]);
        }

        for (int index = first + 1; index + 1 < arguments.Count; index++)
        {
            foreach (string keyword in keywords)
            {
                if (arguments[index].Equals(keyword, StringComparison.OrdinalIgnoreCase))
                {
                    keys.Add(arguments[++index]);

                    break;
                }
            }
        }

        return keys;
    }
}
