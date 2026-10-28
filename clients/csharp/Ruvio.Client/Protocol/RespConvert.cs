using System.Globalization;
using System.Text;

namespace Ruvio.Client;

internal static class RespConvert
{
    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    internal static T? FromBulk<T>(byte[]? bytes)
    {
        if (bytes is null)
        {
            return default;
        }

        Type target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        if (target == typeof(byte[]))
        {
            return (T)(object)bytes;
        }

        string text;

        try
        {
            text = Utf8.GetString(bytes);
        }

        catch (DecoderFallbackException)
        {
            throw RuvioException.Client("bulk string is not UTF-8");
        }

        if (target == typeof(string))
        {
            return (T)(object)text;
        }

        if (target == typeof(long))
        {
            return (T)(object)ParseInt64(text);
        }

        if (target == typeof(int))
        {
            long value = ParseInt64(text);

            if (value is < int.MinValue or > int.MaxValue)
            {
                throw RuvioException.Client($"GET value {value} does not fit in Int32");
            }

            return (T)(object)(int)value;
        }

        throw RuvioException.Client($"GET cannot convert a bulk string to {typeof(T).Name}");
    }

    private static long ParseInt64(string text)
    {
        if (!long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
        {
            throw RuvioException.Client($"GET value is not an integer: {text}");
        }

        return value;
    }
}
