using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace Ruvio.Client;

internal readonly struct PooledFrame : IDisposable
{
    internal PooledFrame(byte[] buffer, int length)
    {
        Buffer = buffer;
        Length = length;
    }

    internal byte[] Buffer { get; }

    internal int Length { get; }

    public void Dispose()
    {
        if (Buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(Buffer);
        }
    }
}

internal static class RespCodec
{
    internal const int MaxLineBytes = 1024 * 1024;

    internal const int MaxBulkBytes = 64 * 1024 * 1024;

    internal const int MaxArrayLength = 1024 * 1024;

    private static readonly Encoding Utf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    internal static byte[] Encode(IReadOnlyList<ReadOnlyMemory<byte>> arguments)
    {
        using PooledFrame frame = EncodeBinaryPooled(arguments);

        return frame.Buffer.AsSpan(0, frame.Length).ToArray();
    }

    internal static PooledFrame CopyPooled(ReadOnlySpan<byte> payload)
    {
        byte[] rented = ArrayPool<byte>.Shared.Rent(payload.Length);
        payload.CopyTo(rented);

        return new PooledFrame(rented, payload.Length);
    }

    internal static PooledFrame EncodeBinaryPooled(IReadOnlyList<ReadOnlyMemory<byte>> arguments)
    {
        if (arguments.Count == 0)
        {
            throw new ArgumentException("A command needs at least one argument.", nameof(arguments));
        }

        int size = HeaderSize(arguments.Count);

        for (int index = 0; index < arguments.Count; index++)
        {
            size += BulkSize(arguments[index].Length);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            return new PooledFrame(rented, WriteBytes(rented.AsSpan(0, size), arguments));
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    internal static byte[] Encode(IReadOnlyList<string> arguments)
    {
        using PooledFrame frame = EncodePooled(arguments);

        return frame.Buffer.AsSpan(0, frame.Length).ToArray();
    }

    internal static PooledFrame EncodePooled(IReadOnlyList<string> arguments)
    {
        if (arguments is string[] array)
        {
            // Arrays skip the covariant IReadOnlyList<T> interface dispatch on every element.
            return EncodePooled(array.AsSpan());
        }

        int size = MeasureCommand(arguments);
        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            return new PooledFrame(rented, WriteStrings(rented.AsSpan(0, size), 0, arguments));
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    internal static PooledFrame EncodeManyPooled(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        int size = Measure(commands);
        byte[] rented = ArrayPool<byte>.Shared.Rent(size);
        
        try
        {
            int written = WriteCommands(rented.AsSpan(0, size), commands);

            return new PooledFrame(rented, written);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    /// <summary>
    /// Encodes only the commands whose <paramref name="owner"/> entry equals <paramref name="node"/>,
    /// in order, into one pooled frame; used to split a pipeline over several connections.
    /// </summary>
    internal static PooledFrame EncodeSelectedPooled(
        IReadOnlyList<IReadOnlyList<string>> commands, int[] owner, int node)
    {
        int size = 0;

        for (int index = 0; index < commands.Count; index++)
        {
            if (owner[index] == node)
            {
                size += MeasureCommand(commands[index]);
            }
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            Span<byte> destination = rented.AsSpan(0, size);
            int cursor = 0;

            for (int index = 0; index < commands.Count; index++)
            {
                if (owner[index] == node)
                {
                    cursor = WriteStrings(destination, cursor, commands[index]);
                }
            }

            return new PooledFrame(rented, cursor);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    internal static byte[] EncodeMany(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        using PooledFrame frame = EncodeManyPooled(commands);

        return frame.Buffer.AsSpan(0, frame.Length).ToArray();
    }

    private static int Measure(IReadOnlyList<IReadOnlyList<string>> commands)
    {
        int size = 0;

        for (int index = 0; index < commands.Count; index++)
        {
            size += MeasureCommand(commands[index]);
        }

        return size;
    }

    private static PooledFrame EncodePooled(ReadOnlySpan<string> arguments)
    {
        if (arguments.Length == 0)
        {
            throw new ArgumentException("A command needs at least one argument.", nameof(arguments));
        }

        int size = HeaderSize(arguments.Length);

        foreach (string argument in arguments)
        {
            size += BulkSize(Utf8.GetByteCount(argument));
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            Span<byte> destination = rented.AsSpan(0, size);
            int cursor = WriteHeader(destination, 0, arguments.Length);

            foreach (string argument in arguments)
            {
                cursor = WriteBulkHeader(destination, cursor, Utf8.GetByteCount(argument));
                cursor += Utf8.GetBytes(argument, destination[cursor..]);
                destination[cursor++] = (byte)'\r';
                destination[cursor++] = (byte)'\n';
            }

            return new PooledFrame(rented, cursor);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(rented);
            throw;
        }
    }

    private static int MeasureCommand(IReadOnlyList<string> command)
    {
        if (command.Count == 0)
        {
            throw new ArgumentException("A command needs at least one argument.", nameof(command));
        }

        int size = HeaderSize(command.Count);

        for (int index = 0; index < command.Count; index++)
        {
            size += BulkSize(Utf8.GetByteCount(command[index]));
        }

        return size;
    }

    internal static PooledFrame WriteVerb(ReadOnlySpan<byte> verb, string argument)
    {
        int argumentBytes = Utf8.GetByteCount(argument);
        int size = HeaderSize(2) + BulkSize(verb.Length) + BulkSize(argumentBytes);
        byte[] rented = ArrayPool<byte>.Shared.Rent(size);
        Span<byte> destination = rented.AsSpan(0, size);
        int cursor = WriteHeader(destination, 0, 2);

        cursor = WriteRawBulk(destination, cursor, verb);
        cursor = WriteStringBulk(destination, cursor, argument, argumentBytes);

        return new PooledFrame(rented, cursor);
    }

    internal static PooledFrame WriteVerb(ReadOnlySpan<byte> verb, ReadOnlySpan<string> arguments)
    {
        int size = HeaderSize(arguments.Length + 1) + BulkSize(verb.Length);
        Span<int> counts = arguments.Length <= 16 ? stackalloc int[arguments.Length] : new int[arguments.Length];

        for (int index = 0; index < arguments.Length; index++)
        {
            counts[index] = Utf8.GetByteCount(arguments[index]);
            size += BulkSize(counts[index]);
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);
        Span<byte> destination = rented.AsSpan(0, size);
        int cursor = WriteHeader(destination, 0, arguments.Length + 1);

        cursor = WriteRawBulk(destination, cursor, verb);

        for (int index = 0; index < arguments.Length; index++)
        {
            cursor = WriteStringBulk(destination, cursor, arguments[index], counts[index]);
        }

        return new PooledFrame(rented, cursor);
    }

    private static int WriteRawBulk(Span<byte> destination, int cursor, ReadOnlySpan<byte> payload)
    {
        cursor = WriteBulkHeader(destination, cursor, payload.Length);
        payload.CopyTo(destination[cursor..]);
        cursor += payload.Length;
        destination[cursor++] = (byte)'\r';
        destination[cursor++] = (byte)'\n';

        return cursor;
    }

    private static int WriteStringBulk(Span<byte> destination, int cursor, string argument, int bytes)
    {
        cursor = WriteBulkHeader(destination, cursor, bytes);
        cursor += Utf8.GetBytes(argument, destination[cursor..]);
        destination[cursor++] = (byte)'\r';
        destination[cursor++] = (byte)'\n';

        return cursor;
    }

    private static int WriteCommands(Span<byte> destination, IReadOnlyList<IReadOnlyList<string>> commands)
    {
        int cursor = 0;

        for (int index = 0; index < commands.Count; index++)
        {
            cursor = WriteStrings(destination, cursor, commands[index]);
        }

        return cursor;
    }

    private delegate int SpanWriter(Span<byte> destination);

    // Small frames are encoded on the stack and copied out once, so no scratch array is allocated per command.
    private static byte[] Finish(int size, SpanWriter write)
    {
        if (size <= 2048)
        {
            Span<byte> stack = stackalloc byte[2048];
            int written = write(stack[..size]);

            return stack[..written].ToArray();
        }

        byte[] rented = ArrayPool<byte>.Shared.Rent(size);

        try
        {
            int written = write(rented.AsSpan(0, size));

            return rented.AsSpan(0, written).ToArray();
        }

        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static int WriteBytes(Span<byte> destination, IReadOnlyList<ReadOnlyMemory<byte>> arguments)
    {
        int cursor = WriteHeader(destination, 0, arguments.Count);

        for (int index = 0; index < arguments.Count; index++)
        {
            ReadOnlyMemory<byte> argument = arguments[index];
            cursor = WriteBulkHeader(destination, cursor, argument.Length);
            argument.Span.CopyTo(destination[cursor..]);
            cursor += argument.Length;
            destination[cursor++] = (byte)'\r';
            destination[cursor++] = (byte)'\n';
        }

        return cursor;
    }

    private static int WriteStrings(Span<byte> destination, int cursor, IReadOnlyList<string> arguments)
    {
        cursor = WriteHeader(destination, cursor, arguments.Count);

        for (int index = 0; index < arguments.Count; index++)
        {
            string argument = arguments[index];
            int bytes = Utf8.GetByteCount(argument);

            cursor = WriteBulkHeader(destination, cursor, bytes);
            cursor += Utf8.GetBytes(argument, destination[cursor..]);
            destination[cursor++] = (byte)'\r';
            destination[cursor++] = (byte)'\n';
        }

        return cursor;
    }

    private static int WriteHeader(Span<byte> destination, int cursor, int count)
    {
        destination[cursor++] = (byte)'*';
        cursor += WriteDecimal(destination[cursor..], count);
        destination[cursor++] = (byte)'\r';
        destination[cursor++] = (byte)'\n';

        return cursor;
    }

    private static int WriteBulkHeader(Span<byte> destination, int cursor, int count)
    {
        destination[cursor++] = (byte)'$';
        cursor += WriteDecimal(destination[cursor..], count);
        destination[cursor++] = (byte)'\r';
        destination[cursor++] = (byte)'\n';

        return cursor;
    }

    private static int WriteDecimal(Span<byte> destination, int value)
    {
        Span<byte> digits = stackalloc byte[10];
        int count = 0;
        int remaining = value;

        do
        {
            digits[count++] = (byte)('0' + (remaining % 10));
            remaining /= 10;
        }

        while (remaining > 0);

        for (int index = 0; index < count; index++)
        {
            destination[index] = digits[count - 1 - index];
        }

        return count;
    }

    private static int HeaderSize(int count)
    {
        return 3 + DecimalDigits(count);
    }

    private static int BulkSize(int bytes)
    {
        return 3 + DecimalDigits(bytes) + bytes + 2;
    }

    private static int DecimalDigits(int value)
    {
        int digits = 1;

        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }
}

internal sealed class RespReader : IDisposable
{
    private readonly Stream _stream;
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(8192);
    private int _start;
    private int _end;

    internal RespReader(Stream stream)
    {
        _stream = stream;
    }

    /// <summary>True after this reply's first byte is taken. A closed socket before that can be retried.</summary>
    internal bool ReplyStarted { get; private set; }

    internal void PrepareReply()
    {
        ReplyStarted = false;
    }

    /// <summary>True when bytes of the next reply are already buffered.</summary>
    internal bool HasBuffered => _start < _end;

    /// <summary>
    /// Waits until at least one byte is buffered, without taking it. A cancelled wait leaves the
    /// reader where it was, so the next read starts the same reply.
    /// </summary>
    internal ValueTask WaitForDataAsync(CancellationToken cancellationToken) =>
        _start < _end ? default : EnsureAsync(1, cancellationToken);

    internal ValueTask<RespValue> ReadAsync(CancellationToken cancellationToken)
    {
        // Completed scalar replies need only their owned response data, not parser Tasks.
        if (TryReadBuffered(out RespValue? reply))
        {
            return new ValueTask<RespValue>(reply!);
        }

        return ReadSlowAsync(cancellationToken);
    }

    private bool TryReadBuffered(out RespValue? reply)
    {
        reply = null;

        if (_start == _end)
        {
            return false;
        }

        int prefix = _buffer[_start];

        if (prefix != (byte)'+' && prefix != (byte)'-' && prefix != (byte)':' && prefix != (byte)'$')
        {
            return false;
        }

        int saved = _start;
        _start++;
        ReplyStarted = true;

        if (!TryTakeLineBytes(out ReadOnlySpan<byte> line, out _))
        {
            _start = saved;

            return false;
        }

        switch (prefix)
        {
            case (byte)'+':
                reply = RespValue.Simple(line);
                break;
            case (byte)'-':
                reply = RespValue.Error(Encoding.UTF8.GetString(line));
                break;
            case (byte)':':
            case (byte)'$':
                if (!TryParseDecimal(line, out long value))
                {
                    throw new RuvioException("bad RESP integer");
                }

                if (prefix == (byte)':')
                {
                    reply = RespValue.FromInteger(value);
                    break;
                }

                if (value == -1)
                {
                    reply = RespValue.Null();
                    break;
                }

                if (value < 0 || value > RespCodec.MaxBulkBytes)
                {
                    throw new RuvioException("bulk length is out of range");
                }

                if (_end - _start < value + 2)
                {
                    _start = saved;

                    return false;
                }

                int length = (int)value;

                if (_buffer[_start + length] != (byte)'\r' || _buffer[_start + length + 1] != (byte)'\n')
                {
                    throw new RuvioException("bulk payload was not terminated");
                }

                reply = RespValue.Bulk(_buffer.AsSpan(_start, length).ToArray());
                _start += length + 2;
                break;
            default:
                throw new RuvioException($"unexpected RESP prefix 0x{prefix:x2}");
        }

        return true;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ReadSlowAsync(CancellationToken cancellationToken)
    {
        await EnsureAsync(1, cancellationToken);

        if (TryReadBuffered(out RespValue? buffered))
            return buffered!;

        int prefix = _buffer[_start++];

        ReplyStarted = true;

        return prefix switch
        {
            (byte)'+' => await ReadSimpleAsync(cancellationToken),
            (byte)'-' => RespValue.Error(await ReadLineAsync(cancellationToken)),
            (byte)':' => RespValue.FromInteger(await ReadIntegerLineAsync(cancellationToken)),
            (byte)'$' => await ReadBulkAsync(cancellationToken),
            (byte)'*' => await ReadArrayAsync(cancellationToken),
            _ => throw new RuvioException($"unexpected RESP prefix 0x{prefix:x2}"),
        };
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ReadBulkAsync(CancellationToken cancellationToken)
    {
        long length = await ReadIntegerLineAsync(cancellationToken);

        if (length == -1)
        {
            return RespValue.Null();
        }

        if (length < 0 || length > RespCodec.MaxBulkBytes)
        {
            throw new RuvioException("bulk length is out of range");
        }

        var payload = new byte[length];
        int filled = 0;

        while (filled < payload.Length)
        {
            int available = _end - _start;

            if (available == 0)
            {
                await EnsureAsync(1, cancellationToken);
                continue;
            }

            int count = Math.Min(available, payload.Length - filled);

            Buffer.BlockCopy(_buffer, _start, payload, filled, count);
            _start += count;
            filled += count;
        }

        await EnsureAsync(2, cancellationToken);

        if (_buffer[_start] != (byte)'\r' || _buffer[_start + 1] != (byte)'\n')
        {
            throw new RuvioException("bulk payload was not terminated");
        }

        _start += 2;

        return RespValue.Bulk(payload);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ReadArrayAsync(CancellationToken cancellationToken)
    {
        long length = await ReadIntegerLineAsync(cancellationToken);

        if (length == -1)
        {
            return RespValue.Null();
        }

        if (length < 0 || length > RespCodec.MaxArrayLength)
        {
            throw new RuvioException("array length is out of range");
        }

        var items = new RespValue[(int)length];

        for (int index = 0; index < items.Length; index++)
        {
            items[index] = await ReadAsync(cancellationToken);
        }

        return RespValue.Array(items);
    }

    public void Dispose()
    {
        byte[] buffer = _buffer;

        _buffer = [];

        if (buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<long> ReadIntegerLineAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryTakeInteger(out long value, out int need))
            {
                return value;
            }

            await EnsureAsync(need, cancellationToken);
        }
    }

    private bool TryTakeInteger(out long value, out int need)
    {
        value = 0;

        if (!TryTakeLineBytes(out ReadOnlySpan<byte> line, out need))
        {
            return false;
        }

        if (!TryParseDecimal(line, out value))
        {
            throw new RuvioException("bad RESP integer");
        }

        return true;
    }

    private static bool TryParseDecimal(ReadOnlySpan<byte> text, out long value)
    {
        value = 0;

        if (text.IsEmpty)
        {
            return false;
        }

        int index = 0;
        bool negative = text[0] == (byte)'-';

        if (negative)
        {
            index = 1;

            if (text.Length == 1)
            {
                return false;
            }
        }

        // Accumulated as a negative number: long.MinValue has no positive counterpart.
        long accumulator = 0;

        for (; index < text.Length; index++)
        {
            int digit = text[index] - (byte)'0';

            if ((uint)digit > 9)
            {
                return false;
            }

            try
            {
                accumulator = checked(accumulator * 10 - digit);
            }

            catch (OverflowException)
            {
                return false;
            }
        }

        try
        {
            value = negative ? accumulator : checked(-accumulator);
        }

        catch (OverflowException)
        {
            return false;
        }

        return true;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<RespValue> ReadSimpleAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryTakeSimple(out RespValue? value, out int need))
            {
                return value!;
            }

            await EnsureAsync(need, cancellationToken);
        }
    }

    private bool TryTakeSimple(out RespValue? value, out int need)
    {
        value = TryTakeLineBytes(out ReadOnlySpan<byte> line, out need) ? RespValue.Simple(line) : null;

        return value is not null;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<string> ReadLineAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryTakeLine(out string? text, out int need))
            {
                return text!;
            }

            await EnsureAsync(need, cancellationToken);
        }
    }

    private bool TryTakeLine(out string? text, out int need)
    {
        text = null;

        if (!TryTakeLineBytes(out ReadOnlySpan<byte> line, out need))
        {
            return false;
        }

        text = Encoding.UTF8.GetString(line);

        return true;
    }

    private bool TryTakeLineBytes(out ReadOnlySpan<byte> line, out int need)
    {
        line = default;
        ReadOnlySpan<byte> pending = _buffer.AsSpan(_start, _end - _start);
        int cr = ByteScan.IndexOf(pending, (byte)'\r');

        if (cr >= 0 && cr + 1 < pending.Length)
        {
            if (pending[cr + 1] != (byte)'\n')
            {
                throw new RuvioException("expected LF after CR");
            }

            if (cr > RespCodec.MaxLineBytes)
            {
                throw new RuvioException("RESP line is too long");
            }

            line = pending[..cr];
            _start += cr + 2;
            need = 0;
            
            return true;
        }

        if (pending.Length > RespCodec.MaxLineBytes)
        {
            throw new RuvioException("RESP line is too long");
        }

        need = pending.Length + 1;

        return false;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask EnsureAsync(int count, CancellationToken cancellationToken)
    {
        while (_end - _start < count)
        {
            if (_start > 0)
            {
                Buffer.BlockCopy(_buffer, _start, _buffer, 0, _end - _start);
                _end -= _start;
                _start = 0;
            }

            if (_end + count > _buffer.Length)
            {
                byte[] bigger = ArrayPool<byte>.Shared.Rent(Math.Max(_buffer.Length * 2, _end + count));

                Buffer.BlockCopy(_buffer, 0, bigger, 0, _end);
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = bigger;
            }

            int read = await _stream.ReadAsync(_buffer.AsMemory(_end, _buffer.Length - _end), cancellationToken);

            if (read == 0)
            {
                throw new RuvioException("connection closed", closedBeforeReply: !ReplyStarted);
            }

            _end += read;
        }
    }

}
