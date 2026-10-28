using System.Buffers;
using Microsoft.AspNetCore.Http;

namespace Ruvio.AspNetCore.Idempotency;

internal static class RequestBuffer
{
    // The body is read into pooled storage. On success the caller owns the array (the pool may hand out a
    // larger one than asked for, so only the first Length bytes count) and gives it back with Release; on a
    // rejection or an exception it has already been returned and Bytes is empty (Length still reports how much was read).
    internal static async Task<(byte[] Bytes, int Length, int? RejectionStatus)> ReadAsync(
        Stream input, long? contentLength, int limit, CancellationToken cancellation)
    {
        if (contentLength < 0)
            return ([], 0, StatusCodes.Status400BadRequest);

        if (contentLength > limit)
            return ([], 0, StatusCodes.Status413PayloadTooLarge);

        int capacity = contentLength is long known ? (int)known : Math.Min(4096, limit);
        byte[] bytes = capacity == 0 ? [] : ArrayPool<byte>.Shared.Rent(capacity);
        byte[]? probe = null;
        int length = 0;

        try
        {
            while (true)
            {
                if (length == capacity)
                {
                    probe ??= new byte[1];

                    if (await input.ReadAsync(probe, cancellation) == 0)
                        break;

                    if (length == limit)
                        return Reject(ref bytes, length, StatusCodes.Status413PayloadTooLarge);

                    if (contentLength.HasValue)
                        return Reject(ref bytes, length, StatusCodes.Status400BadRequest);

                    int grown = Math.Min(limit, Math.Max(1, capacity * 2));
                    byte[] larger = ArrayPool<byte>.Shared.Rent(grown);

                    bytes.AsSpan(0, length).CopyTo(larger);
                    Release(bytes);
                    bytes = larger;
                    capacity = grown;
                    bytes[length++] = probe[0];

                    continue;
                }

                int read = await input.ReadAsync(bytes.AsMemory(length, capacity - length), cancellation);

                if (read == 0)
                    break;

                length += read;
            }

            if (contentLength.HasValue && length != contentLength.Value)
                return Reject(ref bytes, length, StatusCodes.Status400BadRequest);
        }
        catch
        {
            Release(bytes);
            throw;
        }

        return (bytes, length, null);
    }

    internal static void Release(byte[]? bytes)
    {
        if (bytes is { Length: > 0 })
            ArrayPool<byte>.Shared.Return(bytes, clearArray: true);
    }

    private static (byte[] Bytes, int Length, int? RejectionStatus) Reject(ref byte[] bytes, int length, int status)
    {
        Release(bytes);
        bytes = [];

        return (bytes, length, status);
    }
}
