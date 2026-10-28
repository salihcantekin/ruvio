// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-place RESP2/RESP3 reply parser over a reusable {@code byte[]}. Every read either consumes a
 * complete element or throws {@link #INCOMPLETE}, in which case the caller rewinds {@link #pos} to
 * the start of the reply and reads more bytes. Scalars are decoded straight from the buffer, so an
 * integer, OK or nil reply allocates nothing beyond the returned value.
 */
public final class RespReader {
    /** Thrown when the buffer ends inside a reply. Preallocated, no stack trace. */
    public static final class Incomplete extends RuntimeException {
        private Incomplete() {
            super(null, null, false, false);
        }
    }

    /** The shared {@link Incomplete} instance. */
    public static final Incomplete INCOMPLETE = new Incomplete();

    /** A RESP3 push frame ({@code >}): a list of elements. */
    public static final class Push extends ArrayList<Object> {
        Push(int n) {
            super(n);
        }
    }

    /** The bytes being read. */
    public byte[] buf;
    /** The read position. */
    public int pos;
    /** The end of the valid bytes. */
    public int lim;

    /**
     * Creates a reader.
     *
     * @param buf the bytes to read; call {@link #wrap} to set the range
     */
    public RespReader(byte[] buf) {
        this.buf = buf;
    }

    /**
     * Points the reader at a byte range.
     *
     * @param b the bytes
     * @param p the first index to read
     * @param l the end index, exclusive
     */
    public void wrap(byte[] b, int p, int l) {
        buf = b;
        pos = p;
        lim = l;
    }

    /**
     * Returns the type byte at the read position without consuming it.
     *
     * @return the byte
     * @throws Incomplete when no bytes are left
     */
    public byte peek() {
        if (pos >= lim) {
            throw INCOMPLETE;
        }
        return buf[pos];
    }

    /** Index of the '\r' that ends the line starting at {@code from}. */
    private int lineEnd(int from) {
        byte[] b = buf;
        int l = lim - 1;
        for (int i = from; i < l; i++) {
            if (b[i] == '\r') {
                return i;
            }
        }
        throw INCOMPLETE;
    }

    private long parseLong(int from, int end) {
        byte[] b = buf;
        int i = from;
        boolean neg = false;
        if (i < end && b[i] == '-') {
            neg = true;
            i++;
        } else if (i < end && b[i] == '+') {
            i++;
        }
        if (i == end) {
            throw new RuvioException("protocol error: empty integer");
        }
        long v = 0;
        for (; i < end; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) {
                throw new RuvioException("protocol error: bad integer " + new String(b, from, end - from, StandardCharsets.ISO_8859_1));
            }
            v = v * 10 - d;
        }
        return neg ? v : -v;
    }

    /** Reads {@code <type><long>\r\n} and returns the long (aggregate length, bulk length or integer). */
    private long headerLong() {
        int start = pos + 1;
        int end = lineEnd(start);
        long v = parseLong(start, end);
        pos = end + 2;
        return v;
    }

    /** Longest bulk string a reply may announce (64 MiB, as in the other clients). */
    static final long MAX_BULK_BYTES = 64L * 1024 * 1024;

    /** Most elements one array, set, push, map or attribute may announce (1,048,576, as in the other clients). */
    static final long MAX_ELEMENTS = 1L << 20;

    private long bulkHeader() {
        long v = headerLong();
        if (v > MAX_BULK_BYTES) {
            throw new RuvioException("protocol error: bulk length " + v + " is over the limit of " + MAX_BULK_BYTES);
        }
        return v;
    }

    private long countHeader() {
        long v = headerLong();
        if (v > MAX_ELEMENTS) {
            throw new RuvioException("protocol error: element count " + v + " is over the limit of " + MAX_ELEMENTS);
        }
        return v;
    }

    /**
     * Reads one line after the type byte as a String (simple string, error, big number).
     *
     * @return the text
     */
    public String readLine() {
        int start = pos + 1;
        int end = lineEnd(start);
        String s = new String(buf, start, end - start, StandardCharsets.UTF_8);
        pos = end + 2;
        return s;
    }

    /**
     * Consumes the current element when it is a null (RESP2 nil bulk/array or RESP3 {@code _}).
     *
     * @return {@code true} when it was a null
     */
    public boolean tryNull() {
        byte t = peek();
        if (t == '_') {
            lineEnd(pos + 1);
            pos += 3;
            return true;
        }
        if ((t == '$' || t == '*') && pos + 2 < lim && buf[pos + 1] == '-' && buf[pos + 2] == '1') {
            int end = lineEnd(pos + 1);
            pos = end + 2;
            return true;
        }
        return false;
    }

    /**
     * Reads the server error at the current position, consuming it.
     *
     * @return the exception to throw
     */
    public RuvioServerException readError() {
        return new RuvioServerException(readLine());
    }

    /** RESP3 blob error ({@code !<len>\r\n<message>\r\n}); consumes it. */
    private RuvioServerException readBlobError() {
        long len = bulkHeader();
        if (len < 0 || pos + len + 2 > lim) {
            if (len < 0) {
                throw new RuvioException("protocol error: negative blob error length");
            }
            throw INCOMPLETE;
        }
        String message = new String(buf, pos, (int) len, StandardCharsets.UTF_8);
        pos += (int) len + 2;
        return new RuvioServerException(message);
    }

    /** Skips attribute frames ({@code |}) that precede a reply. */
    private void skipAttributes() {
        while (peek() == '|') {
            long n = countHeader();
            for (long i = 0; i < n * 2; i++) {
                skip();
            }
        }
    }

    /** The length of the payload found by {@link #stringSpan()}. */
    public int span;

    /**
     * Locates the payload of a string-like element and sets {@link #span} to its length. Integers and doubles are returned as their text.
     *
     * @return the payload offset in {@link #buf}, or -1 for nil
     */
    public int stringSpan() {
        skipAttributes();
        byte t = peek();
        switch (t) {
            case '$': {
                long len = bulkHeader();
                if (len < 0) {
                    return -1;
                }
                int off = pos;
                if (off + len + 2 > lim) {
                    pos = off; // caller rewinds anyway
                    throw INCOMPLETE;
                }
                pos = off + (int) len + 2;
                span = (int) len;
                return off;
            }
            case '+': case ':': case ',': case '(': {
                int start = pos + 1;
                int end = lineEnd(start);
                pos = end + 2;
                span = end - start;
                return start;
            }
            case '=': {
                long len = bulkHeader();
                int off = pos;
                if (off + len + 2 > lim) {
                    throw INCOMPLETE;
                }
                pos = off + (int) len + 2;
                int skipPrefix = len >= 4 ? 4 : 0; // "txt:"
                span = (int) len - skipPrefix;
                return off + skipPrefix;
            }
            case '_':
                lineEnd(pos + 1);
                pos += 3;
                return -1;
            case '*':
                if (tryNull()) {
                    return -1;
                }
                throw unexpected("string");
            case '-':
                throw readError();
            case '!':
                throw readBlobError();
            default:
                throw unexpected("string");
        }
    }

    /**
     * Reads a bulk string and decodes it with a codec.
     *
     * @param codec the codec
     * @param <T> the decoded type
     * @return the value, or null for nil
     */
    public <T> T readBulk(RuvioCodec<T> codec) {
        int off = stringSpan();
        return off < 0 ? null : codec.decode(buf, off, span);
    }

    /**
     * Reads a string-like element as UTF-8 text.
     *
     * @return the text, or null for nil
     */
    public String readString() {
        int off = stringSpan();
        return off < 0 ? null : new String(buf, off, span, StandardCharsets.UTF_8);
    }

    /**
     * Reads a string-like element as a copy of its bytes.
     *
     * @return the bytes, or null for nil
     */
    public byte[] readBytes() {
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        byte[] out = new byte[span];
        System.arraycopy(buf, off, out, 0, span);
        return out;
    }

    /**
     * Reads an integer reply (or a bulk holding decimal digits).
     *
     * @return the number, or null for nil
     */
    public Long readLongOrNull() {
        skipAttributes();
        byte t = peek();
        if (t == ':') {
            return headerLong();
        }
        if (t == '#') {
            int end = lineEnd(pos + 1);
            boolean v = buf[pos + 1] == 't';
            pos = end + 2;
            return v ? 1L : 0L;
        }
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        return parseLongText(off, span);
    }

    private long parseLongText(int off, int len) {
        return parseLong(off, off + len);
    }

    /**
     * Reads a double from a bulk, simple string, integer or RESP3 double.
     *
     * @return the number, or null for nil
     */
    public Double readDoubleOrNull() {
        int off = stringSpan();
        if (off < 0) {
            return null;
        }
        return parseDouble(buf, off, span);
    }

    /**
     * Parses a number written as text, including {@code inf}, {@code -inf} and {@code nan}.
     *
     * @param b the bytes
     * @param off where the text starts
     * @param len the text length
     * @return the number
     */
    public static double parseDouble(byte[] b, int off, int len) {
        // Fast path: plain integers.
        boolean simple = len > 0 && len < 18;
        long v = 0;
        int i = off;
        int end = off + len;
        boolean neg = false;
        if (simple && (b[i] == '-' || b[i] == '+')) {
            neg = b[i] == '-';
            i++;
            simple = i < end;
        }
        for (; simple && i < end; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) {
                simple = false;
                break;
            }
            v = v * 10 + d;
        }
        if (simple) {
            return neg ? -v : v;
        }
        String s = new String(b, off, len, StandardCharsets.ISO_8859_1);
        switch (s) {
            case "inf": case "+inf": case "Infinity": case "+Infinity":
                return Double.POSITIVE_INFINITY;
            case "-inf": case "-Infinity":
                return Double.NEGATIVE_INFINITY;
            case "nan": case "NaN":
                return Double.NaN;
            default:
                return Double.parseDouble(s);
        }
    }

    /**
     * Reads an aggregate header. RESP3 maps return twice their entry count so callers can treat them as flat arrays.
     *
     * @return the element count, or -1 for nil
     * @throws RuvioServerException when the element is an error
     */
    public int arrayLength() {
        skipAttributes();
        byte t = peek();
        if (t == '*' || t == '~' || t == '>') {
            long n = countHeader();
            return (int) n;
        }
        if (t == '%') {
            return (int) (countHeader() * 2);
        }
        if (t == '_' || t == '$') {
            if (tryNull()) {
                return -1;
            }
        }
        if (t == '-') {
            throw readError();
        }
        throw unexpected("array");
    }

    /**
     * Builds the exception for a reply of the wrong type.
     *
     * @param wanted a description of the expected type
     * @return the exception to throw
     */
    public RuvioException unexpected(String wanted) {
        byte t = pos < lim ? buf[pos] : 0;
        return new RuvioException("unexpected reply type '" + (char) t + "', wanted " + wanted);
    }

    /** Skips one complete element of any type. */
    public void skip() {
        byte t = peek();
        switch (t) {
            case '+': case '-': case ':': case ',': case '(': case '#': case '_': {
                int end = lineEnd(pos + 1);
                pos = end + 2;
                return;
            }
            case '$': case '=': case '!': {
                long len = bulkHeader();
                if (len < 0) {
                    return;
                }
                if (pos + len + 2 > lim) {
                    throw INCOMPLETE;
                }
                pos += (int) len + 2;
                return;
            }
            case '*': case '~': case '>': case '%': case '|': {
                long n = countHeader();
                if (t == '%' || t == '|') {
                    n *= 2;
                }
                for (long i = 0; i < n; i++) {
                    skip();
                }
                if (t == '|') {
                    skip();
                }
                return;
            }
            default:
                throw new RuvioException("protocol error: unknown reply type '" + (char) t + "'");
        }
    }

    /**
     * Generic decode into a tree: bulk to byte[], simple string to String, integer to Long, nil to null, array/set to List, map to LinkedHashMap (bulk keys become Strings), error element to a {@link RuvioServerException} instance, double to Double, boolean to Boolean, push to {@link Push}.
     *
     * @return the tree
     */
    public Object readObject() {
        byte t = peek();
        switch (t) {
            case '$': case '=': {
                return readBytes();
            }
            case '+':
                return readLine();
            case '(':
                return readLine();
            case ':':
                return headerLong();
            case '-':
                return readError();
            case '!':
                return readBlobError();
            case ',': {
                int off = stringSpan();
                return parseDouble(buf, off, span);
            }
            case '#': {
                int end = lineEnd(pos + 1);
                boolean v = buf[pos + 1] == 't';
                pos = end + 2;
                return v;
            }
            case '_': {
                lineEnd(pos + 1);
                pos += 3;
                return null;
            }
            case '*': case '~': case '>': {
                long n = countHeader();
                if (n < 0) {
                    return null;
                }
                List<Object> list = t == '>' ? new Push((int) n) : new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(readObject());
                }
                return list;
            }
            case '%': {
                long n = countHeader();
                Map<Object, Object> map = new LinkedHashMap<>();
                for (long i = 0; i < n; i++) {
                    Object k = readObject();
                    if (k instanceof byte[]) {
                        k = new String((byte[]) k, StandardCharsets.UTF_8);
                    }
                    map.put(k, readObject());
                }
                return map;
            }
            case '|': {
                skipAttributes();
                return readObject();
            }
            default:
                throw new RuvioException("protocol error: unknown reply type '" + (char) t + "'");
        }
    }
}
