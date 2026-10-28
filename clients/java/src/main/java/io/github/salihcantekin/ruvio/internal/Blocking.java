// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * How long a blocking command may legitimately wait, so the command timeout can add it. The rule is
 * the same in every Ruvio client: a command that carries its own wait gets {@code wait + commandTimeoutMillis};
 * a wait of {@code 0} (block forever) or an argument that does not parse means no deadline for that call.
 *
 * <ul>
 * <li>{@code BLPOP}, {@code BRPOP}, {@code BZPOPMIN}, {@code BZPOPMAX}, {@code BRPOPLPUSH}, {@code BLMOVE}: last argument, seconds.</li>
 * <li>{@code BLMPOP}, {@code BZMPOP}: first argument, seconds.</li>
 * <li>{@code XREAD}, {@code XREADGROUP}: {@code BLOCK ms} (absent: not blocking).</li>
 * <li>{@code WAIT}, {@code WAITAOF}: last argument, milliseconds.</li>
 * <li>{@code FILLGET key fillMs WAIT waitMs}: {@code waitMs}.</li>
 * <li>{@code SUBSCRIBE}, {@code PSUBSCRIBE}, {@code SSUBSCRIBE}, {@code MONITOR}: push mode, no deadline.</li>
 * </ul>
 *
 * Internal: not part of the supported API.
 */
public final class Blocking {
    /** The wait of a command that blocks with no bound. */
    public static final long FOREVER = -1;

    private Blocking() {
    }

    /** A request for a blocking command: carries its wait for the synchronous facade. */
    public static final class Req<T> extends Request<T> {
        /** The wait in milliseconds, or {@link #FOREVER}. */
        public final long waitMillis;

        Req(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values, long waitMillis) {
            super(reply, keys, values);
            this.waitMillis = waitMillis;
        }
    }

    /** A routed (cluster) request for a blocking command. */
    public static final class RoutedReq<T> extends RoutedRequest<T> {
        /** The wait in milliseconds, or {@link #FOREVER}. */
        public final long waitMillis;

        RoutedReq(Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values, long waitMillis) {
            super(reply, keys, values);
            this.waitMillis = waitMillis;
        }
    }

    /**
     * Creates the request for a blocking command (a plain or a routed one).
     *
     * @param routed true in cluster mode
     * @param reply the reply decoder
     * @param keys the key codec
     * @param values the value codec
     * @param waitMillis the wait from {@link #waitMillis}
     * @param <T> the result type
     * @return the request
     */
    public static <T> Request<T> request(boolean routed, Reply<T> reply, RuvioCodec<?> keys, RuvioCodec<?> values, long waitMillis) {
        if (routed) {
            return new RoutedReq<>(reply, keys, values, waitMillis);
        }
        return new Req<>(reply, keys, values, waitMillis);
    }

    /**
     * The wait a future returned by a command carries: 0 for a plain command.
     *
     * @param f the future
     * @return milliseconds, 0, or {@link #FOREVER}
     */
    public static long waitOf(CompletableFuture<?> f) {
        if (f instanceof Req) {
            return ((Req<?>) f).waitMillis;
        }
        if (f instanceof RoutedReq) {
            return ((RoutedReq<?>) f).waitMillis;
        }
        return 0;
    }

    /**
     * The timeout for one synchronous call: {@code timeoutMillis} plus the wait, or 0 (none) for a
     * command that waits forever.
     *
     * @param timeoutMillis the command timeout, greater than 0
     * @param f the future
     * @return the timeout in milliseconds; 0 means wait forever
     */
    public static long timeoutFor(long timeoutMillis, CompletableFuture<?> f) {
        long w = waitOf(f);
        if (w == 0) {
            return timeoutMillis;
        }
        return w < 0 || w > Long.MAX_VALUE - timeoutMillis ? 0 : timeoutMillis + w;
    }

    // Scan state of the last bulk string read.
    private static final class Scan {
        int off;
        int len;
        int next;
    }

    /**
     * The wait of the command whose arguments (name first, each a RESP bulk string, no array header)
     * are {@code b[0..end)}. A plain command costs a length and first-letter check and allocates nothing.
     *
     * @param b the encoded arguments
     * @param end the end offset
     * @param argc the number of arguments, name included
     * @return milliseconds, 0 when it does not block, or {@link #FOREVER}
     */
    public static long waitMillis(byte[] b, int end, int argc) {
        // "$<len>\r\n<name>\r\n"
        if (end < 4 || b[0] != '$') {
            return 0;
        }
        int len;
        int s;
        if (b[2] == '\r') {
            len = b[1] - '0';
            s = 4;
        } else if (end > 4 && b[3] == '\r') {
            len = (b[1] - '0') * 10 + (b[2] - '0');
            s = 5;
        } else {
            return 0;
        }
        if (s + len > end || len < 4) {
            return 0;
        }
        int f = b[s] | 0x20;
        boolean candidate;
        switch (len) {
            case 4:
                candidate = f == 'w';
                break;
            case 5:
                candidate = f == 'b' || f == 'x';
                break;
            case 6:
            case 8:
                candidate = f == 'b';
                break;
            case 7:
                candidate = f == 'w' || f == 'f' || f == 'm';
                break;
            case 9:
                candidate = f == 's';
                break;
            case 10:
                candidate = f == 'b' || f == 'x' || f == 'p' || f == 's';
                break;
            default:
                candidate = false;
        }
        return candidate ? classify(b, end, s, len, argc) : 0;
    }

    private static long classify(byte[] b, int end, int s, int len, int argc) {
        int n = argc - 1;
        Scan sc = new Scan();
        int p = s + len + 2;
        if (is(b, s, len, "BLPOP") || is(b, s, len, "BRPOP") || is(b, s, len, "BZPOPMIN")
                || is(b, s, len, "BZPOPMAX") || is(b, s, len, "BRPOPLPUSH") || is(b, s, len, "BLMOVE")) {
            return n == 0 || !skip(b, end, p, n, sc) ? FOREVER : seconds(b, sc);
        }
        if (is(b, s, len, "BLMPOP") || is(b, s, len, "BZMPOP")) {
            return n == 0 || !skip(b, end, p, 1, sc) ? FOREVER : seconds(b, sc);
        }
        if (is(b, s, len, "WAIT") || is(b, s, len, "WAITAOF")) {
            return n == 0 || !skip(b, end, p, n, sc) ? FOREVER : millis(b, sc);
        }
        if (is(b, s, len, "XREAD") || is(b, s, len, "XREADGROUP")) {
            // XREADGROUP GROUP <group> <consumer>: names that may spell BLOCK
            int from = len == 5 ? 0 : 3;
            for (int i = 0; i < n; i++) {
                if (!skip(b, end, p, 1, sc)) {
                    return FOREVER;
                }
                p = sc.next;
                if (i < from) {
                    continue;
                }
                if (is(b, sc.off, sc.len, "STREAMS")) {
                    return 0;
                }
                if (is(b, sc.off, sc.len, "BLOCK")) {
                    return i + 1 < n && skip(b, end, p, 1, sc) ? millis(b, sc) : FOREVER;
                }
            }
            return 0;
        }
        if (is(b, s, len, "FILLGET")) {
            if (n < 4 || !skip(b, end, p, 3, sc) || !is(b, sc.off, sc.len, "WAIT")) {
                return 0;
            }
            return skip(b, end, sc.next, 1, sc) ? millis(b, sc) : FOREVER;
        }
        if (is(b, s, len, "MONITOR") || is(b, s, len, "SUBSCRIBE") || is(b, s, len, "PSUBSCRIBE")
                || is(b, s, len, "SSUBSCRIBE")) {
            return FOREVER;
        }
        return 0;
    }

    /** Reads {@code count} bulk strings from {@code p}; the last one is left in {@code sc}. */
    private static boolean skip(byte[] b, int end, int p, int count, Scan sc) {
        for (int k = 0; k < count; k++) {
            if (p >= end || b[p] != '$') {
                return false;
            }
            int i = p + 1;
            int n = 0;
            while (i < end && b[i] >= '0' && b[i] <= '9') {
                n = n * 10 + (b[i] - '0');
                if (n > (1 << 30)) {
                    return false;
                }
                i++;
            }
            if (i == p + 1 || i + 1 >= end || b[i] != '\r') {
                return false;
            }
            sc.off = i + 2;
            sc.len = n;
            p = sc.off + n + 2;
            if (p > end) {
                return false;
            }
            sc.next = p;
        }
        return true;
    }

    private static boolean is(byte[] b, int off, int len, String word) {
        if (len != word.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int c = b[off + i];
            if (c >= 'a' && c <= 'z') {
                c -= 32;
            }
            if (c != word.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static long seconds(byte[] b, Scan sc) {
        try {
            double v = Double.parseDouble(new String(b, sc.off, sc.len, StandardCharsets.US_ASCII));
            if (!(v > 0) || Double.isInfinite(v) || v * 1000 >= Long.MAX_VALUE / 2) {
                return FOREVER;
            }
            return Math.max(1, (long) Math.ceil(v * 1000));
        } catch (NumberFormatException e) {
            return FOREVER;
        }
    }

    private static long millis(byte[] b, Scan sc) {
        try {
            long v = Long.parseLong(new String(b, sc.off, sc.len, StandardCharsets.US_ASCII));
            return v > 0 ? v : FOREVER;
        } catch (NumberFormatException e) {
            return FOREVER;
        }
    }
}
