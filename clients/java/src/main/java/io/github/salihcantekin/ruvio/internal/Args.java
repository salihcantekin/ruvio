// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioCodec;

import java.time.Duration;

/**
 * Argument builder for commands with optional parts. Arguments are encoded immediately into a
 * private buffer that is copied into the connection buffer in one {@code arraycopy}. Hot fixed-shape
 * commands do not use this class and encode straight into the connection buffer.
 */
public final class Args {
    /** The private buffer the arguments are encoded into. */
    public final RespOut out = new RespOut(128);
    /** How many arguments were added so far. */
    public int count;
    private final RuvioCodec<Object> keys;
    private final RuvioCodec<Object> values;

    /**
     * Creates a builder.
     *
     * @param keys the codec for keys
     * @param values the codec for values
     */
    @SuppressWarnings("unchecked")
    public Args(RuvioCodec<?> keys, RuvioCodec<?> values) {
        this.keys = (RuvioCodec<Object>) keys;
        this.values = (RuvioCodec<Object>) values;
    }

    /**
     * Adds a pre-encoded word (command name, keyword).
     *
     * @param fragment the complete RESP bulk string, as in {@link Cmd}
     * @return this builder
     */
    public Args w(byte[] fragment) {
        out.raw(fragment);
        count++;
        return this;
    }

    /**
     * Adds a text argument.
     *
     * @param text the text (UTF-8)
     * @return this builder
     */
    public Args s(String text) {
        out.bulk(text);
        count++;
        return this;
    }

    /**
     * Adds a binary argument.
     *
     * @param bytes the bytes
     * @return this builder
     */
    public Args b(byte[] bytes) {
        out.bulk(bytes);
        count++;
        return this;
    }

    /**
     * Adds an integer argument.
     *
     * @param value the number
     * @return this builder
     */
    public Args l(long value) {
        out.bulk(value);
        count++;
        return this;
    }

    /**
     * Adds a floating point argument.
     *
     * @param value the number
     * @return this builder
     */
    public Args d(double value) {
        out.bulk(value);
        count++;
        return this;
    }

    /**
     * Adds a key encoded with the key codec.
     *
     * @param key the key (not null)
     * @return this builder
     */
    public Args k(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("null key");
        }
        keys.write(out, key);
        count++;
        return this;
    }

    /**
     * Adds a value encoded with the value codec.
     *
     * @param value the value (not null)
     * @return this builder
     */
    public Args v(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("null value");
        }
        values.write(out, value);
        count++;
        return this;
    }

    /**
     * Adds several keys.
     *
     * @param keys the keys
     * @return this builder
     */
    public Args keys(Object[] keys) {
        for (Object key : keys) {
            k(key);
        }
        return this;
    }

    /**
     * Adds several values.
     *
     * @param values the values
     * @return this builder
     */
    public Args values(Object[] values) {
        for (Object value : values) {
            v(value);
        }
        return this;
    }

    /**
     * Adds several text arguments.
     *
     * @param items the texts
     * @return this builder
     */
    public Args strings(String[] items) {
        for (String item : items) {
            s(item);
        }
        return this;
    }

    /**
     * Adds an argument of any supported type (number, text, bytes).
     *
     * @param value the argument
     * @return this builder
     */
    public Args any(Object value) {
        out.arg(value);
        count++;
        return this;
    }

    /** The longest accepted time to live in whole seconds; keeps the millisecond form from overflowing. */
    private static final long MAX_TTL_SECONDS = Long.MAX_VALUE / 1000;

    /**
     * Adds the time to live of a TTL-on-create command: {@code EX seconds} for a whole number of
     * seconds, {@code PX milliseconds} otherwise (a sub-millisecond rest rounds up to 1 ms).
     *
     * @param ttl the time to live (above zero)
     * @return this builder
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     */
    public Args ttl(Duration ttl) {
        if (ttl == null) {
            throw new IllegalArgumentException("ttl must not be null");
        }
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be above zero");
        }
        if (ttl.getSeconds() >= MAX_TTL_SECONDS) {
            throw new IllegalArgumentException("ttl too large");
        }
        if (ttl.getNano() == 0) {
            return w(Cmd.EX).l(ttl.getSeconds());
        }
        long millis = ttl.getSeconds() * 1000 + (ttl.getNano() + 999_999) / 1_000_000;
        return w(Cmd.PX).l(millis);
    }
}
