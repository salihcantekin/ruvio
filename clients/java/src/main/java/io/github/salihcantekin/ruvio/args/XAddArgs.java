// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

import java.time.Duration;

/**
 * Options of {@code XADD}: entry id, {@code NOMKSTREAM}, {@code MAXLEN}/{@code MINID} trimming and the Ruvio-only {@code DELAY} hold.
 *
 * <p>{@code XAddArgs.builder().maxLen(10_000).delay(5_000)}
 */
public final class XAddArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public XAddArgs() {
    }

    private Duration ttl;
    private String id = "*";
    private boolean noMkStream;
    private long maxLen = -1;
    private String minId;
    private boolean approximate;
    private StreamTrim trim;
    private long delayMillis = -1;

    /**
     * Starts an empty option set (auto-generated id, no trimming).
     *
     * @return a new option set
     */
    public static XAddArgs builder() {
        return new XAddArgs();
    }

    /**
     * Sets the entry id.
     *
     * @param id an explicit id such as {@code 1700000000000-0} or {@code 1700000000000-*}; the default {@code *} generates one. An id not greater than the last one is rejected by the server
     * @return this builder
     */
    public XAddArgs id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets {@code NOMKSTREAM}: do not create the stream when it is missing ({@code xadd} then returns {@code null}).
     *
     * @return this builder
     */
    public XAddArgs noMkStream() {
        noMkStream = true;
        return this;
    }

    /**
     * Sets {@code MAXLEN [~] n}: trim the stream to the newest entries. Ruvio trims exactly either way. Replaces any {@code minId}.
     *
     * @param maxLen the most entries to keep
     * @param approximate whether to send {@code ~}; it makes no difference in Ruvio
     * @return this builder
     */
    public XAddArgs maxLen(long maxLen, boolean approximate) {
        this.maxLen = maxLen;
        this.approximate = approximate;
        return this;
    }

    /**
     * Sets {@code MAXLEN n}: trim the stream to the newest entries.
     *
     * @param maxLen the most entries to keep
     * @return this builder
     */
    public XAddArgs maxLen(long maxLen) {
        return maxLen(maxLen, false);
    }

    /**
     * Sets {@code MINID}: drop entries with an id lower than this. Ignored when {@code maxLen} is also set.
     *
     * @param minId the smallest id to keep
     * @return this builder
     */
    public XAddArgs minId(String minId) {
        this.minId = minId;
        return this;
    }

    /**
     * Sets the trim rule from a {@link StreamTrim}: {@code MAXLEN|MINID =|~ threshold}, sent from pre-encoded keyword bytes. Replaces {@code maxLen} and {@code minId}.
     * Example: {@code XAddArgs.builder().trim(StreamTrim.maxLen(10_000, StreamTrim.Mode.APPROXIMATE))}.
     *
     * @param trim the trim rule
     * @return this builder
     */
    public XAddArgs trim(StreamTrim trim) {
        this.trim = trim;
        return this;
    }

    /**
     * Sets the Ruvio {@code DELAY}: hide the entry from {@code XREADGROUP} for this long (delayed jobs).
     *
     * @param millis the hold in milliseconds (0 or more)
     * @return this builder
     */
    public XAddArgs delay(long millis) {
        this.delayMillis = millis;
        return this;
    }

    /**
     * Sets {@code EX} or {@code PX}: gives the stream a time to live, but only when this call creates it; an existing key keeps its own deadline.
     * Whole seconds are sent as {@code EX}, anything finer as {@code PX}.
     *
     * @param ttl the time to live (above zero)
     * @return this builder
     * @throws IllegalArgumentException when {@code ttl} is zero, negative or too large (checked when the command is built)
     */
    public XAddArgs ttl(Duration ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * Appends these options to a command being encoded. Used by the client; you do not call it.
     *
     * @param a the argument writer
     */
    public void write(Args a) {
        if (ttl != null) a.ttl(ttl);
        if (noMkStream) a.s("NOMKSTREAM");
        if (delayMillis >= 0) a.s("DELAY").l(delayMillis);
        if (trim != null) {
            trim.write(a);
        } else if (maxLen >= 0) {
            a.s("MAXLEN");
            if (approximate) a.s("~");
            a.l(maxLen);
        } else if (minId != null) {
            a.s("MINID");
            if (approximate) a.s("~");
            a.s(minId);
        }
        a.s(id);
    }
}
