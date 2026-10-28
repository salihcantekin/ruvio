// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

import java.time.Duration;

/**
 * Flags of {@code ZADD}. {@code NX} excludes {@code XX}, and {@code GT}, {@code LT} and {@code NX} exclude each other; the server answers {@code ERR} for an invalid mix.
 *
 * <p>{@code ZAddArgs.builder().xx().ch()}
 */
public final class ZAddArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public ZAddArgs() {
    }

    private Duration ttl;
    private boolean nx;
    private boolean xx;
    private boolean gt;
    private boolean lt;
    private boolean ch;

    /**
     * Starts an empty flag set.
     *
     * @return a new flag set
     */
    public static ZAddArgs builder() {
        return new ZAddArgs();
    }

    /**
     * Sets {@code NX}: only add new members, never update existing ones.
     *
     * @return this flag set
     */
    public ZAddArgs nx() {
        nx = true;
        return this;
    }

    /**
     * Sets {@code XX}: only update existing members, never add new ones.
     *
     * @return this flag set
     */
    public ZAddArgs xx() {
        xx = true;
        return this;
    }

    /**
     * Sets {@code GT}: update an existing member only when the new score is greater (new members are still added).
     *
     * @return this flag set
     */
    public ZAddArgs gt() {
        gt = true;
        return this;
    }

    /**
     * Sets {@code LT}: update an existing member only when the new score is lower (new members are still added).
     *
     * @return this flag set
     */
    public ZAddArgs lt() {
        lt = true;
        return this;
    }

    /**
     * Sets {@code CH}: make the reply count changed members as well as new ones.
     *
     * @return this flag set
     */
    public ZAddArgs ch() {
        ch = true;
        return this;
    }

    /**
     * Sets {@code EX} or {@code PX}: gives the sorted set a time to live, but only when this call creates it; an existing key keeps its own deadline.
     * Whole seconds are sent as {@code EX}, anything finer as {@code PX}.
     *
     * @param ttl the time to live (above zero)
     * @return this builder
     * @throws IllegalArgumentException when {@code ttl} is zero, negative or too large (checked when the command is built)
     */
    public ZAddArgs ttl(Duration ttl) {
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
        if (nx) a.s("NX");
        if (xx) a.s("XX");
        if (gt) a.s("GT");
        if (lt) a.s("LT");
        if (ch) a.s("CH");
    }
}
