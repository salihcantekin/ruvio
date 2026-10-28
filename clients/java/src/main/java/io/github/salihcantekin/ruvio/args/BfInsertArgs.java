// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.Args;

import java.time.Duration;

/**
 * Options of {@code BF.INSERT}: how to create the Bloom filter when the key is missing (capacity, error rate, expansion), and whether to create it at all. The creation settings are ignored for an existing filter.
 *
 * <p>{@code BfInsertArgs.builder().capacity(10_000).error(0.01)}
 */
public final class BfInsertArgs {
    /** Creates an empty option set; the static factories are the usual way in. */
    public BfInsertArgs() {
    }

    private Duration ttl;
    private long capacity = -1;
    private double error = -1;
    private long expansion = -1;
    private boolean noCreate;
    private boolean nonScaling;

    /**
     * Starts an empty option set (server defaults).
     *
     * @return a new option set
     */
    public static BfInsertArgs builder() {
        return new BfInsertArgs();
    }

    /**
     * Sets {@code CAPACITY}: how many items the new filter is sized for.
     *
     * @param capacity the expected number of items (above 0)
     * @return this builder
     */
    public BfInsertArgs capacity(long capacity) {
        this.capacity = capacity;
        return this;
    }

    /**
     * Sets {@code ERROR}: the false-positive rate of the new filter.
     *
     * @param rate the rate, between 0 and 1 exclusive (for example {@code 0.01})
     * @return this builder
     */
    public BfInsertArgs error(double rate) {
        this.error = rate;
        return this;
    }

    /**
     * Sets {@code EXPANSION}: the growth factor when the filter fills up and adds a sub-filter.
     *
     * @param expansion the factor (at least 1)
     * @return this builder
     */
    public BfInsertArgs expansion(long expansion) {
        this.expansion = expansion;
        return this;
    }

    /**
     * Sets {@code NOCREATE}: fail with {@code ERR not found} instead of creating a missing filter.
     *
     * @return this builder
     */
    public BfInsertArgs noCreate() {
        noCreate = true;
        return this;
    }

    /**
     * Sets {@code NONSCALING}: do not add sub-filters; inserting into a full filter then fails with an error.
     *
     * @return this builder
     */
    public BfInsertArgs nonScaling() {
        nonScaling = true;
        return this;
    }

    /**
     * Sets {@code EX} or {@code PX}: gives the filter a time to live, but only when this call creates it; an existing key keeps its own deadline.
     * Whole seconds are sent as {@code EX}, anything finer as {@code PX}.
     *
     * @param ttl the time to live (above zero)
     * @return this builder
     * @throws IllegalArgumentException when {@code ttl} is zero, negative or too large (checked when the command is built)
     */
    public BfInsertArgs ttl(Duration ttl) {
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
        if (capacity >= 0) a.s("CAPACITY").l(capacity);
        if (error >= 0) a.s("ERROR").d(error);
        if (expansion >= 0) a.s("EXPANSION").l(expansion);
        if (noCreate) a.s("NOCREATE");
        if (nonScaling) a.s("NONSCALING");
    }
}
