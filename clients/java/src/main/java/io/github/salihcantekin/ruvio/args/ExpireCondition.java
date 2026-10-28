// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.args;

import io.github.salihcantekin.ruvio.internal.RespOut;

/**
 * Condition flag of {@code EXPIRE}, {@code PEXPIRE}, {@code EXPIREAT}, {@code PEXPIREAT} and of {@code HEXPIRE} and the other
 * field-expiry commands. {@link #ALWAYS} sends no flag. A key or field without an expiry counts as infinite for {@link #GT} and {@link #LT}.
 */
public enum ExpireCondition {
    /** Set the expiry unconditionally (no flag is sent). */
    ALWAYS,
    /** Set it only when the field has no expiry. */
    NX,
    /** Set it only when the field already has an expiry. */
    XX,
    /** Set it only when the new expiry is later than the current one. */
    GT,
    /** Set it only when the new expiry is earlier than the current one. */
    LT;

    private final byte[] fragment;

    ExpireCondition() {
        this.fragment = RespOut.fragment(name());
    }

    /**
     * Returns the keyword pre-encoded as a complete RESP bulk string, identical on the wire to {@code name()}.
     *
     * @return the shared encoded bytes; do not modify them
     */
    public byte[] fragment() {
        return fragment;
    }
}
