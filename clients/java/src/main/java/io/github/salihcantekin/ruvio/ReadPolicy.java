// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * Where a cluster client sends read-only commands (those the server flags {@code readonly} and not
 * blocking, such as {@code GET}, {@code HGETALL} or {@code ZRANGE}). Replicas are found in
 * {@code CLUSTER SLOTS}, which lists each shard's replicas after its primary. A standalone connection
 * ignores the policy.
 *
 * <p>Replication is asynchronous: a read from a replica may miss recent writes, including the caller's own.
 * Writes, transactions, blocking commands, scripts not flagged {@code readonly}, Pub/Sub, and any pipeline
 * that holds a write always go to the primaries.
 */
public enum ReadPolicy {
    /** Every command goes to the slot's primary, as before replicas were listed. This is the default. */
    PRIMARY,
    /**
     * Reads go to one replica of the slot's shard when one is known and reachable, otherwise to the primary. A
     * replica that cannot be opened, or answers {@code LOADING}, is skipped until the slot map is reloaded.
     */
    PREFER_REPLICA,
    /** Reads go to a replica of the slot's shard; with none known or reachable they fail with a {@link RuvioException}. */
    REPLICA
}
