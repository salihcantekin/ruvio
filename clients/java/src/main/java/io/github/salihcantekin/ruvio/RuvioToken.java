// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.Link;

/**
 * A held LEASE (fencing token) or SEMAPHORE slot, released with {@code RELEASE key token}. Pass
 * {@link #token()} to the protected resource as the fencing token. Use with try-with-resources. Get one
 * from {@link RuvioClient#tryLease(Object, long)} or {@link RuvioClient#tryAcquirePermit(Object, long, long)}.
 *
 * <pre>{@code
 * try (RuvioToken<String> permit = client.tryAcquirePermit("uploads", 4, 60_000)) {
 *     if (permit != null) {
 *         // at most 4 holders at a time
 *     }
 * }
 * }</pre>
 *
 * @param <K> the key type
 */
public final class RuvioToken<K> implements AutoCloseable {
    private final RuvioAsync<K, ?> commands;
    private final K key;
    private final long token;
    private final long timeout;
    private volatile boolean released;

    RuvioToken(RuvioAsync<K, ?> commands, K key, long token, long timeout) {
        this.commands = commands;
        this.key = key;
        this.token = token;
        this.timeout = timeout;
    }

    /**
     * Returns the lease or semaphore key.
     *
     * @return the key
     */
    public K key() {
        return key;
    }

    /**
     * Returns the number the server gave for this hold. For a lease it is the fencing token.
     *
     * @return the token
     */
    public long token() {
        return token;
    }

    /**
     * Extends the hold to now + {@code ttlMillis} with {@code RENEW key token ttl}. The token does not change.
     *
     * @param ttlMillis the new lifetime in milliseconds
     * @return {@code true} when extended, {@code false} when it had expired or was already released
     */
    public boolean renew(long ttlMillis) {
        if (released) {
            return false;
        }
        return Link.await(commands.renew(key, token, ttlMillis), timeout);
    }

    /**
     * Releases the lease or slot with {@code RELEASE key token}. Calling it again does nothing.
     *
     * @return {@code true} when this call released it, {@code false} when it had expired or was already released
     */
    public boolean release() {
        if (released) {
            return false;
        }
        released = true;
        return Link.await(commands.release(key, token), timeout);
    }

    /** Releases the hold, same as {@link #release()}. */
    @Override
    public void close() {
        release();
    }
}
