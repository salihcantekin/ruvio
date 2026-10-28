// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Sorts the exceptions a Ruvio call throws, for a retry or circuit-breaker policy of your own (resilience4j,
 * Failsafe or a loop): whether trying again later can succeed, and whether the failed call may already have
 * changed data. A {@link CompletionException} or {@link ExecutionException} from the async API is judged by its
 * cause.
 *
 * <p>The client already follows {@code MOVED}/{@code ASK}, waits out {@code TRYAGAIN} during a slot move and finds
 * a new primary after a failover; these helpers judge what still reaches the caller. Retry a call only when
 * {@link #isTransient} is true and either the command is safe to repeat (a read, a {@code SET} of the same value, a
 * command carrying an idempotency key) or {@link #mayHaveRun} is false:
 *
 * <pre>{@code
 * RetryConfig config = RetryConfig.custom()
 *     .maxAttempts(3)
 *     // INCR is not safe to repeat: retry it only when it surely did not run.
 *     .retryOnException(e -> RuvioErrors.isTransient(e) && !RuvioErrors.mayHaveRun(e))
 *     .build();
 * long visits = Retry.of("visits", config).executeSupplier(() -> client.incr("visits"));
 * }</pre>
 */
public final class RuvioErrors {
    private RuvioErrors() {
    }

    /**
     * Tells whether the same call can succeed later without any change on the caller's side: the connection failed
     * or could not be opened ({@link RuvioConnectionException}), no reply came in time
     * ({@link RuvioTimeoutException}), or the server answered {@code LOADING}, {@code MASTERDOWN},
     * {@code CLUSTERDOWN}, {@code TRYAGAIN}, {@code READONLY} or {@code NOREPLICAS}. False for every other server
     * reply (such as {@code WRONGTYPE}, {@code OOM} or a syntax error), for an interrupted or cancelled wait and for
     * any other exception.
     *
     * @param error the exception the call threw
     * @return whether retrying later can succeed
     */
    public static boolean isTransient(Throwable error) {
        Throwable e = unwrap(error);
        if (e instanceof RuvioServerException) {
            switch (((RuvioServerException) e).code()) {
                case "LOADING":
                case "MASTERDOWN":
                case "CLUSTERDOWN":
                case "TRYAGAIN":
                case "READONLY":
                case "NOREPLICAS":
                    return true;
                default:
                    return false;
            }
        }
        return e instanceof RuvioConnectionException || e instanceof RuvioTimeoutException;
    }

    /**
     * Tells whether the failed call may have changed data, so repeating a write that is not safe to repeat could
     * apply it twice. False only for a server error reply, which the server sends instead of running the command;
     * {@code NOREPLICAS} is the exception, because the write was applied on the primary and only the replica
     * confirmation timed out. A failed connection, a timeout or an interrupted wait may have run.
     *
     * @param error the exception the call threw
     * @return whether the command may have been applied
     */
    public static boolean mayHaveRun(Throwable error) {
        Throwable e = unwrap(error);
        if (e instanceof RuvioServerException) {
            return "NOREPLICAS".equals(((RuvioServerException) e).code());
        }
        return true;
    }

    private static Throwable unwrap(Throwable error) {
        if (error == null) {
            throw new NullPointerException("error");
        }
        Throwable e = error;
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }
}
