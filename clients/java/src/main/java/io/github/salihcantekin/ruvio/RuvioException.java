// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * Base class of every exception thrown by this client. Unchecked. Its subclasses are a server error
 * reply, a failed connection and a timeout ({@link RuvioTimeoutException}); it is thrown itself for an
 * interrupted wait, a cancelled future and a failed convenience helper such as {@code getOrCreate}.
 * {@link RuvioErrors} tells which of them a retry can get past.
 */
public class RuvioException extends RuntimeException {
    /**
     * Creates the exception.
     *
     * @param message the description
     */
    public RuvioException(String message) {
        super(message);
    }

    /**
     * Creates the exception with a cause.
     *
     * @param message the description
     * @param cause the underlying error
     */
    public RuvioException(String message, Throwable cause) {
        super(message, cause);
    }
}
