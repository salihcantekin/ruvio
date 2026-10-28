// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

/**
 * No reply came within the command timeout. The command may or may not have run on the server. A
 * subclass of {@link RuvioException}, so code that catches that keeps working.
 */
public class RuvioTimeoutException extends RuvioException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message the description
     */
    public RuvioTimeoutException(String message) {
        super(message);
    }
}
