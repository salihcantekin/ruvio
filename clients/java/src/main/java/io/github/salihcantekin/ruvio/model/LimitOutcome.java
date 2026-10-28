// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.model;

/**
 * Outcome of the {@code *WithRetry} limit calls.
 *
 * @param allowed whether the call was let through
 * @param remaining the allowance left after this call; 0 when refused
 * @param retryAfterMillis milliseconds until the same call would pass; 0 when it was let through (fit for a {@code Retry-After} header)
 */
public record LimitOutcome(boolean allowed, long remaining, long retryAfterMillis) {
}
