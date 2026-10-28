// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

/** The retry classification every client shares: which failures a retry can get past and which may have run. */
class RuvioErrorsTest {
    private static final String[] TRANSIENT_REPLIES = {
        "LOADING Ruvio is loading the dataset in memory",
        "MASTERDOWN Link with MASTER is down",
        "CLUSTERDOWN The cluster is down",
        "TRYAGAIN slot is moving",
        "READONLY You can't write against a read only replica.",
        "NOREPLICAS Not enough good replicas to write.",
    };

    @Test
    void transientCoversOutagesAndRetryableReplies() {
        for (String reply : TRANSIENT_REPLIES) {
            assertTrue(RuvioErrors.isTransient(new RuvioServerException(reply)), reply);
            assertTrue(RuvioErrors.isTransient(new CompletionException(new RuvioServerException(reply))), reply);
        }
        for (String reply : new String[] {"WRONGTYPE Operation against a key", "OOM command not allowed", "ERR syntax error", "READONLYX x", "LOAD"}) {
            assertFalse(RuvioErrors.isTransient(new RuvioServerException(reply)), reply);
        }
        assertTrue(RuvioErrors.isTransient(new RuvioConnectionException("connection closed by server")));
        assertTrue(RuvioErrors.isTransient(new RuvioTimeoutException("no reply within 5 ms")));
        assertTrue(RuvioErrors.isTransient(new ExecutionException(new RuvioTimeoutException("no reply"))));
        assertFalse(RuvioErrors.isTransient(new RuvioException("interrupted")));
        assertFalse(RuvioErrors.isTransient(new IllegalArgumentException("bad")));
        assertThrows(NullPointerException.class, () -> RuvioErrors.isTransient(null));
    }

    @Test
    void onlyARefusingServerReplySurelyDidNotRun() {
        assertFalse(RuvioErrors.mayHaveRun(new RuvioServerException("WRONGTYPE Operation against a key")));
        assertFalse(RuvioErrors.mayHaveRun(new RuvioServerException("TRYAGAIN slot is moving")));
        assertFalse(RuvioErrors.mayHaveRun(new CompletionException(new RuvioServerException("READONLY replica"))));
        assertTrue(RuvioErrors.mayHaveRun(new RuvioServerException("NOREPLICAS Not enough good replicas to write.")));
        assertTrue(RuvioErrors.mayHaveRun(new RuvioServerException("NOREPLICAS")));
        assertTrue(RuvioErrors.mayHaveRun(new RuvioConnectionException("write failed")));
        assertTrue(RuvioErrors.mayHaveRun(new RuvioTimeoutException("no reply")));
        assertTrue(RuvioErrors.mayHaveRun(new RuvioException("interrupted")));
    }
}
