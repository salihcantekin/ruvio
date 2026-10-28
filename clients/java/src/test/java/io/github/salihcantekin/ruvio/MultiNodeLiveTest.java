// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.salihcantekin.ruvio.internal.HashSlot;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Runs against the multi-node cluster scripts/run_with_servers.sh starts: node a (RUVIO_MULTINODE_A_ADDR, slots
 * 0-8191), node b (RUVIO_MULTINODE_B_ADDR, slots 8192-16383) and a node with an older map
 * (RUVIO_MULTINODE_STALE_ADDR) that gives every slot to node a. Skipped without them.
 */
class MultiNodeLiveTest {
    private static final String A = System.getenv("RUVIO_MULTINODE_A_ADDR");
    private static final String B = System.getenv("RUVIO_MULTINODE_B_ADDR");
    private static final String STALE = System.getenv("RUVIO_MULTINODE_STALE_ADDR");

    private static boolean set(String value) {
        return value != null && !value.isEmpty();
    }

    private static RuvioClient<String, String> connect(String address, boolean cluster) {
        RuvioOptions base = RuvioOptions.of(address);
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port()).cluster(cluster)
                .commandTimeoutMillis(10_000).build());
    }

    private static int slot(String key) {
        return HashSlot.of(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void followsMovedFromANodeWithAnOlderMap() {
        assumeTrue(set(A) && set(B) && set(STALE),
                "RUVIO_MULTINODE_A_ADDR / RUVIO_MULTINODE_B_ADDR / RUVIO_MULTINODE_STALE_ADDR not set; skipped");
        // Node b's slot (12182) and node a's (5061).
        String onB = "{foo}java:multinode";
        String onA = "{bar}java:multinode";
        assertEquals(12182, slot(onB));
        assertEquals(5061, slot(onA));

        // Seeded with the stale node, the client first sends node b's key to node a, which answers MOVED.
        try (RuvioClient<String, String> db = connect(STALE, true)) {
            db.set(onB, "b");
            db.set(onA, "a");
            assertEquals("b", db.get(onB));
            assertEquals("a", db.get(onA));
        }

        // Each key is on the node that owns it.
        try (RuvioClient<String, String> nodeB = connect(B, false); RuvioClient<String, String> nodeA = connect(A, false)) {
            assertEquals("b", nodeB.get(onB));
            assertEquals("a", nodeA.get(onA));
        }
    }
}
