// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.salihcantekin.ruvio.internal.HashSlot;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Runs against the replicated pair scripts/run_with_servers.sh starts: a 2-shard primary on RUVIO_REPLICATED_ADDR and
 * its 2-shard replica on RUVIO_REPLICA_ADDR (replica shard i is on port + i). Skipped without them.
 */
class ReplicaReadsLiveTest {
    private static final String PRIMARY = System.getenv("RUVIO_REPLICATED_ADDR");
    private static final String REPLICA = System.getenv("RUVIO_REPLICA_ADDR");

    private static RuvioClient<String, String> client(ReadPolicy policy) {
        assumeTrue(PRIMARY != null && !PRIMARY.isEmpty() && REPLICA != null && !REPLICA.isEmpty(),
                "RUVIO_REPLICATED_ADDR / RUVIO_REPLICA_ADDR not set; replica read test skipped");
        RuvioOptions base = RuvioOptions.of(PRIMARY);
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port()).cluster(true)
                .readPolicy(policy).commandTimeoutMillis(10_000).build());
    }

    @Test
    void durabilityOptionHoldsRepliesUntilAReplicaHasTheWrite() {
        assumeTrue(PRIMARY != null && !PRIMARY.isEmpty(), "RUVIO_REPLICATED_ADDR not set; durability test skipped");
        RuvioOptions base = RuvioOptions.of(PRIMARY);
        try (RuvioClient<String, String> db = RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port())
                .cluster(true).durability(1, 5000).commandTimeoutMillis(10_000).build())) {
            db.set("java:durability", "v");
            assertEquals("v", db.get("java:durability"));
        }
    }

    /** One shard of the replica, without cluster routing. */
    private static RuvioClient<String, String> shard(String key) {
        RuvioOptions base = RuvioOptions.of(REPLICA);
        int slot = HashSlot.of(key.getBytes(StandardCharsets.UTF_8));
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port() + (slot >= 8192 ? 1 : 0))
                .commandTimeoutMillis(10_000).build());
    }

    private static long getCommands(RuvioClient<String, String> shard) {
        for (String line : shard.info().split("\r?\n")) {
            if (line.startsWith("get_commands:")) {
                return Long.parseLong(line.substring("get_commands:".length()).trim());
            }
        }
        throw new AssertionError("INFO reports get_commands");
    }

    private static void waitForCopy(RuvioClient<String, String> shard, String key, String value) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!value.equals(shard.get(key))) {
            assertTrue(System.nanoTime() < deadline, "the replica did not copy " + key);
            Thread.sleep(20);
        }
    }

    /** Writes through the policy client, then checks how much the replica's GET counter grows for one GET. */
    private static void check(ReadPolicy policy, String name, long expectedGrowth) throws InterruptedException {
        String key = "java:replica-reads:" + name;
        try (RuvioClient<String, String> db = client(policy); RuvioClient<String, String> onReplica = shard(key)) {
            db.set(key, "v");
            waitForCopy(onReplica, key, "v");
            long before = getCommands(onReplica);
            assertEquals("v", db.get(key));
            assertEquals(before + expectedGrowth, getCommands(onReplica));
        }
    }

    @Test
    void preferReplicaReadsFromTheReplicaAndWritesToThePrimary() throws InterruptedException {
        check(ReadPolicy.PREFER_REPLICA, "prefer", 1);
    }

    @Test
    void replicaPolicyReadsFromTheReplica() throws InterruptedException {
        check(ReadPolicy.REPLICA, "only", 1);
    }

    @Test
    void primaryPolicyKeepsReadsOnThePrimary() throws InterruptedException {
        check(ReadPolicy.PRIMARY, "primary", 0);
    }
}
