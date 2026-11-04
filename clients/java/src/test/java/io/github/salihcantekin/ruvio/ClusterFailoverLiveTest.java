// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A cluster client leaves a demoted primary after its READONLY reply, on real servers. scripts/run_with_servers.sh
 * starts two 2-shard nodes that can each be primary or replica (RUVIO_CLUSTER_FAILOVER_ADDRS, shard 0 of each; their
 * replication addresses in RUVIO_CLUSTER_FAILOVER_REPLICATION; both comma separated and primary first). No coordinator
 * runs: the test swaps the roles itself with REPLICAOF, as a coordinator does. Skipped without the variables.
 */
class ClusterFailoverLiveTest {
    private static final long PATIENCE_NANOS = TimeUnit.SECONDS.toNanos(10);

    private static String[] pair(String name) {
        String value = System.getenv(name);
        assumeTrue(value != null && !value.isEmpty(), name + " not set; cluster failover live test skipped");
        String[] parts = value.split(",");
        assumeTrue(parts.length == 2, name + " must name two addresses");
        return parts;
    }

    private static RuvioClient<String, String> direct(String address) {
        RuvioOptions base = RuvioOptions.of(address);
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port()).followPrimary(false).commandTimeoutMillis(5_000).build());
    }

    private static boolean isMaster(String address) {
        try (RuvioClient<String, String> c = direct(address)) {
            Object kind = c.role().get(0);
            return "master".equals(kind instanceof byte[] ? new String((byte[]) kind, StandardCharsets.UTF_8) : String.valueOf(kind));
        }
    }

    private static void waitUntil(String what, java.util.function.BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + PATIENCE_NANOS;
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "gave up waiting for " + what);
            Thread.sleep(20);
        }
    }

    @Test
    void aClusterClientReloadsItsSlotsAfterReadonlyFromADemotedPrimary() throws Exception {
        String[] clients = pair("RUVIO_CLUSTER_FAILOVER_ADDRS");
        String[] replication = pair("RUVIO_CLUSTER_FAILOVER_REPLICATION");
        // Whichever node is the primary now, so the test can run again on the same servers.
        int old = isMaster(clients[0]) ? 0 : 1;
        int next = 1 - old;
        RuvioOptions base = RuvioOptions.of(clients[old]);
        RuvioOptions options = RuvioOptions.builder().host(base.host()).port(base.port()).cluster(true).followPrimary(false)
                .commandTimeoutMillis(5_000).build();
        long pid = ProcessHandle.current().pid();
        // One key on each shard: {a} hashes to slot 15495 (shard 1), {b} to slot 3300 (shard 0).
        List<String> keys = List.of("cluster-failover:java:" + pid + ":{a}", "cluster-failover:java:" + pid + ":{b}");
        try (RuvioClient<String, String> client = RuvioClient.connect(options);
                RuvioClient<String, String> onOld = direct(clients[old]);
                RuvioClient<String, String> onNew = direct(clients[next])) {
            assertTrue(client.isCluster(), "cluster mode");
            for (String key : keys) {
                client.set(key, "before");
            }
            // A replica that still copies refuses to be promoted without FORCE.
            waitUntil("the replica to hold the first writes", () -> keys.stream().allMatch(key -> "before".equals(onNew.get(key))));

            onNew.execute("REPLICAOF", "NO", "ONE");
            RuvioOptions toNew = RuvioOptions.of(replication[next]);
            onOld.execute("REPLICAOF", toNew.host(), Integer.toString(toNew.port()));
            waitUntil("the old primary to become a replica", () -> !isMaster(clients[old]));

            // The demoted node names its new primary once that primary has accepted it.
            long started = System.nanoTime();
            while (true) {
                try {
                    client.set(keys.get(0), "after");
                    break;
                } catch (RuvioServerException e) {
                    assertTrue(e.getMessage().startsWith("READONLY"), e.getMessage());
                    long waited = System.nanoTime() - started;
                    assertTrue(waited < TimeUnit.SECONDS.toNanos(5), "still refused after " + waited / 1_000_000 + " ms: " + e.getMessage());
                    Thread.sleep(20);
                }
            }
            assertEquals("after", onNew.get(keys.get(0)));
            // The other shard's slots moved too: no write is refused once the table is reloaded.
            for (int round = 0; round < 10; round++) {
                for (String key : keys) {
                    client.set(key, "v" + round);
                }
            }
            for (String key : keys) {
                assertEquals("v9", onNew.get(key));
            }
        }
    }
}
