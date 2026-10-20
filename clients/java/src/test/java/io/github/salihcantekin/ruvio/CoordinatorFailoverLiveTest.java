// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Clients follow a failover that real coordinators carry out. scripts/run_with_servers.sh starts a group of two data
 * nodes (RUVIO_COORDINATOR_GROUP_ADDRS, their pids in RUVIO_COORDINATOR_GROUP_PIDS) watched by three
 * {@code ruvio coordinator} processes (RUVIO_COORDINATOR_ADDRS, the group named RUVIO_COORDINATOR_MASTER). The test
 * kills the primary with SIGKILL, so the group serves one run of the script. Skipped without the variables.
 */
class CoordinatorFailoverLiveTest {
    /** The coordinators notice the kill after down_after_ms (1 s) and promote within a few ticks. */
    private static final long PATIENCE_NANOS = TimeUnit.SECONDS.toNanos(15);
    /**
     * Each client's first write after the kill comes within this, the seeds client included: the replica that refused
     * is taken once it is promoted, not after the rounds run out (about 7 s).
     */
    private static final long FOLLOW_BOUND_MILLIS = 5_000;

    private static String[] list(String name) {
        String value = System.getenv(name);
        assumeTrue(value != null && !value.isEmpty(), name + " not set; coordinator failover live test skipped");
        return value.split(",");
    }

    private static RuvioClient<String, String> direct(String address) {
        RuvioOptions base = RuvioOptions.of(address);
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port()).commandTimeoutMillis(5_000).build());
    }

    private static boolean isMaster(RuvioClient<String, String> client) {
        Object kind = client.role().get(0);
        return "master".equals(kind instanceof byte[] ? new String((byte[]) kind, StandardCharsets.UTF_8) : String.valueOf(kind));
    }

    /** Writes {@code value} to {@code key} until a write succeeds; the milliseconds from {@code started}. */
    private static long writeUntilItSucceeds(RuvioClient<String, String> client, String key, String value, long started) {
        while (true) {
            try {
                client.set(key, value);
                return (System.nanoTime() - started) / 1_000_000;
            } catch (RuvioException e) {
                assertTrue(System.nanoTime() - started < PATIENCE_NANOS, key + ": no write succeeded: " + e);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    @Test
    @Timeout(60)
    void clientsReachTheNewPrimaryAfterTheCoordinatorsFailOver() throws Exception {
        String[] coordinators = list("RUVIO_COORDINATOR_ADDRS");
        String master = list("RUVIO_COORDINATOR_MASTER")[0];
        String[] members = list("RUVIO_COORDINATOR_GROUP_ADDRS");
        String[] pids = list("RUVIO_COORDINATOR_GROUP_PIDS");
        String tag = "coordinated:java:" + ProcessHandle.current().pid() + ":";
        // One client asks the coordinators for the primary, the other has the data nodes as seeds.
        Map<String, RuvioOptions> options = new LinkedHashMap<>();
        options.put("sentinelMaster", RuvioOptions.builder().seeds(coordinators).sentinelMaster(master)
                .connectTimeoutMillis(1_000).commandTimeoutMillis(5_000).build());
        options.put("seeds", RuvioOptions.builder().seeds(members).connectTimeoutMillis(1_000).commandTimeoutMillis(5_000).build());

        try (RuvioClient<String, String> first = direct(members[0]);
                RuvioClient<String, String> second = direct(members[1]);
                RuvioClient<String, String> throughCoordinators = RuvioClient.connect(options.get("sentinelMaster"));
                RuvioClient<String, String> throughSeeds = RuvioClient.connect(options.get("seeds"))) {
            int old = isMaster(first) ? 0 : 1;
            RuvioClient<String, String> onNew = old == 0 ? second : first;
            assertTrue(isMaster(old == 0 ? first : second) && !isMaster(onNew), "the group does not have exactly one primary");
            Map<String, RuvioClient<String, String>> clients = new LinkedHashMap<>();
            clients.put("sentinelMaster", throughCoordinators);
            clients.put("seeds", throughSeeds);
            clients.forEach((name, client) -> client.set(tag + name, "before"));
            // Only a replica that holds every acknowledged write is promoted.
            long deadline = System.nanoTime() + PATIENCE_NANOS;
            for (String name : clients.keySet()) {
                while (!"before".equals(onNew.get(tag + name))) {
                    assertTrue(System.nanoTime() < deadline, "the replica did not hold the first writes");
                    Thread.sleep(20);
                }
            }

            ProcessHandle primary = ProcessHandle.of(Long.parseLong(pids[old])).orElseThrow();
            assertTrue(primary.destroyForcibly(), "kill -9 " + pids[old]);
            long started = System.nanoTime();

            // Every client writes at once, so none waits for another.
            Map<String, CompletableFuture<Long>> took = new LinkedHashMap<>();
            clients.forEach((name, client) ->
                    took.put(name, CompletableFuture.supplyAsync(() -> writeUntilItSucceeds(client, tag + name, "after", started))));
            Map<String, Long> millis = new LinkedHashMap<>();
            for (Map.Entry<String, CompletableFuture<Long>> entry : took.entrySet()) {
                millis.put(entry.getKey(), entry.getValue().get());
            }
            System.out.println("first write after the kill (ms): " + millis);
            millis.forEach((name, ms) -> assertTrue(ms < FOLLOW_BOUND_MILLIS, name + ": the first write took " + ms + " ms"));

            assertTrue(isMaster(onNew), "the surviving member is not the primary");
            for (String name : clients.keySet()) {
                assertEquals("after", onNew.get(tag + name));
            }
        }
    }
}
