// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * A client that follows a failover (two seeds) must allocate no more per command on the healthy path than a client with a
 * single seed. The calling thread's allocated bytes are counted over many commands after a warm-up, for the sync and
 * the async API. Runs against the failover pair of scripts/run_with_servers.sh (RUVIO_FAILOVER_ADDRS); skipped without it.
 *
 * <p>Tolerances: the difference measured over 30 runs stayed within -85 KB and +52 KB for async (first run of a JVM:
 * JIT compilation) and within -7 KB and +8 KB for sync, while the retained-frame design this test guards against cost 75 to 85 bytes
 * per command: about 800 KB over {@link #ASYNC_OPS} async and 125 KB over {@link #SYNC_OPS} sync commands. The limits are
 * {@link #ASYNC_TOLERANCE} (2.4 times the largest async spread) and {@link #SYNC_TOLERANCE} (6 times the largest sync spread, 2.6
 * times under the regression). The sync half is short because a sync command waits a full round trip.
 */
class FollowAllocLiveTest {
    private static final int SYNC_OPS = 1_500;
    private static final int SYNC_WARMUP = 600;
    private static final int ASYNC_OPS = 10_000;
    private static final int ASYNC_WARMUP = 8_000;
    /** Total bytes the follower may allocate above the single-seed client over the measured commands. */
    private static final long ASYNC_TOLERANCE = 128 * 1024;
    private static final long SYNC_TOLERANCE = 48 * 1024;

    private static final com.sun.management.ThreadMXBean THREADS =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static String[] pair() {
        String value = System.getenv("RUVIO_FAILOVER_ADDRS");
        assumeTrue(value != null && !value.isEmpty(), "RUVIO_FAILOVER_ADDRS not set; allocation test skipped");
        String[] parts = value.split(",");
        assumeTrue(parts.length == 2, "RUVIO_FAILOVER_ADDRS must name two addresses");
        return parts;
    }

    private static boolean isMaster(String address) {
        RuvioOptions o = RuvioOptions.of(address);
        try (RuvioClient<String, String> c = RuvioClient.connect(
                RuvioOptions.builder().host(o.host()).port(o.port()).commandTimeoutMillis(5_000).build())) {
            Object role = c.role().get(0);
            String text = role instanceof byte[] ? new String((byte[]) role, java.nio.charset.StandardCharsets.UTF_8) : String.valueOf(role);
            return "master".equals(text);
        }
    }

    private static RuvioClient<String, String> single(String address) {
        RuvioOptions o = RuvioOptions.of(address);
        return RuvioClient.connect(RuvioOptions.builder().host(o.host()).port(o.port()).commandTimeoutMillis(10_000).build());
    }

    private static RuvioClient<String, String> following(String a, String b) {
        return RuvioClient.connect(RuvioOptions.builder().seeds(a, b).commandTimeoutMillis(10_000).build());
    }

    private static void syncOps(RuvioClient<String, String> c, int n) {
        String v = "0123456789abcdef";
        for (int i = 0; i < n; i += 2) {
            String k = "alloc:" + (i & 1023);
            c.set(k, v);
            c.get(k);
        }
    }

    private static void asyncOps(RuvioClient<String, String> c, int n) {
        RuvioAsync<String, String> as = c.async();
        String v = "0123456789abcdef";
        CompletableFuture<?>[] batch = new CompletableFuture<?>[500];
        for (int i = 0; i < n; i += batch.length) {
            for (int j = 0; j < batch.length; j++) {
                String k = "alloc:" + ((i + j) & 1023);
                batch[j] = (j & 1) == 0 ? as.set(k, v) : as.get(k);
            }
            CompletableFuture.allOf(batch).join();
        }
    }

    /** Bytes the calling thread allocated while running {@code ops} commands after a warm-up. */
    private static long measure(RuvioClient<String, String> c, Consumer<RuvioClient<String, String>> warm,
            Consumer<RuvioClient<String, String>> run) {
        warm.accept(c);
        long id = Thread.currentThread().getId();
        long before = THREADS.getThreadAllocatedBytes(id);
        run.accept(c);
        return THREADS.getThreadAllocatedBytes(id) - before;
    }

    private void compare(String label, int ops, long tolerance, Consumer<RuvioClient<String, String>> warm, Consumer<RuvioClient<String, String>> run) {
        String[] seeds = pair();
        String primary = isMaster(seeds[0]) ? seeds[0] : seeds[1];
        String other = primary.equals(seeds[0]) ? seeds[1] : seeds[0];
        long plain;
        long followed;
        try (RuvioClient<String, String> c = single(primary)) {
            plain = measure(c, warm, run);
        }
        try (RuvioClient<String, String> c = following(primary, other)) {
            followed = measure(c, warm, run);
        }
        String line = label + ": single seed " + plain + " bytes, following " + followed + " bytes over " + ops + " commands, difference " + (followed - plain);
        System.out.println(line);
        assertTrue(followed - plain <= tolerance, line + " (tolerance " + tolerance + ")");
    }

    @Test
    void syncHealthyPathAllocatesNoMoreWhenFollowing() {
        compare("sync", SYNC_OPS, SYNC_TOLERANCE, c -> syncOps(c, SYNC_WARMUP), c -> syncOps(c, SYNC_OPS));
    }

    @Test
    void asyncHealthyPathAllocatesNoMoreWhenFollowing() {
        compare("async", ASYNC_OPS, ASYNC_TOLERANCE, c -> asyncOps(c, ASYNC_WARMUP), c -> asyncOps(c, ASYNC_OPS));
    }
}
