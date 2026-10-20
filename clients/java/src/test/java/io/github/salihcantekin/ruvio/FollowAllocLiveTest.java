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
 * <p>Tolerance: over 10 runs of this measurement the difference stayed within -38 KB and +75 KB (the largest one in the very
 * first run of a JVM, from JIT compilation), while the retained-frame design this test guards against cost 37 to 80 bytes per command,
 * 0.7 to 1.6 MB over {@link #OPS} commands. The limit is {@link #TOLERANCE} bytes in total: 10 bytes per command, about
 * 2.7 times the largest spread seen and a quarter to a tenth of the smallest regression.
 */
class FollowAllocLiveTest {
    private static final int OPS = 20_000;
    private static final int WARMUP = 6_000;
    private static final int ASYNC_WARMUP = 20_000;
    /** Total bytes the follower may allocate above the single-seed client over {@link #OPS} commands. */
    private static final long TOLERANCE = 200 * 1024;

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

    private void compare(String label, Consumer<RuvioClient<String, String>> warm, Consumer<RuvioClient<String, String>> run) {
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
        String line = label + ": single seed " + plain + " bytes, following " + followed + " bytes over " + OPS + " commands, difference " + (followed - plain);
        System.out.println(line);
        assertTrue(followed - plain <= TOLERANCE, line + " (tolerance " + TOLERANCE + ")");
    }

    @Test
    void syncHealthyPathAllocatesNoMoreWhenFollowing() {
        compare("sync", c -> syncOps(c, WARMUP), c -> syncOps(c, OPS));
    }

    @Test
    void asyncHealthyPathAllocatesNoMoreWhenFollowing() {
        compare("async", c -> asyncOps(c, ASYNC_WARMUP), c -> asyncOps(c, OPS));
    }
}
