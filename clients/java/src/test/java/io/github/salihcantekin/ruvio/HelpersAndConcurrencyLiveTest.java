// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ClientKillArgs;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HelpersAndConcurrencyLiveTest {
    static RuvioClient<String, String> c;

    @BeforeAll
    static void open() {
        c = Live.client();
    }

    @AfterAll
    static void close() {
        if (c != null) c.close();
    }

    @Test
    void lockNeverReleasesSomeoneElses() throws Exception {
        RuvioLock<String> a = c.tryLock("lk", 10_000);
        assertNotNull(a);
        assertNull(c.tryLock("lk", 10_000));
        assertTrue(a.isHeld());
        assertTrue(a.renew(20_000));
        assertTrue(c.pttl("lk") > 10_000);
        a.release();
        assertFalse(a.release());
        // lock expires, another owner takes it; the first owner's close must not delete it
        RuvioLock<String> shortLived = c.tryLock("lk2", 30);
        Thread.sleep(80);
        RuvioLock<String> other = c.tryLock("lk2", 10_000);
        assertNotNull(other);
        assertFalse(shortLived.renew(1000));
        shortLived.close();
        assertEquals(other.token(), c.get("lk2"));
        other.close();
        assertNull(c.get("lk2"));
        try (RuvioLock<String> t = c.lock("lk3", 5_000, 100)) {
            assertNotNull(t);
            assertNull(c.lock("lk3", 5_000, 30));
        }
        assertNull(c.get("lk3"));
    }

    @Test
    void leaseAndPermits() {
        try (RuvioToken<String> l1 = c.tryLease("hl", 10_000)) {
            assertNotNull(l1);
            assertNull(c.tryLease("hl", 10_000));
            long first = l1.token();
            l1.release();
            try (RuvioToken<String> l2 = c.tryLease("hl", 10_000)) {
                assertTrue(l2.token() > first);
            }
        }
        try (RuvioToken<String> p1 = c.tryAcquirePermit("hs", 1, 10_000)) {
            assertNotNull(p1);
            assertNull(c.tryAcquirePermit("hs", 1, 10_000));
        }
        assertNotNull(c.tryAcquirePermit("hs", 1, 10_000));
    }

    @Test
    void rateLimiter() {
        int allowed = 0;
        for (int i = 0; i < 8; i++) {
            if (c.rateLimit("rl", 5, 10_000).allowed()) allowed++;
        }
        assertEquals(5, allowed);
        assertEquals(0, c.rateLimit("rl", 5, 10_000).remaining());
    }

    @Test
    void getOrCreateRunsFactoryOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<String>> fs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            fs.add(pool.submit(() -> c.getOrCreate("goc", 10_000, () -> {
                calls.incrementAndGet();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "built";
            })));
        }
        for (Future<String> f : fs) {
            assertEquals("built", f.get());
        }
        pool.shutdown();
        assertEquals(1, calls.get());
        assertEquals("built", c.getOrCreate("goc", 10_000, () -> "again"));
        assertEquals(1, calls.get());
        // stale-while-revalidate
        c.getOrCreate("swr", 40, 10_000, 2_000, () -> "v1");
        Thread.sleep(100);
        assertEquals("v2", c.getOrCreate("swr", 10_000, 10_000, 2_000, () -> "v2"));
    }

    @Test
    void idempotentRunsOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<String>> fs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            fs.add(pool.submit(() -> c.idempotent("idm", "fp1", 10_000, 5_000, () -> {
                calls.incrementAndGet();
                try {
                    Thread.sleep(60);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "result-é";
            })));
        }
        for (Future<String> f : fs) {
            assertEquals("result-é", f.get());
        }
        pool.shutdown();
        assertEquals(1, calls.get());
        assertThrows(RuvioException.class, () -> c.idempotent("idm", "other", 10_000, 100, () -> "x"));
    }

    @Test
    void idempotentAbortOnErrorFreesTheKey() {
        assertThrows(IllegalStateException.class, () -> c.idempotent("idm-abort", "fp", 10_000, 100, () -> {
            throw new IllegalStateException("boom");
        }, true));
        assertEquals("again", c.idempotent("idm-abort", "fp", 10_000, 100, () -> "again"));
        // without the flag the key stays pending
        assertThrows(IllegalStateException.class, () -> c.idempotent("idm-keep", "fp", 10_000, 100, () -> {
            throw new IllegalStateException("boom");
        }));
        assertThrows(RuvioException.class, () -> c.idempotent("idm-keep", "fp", 10_000, 100, () -> "x"));
    }

    @Test
    void manyThreadsShareOneConnection() throws Exception {
        int threads = 32;
        int per = 2_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            fs.add(pool.submit(() -> {
                for (int i = 0; i < per; i++) {
                    String k = "cc:" + id + ":" + (i % 16);
                    String v = id + "/" + i;
                    c.set(k, v);
                    assertEquals(v, c.get(k), "reply routed to the right caller");
                    c.incr("cc:total");
                }
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
        assertEquals((long) threads * per, Long.parseLong(c.get("cc:total")));
        // async fan-in
        List<CompletableFuture<String>> gets = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            gets.add(c.async().get("cc:total"));
        }
        for (CompletableFuture<String> g : gets) {
            assertEquals(String.valueOf(threads * per), g.join());
        }
    }

    @Test
    void reconnectsAfterServerDropsTheSocket() {
        try (RuvioClient<String, String> x = RuvioClient.connect(Live.options(Live.DB))) {
            long id = x.clientId();
            c.clientKill(ClientKillArgs.id(id));
            boolean ok = false;
            for (int i = 0; i < 3 && !ok; i++) {
                try {
                    x.ping();
                    ok = true;
                } catch (RuvioConnectionException e) {
                    // the first call after the drop may fail
                }
            }
            assertTrue(ok);
        }
    }

    @Test
    void largePipelineWithinInFlightBound() {
        RuvioOptions small = Live.options(Live.DB).toBuilder().maxInFlight(64).build();
        try (RuvioClient<String, String> x = RuvioClient.connect(small)) {
            List<CompletableFuture<Long>> fs = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                fs.add(x.async().incr("lp:n")); // blocks briefly when 64 are outstanding, never deadlocks
            }
            assertEquals(5000L, fs.get(4999).join());
        }
    }
}
