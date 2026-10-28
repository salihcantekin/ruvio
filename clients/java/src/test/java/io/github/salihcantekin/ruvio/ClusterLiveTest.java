// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.internal.ClusterNode;
import io.github.salihcantekin.ruvio.internal.ClusterRouter;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.PubSubMessage;
import io.github.salihcantekin.ruvio.model.ScanResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Cluster routing against a live 3-shard server (RUVIO_CLUSTER_ADDR). */
class ClusterLiveTest {
    private RuvioClient<String, String> c;

    @BeforeEach
    void connect() {
        c = ClusterLive.client();
    }

    @AfterEach
    void close() {
        if (c != null) {
            c.close();
        }
    }

    private Map<String, Long> perNodeSizes() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String n : c.nodeAddresses()) {
            out.put(n, c.onNode(n).dbsize());
        }
        return out;
    }

    @Test
    void reportsSlotMovesAndRefusesARangeAlreadyInPlace() {
        assertTrue(c.moveSlotsStatus().containsKey("state"));
        // Slot 0 is on shard 0: nothing to move, and the server says why.
        RuvioServerException refused = assertThrows(RuvioServerException.class, () -> c.moveSlots(0, 10, 0));
        assertTrue(refused.getMessage().contains("already on that shard"), refused.getMessage());
    }

    @Test
    void detectsTheClusterAndBuildsTheSlotTable() {
        assertTrue(c.isCluster());
        assertEquals(3, c.nodeAddresses().size());
        ClusterRouter r = c.router();
        Set<ClusterNode> owners = new HashSet<>();
        for (int s = 0; s < ClusterRouter.SLOTS; s++) {
            owners.add(r.owner(s));
        }
        assertEquals(3, owners.size());
    }

    @Test
    void localHashSlotMatchesClusterKeyslot() {
        List<String> keys = new ArrayList<>(Arrays.asList("foo", "bar", "a", "{user1}.x", "{user1}.y", "x{tag}y", "{}k", "{a}{b}",
                "", "key with spaces", "ünïcode-键", "123456789", "{", "}", "a}{b"));
        for (int i = 0; i < 200; i++) {
            keys.add("k:" + i * 7919);
        }
        for (String k : keys) {
            if (k.isEmpty()) {
                continue; // an empty key is not sendable through some codecs; slot 0 is checked in the unit test
            }
            assertEquals(c.clusterKeyslot(k), (long) RuvioClient.hashSlot(k), "slot of " + k);
        }
    }

    @Test
    void keysSpreadOverAllThreeShardsAndReadBack() {
        int n = 300;
        for (int i = 0; i < n; i++) {
            c.set("spread:" + i, "v" + i);
        }
        Map<String, Long> sizes = perNodeSizes();
        long total = 0;
        for (long s : sizes.values()) {
            assertTrue(s > 0, "every shard holds keys: " + sizes);
            total += s;
        }
        assertEquals(n, total);
        for (int i = 0; i < n; i++) {
            assertEquals("v" + i, c.get("spread:" + i));
        }
        // Each key lives on the node CLUSTER SLOTS says; the same node answers a direct read.
        for (int i = 0; i < 30; i++) {
            String k = "spread:" + i;
            ClusterNode owner = c.router().owner(RuvioClient.hashSlot(k));
            assertEquals("v" + i, c.onNode(owner.address).get(k));
        }
    }

    @Test
    void crossSlotMgetMsetDelKeepTheCallersOrder() {
        int n = 60;
        Map<String, String> pairs = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            pairs.put("ms:" + i, "val" + i);
        }
        c.mset(pairs);
        assertTrue(perNodeSizes().values().stream().allMatch(s -> s > 0));
        List<String> order = new ArrayList<>(pairs.keySet());
        Collections.shuffle(order, new java.util.Random(42));
        order.add(10, "ms:missing");
        List<String> got = c.mget(order.toArray(new String[0]));
        assertEquals(order.size(), got.size());
        for (int i = 0; i < order.size(); i++) {
            String k = order.get(i);
            assertEquals(k.equals("ms:missing") ? null : pairs.get(k), got.get(i), "position " + i);
        }
        assertEquals((long) n, c.exists(pairs.keySet().toArray(new String[0])));
        assertEquals((long) n, c.touch(pairs.keySet().toArray(new String[0])));
        String[] firstTen = order.subList(0, 10).toArray(new String[0]);
        long present = Arrays.stream(firstTen).filter(k -> !k.equals("ms:missing")).count();
        assertEquals(present, c.unlink(firstTen));
        assertEquals((long) n - present, c.del(pairs.keySet().toArray(new String[0])));
        assertEquals(0L, perNodeSizes().values().stream().mapToLong(Long::longValue).sum());
    }

    @Test
    void sameHashtagMultiKeyCommandsStayWhole() {
        c.mset(new LinkedHashMap<>(Map.of("{h}a", "1", "{h}b", "2", "{h}c", "3")));
        assertEquals(Arrays.asList("1", "2", "3"), c.mget("{h}a", "{h}b", "{h}c"));
        assertEquals(3L, c.sadd("{h}set1", "x", "y", "z"));
        assertEquals(1L, c.sadd("{h}set2", "x"));
        assertEquals(Set.of("x"), new HashSet<>(c.sinter("{h}set1", "{h}set2")));
        c.rename("{h}a", "{h}renamed");
        assertEquals("1", c.get("{h}renamed"));
        assertEquals(1L, perNodeSizes().values().stream().filter(s -> s > 0).count(), "one hashtag lives on one shard");
    }

    @Test
    void otherCrossSlotCallsFailFast() {
        c.set("xs:a", "1");
        c.set("xs:b", "2");
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.rename("xs:a", "xs:b"));
        assertEquals("CROSSSLOT", e.code());
        assertThrows(RuvioServerException.class, () -> c.sunion("xs:a", "xs:b"));
        assertThrows(RuvioServerException.class, () -> c.msetnx(Map.of("xs:a", "x", "xs:b", "y")));
        assertEquals("1", c.get("xs:a"));
        assertEquals("2", c.get("xs:b"));
    }

    @Test
    void pipelineKeepsTheOrderAcrossNodes() {
        int n = 500;
        Pipeline<String, String> p = c.async().pipeline();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(p.set("pl:" + i, "v" + i));
        }
        assertEquals(n, p.flush());
        for (CompletableFuture<?> f : futures) {
            f.join();
        }
        Pipeline<String, String> g = c.async().pipeline();
        List<CompletableFuture<String>> gets = new ArrayList<>();
        List<CompletableFuture<Long>> incrs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            gets.add(g.get("pl:" + i));
            incrs.add(g.incr("plc:" + (i % 5)));
        }
        List<CompletableFuture<List<String>>> mgets = new ArrayList<>();
        mgets.add(g.mget("pl:1", "pl:2", "pl:3", "pl:4"));
        g.sync();
        for (int i = 0; i < n; i++) {
            assertEquals("v" + i, gets.get(i).join());
            assertEquals((long) (i / 5) + 1, incrs.get(i).join(), "incr " + i);
        }
        assertEquals(Arrays.asList("v1", "v2", "v3", "v4"), mgets.get(0).join());
        assertEquals(0, p.size());
    }

    @Test
    void staleSlotTableStillGetsTheRightAnswers() {
        // Ruvio forwards a misrouted key to its owner instead of answering MOVED, so a wrong
        // table still gets the right values; MOVED handling itself is covered by ClusterUnitTest.
        for (int i = 0; i < 100; i++) {
            c.set("mv:" + i, "v" + i);
        }
        ClusterRouter r = c.router();
        ClusterNode wrong = r.owner(0);
        String k = "mv:0";
        int slot = RuvioClient.hashSlot(k);
        ClusterNode right = r.owner(slot);
        ClusterNode other = r.masters().stream().filter(x -> x != right).findFirst().get();
        r.forceSlot(slot, other);
        assertEquals("v0", c.get(k));
        for (int s = 0; s < ClusterRouter.SLOTS; s++) {
            r.forceSlot(s, wrong);
        }
        for (int i = 0; i < 100; i++) {
            assertEquals("v" + i, c.get("mv:" + i));
        }
        // Stale split command: MGET over a broken table still returns the right values in order.
        for (int s = 0; s < ClusterRouter.SLOTS; s++) {
            r.forceSlot(s, wrong);
        }
        List<String> got = c.mget("mv:1", "mv:2", "mv:3", "mv:4", "mv:5");
        assertEquals(Arrays.asList("v1", "v2", "v3", "v4", "v5"), got);
    }

    @Test
    void transactionOnOneSlotWorksAndCrossSlotFails() {
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<Void> s = tx.set("{tx}a", "1");
            CompletableFuture<Long> i = tx.incr("{tx}a");
            CompletableFuture<Long> j = tx.incr("{tx}b");
            assertTrue(tx.exec());
            s.join();
            assertEquals(2L, i.join());
            assertEquals(1L, j.join());
        }
        assertEquals("2", c.get("{tx}a"));
        try (Transaction<String, String> tx = c.multi()) {
            tx.set("tx:one", "1");
            RuvioServerException e = assertThrows(RuvioServerException.class, () -> tx.set("tx:two", "2"));
            assertEquals("CROSSSLOT", e.code());
            assertTrue(tx.exec());
        }
        assertEquals("1", c.get("tx:one"));
        assertNull(c.get("tx:two"));
        try (Transaction<String, String> tx = c.multi()) {
            assertThrows(RuvioServerException.class, () -> tx.watch("tx:one", "tx:two"));
        }
        // WATCH on one slot: a concurrent write aborts the transaction.
        try (Transaction<String, String> tx = c.multi()) {
            tx.watch("{w}k");
            c.set("{w}k", "changed");
            CompletableFuture<Long> n = tx.incr("{w}n");
            assertFalse(tx.exec());
            assertTrue(n.isCompletedExceptionally());
        }
    }

    @Test
    void helpersWorkUnchangedOnTheCluster() throws Exception {
        // lock
        RuvioLock<String> lock = c.tryLock("h:lock", 5_000);
        assertNotNull(lock);
        assertNull(c.tryLock("h:lock", 5_000), "second holder refused");
        lock.close();
        RuvioLock<String> again = c.tryLock("h:lock", 5_000);
        assertNotNull(again);
        again.close();
        // lease and semaphore
        RuvioToken<String> lease = c.tryLease("h:lease", 5_000);
        assertNotNull(lease);
        assertNull(c.tryLease("h:lease", 5_000));
        lease.close();
        RuvioToken<String> p1 = c.tryAcquirePermit("h:sem", 2, 5_000);
        RuvioToken<String> p2 = c.tryAcquirePermit("h:sem", 2, 5_000);
        assertNotNull(p1);
        assertNotNull(p2);
        assertNull(c.tryAcquirePermit("h:sem", 2, 5_000));
        p1.close();
        p2.close();
        // rate limit
        for (int i = 0; i < 3; i++) {
            assertTrue(c.rateLimit("h:rate", 3, 60_000).allowed());
        }
        assertFalse(c.rateLimit("h:rate", 3, 60_000).allowed());
        // cache-aside
        AtomicLong calls = new AtomicLong();
        assertEquals("computed", c.getOrCreate("h:cache", 10_000, () -> {
            calls.incrementAndGet();
            return "computed";
        }));
        assertEquals("computed", c.getOrCreate("h:cache", 10_000, () -> {
            calls.incrementAndGet();
            return "again";
        }));
        assertEquals(1, calls.get());
        // idempotency
        assertEquals("result-1", c.idempotent("h:idem", "fp", 10_000, 2_000, () -> "result-1"));
        assertEquals("result-1", c.idempotent("h:idem", "fp", 10_000, 2_000, () -> "result-2"));
        // capped list and bloom
        c.pushRecent("h:recent", 3, "a", "b", "c", "d");
        assertEquals(3, c.recent("h:recent", 10).size());
        c.seenBefore("h:bloom", "x", "y");
        assertEquals(Arrays.asList(true, true, false), c.mightContain("h:bloom", "x", "y", "z"));
        // typed json
        c.jsonSetObject("h:json", "$", "{\"a\":1}");
        assertNotNull(c.jsonGet("h:json", "$"));
    }

    @Test
    void scanIteratorWalksEveryNode() {
        Set<String> expected = new HashSet<>();
        for (int i = 0; i < 400; i++) {
            c.set("sc:" + i, "v");
            expected.add("sc:" + i);
        }
        Set<String> seen = new HashSet<>();
        for (String k : c.scanIterable(null)) {
            seen.add(k);
        }
        assertEquals(expected, seen);
        assertEquals(expected.size(), c.scanStream(null).count());
        // A single typed scan call only reaches the node it lands on.
        Set<String> oneNode = new HashSet<>();
        String cursor = "0";
        do {
            ScanResult<String> page = c.scan(cursor);
            oneNode.addAll(page.items());
            cursor = page.cursor();
        } while (!"0".equals(cursor));
        assertTrue(oneNode.size() < expected.size());
        // Per-key scans route by key.
        for (int i = 0; i < 50; i++) {
            c.hset("sc:h", "f" + i, "v" + i);
            c.sadd("sc:s", "m" + i);
        }
        int fields = 0;
        for (KeyValue<String, String> kv : c.hscanIterable("sc:h", null)) {
            fields++;
        }
        assertEquals(50, fields);
        int members = 0;
        for (String m : c.sscanIterable("sc:s", null)) {
            members++;
        }
        assertEquals(50, members);
    }

    @Test
    void blockingCommandsRunOnADedicatedClusterClient() throws Exception {
        try (RuvioClient<String, String> blocker = c.dedicated()) {
            assertTrue(blocker.isCluster());
            ExecutorService ex = Executors.newSingleThreadExecutor();
            try {
                Future<KeyValue<String, String>> f = ex.submit(() -> blocker.blpop(5, "blk:list"));
                Thread.sleep(200);
                c.lpush("blk:list", "item");
                KeyValue<String, String> kv = f.get(10, TimeUnit.SECONDS);
                assertEquals("blk:list", kv.key());
                assertEquals("item", kv.value());
            } finally {
                ex.shutdownNow();
            }
            // The shared connection stayed free while the blocker waited.
            assertEquals(1L, c.incr("blk:free"));
        }
    }

    @Test
    void shardPubSubFollowsTheChannelSlot() throws Exception {
        List<PubSubMessage> got = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);
        try (RuvioPubSub sub = c.pubSub(m -> {
            got.add(m);
            latch.countDown();
        })) {
            String[] channels = {"ps:a", "ps:b", "ps:c", "ps:d", "ps:e", "ps:f"};
            Set<String> owners = new HashSet<>();
            for (String ch : channels) {
                owners.add(c.router().owner(RuvioClient.hashSlot(ch)).address);
            }
            sub.ssubscribe(channels);
            assertEquals(new HashSet<>(Arrays.asList(channels)), sub.shardChannels());
            assertTrue(owners.size() > 1, "the test channels cover more than one shard");
            for (int i = 0; i < 3; i++) {
                c.spublish(channels[i], "m" + i);
            }
            assertTrue(latch.await(5, TimeUnit.SECONDS), "messages received: " + got);
            assertEquals(3, got.size());
        }
    }

    @Test
    void aDroppedNodeConnectionDoesNotBreakOtherNodes() {
        for (int i = 0; i < 60; i++) {
            c.set("drop:" + i, "v" + i);
        }
        ClusterRouter r = c.router();
        ClusterNode victim = r.masters().get(1);
        victim.link.get().close(); // kill only that node's socket
        // The other nodes keep answering at once.
        int served = 0;
        for (int i = 0; i < 60; i++) {
            if (r.owner(RuvioClient.hashSlot("drop:" + i)) != victim) {
                assertEquals("v" + i, c.get("drop:" + i));
                served++;
            }
        }
        assertTrue(served > 0);
        // The dropped node reconnects lazily on its next command.
        for (int i = 0; i < 60; i++) {
            assertEquals("v" + i, c.get("drop:" + i));
        }
    }

    @Test
    void concurrentCallersShareTheNodeConnections() throws Exception {
        int threads = 8;
        int each = 500;
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                fs.add(ex.submit(() -> {
                    for (int i = 0; i < each; i++) {
                        c.incr("cc:" + (i % 20));
                    }
                }));
            }
            for (Future<?> f : fs) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            ex.shutdownNow();
        }
        long total = 0;
        for (int i = 0; i < 20; i++) {
            total += Long.parseLong(c.get("cc:" + i));
        }
        assertEquals((long) threads * each, total);
    }

    @Test
    void keylessCommandsGoToOneNode() {
        assertEquals("PONG", c.ping());
        assertNotNull(c.info());
        // DBSIZE is per node (documented): it counts the node it lands on, not the cluster.
        for (int i = 0; i < 90; i++) {
            c.set("kl:" + i, "v");
        }
        long local = c.dbsize();
        assertTrue(local > 0 && local < 90);
    }
}
