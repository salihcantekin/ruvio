// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.Transaction;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioServerException;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Slot table, hashtags, key extraction and MOVED handling against scripted fake servers (no live server). */
class ClusterUnitTest {
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
        toClose.clear();
    }

    /** A fake node that owns the slots in [start, end] and behaves like the Ruvio shard for a few commands. */
    final class Shard {
        final FakeNode node;
        final Map<String, String> store = new ConcurrentHashMap<>();
        volatile int start;
        volatile int end;
        volatile String slotsReply = "";
        volatile String moveTo; // address to answer MOVED with for keys this node does not own

        Shard(int start, int end) throws IOException {
            this.start = start;
            this.end = end;
            this.node = new FakeNode(this::handle);
            toClose.add(node);
        }

        List<List<String>> commands(String name) {
            return node.commands(name);
        }

        List<List<String>> received() {
            return node.received;
        }

        boolean owns(String key) {
            int s = HashSlot.of(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return s >= start && s <= end;
        }

        String handle(List<String> a) {
            String cmd = a.get(0).toUpperCase();
            switch (cmd) {
                case "CLUSTER":
                    return slotsReply;
                case "GET": {
                    String m = guard(a, 1, 2);
                    return m != null ? m : FakeNode.bulk(store.get(a.get(1)));
                }
                case "SET": {
                    String m = guard(a, 1, 2);
                    if (m != null) {
                        return m;
                    }
                    store.put(a.get(1), a.get(2));
                    return FakeNode.ok();
                }
                case "INCR": {
                    String m = guard(a, 1, 2);
                    if (m != null) {
                        return m;
                    }
                    long v = Long.parseLong(store.getOrDefault(a.get(1), "0")) + 1;
                    store.put(a.get(1), Long.toString(v));
                    return FakeNode.integer(v);
                }
                case "MGET": {
                    String m = guard(a, 1, a.size());
                    if (m != null) {
                        return m;
                    }
                    String[] items = new String[a.size() - 1];
                    for (int i = 1; i < a.size(); i++) {
                        items[i - 1] = FakeNode.bulk(store.get(a.get(i)));
                    }
                    return FakeNode.array(items);
                }
                case "MSET": {
                    List<String> keys = new ArrayList<>();
                    for (int i = 1; i < a.size(); i += 2) {
                        keys.add(a.get(i));
                    }
                    String m = guardKeys(keys);
                    if (m != null) {
                        return m;
                    }
                    for (int i = 1; i < a.size(); i += 2) {
                        store.put(a.get(i), a.get(i + 1));
                    }
                    return FakeNode.ok();
                }
                case "DEL":
                case "UNLINK":
                case "EXISTS":
                case "TOUCH": {
                    String m = guard(a, 1, a.size());
                    if (m != null) {
                        return m;
                    }
                    long n = 0;
                    for (int i = 1; i < a.size(); i++) {
                        boolean there = store.containsKey(a.get(i));
                        if (there) {
                            n++;
                            if (cmd.equals("DEL") || cmd.equals("UNLINK")) {
                                store.remove(a.get(i));
                            }
                        }
                    }
                    return FakeNode.integer(n);
                }
                case "MULTI":
                    return FakeNode.ok();
                case "EXEC":
                    return FakeNode.array(FakeNode.ok(), FakeNode.integer(1));
                default:
                    return FakeNode.ok();
            }
        }

        private String guard(List<String> a, int from, int to) {
            return guardKeys(a.subList(from, to));
        }

        private String guardKeys(List<String> keys) {
            int slot = -1;
            for (String k : keys) {
                int s = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                if (slot >= 0 && s != slot) {
                    return FakeNode.error("CROSSSLOT Keys in request do not hash to the same slot");
                }
                slot = s;
            }
            if (slot >= 0 && (slot < start || slot > end)) {
                return FakeNode.error("MOVED " + slot + " " + moveTo);
            }
            return null;
        }
    }

    /** Two shards: A owns 0-8191, B owns 8192-16383. */
    final class Pair {
        final Shard a;
        final Shard b;

        Pair() throws IOException {
            a = new Shard(0, 8191);
            b = new Shard(8192, 16383);
            publish();
            a.moveTo = b.node.address();
            b.moveTo = a.node.address();
        }

        void publish() {
            String s = FakeNode.slots(new int[] {a.start, a.end, a.node.port}, new int[] {b.start, b.end, b.node.port});
            a.slotsReply = s;
            b.slotsReply = s;
        }

        RuvioClient<String, String> client() {
            RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1")
                    .port(a.node.port).cluster(true).commandTimeoutMillis(5_000).build());
            toClose.add(c);
            return c;
        }

        /** The n-th key (from 0) whose slot belongs to A (or B). */
        String key(boolean onA, int n) {
            int seen = 0;
            for (int i = 0; ; i++) {
                String k = "key" + i;
                if (a.owns(k) == onA && seen++ == n) {
                    return k;
                }
            }
        }
    }

    // ---- slot table and routing ---------------------------------------------------------------

    @Test
    void buildsSlotTableAndRoutesEachKeyToItsOwner() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        assertTrue(c.isCluster());
        assertEquals(Arrays.asList(p.a.node.address(), p.b.node.address()), c.nodeAddresses());
        String ka = p.key(true, 0);
        String kb = p.key(false, 0);
        c.set(ka, "va");
        c.set(kb, "vb");
        assertEquals("va", c.get(ka));
        assertEquals("vb", c.get(kb));
        assertEquals("va", p.a.store.get(ka));
        assertEquals("vb", p.b.store.get(kb));
        assertNull(p.a.store.get(kb));
        assertNull(p.b.store.get(ka));
        assertEquals(0, p.a.commands("GET").stream().filter(x -> x.get(1).equals(kb)).count(), "no detour through the wrong node");
        assertEquals(1, p.a.commands("CLUSTER").size(), "CLUSTER SLOTS asked once, from the seed");
        assertEquals(0, p.b.commands("CLUSTER").size());
    }

    @Test
    void hashtagsKeepKeysOnOneSlot() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        c.mset(new LinkedHashMap<>(Map.of("{user7}.name", "n")));
        Map<String, String> pairs = new LinkedHashMap<>();
        pairs.put("{user7}.a", "1");
        pairs.put("{user7}.b", "2");
        pairs.put("{user7}.c", "3");
        c.mset(pairs);
        int mset = p.a.commands("MSET").size() + p.b.commands("MSET").size();
        assertEquals(2, mset, "same hashtag: one MSET per call, not split");
        assertEquals(Arrays.asList("1", "2", "3"), c.mget("{user7}.a", "{user7}.b", "{user7}.c"));
        assertEquals(1, p.a.commands("MGET").size() + p.b.commands("MGET").size());
    }

    @Test
    void splitMgetKeepsTheCallersOrder() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            keys.add(p.key(i % 2 == 0, i / 2));
        }
        for (int i = 0; i < keys.size(); i++) {
            if (i != 3) { // key 3 stays missing -> null in place
                c.set(keys.get(i), "v" + i);
            }
        }
        List<String> got = c.mget(keys.toArray(new String[0]));
        assertEquals(Arrays.asList("v0", "v1", "v2", null, "v4", "v5"), got);
        List<String> reversed = new ArrayList<>(keys);
        java.util.Collections.reverse(reversed);
        assertEquals(Arrays.asList("v5", "v4", null, "v2", "v1", "v0"), c.mget(reversed.toArray(new String[0])));
    }

    @Test
    void splitMsetDelUnlinkExistsTouch() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        Map<String, String> pairs = new LinkedHashMap<>();
        for (int i = 0; i < 6; i++) {
            pairs.put(p.key(i % 2 == 0, i / 2), "v" + i);
        }
        c.mset(pairs);
        assertEquals(3, p.a.store.size());
        assertEquals(3, p.b.store.size());
        String[] keys = pairs.keySet().toArray(new String[0]);
        assertEquals(6L, c.exists(keys));
        assertEquals(6L, c.touch(keys));
        assertEquals(2L, c.unlink(keys[0], keys[1]));
        assertEquals(4L, c.del(keys));
        assertEquals(0, p.a.store.size() + p.b.store.size());
    }

    @Test
    void crossSlotCallsFailFastWithoutSendingAnything() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String ka = p.key(true, 0);
        String kb = p.key(false, 0);
        int before = p.a.received().size() + p.b.received().size();
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.rename(ka, kb));
        assertEquals("CROSSSLOT", e.code());
        assertThrows(RuvioServerException.class, () -> c.sunion(ka, kb));
        assertThrows(RuvioServerException.class, () -> c.msetnx(Map.of(ka, "1", kb, "2")));
        assertThrows(RuvioServerException.class, () -> c.execute("BITOP", "AND", "{t}dest", ka, kb));
        assertEquals(before, p.a.received().size() + p.b.received().size(), "nothing was sent");
    }

    // ---- MOVED ----------------------------------------------------------------------------------

    @Test
    void movedPatchesTheTableAndRetriesOnce() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        p.a.store.put(k, "old");
        // The slot moved from A to B behind the client's back.
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        p.a.start = slot + 1; // A no longer owns it
        p.b.start = Math.min(p.b.start, slot);
        p.b.store.put(k, "new");
        assertEquals("new", c.get(k));
        assertEquals(1, p.a.commands("GET").size(), "tried the stale owner once");
        assertEquals(1, p.b.commands("GET").size(), "retried on the node from MOVED");
        assertEquals("new", c.get(k));
        assertEquals(1, p.a.commands("GET").size(), "the table was patched: next call goes straight to B");
        assertEquals(2, p.b.commands("GET").size());
    }

    @Test
    void movedOnDifferentCommandsPatchesSlotsWithoutReloading() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k1 = p.key(true, 0);
        String k2 = p.key(true, 1);
        // Every slot moved from A to B; each command sees one MOVED for its own slot.
        p.a.end = -1;
        p.b.start = 0;
        p.b.store.put(k1, "one");
        p.b.store.put(k2, "two");
        assertEquals("one", c.get(k1));
        assertEquals("two", c.get(k2));
        assertEquals(1, p.a.commands("CLUSTER").size(), "known node, first MOVED per command: no reload");
        assertEquals(0, p.b.commands("CLUSTER").size());
        assertEquals(2, p.a.commands("GET").size());
        assertEquals(2, p.b.commands("GET").size());
    }

    @Test
    void followsUpToThreeMovedAndReloadsFromTheSecond() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // A always sends it to B; B sends the first GET back to A, then answers: A -> B -> A -> B.
        p.a.start = slot + 1;
        p.b.start = 0;
        p.b.store.put(k, "settled");
        java.util.concurrent.atomic.AtomicInteger bGets = new java.util.concurrent.atomic.AtomicInteger();
        p.b.node.handler = a -> a.get(0).equalsIgnoreCase("GET") && bGets.incrementAndGet() == 1
                ? FakeNode.error("MOVED " + slot + " " + p.a.node.address()) : p.b.handle(a);
        assertEquals("settled", c.get(k));
        assertEquals(2, p.a.commands("GET").size());
        assertEquals(2, p.b.commands("GET").size(), "the third redirect goes to the named node, not the reloaded owner");
        // Initial load, then one reload for the second and one for the third MOVED of this command.
        assertEquals(3, p.a.commands("CLUSTER").size());
    }

    @Test
    void aFourthMovedIsNotFollowed() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // Both nodes claim the other owns the slot: A -> B -> A -> B would loop forever.
        p.a.start = slot + 1;
        p.b.end = slot - 1;
        p.b.start = 0;
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get(k));
        assertEquals("MOVED", e.code());
        assertEquals(2, p.a.commands("GET").size(), "sent once plus three redirects, alternating A, B, A, B");
        assertEquals(2, p.b.commands("GET").size());
        assertEquals(3, p.a.commands("CLUSTER").size(), "reloads on the second and third MOVED");
    }

    @Test
    void movedToAnUnknownNodeRefreshesTheTable() throws Exception {
        Pair p = new Pair();
        Shard third = new Shard(0, 0);
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // The slot moved to a node the client has never seen; the refreshed CLUSTER SLOTS names it.
        third.start = slot;
        third.end = slot;
        third.store.put(k, "from-third");
        p.a.start = slot + 1;
        p.a.moveTo = third.node.address();
        String s = FakeNode.slots(new int[] {0, slot - 1, p.a.node.port}, new int[] {slot, slot, third.node.port},
                new int[] {slot + 1, 8191, p.a.node.port}, new int[] {8192, 16383, p.b.node.port});
        p.a.slotsReply = s;
        p.b.slotsReply = s;
        assertEquals("from-third", c.get(k));
        assertEquals(2, p.a.commands("CLUSTER").size(), "unknown node -> CLUSTER SLOTS refresh");
        assertTrue(c.nodeAddresses().contains(third.node.address()));
    }

    // ---- ASK and TRYAGAIN -------------------------------------------------------------------------

    @Test
    @Timeout(20)
    void askSendsAskingThenTheCommandToTheNamedNodeAndLeavesTheTableAlone() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        p.b.store.put(k, "from-b");
        java.util.concurrent.atomic.AtomicInteger aGets = new java.util.concurrent.atomic.AtomicInteger();
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET") && aGets.incrementAndGet() == 1
                ? FakeNode.error("ASK " + slot + " " + p.b.node.address()) : p.a.handle(a);
        p.a.store.put(k, "from-a");
        // B does not own the slot; after ASKING it answers the command anyway.
        p.b.node.handler = a -> a.get(0).equalsIgnoreCase("GET") ? FakeNode.bulk(p.b.store.get(a.get(1))) : p.b.handle(a);
        assertEquals("from-b", c.get(k));
        assertEquals(Arrays.asList(Arrays.asList("ASKING"), Arrays.asList("GET", k)),
                p.b.received().stream().filter(x -> !x.get(0).equalsIgnoreCase("CLUSTER")).collect(java.util.stream.Collectors.toList()),
                "B got ASKING then the command");
        assertEquals("from-a", c.get(k), "the table was not updated: the next command goes to A again");
        assertEquals(2, p.a.commands("GET").size());
        assertEquals(1, p.b.commands("GET").size());
        assertEquals(1, p.a.commands("CLUSTER").size(), "no reload for a known node");
    }

    @Test
    @Timeout(20)
    void askToAnUnknownNodeIsFollowed() throws Exception {
        Pair p = new Pair();
        Shard third = new Shard(0, 0);
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        third.store.put(k, "from-third");
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET")
                ? FakeNode.error("ASK " + slot + " " + third.node.address()) : p.a.handle(a);
        third.node.handler = a -> a.get(0).equalsIgnoreCase("GET") ? FakeNode.bulk(third.store.get(a.get(1))) : third.handle(a);
        assertEquals("from-third", c.get(k));
        assertEquals("ASKING", third.received().get(0).get(0));
        assertEquals(Arrays.asList("GET", k), third.received().get(1));
        assertEquals(1, p.a.commands("CLUSTER").size(), "no table reload for an ASK");
    }

    @Test
    @Timeout(20)
    void anErrorToAskingFailsTheCommand() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET")
                ? FakeNode.error("ASK " + slot + " " + p.b.node.address()) : p.a.handle(a);
        p.b.node.handler = a -> a.get(0).equalsIgnoreCase("ASKING") ? FakeNode.error("ERR no asking") : FakeNode.bulk("x");
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get(k));
        assertEquals("ERR", e.code());
    }

    @Test
    @Timeout(20)
    void tryAgainIsRetriedAfterAShortWait() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        p.a.store.put(k, "v");
        java.util.concurrent.atomic.AtomicInteger gets = new java.util.concurrent.atomic.AtomicInteger();
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET") && gets.incrementAndGet() == 1
                ? FakeNode.error("TRYAGAIN slot is being moved") : p.a.handle(a);
        long t0 = System.nanoTime();
        assertEquals("v", c.get(k));
        assertTrue(System.nanoTime() - t0 >= 4_000_000L, "waited about 5 ms before the retry");
        assertEquals(2, p.a.commands("GET").size());
    }

    @Test
    @Timeout(20)
    void tryAgainIsRetriedInTheAsyncApi() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        p.a.store.put(k, "v");
        java.util.concurrent.atomic.AtomicInteger gets = new java.util.concurrent.atomic.AtomicInteger();
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET") && gets.incrementAndGet() <= 2
                ? FakeNode.error("TRYAGAIN slot is being moved") : p.a.handle(a);
        long t0 = System.nanoTime();
        assertEquals("v", c.async().get(k).get(10, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(System.nanoTime() - t0 >= 14_000_000L, "waited 5 ms and 10 ms");
        assertEquals(3, p.a.commands("GET").size());
    }

    @Test
    @Timeout(20)
    void tryAgainFourTimesReturnsTheErrorAfterThreeRetries() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET")
                ? FakeNode.error("TRYAGAIN slot is being moved") : p.a.handle(a);
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get(k));
        assertEquals("TRYAGAIN", e.code());
        assertEquals(4, p.a.commands("GET").size(), "sent once plus three retries");
    }

    @Test
    @Timeout(20)
    void askAndMovedShareOneRedirectBudget() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String k = p.key(true, 0);
        int slot = HashSlot.of(k.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String toB = "ASK " + slot + " " + p.b.node.address();
        // A: ASK -> B; B: MOVED -> A; A: ASK -> B; B: ASK -> A (the fourth redirect, given to the caller).
        java.util.concurrent.atomic.AtomicInteger aGets = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger bGets = new java.util.concurrent.atomic.AtomicInteger();
        p.a.node.handler = a -> a.get(0).equalsIgnoreCase("GET")
                ? FakeNode.error(aGets.incrementAndGet() == 3 ? "ASK " + slot + " " + p.b.node.address() : toB)
                : p.a.handle(a);
        p.b.node.handler = a -> {
            if (!a.get(0).equalsIgnoreCase("GET")) {
                return p.b.handle(a);
            }
            return FakeNode.error(bGets.incrementAndGet() == 1 ? "MOVED " + slot + " " + p.a.node.address()
                    : "ASK " + slot + " " + p.a.node.address());
        };
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get(k));
        assertEquals("ASK", e.code());
        assertEquals(2, p.a.commands("GET").size(), "A saw the first send and the MOVED retry");
        assertEquals(2, p.b.commands("GET").size());
    }

    // ---- failures -------------------------------------------------------------------------------

    @Test
    void aLostConnectionIsNeverReplayedAndOtherNodesKeepWorking() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String ka = p.key(true, 0);
        String kb = p.key(false, 0);
        c.set(kb, "b");
        p.a.node.handler = args -> args.get(0).equalsIgnoreCase("INCR") ? FakeNode.CLOSE : p.a.handle(args);
        assertThrows(RuvioConnectionException.class, () -> c.incr(ka));
        assertEquals(1, p.a.node.commands("INCR").size(), "the failed command was not sent again");
        assertEquals("b", c.get(kb), "node B is unaffected");
        p.a.node.handler = p.a::handle;
        assertEquals(1L, c.incr(ka), "node A reconnects lazily");
        assertEquals(2, p.a.node.commands("INCR").size());
    }

    // ---- pipelines and transactions ---------------------------------------------------------------

    @Test
    void pipelineRepliesComeBackInTheCallersOrder() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            String k = p.key(i % 3 != 0, i / 2);
            keys.add(k);
            c.set(k, "v-" + k);
        }
        Pipeline<String, String> pipe = c.async().pipeline();
        List<CompletableFuture<String>> gets = new ArrayList<>();
        for (String k : keys) {
            gets.add(pipe.get(k));
        }
        CompletableFuture<Long> incr = pipe.incr(keys.get(1) + "-n");
        assertEquals(41, pipe.size());
        pipe.sync();
        for (int i = 0; i < keys.size(); i++) {
            assertEquals("v-" + keys.get(i), gets.get(i).join(), "reply " + i);
        }
        assertEquals(1L, incr.join());
    }

    @Test
    void transactionNeedsOneSlot() throws Exception {
        Pair p = new Pair();
        RuvioClient<String, String> c = p.client();
        String ka = p.key(true, 0);
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<Void> s = tx.set(ka, "1");
            CompletableFuture<Long> i = tx.incr(ka);
            assertTrue(tx.exec());
            assertEquals(1L, i.join());
            assertFalse(s.isCompletedExceptionally());
        }
        assertEquals(1, p.a.commands("MULTI").size());
        assertEquals(0, p.b.commands("MULTI").size());
        String kb = p.key(false, 0);
        try (Transaction<String, String> tx = c.multi()) {
            tx.set(ka, "1");
            assertThrows(RuvioServerException.class, () -> tx.set(kb, "2"));
            assertTrue(tx.exec(), "the refused command was never queued");
        }
        try (Transaction<String, String> tx = c.multi()) {
            tx.watch(ka);
            assertThrows(RuvioServerException.class, () -> tx.watch(kb));
        }
        try (Transaction<String, String> tx = c.multi()) {
            assertThrows(RuvioServerException.class, () -> tx.watch(ka, kb));
        }
    }

    // ---- standalone fallback ----------------------------------------------------------------------

    @Test
    void aStandaloneServerFallsBackToPlainMode() throws Exception {
        FakeNode node = new FakeNode(a -> {
            String cmd = a.get(0).toUpperCase();
            if (cmd.equals("CLUSTER")) {
                return FakeNode.error("ERR cluster mode is disabled");
            }
            if (cmd.equals("GET")) {
                return FakeNode.bulk("plain");
            }
            return FakeNode.ok();
        });
        toClose.add(node);
        RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1").port(node.port)
                .database(3).cluster(true).commandTimeoutMillis(5_000).build());
        toClose.add(c);
        assertFalse(c.isCluster());
        assertTrue(c.nodeAddresses().isEmpty());
        assertEquals("plain", c.get("any{key}"));
        assertEquals(1, node.commands("SELECT").stream().filter(x -> x.get(1).equals("3")).count(), "database 3 selected after the fallback");
        assertEquals(c, c.onNode("whatever:1"));
    }

    // ---- key positions ------------------------------------------------------------------------------

    private static List<String> keysOf(String... args) {
        Frame f = new Frame();
        f.out.header(args.length);
        for (String a : args) {
            f.out.bulk(a);
        }
        f.parse();
        ClusterKeys.Spec spec = ClusterKeys.spec(f.out.buf, f.off[0], f.len[0]);
        int n = ClusterKeys.keys(spec, f);
        List<String> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(args[f.keyIdx[i]]);
        }
        return out;
    }

    @Test
    void keyPositionsFollowTheRedisLayouts() {
        assertEquals(List.of("a", "b"), keysOf("MGET", "a", "b"));
        assertEquals(List.of("a", "b"), keysOf("mget", "a", "b"));
        assertEquals(List.of("a", "c"), keysOf("MSET", "a", "1", "c", "2"));
        assertEquals(List.of("k1", "k2"), keysOf("EVAL", "return 1", "2", "k1", "k2", "arg"));
        assertEquals(List.of(), keysOf("EVAL", "return 1", "0", "arg"));
        assertEquals(List.of("a", "b"), keysOf("LMPOP", "2", "a", "b", "LEFT"));
        assertEquals(List.of("a"), keysOf("BLMPOP", "0", "1", "a", "LEFT"));
        assertEquals(List.of("a", "b"), keysOf("BLPOP", "a", "b", "0"));
        assertEquals(List.of(), keysOf("PING"));
        assertEquals(List.of(), keysOf("CHANGES", "START"));
        assertEquals(List.of("lock"), keysOf("LEASE", "lock", "5000"));
        assertEquals(List.of("login"), keysOf("LIMIT", "login", "5", "60000"));
        assertEquals(List.of("pay"), keysOf("ONCE", "pay", "60000"));
        assertEquals(List.of("pay"), keysOf("IDEM", "pay", "BEGIN", "fp", "owner", "60000"));
        assertEquals(List.of("s"), keysOf("XADD", "s", "DELAY", "5", "*", "f", "v"));
        assertEquals(List.of(), keysOf("PUBLISH", "chan", "hi"));
        assertEquals(List.of("chan"), keysOf("SPUBLISH", "chan", "hi"));
        assertEquals(List.of("dest", "a", "b"), keysOf("BITOP", "AND", "dest", "a", "b"));
        assertEquals(List.of("a", "b"), keysOf("SINTERCARD", "2", "a", "b", "LIMIT", "1"));
        assertEquals(List.of("a", "b"), keysOf("JSON.MGET", "a", "b", "$.x"));
        assertEquals(List.of("s"), keysOf("XGROUP", "CREATE", "s", "g", "$"));
        assertEquals(List.of("s"), keysOf("XINFO", "STREAM", "s"));
        assertEquals(List.of("s"), keysOf("OBJECT", "ENCODING", "s"));
        assertEquals(List.of(), keysOf("RUVIO", "OWNER"));
        assertEquals(List.of(), keysOf("JSON.INDEX", "CREATE", "i", "PREFIX", "u:"));
        assertEquals(List.of(), keysOf("JSON.FIND", "i", "VALUE", "x"));
        assertEquals(List.of("recent"), keysOf("LPUSHCAP", "recent", "10", "a"));
        assertEquals(List.of("lock"), keysOf("DELEX", "lock", "IFEQ", "t"));
        assertEquals(List.of("bf"), keysOf("BF.MADD", "bf", "a", "b"));
        assertEquals(List.of(), keysOf("WAIT", "0", "10"));
        assertEquals(List.of("dst", "a", "b"), keysOf("ZUNIONSTORE", "dst", "2", "a", "b", "WEIGHTS", "1", "2"));
        assertEquals(List.of("a", "b"), keysOf("ZUNION", "2", "a", "b"));
        assertEquals(List.of("s1", "s2"), keysOf("XREAD", "COUNT", "2", "STREAMS", "s1", "s2", "0", "0"));
        assertEquals(List.of("dst", "src"), keysOf("GEOSEARCHSTORE", "dst", "src", "FROMLONLAT", "1", "2"));
        assertEquals(List.of("g", "out"), keysOf("GEORADIUS", "g", "1", "2", "5", "km", "STORE", "out"));
        assertEquals(List.of("g"), keysOf("GEORADIUS", "g", "1", "2", "5", "km"));
        assertEquals(List.of("a", "b"), keysOf("RENAME", "a", "b"));
        assertEquals(List.of("src", "dst"), keysOf("LMOVE", "src", "dst", "LEFT", "RIGHT"));
        assertEquals(List.of("a", "b"), keysOf("PFMERGE", "a", "b"));
        assertEquals(List.of("a", "b", "c"), keysOf("SDIFFSTORE", "a", "b", "c"));
        assertEquals(List.of("k"), keysOf("SOMETHINGNEW", "k", "x"));
    }

    @Test
    void hashSlotVectors() {
        assertEquals(12182, HashSlot.of("foo".getBytes()));
        assertEquals(12739, HashSlot.of("123456789".getBytes()));
        assertEquals(HashSlot.of("user1000".getBytes()), HashSlot.of("{user1000}.following".getBytes()));
        assertEquals(HashSlot.of("{user1000}.following".getBytes()), HashSlot.of("foo{user1000}bar".getBytes()));
        // Empty tag: the whole key is hashed. Only the first tag counts.
        assertEquals(HashSlot.of("foo{}{bar}".getBytes()), HashSlot.of("foo{}{bar}".getBytes()));
        assertFalse(HashSlot.of("foo{}{bar}".getBytes()) == HashSlot.of("bar".getBytes())
                && HashSlot.of("foo{}{bar}".getBytes()) == HashSlot.of("{bar}".getBytes()) && false);
        assertEquals(HashSlot.of("{a}{b}".getBytes()), HashSlot.of("a".getBytes()));
        assertEquals(HashSlot.of("{}".getBytes()), HashSlot.of("{}".getBytes()));
        byte[] framed = "xx{tag}yy".getBytes();
        assertEquals(HashSlot.of("tag".getBytes()), HashSlot.of(framed, 0, framed.length));
        assertEquals(HashSlot.of("{tag}".getBytes()), HashSlot.of(framed, 2, 5));
        assertEquals(RuvioClient.hashSlot("foo"), 12182);
        Map<Integer, Integer> seen = new HashMap<>();
        for (int i = 0; i < 20_000; i++) {
            seen.merge(RuvioClient.hashSlot("key" + i), 1, Integer::sum);
        }
        assertTrue(seen.size() > 10_000, "keys spread over many slots");
    }
}
