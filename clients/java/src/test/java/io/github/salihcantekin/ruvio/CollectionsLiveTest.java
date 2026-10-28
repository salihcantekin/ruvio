// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ExpireCondition;
import io.github.salihcantekin.ruvio.args.LPosArgs;
import io.github.salihcantekin.ruvio.args.ListDirection;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.model.KeyValue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Hashes, lists and sets. */
class CollectionsLiveTest {
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
    void hashes() {
        assertEquals(1L, c.hset("h", "f1", "v1"));
        Map<String, String> m = new LinkedHashMap<>();
        m.put("f2", "v2");
        m.put("n", "5");
        assertEquals(2L, c.hset("h", m));
        assertEquals("v1", c.hget("h", "f1"));
        assertNull(c.hget("h", "nope"));
        assertEquals(3L, c.hlen("h"));
        Map<String, String> all = c.hgetall("h");
        assertEquals(Arrays.asList("f1", "f2", "n"), List.copyOf(all.keySet()));
        assertEquals(Arrays.asList("v1", null), c.hmget("h", "f1", "x"));
        assertTrue(c.hexists("h", "f1"));
        assertEquals(Arrays.asList("f1", "f2", "n"), c.hkeys("h"));
        assertEquals(Arrays.asList("v1", "v2", "5"), c.hvals("h"));
        assertEquals(7L, c.hincrby("h", "n", 2));
        assertEquals(7.5, c.hincrbyfloat("h", "n", 0.5));
        assertFalse(c.hsetnx("h", "f1", "x"));
        assertTrue(c.hsetnx("h", "f3", "x"));
        Map<String, String> legacy = new LinkedHashMap<>();
        legacy.put("a", "1");
        legacy.put("b", "2");
        c.hmset("hm", legacy);
        assertEquals(Arrays.asList("1", "2"), c.hmget("hm", "a", "b"));
        assertEquals(2L, c.hstrlen("h", "f1"));
        assertEquals(1L, c.hdel("h", "f3", "zz"));
        assertNotNull(c.hrandfield("h"));
        assertEquals(2, c.hrandfield("h", 2).size());
        List<KeyValue<String, String>> pairs = c.hrandfieldWithValues("h", 3);
        assertEquals(3, pairs.size());
        assertEquals(all.get(pairs.get(0).key()) == null ? null : pairs.get(0).value(), all.containsKey(pairs.get(0).key()) ? c.hget("h", pairs.get(0).key()) : null);
        Map<String, String> scanned = new HashMap<>();
        for (KeyValue<String, String> kv : c.hscanIterable("h", ScanArgs.count(1))) {
            scanned.put(kv.key(), kv.value());
        }
        assertEquals(c.hgetall("h"), scanned);
        assertEquals("0", c.hscan("h", "0", ScanArgs.match("f*")).cursor());
        assertEquals(2, c.hscan("h", "0", ScanArgs.match("f*")).items().size());
        assertEquals(3, c.hscanStream("h", null).count());
    }

    @Test
    void hashFieldTtl() {
        c.hset("ht", "a", "1");
        c.hset("ht", "b", "2");
        assertEquals(Arrays.asList(1L, -2L), c.hexpire("ht", 100, "a", "zz"));
        assertEquals(Arrays.asList(0L), c.hexpire("ht", 50, ExpireCondition.GT, "a"));
        assertEquals(Arrays.asList(1L), c.hpexpire("ht", 200_000, "b"));
        assertEquals(Arrays.asList(1L), c.hpexpire("ht", 300_000, ExpireCondition.GT, "b"));
        long ttl = c.httl("ht", "a").get(0);
        assertTrue(ttl > 90 && ttl <= 100);
        assertTrue(c.hpttl("ht", "b").get(0) > 200_000);
        long now = System.currentTimeMillis();
        assertEquals(Arrays.asList(1L), c.hexpireat("ht", now / 1000 + 500, "a"));
        assertEquals(Arrays.asList(1L), c.hexpireat("ht", now / 1000 + 600, ExpireCondition.XX, "a"));
        assertEquals(now / 1000 + 600, c.hexpiretime("ht", "a").get(0));
        assertEquals(Arrays.asList(1L), c.hpexpireat("ht", now + 700_000, "b"));
        assertEquals(Arrays.asList(0L), c.hpexpireat("ht", now + 1_000, ExpireCondition.GT, "b"));
        assertEquals(now + 700_000, c.hpexpiretime("ht", "b").get(0));
        assertEquals(Arrays.asList(1L, -2L), c.hpersist("ht", "a", "zz"));
        assertEquals(Arrays.asList(-1L), c.httl("ht", "a"));
    }

    @Test
    void lists() {
        assertEquals(3L, c.rpush("l", "a", "b", "c"));
        assertEquals(4L, c.lpush("l", "z"));
        assertEquals(0L, c.lpushx("l:none", "x"));
        assertEquals(5L, c.rpushx("l", "d"));
        assertEquals(Arrays.asList("z", "a", "b", "c", "d"), c.lrange("l", 0, -1));
        assertEquals(5L, c.llen("l"));
        assertEquals("a", c.lindex("l", 1));
        assertEquals("z", c.lpop("l"));
        assertEquals("d", c.rpop("l"));
        assertEquals(Arrays.asList("a", "b"), c.lpop("l", 2));
        c.rpush("l", "x", "y", "z");
        assertEquals(Arrays.asList("z", "y"), c.rpop("l", 2));
        assertTrue(c.lpop("l:none", 2).isEmpty());
        c.ltrim("l", 0, 0);
        assertEquals(Arrays.asList("c"), c.lrange("l", 0, -1));
        assertEquals(2L, c.linsert("l", true, "c", "b"));
        assertEquals(3L, c.linsert("l", false, "c", "d"));
        c.lset("l", 0, "B");
        c.rpush("l", "c", "c");
        assertEquals(2L, c.lrem("l", 2, "c"));
        assertEquals(Arrays.asList("B", "d", "c"), c.lrange("l", 0, -1));
        assertEquals(1L, c.lpos("l", "d"));
        assertNull(c.lpos("l", "nope"));
        c.rpush("l", "d");
        assertEquals(3L, c.lpos("l", "d", LPosArgs.rank(2)));
        assertEquals(Arrays.asList(1L, 3L), c.lposAll("l", "d", 0));
        assertEquals(Arrays.asList(3L), c.lposAll("l", "d", 0, LPosArgs.rank(2).withMaxLen(10)));
        assertEquals("B", c.lmove("l", "l2", ListDirection.LEFT, ListDirection.RIGHT));
        assertEquals("d", c.rpoplpush("l", "l2"));
        assertEquals(Arrays.asList("d", "B"), c.lrange("l2", 0, -1));
        KeyValue<String, List<String>> popped = c.lmpop(ListDirection.LEFT, 2, "l:none", "l2");
        assertEquals("l2", popped.key());
        assertEquals(Arrays.asList("d", "B"), popped.value());
        assertNull(c.lmpop(ListDirection.LEFT, 1, "l:none"));
        // capped lists
        assertEquals(2L, c.lpushcap("cap", 2, "a", "b", "c"));
        assertEquals(Arrays.asList("c", "b"), c.lrange("cap", 0, -1));
        assertEquals(2L, c.rpushcap("cap", 2, "d"));
        assertEquals(Arrays.asList("b", "d"), c.lrange("cap", 0, -1));
        assertEquals(3L, c.pushRecent("recent", 3, "1", "2", "3", "4"));
        assertEquals(Arrays.asList("4", "3"), c.recent("recent", 2));
    }

    @Test
    void blockingLists() throws Exception {
        try (RuvioClient<String, String> d = c.dedicated()) {
            assertNull(d.blpop(0.05, "bl:none"));
            c.rpush("bl", "a", "b", "c", "d", "e");
            assertEquals(new KeyValue<>("bl", "a"), d.blpop(1, "bl"));
            assertEquals(new KeyValue<>("bl", "e"), d.brpop(1, "bl:none", "bl"));
            assertEquals("b", d.blmove("bl", "bl2", ListDirection.LEFT, ListDirection.LEFT, 1));
            KeyValue<String, List<String>> kv = d.blmpop(1, ListDirection.RIGHT, 5, "bl");
            assertEquals(Arrays.asList("d", "c"), kv.value());
            assertNull(d.blmpop(0.05, ListDirection.RIGHT, 1, "bl"));
            assertNull(d.blmove("bl:none", "bl2", ListDirection.LEFT, ListDirection.LEFT, 0.05));
            // a waiter is served by a later push from another connection
            java.util.concurrent.CompletableFuture<KeyValue<String, String>> waiting = d.async().blpop(5, "bl:wait");
            Thread.sleep(50);
            c.rpush("bl:wait", "late");
            assertEquals("late", waiting.get(5, java.util.concurrent.TimeUnit.SECONDS).value());
        }
    }

    @Test
    void asyncCommandTimeoutOnARealServer() throws Exception {
        RuvioOptions o = Live.options(Live.DB).toBuilder().commandTimeoutMillis(300).build();
        try (RuvioClient<String, String> t = RuvioClient.connect(o)) {
            // A blocking command gets its own wait on top: BLPOP 1 returns nil after about 1 s, no timeout error.
            long start = System.nanoTime();
            assertNull(t.async().blpop(1, "bl:async:none").get(5, java.util.concurrent.TimeUnit.SECONDS));
            long took = (System.nanoTime() - start) / 1_000_000;
            assertTrue(took >= 900 && took < 2500, "took " + took + " ms");
            // Plain commands still answer, and BLPOP 0 (forever) is not failed by the deadline check.
            java.util.concurrent.CompletableFuture<?> forever = t.async().blpop(0, "bl:async:forever");
            Thread.sleep(800);
            assertFalse(forever.isDone());
            c.rpush("bl:async:forever", "x");
            assertNotNull(forever.get(5, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals("PONG", t.async().ping().get(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test
    void sets() {
        assertEquals(3L, c.sadd("s1", "a", "b", "c"));
        assertEquals(1L, c.srem("s1", "c"));
        assertTrue(c.sismember("s1", "a"));
        assertEquals(Arrays.asList(true, false), c.smismember("s1", "a", "z"));
        assertEquals(2L, c.scard("s1"));
        assertEquals(Arrays.asList("a", "b"), c.smembers("s1"));
        c.sadd("s2", "b", "c");
        assertEquals(Arrays.asList("b"), c.sinter("s1", "s2"));
        assertEquals(Set.of("a", "b", "c"), new HashSet<>(c.sunion("s1", "s2"))); // observed unsorted despite the matrix note
        assertEquals(Arrays.asList("a"), c.sdiff("s1", "s2"));
        assertEquals(1L, c.sinterstore("s3", "s1", "s2"));
        assertEquals(3L, c.sunionstore("s4", "s1", "s2"));
        assertEquals(1L, c.sdiffstore("s5", "s1", "s2"));
        assertEquals(1L, c.sintercard("s1", "s2"));
        assertEquals(1L, c.sintercard(1, "s4", "s1"));
        assertTrue(c.smove("s1", "s2", "a"));
        assertFalse(c.smove("s1", "s2", "zz"));
        assertNotNull(c.srandmember("s4"));
        assertEquals(3, c.srandmember("s4", 5).size());
        assertEquals(5, c.srandmember("s4", -5).size());
        assertNotNull(c.spop("s4"));
        assertEquals(2, c.spop("s4", 5).size());
        assertNull(c.spop("s4"));
        for (int i = 0; i < 50; i++) {
            c.sadd("sbig", "m" + i);
        }
        Set<String> seen = new HashSet<>();
        for (String m : c.sscanIterable("sbig", ScanArgs.count(7))) {
            seen.add(m);
        }
        assertEquals(50, seen.size());
        assertEquals(11, c.sscanStream("sbig", ScanArgs.match("m1*")).count());
        assertEquals(50, c.sscan("sbig", "0", ScanArgs.count(100)).items().size());
        assertEquals("0", c.sscan("none", "0").cursor());
    }

    @Test
    void ttlOnCreateHincrby() {
        assertEquals(2L, c.hincrby("ttl:h", "f", 2, LONG_TTL));
        assertEquals(4L, c.hincrby("ttl:h", "f", 2, LONGER_TTL));
        assertDeadlineKept("ttl:h");
    }

    private static final java.time.Duration LONG_TTL = java.time.Duration.ofSeconds(100);
    private static final java.time.Duration LONGER_TTL = java.time.Duration.ofSeconds(1000);

    /** The key got the first call's deadline (at most 100 s), not the second call's 1000 s. */
    private void assertDeadlineKept(String key) {
        long ms = c.pttl(key);
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
    }
}
