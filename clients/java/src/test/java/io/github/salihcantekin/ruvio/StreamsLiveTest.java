// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.XAddArgs;
import io.github.salihcantekin.ruvio.args.XClaimArgs;
import io.github.salihcantekin.ruvio.args.XReadArgs;
import io.github.salihcantekin.ruvio.model.AutoClaimResult;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.PendingEntry;
import io.github.salihcantekin.ruvio.model.PendingSummary;
import io.github.salihcantekin.ruvio.model.StreamEntry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StreamsLiveTest {
    static RuvioClient<String, String> c;

    @BeforeAll
    static void open() {
        c = Live.client();
    }

    @AfterAll
    static void close() {
        if (c != null) c.close();
    }

    static Map<String, String> f(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    static List<KeyValue<String, String>> kv(String... kv) {
        List<KeyValue<String, String>> out = new ArrayList<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.add(new KeyValue<>(kv[i], kv[i + 1]));
        }
        return out;
    }

    @Test
    void repeatedFieldsKeepTheirOrder() {
        c.execute("XADD", "st:dup", "1-0", "f", "1", "g", "", "f", "3");
        List<KeyValue<String, String>> want = kv("f", "1", "g", "", "f", "3");
        assertEquals(want, c.xrange("st:dup", "-", "+").get(0).fields());
        assertEquals(want, c.xrevrange("st:dup", "+", "-").get(0).fields());
        assertEquals(want, c.xread("st:dup", "0").get("st:dup").get(0).fields());
        c.xgroupCreate("st:dup", "g", "0");
        assertEquals(want, c.xreadgroup("g", "c", "st:dup").get("st:dup").get(0).fields());
    }

    @Test
    void basics() {
        String id1 = c.xadd("st", f("a", "1", "b", "2"));
        assertTrue(id1.contains("-"));
        String id2 = c.xadd("st", "9999999999999-1", f("a", "3"));
        assertEquals("9999999999999-1", id2);
        assertNull(c.xadd("st:none", XAddArgs.builder().noMkStream(), f("a", "1")));
        assertNotNull(c.xadd("st", XAddArgs.builder().maxLen(5, true), f("a", "4")));
        assertEquals(3L, c.xlen("st"));
        List<StreamEntry<String, String>> all = c.xrange("st", "-", "+");
        assertEquals(3, all.size());
        assertEquals(kv("a", "1", "b", "2"), all.get(0).fields());
        assertEquals(1, c.xrange("st", "-", "+", 1).size());
        assertEquals(id2, c.xrevrange("st", "+", "-", 2).get(1).id());
        assertEquals(3, c.xrevrange("st", "+", "-").size());
        Map<String, List<StreamEntry<String, String>>> read = c.xread("st", "0");
        assertEquals(3, read.get("st").size());
        Map<String, String> streams = new LinkedHashMap<>();
        streams.put("st", id1);
        assertEquals(2, c.xread(XReadArgs.builder().count(5), streams).get("st").size());
        assertEquals(1L, c.xdel("st", id1, id1));
        assertEquals(1L, c.xtrimMaxlen("st", 1, false));
        assertEquals(1L, c.xlen("st"));
        assertEquals(0L, c.xtrimMinid("st", "0-1", true));
        c.xsetid("st", "99999999999999-0");
        assertThrows(RuvioServerException.class, () -> c.xadd("st", "1-1", f("x", "y")));
        Map<String, Object> info = c.xinfoStream("st");
        assertEquals(1L, info.get("length"));
        // MINID trim with XADD
        assertNotNull(c.xadd("st", XAddArgs.builder().minId("99999999999999-0"), f("z", "1")));
    }

    @Test
    void groups() throws Exception {
        c.xgroupCreate("gs", "g", "$", true);
        assertThrows(RuvioServerException.class, () -> c.xgroupCreate("gs", "g", "$"));
        c.xgroupCreate("gs", "g2", "0");
        assertTrue(c.xgroupCreateconsumer("gs", "g", "c1"));
        assertFalse(c.xgroupCreateconsumer("gs", "g", "c1"));
        String a = c.xadd("gs", f("n", "1"));
        String b = c.xadd("gs", f("n", "2"));
        Map<String, List<StreamEntry<String, String>>> got = c.xreadgroup("g", "c1", "gs");
        assertEquals(2, got.get("gs").size());
        assertTrue(c.xreadgroup("g", "c1", "gs").isEmpty() || c.xreadgroup("g", "c1", "gs").get("gs").isEmpty());
        PendingSummary sum = c.xpending("gs", "g");
        assertEquals(2, sum.count());
        assertEquals(a, sum.smallestId());
        assertEquals(2L, sum.consumers().get("c1"));
        List<PendingEntry> rows = c.xpending("gs", "g", "-", "+", 10);
        assertEquals(2, rows.size());
        assertEquals("c1", rows.get(0).consumer());
        assertEquals(1L, rows.get(0).deliveryCount());
        assertEquals(2, c.xpending("gs", "g", 0, "-", "+", 10, "c1").size());
        Thread.sleep(5);
        List<StreamEntry<String, String>> claimed = c.xclaim("gs", "g", "c2", 1, a);
        assertEquals(a, claimed.get(0).id());
        assertEquals(Arrays.asList(b), c.xclaimJustid("gs", "g", "c2", 0, XClaimArgs.builder().retryCount(3), b));
        assertEquals(1, c.xclaim("gs", "g", "c3", 0, XClaimArgs.builder().idle(10).force(), a).size());
        AutoClaimResult<String, String> ac = c.xautoclaim("gs", "g", "c4", 0, "0-0");
        assertEquals(2, ac.entries().size());
        assertEquals("0-0", ac.nextId());
        AutoClaimResult<String, String> ids = c.xautoclaim("gs", "g", "c5", 0, "0-0", 1, true);
        assertEquals(1, ids.claimedIds().size());
        assertTrue(ids.entries().isEmpty());
        assertEquals(1L, c.xack("gs", "g", b)); // c5 still holds a
        assertEquals(1L, c.xgroupDelconsumer("gs", "g", "c5"));
        c.xgroupSetid("gs", "g", "0");
        assertEquals(2, c.xreadgroup("g", "c6", XReadArgs.builder().count(10).noAck(), "gs", ">").get("gs").size());
        List<Map<String, Object>> groups = c.xinfoGroups("gs");
        assertEquals(2, groups.size());
        assertEquals("g", groups.get(0).get("name"));
        assertFalse(c.xinfoConsumers("gs", "g").isEmpty());
        assertTrue(c.xgroupDestroy("gs", "g2"));
        assertFalse(c.xgroupDestroy("gs", "g2"));
        // DELAY hides the entry from XREADGROUP until it passes. The 3 s delay leaves the read
        // right after a wide margin; the entry then shows up within a deadline.
        c.xgroupCreate("ds", "dg", "$", true);
        c.xadd("ds", XAddArgs.builder().delay(3_000), f("d", "1"));
        Map<String, List<StreamEntry<String, String>>> early = c.xreadgroup("dg", "x", "ds");
        assertTrue(early.isEmpty() || early.get("ds").isEmpty());
        long deadline = System.nanoTime() + 15_000_000_000L;
        Map<String, List<StreamEntry<String, String>>> late = Map.of();
        while ((late.isEmpty() || late.get("ds").isEmpty()) && System.nanoTime() < deadline) {
            Thread.sleep(50);
            late = c.xreadgroup("dg", "x", "ds");
        }
        assertEquals(1, late.get("ds").size());
    }

    @Test
    void blockingRead() throws Exception {
        try (RuvioClient<String, String> d = c.dedicated()) {
            // From 0-0 on the empty stream rather than `$`: the XADD may reach the server before
            // the blocking read does, and `$` would then miss it.
            Map<String, String> fromStart = new LinkedHashMap<>();
            fromStart.put("bs", "0-0");
            var waiting = d.async().xread(XReadArgs.builder().block(10_000).count(1), fromStart);
            Thread.sleep(50);
            c.xadd("bs", f("k", "v"));
            assertEquals("v", waiting.get(15, java.util.concurrent.TimeUnit.SECONDS).get("bs").get(0).fields().get(0).value());
            Map<String, String> s = new LinkedHashMap<>();
            s.put("bs", "$");
            assertTrue(d.xread(XReadArgs.builder().block(20), s).isEmpty());
        }
    }

    @Test
    void ttlOnCreateXadd() {
        Map<String, String> f = new LinkedHashMap<>();
        f.put("a", "1");
        assertNotNull(c.xadd("ttl:s", XAddArgs.builder().ttl(LONG_TTL), f));
        assertNotNull(c.xadd("ttl:s", XAddArgs.builder().ttl(LONGER_TTL), f));
        assertDeadlineKept("ttl:s");
        assertEquals(2L, c.xlen("ttl:s"));
    }

    private static final java.time.Duration LONG_TTL = java.time.Duration.ofSeconds(100);
    private static final java.time.Duration LONGER_TTL = java.time.Duration.ofSeconds(1000);

    /** The key got the first call's deadline (at most 100 s), not the second call's 1000 s. */
    private void assertDeadlineKept(String key) {
        long ms = c.pttl(key);
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
    }
}
