// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.BfInsertArgs;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JsonBloomLiveTest {
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
    void json() {
        assertTrue(c.jsonSet("j", "$", "{\"a\":1,\"s\":\"x\",\"arr\":[1,2,3],\"o\":{\"k\":true},\"f\":false}"));
        assertEquals("1", c.jsonGet("j", "$.a"));
        assertTrue(c.jsonGet("j").startsWith("{"));
        assertNull(c.jsonGet("j:none"));
        assertFalse(c.jsonSetNx("j", "$.a", "2"));
        assertTrue(c.jsonSetXx("j", "$.a", "2"));
        assertFalse(c.jsonSetXx("j", "$.zz", "2"));
        assertTrue(c.jsonSetNx("j", "$.b", "\"new\""));
        assertEquals("integer", c.jsonType("j", "$.a"));
        assertEquals("array", c.jsonType("j", "$.arr"));
        assertNull(c.jsonType("j", "$.missing"));
        assertEquals(1L, c.jsonStrlen("j", "$.s"));
        assertEquals(3L, c.jsonArrlen("j", "$.arr"));
        assertEquals(Arrays.asList("k"), c.jsonObjkeys("j", "$.o"));
        assertEquals(1L, c.jsonObjlen("j", "$.o"));
        assertEquals("5", c.jsonNumincrby("j", "$.a", 3));
        assertEquals("10", c.jsonNummultby("j", "$.a", 2));
        assertTrue(c.jsonToggle("j", "$.f"));
        assertEquals(5L, c.jsonArrappend("j", "$.arr", "4", "\"five\""));
        assertEquals(6L, c.jsonArrinsert("j", "$.arr", 0, "0"));
        assertEquals("\"five\"", c.jsonArrpop("j", "$.arr"));
        assertEquals("0", c.jsonArrpop("j", "$.arr", 0));
        assertEquals(2L, c.jsonArrindex("j", "$.arr", "3"));
        assertEquals(-1L, c.jsonArrindex("j", "$.arr", "3", 0, 1));
        assertEquals(2L, c.jsonArrtrim("j", "$.arr", 0, 1));
        assertEquals(3L, c.jsonStrappend("j", "$.s", "\"yz\""));
        c.jsonMerge("j", "$.o", "{\"m\":1}");
        c.jmerge("j", "$.o", "{\"n\":2}");
        assertEquals(3L, c.jsonObjlen("j", "$.o"));
        assertEquals(1L, c.jsonClear("j", "$.o"));
        assertEquals(1L, c.jsonDel("j", "$.b"));
        assertEquals(1L, c.jsonForget("j", "$.s"));
        c.jsonSet("j2", "$", "[9]");
        assertEquals("9", c.jsonArrpop("j2"));
        c.jsonSet("j3", "$", "{\"a\":1}");
        List<String> mget = c.jsonMget("$.a", "j3", "j:none");
        assertEquals("1", mget.get(0));
        assertNull(mget.get(1));
        assertEquals(1L, c.jsonDel("j3"));
        // typed helpers through the pluggable codec
        assertTrue(c.jsonSetObject("j4", "$", "{\"name\":\"ada\"}"));
        assertEquals("\"ada\"", c.jsonGetObject("j4", "$.name", String.class));
        assertEquals(Arrays.asList("\"ada\"", null), c.jsonMgetObjects("$.name", String.class, "j4", "j:none"));
        c.setJsonCodec(new JsonCodec() {
            @Override
            public String toJson(Object value) {
                return "{\"n\":" + ((int[]) value)[0] + "}";
            }

            @Override
            public <T> T fromJson(String json, Class<T> type) {
                return type.cast(Integer.valueOf(json.replaceAll("[^0-9]", "")));
            }
        });
        assertTrue(c.jsonSetObject("j5", "$", new int[] {42}));
        assertEquals(42, c.jsonGetObject("j5", "$.n", Integer.class));
        c.setJsonCodec(JsonCodec.PASSTHROUGH);
        assertThrows(IllegalArgumentException.class, () -> c.jsonSetObject("j6", "$", 5));
    }

    @Test
    void jsonIndex() throws Exception {
        // JSON.INDEX works on database 0 only.
        try (RuvioClient<String, String> z = RuvioClient.connect(Live.options(0))) {
            String p = "jtest" + System.nanoTime() + ":";
            z.jsonSet(p + "1", "$", "{\"city\":\"Oslo\"}");
            z.jsonSet(p + "2", "$", "{\"city\":\"Rome\"}");
            z.jsonSet(p + "3", "$", "{\"city\":\"Oslo\"}");
            String index = "ix" + System.nanoTime();
            z.jsonIndexCreate(index, p, "$.city", "string");
            assertTrue(z.jsonIndexList().stream().anyMatch(l -> l.startsWith(index)));
            List<String> found = null;
            for (int i = 0; i < 100 && found == null; i++) {
                try {
                    found = z.jsonFind(index, "Oslo");
                } catch (RuvioServerException building) {
                    Thread.sleep(10);
                }
            }
            assertEquals(Arrays.asList(p + "1", p + "3"), found);
            assertEquals(Arrays.asList(p + "3"), z.jsonFind(index, "Oslo", 10, p + "1"));
            assertEquals(Arrays.asList(p + "1"), z.jsonFind(index, "Oslo", 1, null));
            assertEquals(1L, z.jsonIndexDrop(index));
            assertEquals(0L, z.jsonIndexDrop(index));
            z.del(p + "1", p + "2", p + "3");
        }
    }

    @Test
    void bloom() {
        c.bfReserve("bf", 0.01, 1000);
        assertThrows(RuvioServerException.class, () -> c.bfReserve("bf", 0.01, 1000));
        c.bfReserve("bf2", 0.01, 100, 4, false);
        c.bfReserve("bf3", 0.01, 2, 0, true);
        assertTrue(c.bfAdd("bf", "a"));
        assertFalse(c.bfAdd("bf", "a"));
        assertEquals(Arrays.asList(false, true), c.bfMadd("bf", "a", "b"));
        assertTrue(c.bfExists("bf", "b"));
        assertFalse(c.bfExists("bf", "zz"));
        assertEquals(Arrays.asList(true, false), c.bfMexists("bf", "a", "zz"));
        assertEquals(2L, c.bfCard("bf"));
        Map<String, Object> info = c.bfInfo("bf");
        assertEquals(1000L, info.get("Capacity"));
        assertEquals(1000L, c.bfInfo("bf", "CAPACITY"));
        assertEquals(Arrays.asList(true, true), c.bfInsert("bfi", BfInsertArgs.builder().capacity(50).error(0.001).expansion(2), "x", "y"));
        assertEquals(Arrays.asList(false), c.bfInsert("bfi", null, "x"));
        assertThrows(RuvioServerException.class, () -> c.bfInsert("bf:none", BfInsertArgs.builder().noCreate(), "x"));
        assertEquals(Arrays.asList(true), c.bfInsert("bfn", BfInsertArgs.builder().nonScaling(), "x"));
        // "seen before" helper
        assertEquals(Arrays.asList(false, false), c.seenBefore("seen", "u1", "u2"));
        assertEquals(Arrays.asList(true, false), c.seenBefore("seen", "u1", "u3"));
        assertEquals(Arrays.asList(true, false), c.mightContain("seen", "u2", "u9"));
    }

    @Test
    void ttlOnCreateJsonAndBloom() {
        assertTrue(c.jsonSet("ttl:j", "$", "{\"a\":1}", LONG_TTL));
        assertTrue(c.jsonSet("ttl:j", "$.a", "2", LONGER_TTL));
        assertDeadlineKept("ttl:j");
        assertTrue(c.jsonSetNx("ttl:jn", "$", "{}", LONG_TTL));
        assertFalse(c.jsonSetNx("ttl:jn", "$", "{}", LONGER_TTL));
        assertDeadlineKept("ttl:jn");
        assertFalse(c.jsonSetXx("ttl:jx", "$", "{}", LONG_TTL));
        assertEquals(0L, c.exists("ttl:jx"));

        assertEquals(Arrays.asList(true), c.bfInsert("ttl:bf", BfInsertArgs.builder().ttl(LONG_TTL).capacity(100), "a"));
        assertEquals(Arrays.asList(true), c.bfInsert("ttl:bf", BfInsertArgs.builder().ttl(LONGER_TTL), "b"));
        assertDeadlineKept("ttl:bf");
    }

    private static final java.time.Duration LONG_TTL = java.time.Duration.ofSeconds(100);
    private static final java.time.Duration LONGER_TTL = java.time.Duration.ofSeconds(1000);

    /** The key got the first call's deadline (at most 100 s), not the second call's 1000 s. */
    private void assertDeadlineKept(String key) {
        long ms = c.pttl(key);
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
    }
}
