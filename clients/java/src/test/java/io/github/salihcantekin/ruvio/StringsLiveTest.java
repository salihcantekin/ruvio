// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.SetOptions;
import io.github.salihcantekin.ruvio.model.FillResult;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StringsLiveTest {
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
    void basicSetGet() {
        c.set("s:a", "hello");
        assertEquals("hello", c.get("s:a"));
        assertNull(c.get("s:missing"));
        assertEquals("héllo ✓ 😀", roundTrip("héllo ✓ 😀"));
        assertEquals(5L, c.strlen("s:a"));
        assertEquals(10L, c.append("s:a", "world"));
        assertEquals("hello", c.getrange("s:a", 0, 4));
        assertEquals("world", c.substr("s:a", 5, -1));
        assertEquals(10L, c.setrange("s:a", 0, "HELLO"));
        assertEquals("HELLOworld", c.getset("s:a", "x"));
        assertEquals("x", c.getdel("s:a"));
        assertNull(c.get("s:a"));
    }

    private String roundTrip(String v) {
        c.set("s:u", v);
        return c.get("s:u");
    }

    @Test
    void setOptions() {
        assertTrue(c.set("s:o", "1", SetOptions.nx().expireMillis(10_000)));
        assertFalse(c.set("s:o", "2", SetOptions.nx()));
        assertTrue(c.set("s:o", "3", SetOptions.xx().keepTtl()));
        assertTrue(c.pttl("s:o") > 0);
        assertEquals("3", c.setGet("s:o", "4"));
        assertEquals("4", c.setGet("s:o", "5", SetOptions.ex(100)));
        assertTrue(c.ttl("s:o") > 90);
        assertTrue(c.set("s:o", "6", SetOptions.builder().expireAtSeconds(System.currentTimeMillis() / 1000 + 100)));
        assertTrue(c.set("s:o", "7", SetOptions.builder().expireAtMillis(System.currentTimeMillis() + 100_000)));
        // IFEQ
        assertFalse(c.setIfEq("s:o", "8", "nope"));
        assertTrue(c.setIfEq("s:o", "8", "7"));
        assertTrue(c.setIfEq("s:o", "9", "8", SetOptions.px(50_000)));
        assertTrue(c.setIfGreater("s:wm", 10));
        assertFalse(c.setIfGreater("s:wm", 10));
        assertTrue(c.setIfGreater("s:wm", 11, SetOptions.px(50_000)));
        assertFalse(c.setIfLess("s:wm", 12));
        assertTrue(c.setIfLess("s:wm", -1));
        assertEquals("9", c.get("s:o"));
        assertTrue(c.pttl("s:o") <= 50_000);
        c.setex("s:ex", 100, "v");
        assertTrue(c.ttl("s:ex") > 0);
        c.psetex("s:px", 100_000, "v");
        assertTrue(c.pttl("s:px") > 0);
        assertTrue(c.setnx("s:nx", "v"));
        assertFalse(c.setnx("s:nx", "v"));
    }

    @Test
    void multi() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("s:m1", "a");
        m.put("s:m2", "b");
        c.mset(m);
        assertEquals(Arrays.asList("a", "b", null), c.mget("s:m1", "s:m2", "s:m3"));
        Map<String, String> m2 = new LinkedHashMap<>();
        m2.put("s:m3", "c");
        m2.put("s:m1", "z");
        assertFalse(c.msetnx(m2));
        m2.remove("s:m1");
        assertTrue(c.msetnx(m2));
    }

    @Test
    void counters() {
        assertEquals(1L, c.incr("s:n"));
        assertEquals(0L, c.decr("s:n"));
        assertEquals(10L, c.incrby("s:n", 10));
        assertEquals(7L, c.decrby("s:n", 3));
        assertEquals(9L, c.incrbyMax("s:n", 2, 10));
        assertNull(c.incrbyMax("s:n", 2, 10));
        assertEquals("9", c.get("s:n"));
        assertEquals(10.5, c.incrbyfloat("s:f", 10.5));
        assertEquals(Long.MIN_VALUE + 1, c.incrby("s:big", Long.MIN_VALUE + 1));
        // TAKE
        c.set("s:t", "5");
        assertEquals(2L, c.take("s:t", 3));
        assertNull(c.take("s:t", 3));
        assertEquals("2", c.get("s:t"));
        // TRANSFER / HTRANSFER
        c.set("{w}a", "10");
        assertEquals(java.util.Arrays.asList(6L, 4L), c.transfer("{w}a", "{w}b", 4));
        assertNull(c.transfer("{w}a", "{w}b", 7));
        assertNull(c.transfer("{w}a", "{w}b", 3, 6));
        assertEquals(java.util.Arrays.asList(4L, 6L), c.transfer("{w}a", "{w}b", 2, 6));
        c.hset("s:th", "f", "10");
        assertEquals(java.util.Arrays.asList(6L, 4L), c.htransfer("s:th", "f", "g", 4));
        assertNull(c.htransfer("s:th", "f", "g", 3, 5));
    }

    @Test
    void ruvioStringExtensions() {
        assertEquals(1L, c.setv("s:v", "a"));
        assertEquals(2L, c.setv("s:v", "b", 1));
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.setv("s:v", "c", 1));
        assertEquals("ERR", e.code());
        assertEquals("b", c.get("s:v"));
        c.set("s:d", "x");
        assertFalse(c.delex("s:d", "y"));
        assertTrue(c.delex("s:d", "x"));
        assertEquals(0L, c.exists("s:d"));
        c.set("s:g", "v");
        assertEquals("v", c.getex("s:g", 5_000));
        assertTrue(c.pttl("s:g") > 0 && c.pttl("s:g") <= 5_000);
        assertNull(c.getex("s:none", 5_000));
    }

    @Test
    void fill() {
        FillResult<String> first = c.fillget("s:fill", 2_000);
        assertEquals(FillResult.State.FILL, first.state());
        assertNotNull(first.token());
        assertEquals(FillResult.State.BUSY, c.fillget("s:fill", 2_000).state());
        assertEquals(FillResult.State.BUSY, c.fillgetWait("s:fill", 2_000, 5).state());
        assertFalse(c.fillset("s:fill", "v", 10_000, "999999"));
        assertTrue(c.fillset("s:fill", "v", 10_000, first.token()));
        FillResult<String> hit = c.fillget("s:fill", 2_000);
        assertEquals(FillResult.State.HIT, hit.state());
        assertEquals("v", hit.value());
        // SWR: expires after 50 ms, stays stale for 10 s
        FillResult<String> f2 = c.fillget("s:swr", 2_000);
        assertTrue(c.fillset("s:swr", "old", 50, f2.token(), 10_000));
        sleep(120);
        FillResult<String> stale = c.fillget("s:swr", 2_000);
        assertEquals(FillResult.State.STALE, stale.state());
        assertEquals("old", stale.value());
    }

    @Test
    void binaryValues() {
        RuvioClient<String, byte[]> b = c.binaryValues();
        byte[] v = {0, (byte) 255, 13, 10, (byte) 128, 36};
        b.set("s:bin", v);
        assertArrayEquals(v, b.get("s:bin"));
        RuvioClient<byte[], byte[]> raw = c.withCodecs(RuvioCodec.BYTES, RuvioCodec.BYTES);
        byte[] key = {1, 2, 3, 0, 13, 10};
        raw.set(key, v);
        assertArrayEquals(v, raw.get(key));
        assertEquals(1L, raw.del(key));
        assertEquals("abc", new String(b.get("s:bin2") == null ? "abc".getBytes(StandardCharsets.UTF_8) : new byte[0], StandardCharsets.UTF_8));
    }

    @Test
    void largeValue() {
        char[] chars = new char[3 * 1024 * 1024 + 7];
        Arrays.fill(chars, 'q');
        String big = new String(chars);
        c.set("s:large", big);
        assertEquals(big, c.get("s:large"));
        List<String> l = c.mget("s:large", "s:large");
        assertEquals(big, l.get(1));
    }

    @Test
    void errorsAreTyped() {
        c.lpush("s:list", "x");
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get("s:list"));
        assertEquals("WRONGTYPE", e.code());
        // the connection stays usable after an error
        assertEquals("PONG", c.ping());
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void ttlOnCreateCounters() {
        c.incr("ttl:incr", LONG_TTL);
        assertDeadlineKept("ttl:incr");
        assertEquals(2L, c.incr("ttl:incr", LONGER_TTL));
        assertDeadlineKept("ttl:incr");

        assertEquals(-1L, c.decr("ttl:decr", LONG_TTL));
        assertEquals(-2L, c.decr("ttl:decr", LONGER_TTL));
        assertDeadlineKept("ttl:decr");

        assertEquals(5L, c.incrby("ttl:by", 5, LONG_TTL));
        assertEquals(10L, c.incrby("ttl:by", 5, LONGER_TTL));
        assertDeadlineKept("ttl:by");

        assertEquals(-5L, c.decrby("ttl:dby", 5, LONG_TTL));
        assertEquals(-10L, c.decrby("ttl:dby", 5, LONGER_TTL));
        assertDeadlineKept("ttl:dby");

        assertEquals(2L, c.incrbyMax("ttl:max", 2, 3, LONG_TTL));
        assertNull(c.incrbyMax("ttl:max", 2, 3, LONGER_TTL));
        assertEquals(3L, c.incrbyMax("ttl:max", 1, 3, LONGER_TTL));
        assertDeadlineKept("ttl:max");

        assertEquals(1.5, c.incrbyfloat("ttl:f", 1.5, LONG_TTL));
        assertEquals(3.0, c.incrbyfloat("ttl:f", 1.5, LONGER_TTL));
        assertDeadlineKept("ttl:f");

        c.incr("ttl:ms", java.time.Duration.ofMillis(90_500));
        long ms = c.pttl("ttl:ms");
        assertTrue(ms > 0 && ms <= 90_500, "pttl " + ms);
    }

    @Test
    void ttlOnCreateSetbitAndErrors() {
        assertEquals(0L, c.setbit("ttl:bit", 7, true, LONG_TTL));
        assertEquals(1L, c.setbit("ttl:bit", 7, true, LONGER_TTL));
        assertDeadlineKept("ttl:bit");
        assertThrows(IllegalArgumentException.class, () -> c.incr("ttl:bad", java.time.Duration.ZERO));
        assertEquals(0L, c.exists("ttl:bad"));
    }

    private static final java.time.Duration LONG_TTL = java.time.Duration.ofSeconds(100);
    private static final java.time.Duration LONGER_TTL = java.time.Duration.ofSeconds(1000);

    /** The key got the first call's deadline (at most 100 s), not the second call's 1000 s. */
    private void assertDeadlineKept(String key) {
        long ms = c.pttl(key);
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
    }
}
