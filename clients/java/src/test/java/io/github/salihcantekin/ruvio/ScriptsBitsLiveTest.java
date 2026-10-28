// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.BitFieldArgs;
import io.github.salihcantekin.ruvio.args.BitOp;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Scripts, functions, bitmaps and HyperLogLog. */
class ScriptsBitsLiveTest {
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
    void scripts() {
        assertEquals("v", c.eval("ruvio.call('SET', KEYS[1], ARGV[1]); return ruvio.call('GET', KEYS[1])", new String[] {"sc:k"}, "v"));
        assertEquals(3L, c.eval("return 3", new String[0]));
        assertEquals(Arrays.asList(1L, "two"), c.eval("return {1, 'two'}", null));
        String sha = c.scriptLoad("return ARGV[1]");
        assertEquals(40, sha.length());
        assertEquals("x", c.evalsha(sha, new String[0], "x"));
        assertEquals(Arrays.asList(true, false), c.scriptExists(sha, "0000000000000000000000000000000000000000"));
        RuvioServerException busy = assertThrows(RuvioServerException.class, () -> c.scriptKill());
        assertEquals("NOTBUSY", busy.code());
        c.scriptFlush();
        assertEquals(Arrays.asList(false), c.scriptExists(sha));
        RuvioServerException noscript = assertThrows(RuvioServerException.class, () -> c.evalsha(sha, new String[0]));
        assertEquals("NOSCRIPT", noscript.code());
        assertEquals("again", c.evalCached("return ARGV[1]", new String[0], "again"));
        assertEquals("again", c.evalCached("return ARGV[1]", new String[0], "again"));
        c.functionLoad("jfn", "return ARGV[1] .. KEYS[1]");
        assertTrue(c.functionList().contains("jfn"));
        assertEquals("ak", c.fcall("jfn", new String[] {"k"}, "a"));
        assertTrue(c.functionDelete("jfn"));
        assertFalse(c.functionDelete("jfn"));
    }

    @Test
    void bitmaps() {
        assertEquals(0L, c.setbit("b", 7, true));
        assertEquals(1L, c.setbit("b", 7, true));
        assertEquals(1L, c.getbit("b", 7));
        assertEquals(0L, c.getbit("b", 100));
        c.set("bs", "foobar");
        assertEquals(26L, c.bitcount("bs"));
        assertEquals(4L, c.bitcount("bs", 0, 0));
        assertEquals(1L, c.bitcount("bs", 1, 1, true) + 0 * 0);
        assertEquals(1L, c.bitpos("bs", true));
        assertEquals(9L, c.bitpos("bs", true, 1));
        assertEquals(-1L, c.bitpos("b", true, 1, 1));
        c.set("b1", "ab");
        c.set("b2", "a ");
        assertEquals(2L, c.bitop(BitOp.AND, "bdst", "b1", "b2"));
        assertEquals(2L, c.bitop(BitOp.NOT, "bnot", "b1"));
        List<Long> r = c.bitfield("bf", BitFieldArgs.builder().set("u8", 0, 200).incrBy("u8", 0, 100).overflow(BitFieldArgs.Overflow.FAIL).incrBy("u8", 0, 250).get("u8", 0));
        assertEquals(Arrays.asList(0L, 44L, null, 44L), r);
        assertEquals(Arrays.asList(44L), c.bitfieldRo("bf", BitFieldArgs.builder().get("u8", "#0")));
    }

    @Test
    void hyperloglog() {
        assertTrue(c.pfadd("h1", "a", "b", "c"));
        assertFalse(c.pfadd("h1", "a"));
        assertTrue(c.pfadd("h0"));
        c.pfadd("h2", "c", "d");
        assertEquals(3L, c.pfcount("h1"));
        assertEquals(4L, c.pfcount("h1", "h2"));
        c.pfmerge("h3", "h1", "h2");
        assertEquals(4L, c.pfcount("h3"));
    }
}
