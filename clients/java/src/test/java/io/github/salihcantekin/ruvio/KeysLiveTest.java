// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.RestoreArgs;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.model.ChangeNote;
import io.github.salihcantekin.ruvio.model.IdemResult;
import io.github.salihcantekin.ruvio.model.LimitOutcome;
import io.github.salihcantekin.ruvio.model.RateLimitResult;
import io.github.salihcantekin.ruvio.model.ScanResult;

import io.github.salihcantekin.ruvio.args.ExpireCondition;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class KeysLiveTest {
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
    void keyBasics() {
        c.set("k:a", "1");
        c.set("k:b", "2");
        assertEquals(2L, c.exists("k:a", "k:b", "k:none"));
        assertEquals("string", c.type("k:a"));
        assertEquals("none", c.type("k:none"));
        c.rename("k:a", "k:c");
        assertFalse(c.renamenx("k:c", "k:b"));
        assertTrue(c.renamenx("k:c", "k:d"));
        assertEquals(1L, c.del("k:d"));
        assertEquals(1L, c.unlink("k:b"));
        assertEquals(0L, c.touch("k:none"));
        c.set("k:t", "1");
        assertEquals(1L, c.touch("k:t"));
        assertTrue(c.copy("k:t", "k:t2"));
        assertFalse(c.copy("k:t", "k:t2"));
        assertTrue(c.copy("k:t", "k:t2", true));
        assertTrue(c.dbsize() >= 2);
        assertNotNull(c.randomkey());
        assertTrue(c.keys("k:t*").contains("k:t2"));
    }

    @Test
    void expiry() {
        c.set("k:e", "1");
        assertEquals(-1L, c.ttl("k:e"));
        assertTrue(c.expire("k:e", 100));
        assertTrue(c.ttl("k:e") > 90);
        assertTrue(c.pexpire("k:e", 100_000));
        assertTrue(c.pttl("k:e") > 90_000);
        long now = System.currentTimeMillis();
        assertTrue(c.expireat("k:e", now / 1000 + 200));
        assertEquals(now / 1000 + 200, c.expiretime("k:e"));
        assertTrue(c.pexpireat("k:e", now + 300_000));
        assertEquals(now + 300_000, c.pexpiretime("k:e"));
        assertTrue(c.persist("k:e"));
        assertEquals(-1L, c.pttl("k:e"));
        assertEquals(-2L, c.ttl("k:none"));
        c.expire("k:e", 0); // matrix says error; this server build deletes the key instead
        assertEquals(0L, c.exists("k:e"));
    }

    @Test
    void scanAndIterators() {
        for (int i = 0; i < 120; i++) {
            c.set("k:scan:" + i, "v");
        }
        Set<String> seen = new HashSet<>();
        String cursor = "0";
        do {
            ScanResult<String> page = c.scan(cursor, ScanArgs.match("k:scan:*").limit(25));
            seen.addAll(page.items());
            cursor = page.cursor();
        } while (!"0".equals(cursor));
        assertEquals(120, seen.size());
        Set<String> it = new HashSet<>();
        for (String k : c.scanIterable(ScanArgs.match("k:scan:*").limit(7))) {
            it.add(k);
        }
        assertEquals(seen, it);
        assertEquals(120, c.scanStream(ScanArgs.match("k:scan:*").type("string")).count());
        assertTrue(c.scan("0").items().size() > 0);
    }

    @Test
    void dbs() {
        try (RuvioClient<String, String> other = RuvioClient.connect(Live.options(10))) {
            other.flushdb();
            other.set("k:db", "x");
            assertTrue(other.move("k:db", Live.DB));
            assertEquals("x", c.get("k:db"));
            other.set("k:swap", "in10");
            other.swapdb(10, 11);
            assertNull(other.get("k:swap"));
            other.select(11);
            assertEquals("in10", other.get("k:swap"));
            other.flushdb();
            other.select(10);
            other.flushdb();
        }
    }

    @Test
    void objectMemoryDumpRestore() {
        c.rpush("k:list", "a", "b");
        assertEquals("quicklist", c.objectEncoding("k:list"));
        assertThrows(RuvioServerException.class, () -> c.objectIdletime("k:list"));
        assertThrows(RuvioServerException.class, () -> c.objectFreq("k:list"));
        assertTrue(c.memoryUsage("k:list") > 0);
        assertTrue(c.memoryUsage("k:list", 5) > 0);
        assertNull(c.memoryUsage("k:nope"));
        byte[] dump = c.dump("k:list");
        assertNotNull(dump);
        assertNull(c.dump("k:nope"));
        c.restore("k:list2", 0, dump);
        assertEquals(List.of("a", "b"), c.lrange("k:list2", 0, -1));
        assertThrows(RuvioServerException.class, () -> c.restore("k:list2", 0, dump));
        c.restore("k:list2", System.currentTimeMillis() + 100_000, dump, RestoreArgs.builder().replace().absTtl());
        assertTrue(c.pttl("k:list2") > 0);
    }

    @Test
    void coordination() {
        // LIMIT
        assertEquals(1L, c.limit("k:lim", 2, 10_000));
        assertEquals(0L, c.limit("k:lim", 2, 10_000));
        assertNull(c.limit("k:lim", 2, 10_000));
        RateLimitResult r = c.rateLimit("k:lim2", 1, 10_000);
        assertTrue(r.allowed());
        assertFalse(c.rateLimit("k:lim2", 1, 10_000).allowed());
        assertEquals(1L, c.limitSliding("k:slide", 2, 60_000));
        assertEquals(0L, c.limitSliding("k:slide", 2, 60_000));
        LimitOutcome refused = c.limitSlidingWithRetry("k:slide", 2, 60_000);
        assertFalse(refused.allowed());
        assertTrue(refused.retryAfterMillis() > 0);
        assertEquals(3L, c.limitTokens("k:bucket", 5, 60_000, 2));
        assertEquals(new LimitOutcome(true, 0, 0), c.limitTokensWithRetry("k:bucket", 5, 60_000, 3));
        assertNull(c.limitTokens("k:bucket", 5, 60_000, 1));
        // ONCE
        assertTrue(c.once("k:once", 10_000));
        assertFalse(c.once("k:once", 10_000));
        // LEASE / RELEASE
        Long t1 = c.lease("k:lease", 10_000);
        assertNotNull(t1);
        assertNull(c.lease("k:lease", 10_000));
        assertFalse(c.release("k:lease", t1 + 100));
        assertFalse(c.renew("k:lease", t1 + 100, 20_000));
        assertTrue(c.renew("k:lease", t1, 20_000));
        assertTrue(c.release("k:lease", t1));
        assertFalse(c.renew("k:lease", t1, 20_000));
        Long t2 = c.lease("k:lease", 10_000);
        assertTrue(t2 > t1, "fencing tokens increase");
        // SEMAPHORE
        Long s1 = c.semaphore("k:sem", 2, 10_000);
        Long s2 = c.semaphore("k:sem", 2, 10_000);
        assertNotNull(s1);
        assertNotNull(s2);
        assertNull(c.semaphore("k:sem", 2, 10_000));
        assertTrue(c.renew("k:sem", s1, 20_000));
        assertNull(c.semaphore("k:sem", 2, 10_000));
        assertTrue(c.release("k:sem", s1));
        assertNotNull(c.semaphore("k:sem", 2, 10_000));
        // IDEM
        IdemResult a = c.idemBegin("k:idem", "fp", "o1", 10_000);
        assertEquals(IdemResult.State.ACQUIRED, a.state());
        assertEquals(IdemResult.State.PENDING, c.idemBegin("k:idem", "fp", "o2", 10_000).state());
        assertEquals(IdemResult.State.CONFLICT, c.idemBegin("k:idem", "other", "o2", 10_000).state());
        assertEquals(1L, c.idemComplete("k:idem", "fp", "o1", "done"));
        IdemResult done = c.idemBegin("k:idem", "fp", "o3", 10_000);
        assertEquals(IdemResult.State.COMPLETED, done.state());
        assertEquals("done", done.resultString());
        assertEquals(1L, c.idemComplete("k:idem", "fp", "o1", "done".getBytes()));
        // ABORT frees a pending record, never a completed one, and only for its owner
        assertEquals(IdemResult.State.ACQUIRED, c.idemBegin("k:idem-abort", "fp", "o1", 10_000).state());
        assertFalse(c.idemAbort("k:idem-abort", "fp", "o2"));
        assertFalse(c.idemAbort("k:idem-abort", "other", "o1"));
        assertTrue(c.idemAbort("k:idem-abort", "fp", "o1"));
        assertEquals(IdemResult.State.ACQUIRED, c.idemBegin("k:idem-abort", "fp", "o2", 10_000).state());
        assertEquals(1L, c.idemComplete("k:idem-abort", "fp", "o2", "kept"));
        assertFalse(c.idemAbort("k:idem-abort", "fp", "o2"));
        assertEquals("kept", c.idemBegin("k:idem-abort", "fp", "o3", 10_000).resultString());
        // CHANGES
        long cursor = c.changesStart();
        c.set("k:chg", "1");
        List<ChangeNote> notes = c.changes(cursor);
        assertTrue(notes.stream().anyMatch(n -> n.text().contains("k:chg")), notes.toString());
        assertTrue(notes.get(0).sequence() > cursor);
    }

    @Test
    void expireConditions() {
        c.set("xc:k", "v");
        assertFalse(c.expire("xc:k", 100, ExpireCondition.XX), "no deadline yet");
        assertFalse(c.expire("xc:k", 100, ExpireCondition.GT), "no deadline counts as infinite");
        assertTrue(c.expire("xc:k", 100, ExpireCondition.LT));
        assertFalse(c.expire("xc:k", 200, ExpireCondition.NX), "already has a deadline");
        assertTrue(c.expire("xc:k", 200, ExpireCondition.GT));
        assertTrue(c.expire("xc:k", 150, ExpireCondition.LT));
        assertTrue(c.expire("xc:k", 100, ExpireCondition.XX));
        long ms = c.pttl("xc:k");
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
        assertTrue(c.expire("xc:k", 500, ExpireCondition.ALWAYS));

        assertTrue(c.pexpire("xc:k", 50_000, ExpireCondition.LT));
        assertFalse(c.pexpire("xc:k", 60_000, ExpireCondition.LT));
        long future = System.currentTimeMillis() / 1000 + 10_000;
        assertTrue(c.expireat("xc:k", future, ExpireCondition.GT));
        assertFalse(c.expireat("xc:k", future - 5_000, ExpireCondition.GT));
        assertTrue(c.pexpireat("xc:k", future * 1000 - 1_000_000, ExpireCondition.LT));
        assertFalse(c.pexpireat("xc:k", future * 1000, ExpireCondition.LT));
        c.set("xc:n", "v");
        assertTrue(c.pexpireat("xc:n", future * 1000, ExpireCondition.NX));
        assertFalse(c.expire("xc:missing", 10, ExpireCondition.NX));
    }
}
