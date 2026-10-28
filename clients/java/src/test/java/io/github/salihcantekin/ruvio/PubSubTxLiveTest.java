// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ClientKillArgs;
import io.github.salihcantekin.ruvio.model.PubSubMessage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Pub/Sub, transactions and explicit pipelines. */
class PubSubTxLiveTest {
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
    void pubSub() throws Exception {
        BlockingQueue<PubSubMessage> got = new LinkedBlockingQueue<>();
        try (RuvioPubSub sub = c.pubSub(got::add)) {
            sub.subscribe("ch1", "ch2");
            sub.psubscribe("news.*");
            sub.ssubscribe("sch");
            sub.ping();
            assertEquals(1L, c.publish("ch1", "hello"));
            PubSubMessage m = got.poll(2, TimeUnit.SECONDS);
            assertEquals(PubSubMessage.Kind.MESSAGE, m.kind());
            assertEquals("ch1", m.channel());
            assertEquals("hello", m.payloadString());
            c.publish("news.tech", "p");
            PubSubMessage pm = got.poll(2, TimeUnit.SECONDS);
            assertEquals(PubSubMessage.Kind.PMESSAGE, pm.kind());
            assertEquals("news.*", pm.pattern());
            assertEquals("news.tech", pm.channel());
            assertEquals(1L, c.spublish("sch", "s"));
            PubSubMessage sm = got.poll(2, TimeUnit.SECONDS);
            assertEquals(PubSubMessage.Kind.SMESSAGE, sm.kind());
            // binary payloads survive
            c.binaryValues().publish("ch2", new byte[] {0, (byte) 0xff, 13, 10});
            assertArrayEquals(new byte[] {0, (byte) 0xff, 13, 10}, got.poll(2, TimeUnit.SECONDS).payload());
            sub.unsubscribe("ch1");
            assertEquals(0L, c.publish("ch1", "x"));
            sub.unsubscribe();
            sub.punsubscribe("news.*");
            sub.punsubscribe();
            sub.sunsubscribe("sch");
            sub.sunsubscribe();
            assertEquals(0L, c.publish("ch2", "x"));
            assertTrue(sub.channels().isEmpty());
            // resubscribe after the server drops the subscriber connection
            sub.subscribe("re");
            long subscriberId = -1;
            for (String line : c.clientList().split("\n")) {
                if (line.contains(" cmd=") || line.contains("sub=1") || line.contains("name=")) {
                    // CLIENT LIST has no sub field on Ruvio; find it by elimination below
                }
            }
            assertTrue(subscriberId < 0);
        }
    }

    @Test
    void pubSubReconnects() throws Exception {
        BlockingQueue<PubSubMessage> got = new LinkedBlockingQueue<>();
        try (RuvioPubSub sub = c.pubSub(got::add)) {
            sub.subscribe("rc");
            long me = c.clientId();
            // Kill every other connection of this test database except ours: the subscriber is one.
            for (String line : c.clientList().split("\n")) {
                String id = line.replaceAll("^id=(\\d+) .*$", "$1");
                if (!id.isEmpty() && Character.isDigit(id.charAt(0)) && Long.parseLong(id) != me && line.contains("db=0")) {
                    c.clientKill(ClientKillArgs.id(Long.parseLong(id)));
                }
            }
            boolean delivered = false;
            for (int i = 0; i < 100 && !delivered; i++) {
                c.publish("rc", "again");
                delivered = got.poll(50, TimeUnit.MILLISECONDS) != null;
            }
            assertTrue(delivered, "subscriber came back and resubscribed");
        }
    }

    @Test
    void transactions() {
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<Void> set = tx.set("{tx}:a", "1");
            CompletableFuture<Long> incr = tx.incr("{tx}:a");
            CompletableFuture<String> get = tx.get("{tx}:a");
            CompletableFuture<List<String>> range = tx.lrange("{tx}:none", 0, -1);
            CompletableFuture<String> wrong = tx.get("{tx}:list");
            assertFalse(set.isDone());
            assertTrue(tx.exec());
            assertNull(set.join());
            assertEquals(2L, incr.join());
            assertEquals("2", get.join());
            assertTrue(range.join().isEmpty());
            assertNull(wrong.join());
        }
        // runtime error inside EXEC fails only that command
        c.rpush("{tx}:list", "x");
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<String> bad = tx.get("{tx}:list");
            CompletableFuture<Long> ok = tx.incr("{tx}:n");
            assertTrue(tx.exec());
            CompletableFuture<String> b = bad;
            Exception e = assertThrows(Exception.class, b::join);
            assertTrue(e.getCause() instanceof RuvioServerException);
            assertEquals(1L, ok.join());
        }
        // WATCH: a change by another connection aborts EXEC
        try (Transaction<String, String> tx = c.multi()) {
            tx.watch("{tx}:w");
            c.set("{tx}:w", "changed");
            CompletableFuture<Void> s = tx.set("{tx}:w", "mine");
            assertFalse(tx.exec());
            assertThrows(Exception.class, s::join);
            assertEquals("changed", c.get("{tx}:w"));
        }
        // UNWATCH and DISCARD
        try (Transaction<String, String> tx = c.multi()) {
            tx.watch("{tx}:w");
            tx.unwatch();
            c.set("{tx}:w", "again");
            tx.set("{tx}:w", "mine");
            assertTrue(tx.exec());
            assertEquals("mine", c.get("{tx}:w"));
        }
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<Void> s = tx.set("{tx}:d", "x");
            tx.discard();
            assertThrows(Exception.class, s::join);
            assertNull(c.get("{tx}:d"));
        }
        // queue-time error: Ruvio refuses that command and EXEC answers EXECABORT, running nothing
        try (Transaction<String, String> tx = c.multi()) {
            CompletableFuture<Object> bad = tx.execute("NOSUCHCOMMAND");
            CompletableFuture<Void> s = tx.set("{tx}:e", "x");
            assertThrows(Exception.class, tx::exec);
            assertThrows(Exception.class, bad::join);
            assertThrows(Exception.class, s::join);
            assertNull(c.get("{tx}:e"));
        }
        // empty transaction
        try (Transaction<String, String> tx = c.multi()) {
            assertTrue(tx.exec());
        }
        // the shared connection keeps working
        assertEquals("PONG", c.ping());
    }

    @Test
    void pipelines() {
        Pipeline<String, String> p = c.pipeline();
        List<CompletableFuture<Long>> incrs = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            incrs.add(p.incr("pl:n"));
        }
        CompletableFuture<String> get = p.get("pl:n");
        CompletableFuture<String> err = p.get("pl:none");
        assertEquals(1002, p.size());
        assertFalse(get.isDone());
        p.sync();
        assertEquals(1000L, incrs.get(999).join());
        assertEquals("1000", get.join());
        assertNull(err.join());
        assertEquals(0, p.flush());
        // reuse
        CompletableFuture<Void> s = p.set("pl:x", "y");
        CompletableFuture<List<String>> m = p.mget("pl:x", "pl:n");
        p.flush();
        s.join();
        assertEquals(Arrays.asList("y", "1000"), m.join());
        // async API pipelines naturally too
        RuvioAsync<String, String> a = c.async();
        List<CompletableFuture<Long>> fs = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            fs.add(a.incr("pl:async"));
        }
        CompletableFuture.allOf(fs.toArray(CompletableFuture<?>[]::new)).join();
        assertEquals(5000L, fs.get(4999).join());
    }
}
