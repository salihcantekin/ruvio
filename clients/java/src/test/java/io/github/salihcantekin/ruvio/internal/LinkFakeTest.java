// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioErrors;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.RuvioTimeoutException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Connection handshake, failure and reconnect behaviour against scripted servers (no live server needed). */
class LinkFakeTest {
    private static String ok(List<String> args) {
        switch (args.get(0).toUpperCase()) {
            case "PING":
                return "+PONG\r\n";
            case "GET":
                return "$5\r\nvalue\r\n";
            default:
                return "+OK\r\n";
        }
    }

    private static RuvioOptions.Builder to(FakeNode n) {
        return RuvioOptions.builder().host("127.0.0.1").port(n.port).connectTimeoutMillis(2000);
    }

    @Test
    void handshakeSendsOnlyWhatTheOptionsAsk() throws Exception {
        try (FakeNode plain = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(plain).build()).close();
            assertTrue(plain.received.isEmpty(), "no options, no handshake commands");
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).password("pw").build()).close();
            assertEquals(List.of("AUTH", "pw"), n.received.get(0));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).credentials("bob", "pw").build()).close();
            assertEquals(List.of("AUTH", "bob", "pw"), n.received.get(0));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).database(3).clientName("svc").build()).close();
            assertEquals(List.of("SELECT", "3"), n.received.get(0));
            assertEquals(List.of("CLIENT", "SETNAME", "svc"), n.received.get(1));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).password("pw").database(1).clientName("x").build()).close();
            assertEquals("AUTH", n.received.get(0).get(0));
            assertEquals("SELECT", n.received.get(1).get(0));
            assertEquals("CLIENT", n.received.get(2).get(0));
        }
    }

    @Test
    void durabilityIsSentOnConnectAndByTheMethod() throws Exception {
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).durability(2).build()).close();
            assertEquals(List.of("CLIENT", "DURABILITY", "REPLICAS", "2"), n.received.get(0));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient.connect(to(n).database(1).durability(1, 250).build()).close();
            assertEquals(List.of("SELECT", "1"), n.received.get(0));
            assertEquals(List.of("CLIENT", "DURABILITY", "REPLICAS", "1", "TIMEOUT", "250"), n.received.get(1));
        }
        try (FakeNode n = new FakeNode(LinkFakeTest::ok); RuvioClient<String, String> c = RuvioClient.connect(to(n).build())) {
            c.clientDurability(3);
            c.clientDurability(0, 100);
            assertEquals(List.of("CLIENT", "DURABILITY", "REPLICAS", "3"), n.received.get(0));
            assertEquals(List.of("CLIENT", "DURABILITY", "REPLICAS", "0", "TIMEOUT", "100"), n.received.get(1));
        }
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("CLIENT") ? "-ERR CLIENT DURABILITY needs replication on this server\r\n" : "+OK\r\n")) {
            RuvioServerException e = assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).durability(1).build()));
            assertTrue(e.getMessage().contains("needs replication"));
        }
    }

    @Test
    void aRefusedHandshakeFailsTheConnectWithTheServerMessage() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("AUTH") ? "-WRONGPASS invalid username-password pair\r\n" : "+OK\r\n")) {
            RuvioServerException e = assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).password("bad").build()));
            assertTrue(e.getMessage().contains("WRONGPASS"));
        }
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("SELECT") ? "-ERR DB index is out of range\r\n" : "+OK\r\n")) {
            assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).database(99).build()));
        }
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("CLIENT") ? "-ERR no\r\n" : "+OK\r\n")) {
            assertThrows(RuvioServerException.class, () -> RuvioClient.connect(to(n).clientName("x").build()));
        }
    }

    @Test
    void connectingToAClosedPortIsAConnectionException() throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        RuvioConnectionException e = assertThrows(RuvioConnectionException.class,
                () -> RuvioClient.connect(RuvioOptions.builder().port(port).connectTimeoutMillis(500).build()));
        assertTrue(e.getMessage().contains("cannot connect"));
    }

    @Test
    void aDroppedConnectionFailsTheCallAndTheNextCallReconnects() throws Exception {
        AtomicInteger gets = new AtomicInteger();
        try (FakeNode n = new FakeNode(a -> {
            if (a.get(0).equals("GET") && gets.getAndIncrement() == 0) {
                return FakeNode.CLOSE;
            }
            return ok(a);
        })) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).build())) {
                assertThrows(RuvioConnectionException.class, () -> c.get("k"));
                assertEquals("value", c.get("k"), "the next command reopens the socket");
                assertEquals("PONG", c.ping());
            }
        }
    }

    @Test
    void aSilentServerHitsTheCommandTimeout() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("GET") ? FakeNode.SILENT : ok(a))) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).commandTimeoutMillis(150).build())) {
                long start = System.nanoTime();
                RuvioException e = assertThrows(RuvioException.class, () -> c.get("k"));
                assertTrue(e.getMessage().contains("no reply within 150 ms"), e.getMessage());
                assertInstanceOf(RuvioTimeoutException.class, e);
                assertTrue(RuvioErrors.isTransient(e) && RuvioErrors.mayHaveRun(e), "a timeout is transient and may have run");
                assertTrue((System.nanoTime() - start) / 1_000_000 < 2000, "the timeout must not hang");
            }
        }
    }

    @Test
    void anAsyncFutureFailsAtTheCommandTimeoutAndALateReplyIsDropped() throws Exception {
        try (FakeNode n = new FakeNode(a -> {
            if (a.get(0).equals("GET") && a.get(1).equals("slow")) {
                try {
                    Thread.sleep(450); // answers after the 300 ms limit
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "$4\r\nlate\r\n";
            }
            return ok(a);
        })) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).commandTimeoutMillis(300).build())) {
                long start = System.nanoTime();
                java.util.concurrent.CompletableFuture<String> slow = c.async().get("slow");
                java.util.concurrent.ExecutionException e = assertThrows(java.util.concurrent.ExecutionException.class, () -> slow.get(5, java.util.concurrent.TimeUnit.SECONDS));
                long took = (System.nanoTime() - start) / 1_000_000;
                assertTrue(e.getCause() instanceof RuvioException, String.valueOf(e.getCause()));
                assertTrue(e.getCause().getMessage().contains("no reply within 300 ms"), e.getCause().getMessage());
                assertTrue(took >= 290 && took < 440, "failed after " + took + " ms");
                // The late reply is read and dropped: the next command gets its own reply, and the connection lives on.
                java.util.concurrent.CompletableFuture<String> behind = c.async().get("fast");
                assertEquals("value", behind.get(5, java.util.concurrent.TimeUnit.SECONDS));
                assertEquals("value", c.async().get("again").get(5, java.util.concurrent.TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void anAsyncBlockingCommandGetsItsOwnWaitAndOneThatWaitsForeverNone() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("BLPOP") ? FakeNode.SILENT : ok(a))) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).commandTimeoutMillis(200).build())) {
                long start = System.nanoTime();
                java.util.concurrent.CompletableFuture<?> timed = c.async().blpop(1, "k");
                assertThrows(java.util.concurrent.ExecutionException.class, () -> timed.get(5, java.util.concurrent.TimeUnit.SECONDS));
                long took = (System.nanoTime() - start) / 1_000_000;
                assertTrue(took >= 1150 && took < 2000, "BLPOP 1 + 200 ms failed after " + took + " ms");
                java.util.concurrent.CompletableFuture<?> forever = c.async().blpop(0, "k");
                assertThrows(java.util.concurrent.TimeoutException.class, () -> forever.get(700, java.util.concurrent.TimeUnit.MILLISECONDS));
                assertFalse(forever.isDone(), "BLPOP 0 has no deadline");
            }
        }
    }

    @Test
    void theCommandTimeoutDefaultsToThirtySeconds() {
        assertEquals(30_000, RuvioOptions.builder().build().commandTimeoutMillis);
        assertEquals(0, RuvioOptions.builder().commandTimeoutMillis(0).build().commandTimeoutMillis);
    }

    @Test
    void aBlockingCommandWaitsItsOwnTimeoutOnTop() throws Exception {
        try (FakeNode n = new FakeNode(a -> {
            if (!a.get(0).equals("BLPOP")) {
                return a.get(0).equals("PING") ? FakeNode.SILENT : ok(a);
            }
            try {
                Thread.sleep(1000); // an empty list: nil when BLPOP's 1 s runs out
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "*-1\r\n";
        })) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).commandTimeoutMillis(200).build())) {
                long start = System.nanoTime();
                assertNull(c.blpop(1, "k"));
                long took = (System.nanoTime() - start) / 1_000_000;
                assertTrue(took >= 900 && took < 3000, "took " + took + " ms");
                // The plain limit still holds for the next command.
                RuvioException e = assertThrows(RuvioException.class, c::ping);
                assertTrue(e.getMessage().contains("no reply within 200 ms"), e.getMessage());
            }
        }
    }

    @Test
    void blockingWaitTable() {
        Object[][] cases = {
            {new String[] {"GET", "k"}, 0L},
            {new String[] {"SET", "k", "v"}, 0L},
            {new String[] {"blpop", "a", "b", "1"}, 1000L},
            {new String[] {"BLPOP", "k", "0"}, Blocking.FOREVER},
            {new String[] {"BLPOP", "k", "0.5"}, 500L},
            {new String[] {"BLPOP", "k", "soon"}, Blocking.FOREVER},
            {new String[] {"BLPOP", "k", "-1"}, Blocking.FOREVER},
            {new String[] {"BRPOP", "k", "2"}, 2000L},
            {new String[] {"BZPOPMIN", "k", "1.5"}, 1500L},
            {new String[] {"BZPOPMAX", "k", "3"}, 3000L},
            {new String[] {"BRPOPLPUSH", "a", "b", "4"}, 4000L},
            {new String[] {"BLMOVE", "a", "b", "LEFT", "RIGHT", "0.25"}, 250L},
            {new String[] {"BLMPOP", "2", "2", "a", "b", "LEFT"}, 2000L},
            {new String[] {"BZMPOP", "0", "1", "z", "MIN"}, Blocking.FOREVER},
            {new String[] {"XREAD", "STREAMS", "s", "$"}, 0L},
            {new String[] {"XREAD", "COUNT", "1", "BLOCK", "700", "STREAMS", "s", "$"}, 700L},
            {new String[] {"XREAD", "BLOCK", "0", "STREAMS", "s", "$"}, Blocking.FOREVER},
            {new String[] {"XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"}, 0L},
            {new String[] {"XREADGROUP", "GROUP", "BLOCK", "c", "STREAMS", "s", ">"}, 0L},
            {new String[] {"XREADGROUP", "GROUP", "g", "c", "BLOCK", "50", "STREAMS", "s", ">"}, 50L},
            {new String[] {"WAIT", "1", "100"}, 100L},
            {new String[] {"WAIT", "1", "0"}, Blocking.FOREVER},
            {new String[] {"WAITAOF", "1", "0", "200"}, 200L},
            {new String[] {"FILLGET", "k", "1000"}, 0L},
            {new String[] {"FILLGET", "k", "1000", "WAIT", "300"}, 300L},
            {new String[] {"FILLGET", "k", "1000", "WAIT", "0"}, Blocking.FOREVER},
            {new String[] {"MONITOR"}, Blocking.FOREVER},
            {new String[] {"SUBSCRIBE", "news"}, Blocking.FOREVER},
            {new String[] {"PSUBSCRIBE", "n*"}, Blocking.FOREVER},
            {new String[] {"SSUBSCRIBE", "news"}, Blocking.FOREVER},
            {new String[] {"PUBLISH", "news", "x"}, 0L},
            {new String[] {"BLPOP"}, Blocking.FOREVER},
        };
        for (Object[] c : cases) {
            String[] args = (String[]) c[0];
            Args a = new Args(io.github.salihcantekin.ruvio.RuvioCodec.UTF8, io.github.salihcantekin.ruvio.RuvioCodec.UTF8);
            for (String x : args) {
                a.s(x);
            }
            assertEquals((long) (Long) c[1], Blocking.waitMillis(a.out.buf, a.out.pos, a.count), String.join(" ", args));
        }
        assertEquals(0, Blocking.waitMillis(new byte[] {'x', 'y', 'z', 'w', 'v'}, 5, 1));
        // The timeout one synchronous call gets.
        java.util.concurrent.CompletableFuture<Object> plain = new java.util.concurrent.CompletableFuture<>();
        assertEquals(30_000, Blocking.timeoutFor(30_000, plain));
        assertEquals(31_000, Blocking.timeoutFor(30_000, Blocking.request(false, Reply.RAW, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, 1000)));
        assertEquals(31_000, Blocking.timeoutFor(30_000, Blocking.request(true, Reply.RAW, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, 1000)));
        assertEquals(0, Blocking.timeoutFor(30_000, Blocking.request(false, Reply.RAW, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, io.github.salihcantekin.ruvio.RuvioCodec.UTF8, Blocking.FOREVER)));
    }

    @Test
    void usingAClosedClientFails() throws Exception {
        try (FakeNode n = new FakeNode(LinkFakeTest::ok)) {
            RuvioClient<String, String> c = RuvioClient.connect(to(n).build());
            c.close();
            c.close(); // idempotent
            RuntimeException e = assertThrows(RuntimeException.class, () -> c.get("k"));
            assertTrue(e instanceof RuvioConnectionException || e instanceof RuvioException, e.toString());
        }
    }

    @Test
    void serverErrorsKeepTheConnectionUsable() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.get(0).equals("GET") ? "-WRONGTYPE bad kind\r\n" : ok(a))) {
            try (RuvioClient<String, String> c = RuvioClient.connect(to(n).build())) {
                RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.get("k"));
                assertTrue(e.getMessage().startsWith("WRONGTYPE"));
                assertEquals("PONG", c.ping());
            }
        }
    }

    @Test
    void optionsBuilderRoundTripsAndParsesHostPort() {
        RuvioOptions o = RuvioOptions.builder().host("h").port(1234).credentials("u", "p").database(2).clientName("n")
                .connectTimeoutMillis(10).commandTimeoutMillis(20).maxInFlight(64).cluster(true).build();
        RuvioOptions copy = o.toBuilder().build();
        assertEquals("h", copy.host());
        assertEquals(1234, copy.port());
        assertEquals(2, copy.database());
        assertTrue(copy.cluster());
        assertEquals("u", copy.username);
        assertEquals("p", copy.password);
        assertEquals("n", copy.clientName);
        assertEquals(20, copy.commandTimeoutMillis);
        assertEquals(64, copy.maxInFlight);
        assertEquals(10, copy.connectTimeoutMillis);

        RuvioOptions parsed = RuvioOptions.of("cache.local:7000");
        assertEquals("cache.local", parsed.host());
        assertEquals(7000, parsed.port());
        assertEquals(6379, RuvioOptions.of("cache.local").port(), "a bare host uses the default port");
        assertThrows(RuntimeException.class, () -> RuvioOptions.of("host:notaport"));
    }
}
