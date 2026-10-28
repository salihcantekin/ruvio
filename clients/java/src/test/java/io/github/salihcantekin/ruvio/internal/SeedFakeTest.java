// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Seed addresses: connect order, standalone reconnect order and the cluster slot map reload, against scripted servers. */
class SeedFakeTest {
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
        toClose.clear();
    }

    private FakeNode node(java.util.function.Function<List<String>, String> handler) throws IOException {
        FakeNode n = new FakeNode(handler);
        toClose.add(n);
        return n;
    }

    private static int closedPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static String plain(List<String> a, String get) {
        switch (a.get(0).toUpperCase()) {
            case "PING":
                return "+PONG\r\n";
            case "GET":
                return get;
            default:
                return FakeNode.ok();
        }
    }

    private static RuvioOptions.Builder seeds(String... addresses) {
        // The scripted seeds do not answer ROLE; the rotation is what is checked (FollowFailoverFakeTest has the rest).
        return RuvioOptions.builder().seeds(addresses).followPrimary(false).connectTimeoutMillis(500).commandTimeoutMillis(5_000);
    }

    @Test
    void connectSkipsASeedThatCannotBeReached() throws Exception {
        FakeNode live = node(a -> plain(a, FakeNode.bulk("v")));
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds("127.0.0.1:" + closedPort(), live.address()).build())) {
            assertEquals("PONG", c.ping());
            assertEquals(1, live.commands("PING").size());
        }
    }

    @Test
    void connectFailsWithTheLastErrorWhenNoSeedAnswers() throws Exception {
        String first = "127.0.0.1:" + closedPort();
        String last = "127.0.0.1:" + closedPort();
        RuvioConnectionException e = assertThrows(RuvioConnectionException.class, () -> RuvioClient.connect(seeds(first, last).build()));
        assertTrue(e.getMessage().contains(last), e.getMessage());
    }

    @Test
    void aRefusedAuthStopsBeforeTheNextSeed() throws Exception {
        FakeNode refusing = node(a -> a.get(0).equals("AUTH") ? "-WRONGPASS bad\r\n" : FakeNode.ok());
        FakeNode other = node(a -> FakeNode.ok());
        assertThrows(RuvioServerException.class, () -> RuvioClient.connect(seeds(refusing.address(), other.address()).password("x").build()));
        assertTrue(other.received.isEmpty(), "the second seed was not tried");
    }

    @Test
    void aDroppedConnectionMovesOnToTheNextSeed() throws Exception {
        FakeNode first = node(a -> a.get(0).equals("GET") ? FakeNode.CLOSE : plain(a, FakeNode.bulk("from-first")));
        FakeNode second = node(a -> plain(a, FakeNode.bulk("from-second")));
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), second.address()).build())) {
            assertThrows(RuvioConnectionException.class, () -> c.get("k"));
            first.handler = a -> plain(a, FakeNode.bulk("from-first"));
            assertEquals("from-second", c.get("k"), "the reconnect starts with the seed after the one that dropped");
            assertEquals(1, first.commands("GET").size());
        }
    }

    @Test
    void aReconnectAttemptTriesEverySeedOnceAndEndsWithTheOneThatDropped() throws Exception {
        FakeNode first = node(a -> a.get(0).equals("GET") ? FakeNode.CLOSE : plain(a, FakeNode.bulk("from-first")));
        int dead = closedPort();
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), "127.0.0.1:" + dead).build())) {
            assertThrows(RuvioConnectionException.class, () -> c.get("k"));
            first.handler = a -> plain(a, FakeNode.bulk("from-first"));
            assertEquals("from-first", c.get("k"), "the dead seed is skipped and the dropped one is tried last");
        }
    }

    @Test
    void aSubscriberOpensOnTheAddressTheCommandConnectionIsOn() throws Exception {
        FakeNode first = node(a -> a.get(0).equals("GET") ? FakeNode.CLOSE : plain(a, FakeNode.bulk("one")));
        FakeNode second = node(a -> plain(a, FakeNode.bulk("two")));
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), second.address()).build())) {
            assertThrows(RuvioConnectionException.class, () -> c.get("k"));
            assertEquals("two", c.get("k"));
            int before = first.sockets.size();
            try (io.github.salihcantekin.ruvio.RuvioPubSub sub = c.pubSub(m -> { })) {
                assertEquals(2, second.awaitSockets(2), "the subscriber joined the second seed");
                assertEquals(before, first.sockets.size(), "the first seed was not tried");
            }
        }
    }

    @Test
    void anEmptySeedHostOrABadPortIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> RuvioOptions.builder().seeds(":6379"));
        assertThrows(IllegalArgumentException.class, () -> RuvioOptions.builder().seeds("[]:6379"));
        assertThrows(IllegalArgumentException.class, () -> RuvioOptions.builder().seeds("host:abc"));
        assertThrows(IllegalArgumentException.class, () -> RuvioOptions.builder().seeds("host:70000"));
        assertThrows(IllegalArgumentException.class, () -> RuvioOptions.builder().seeds("host"));
        RuvioOptions ok = RuvioOptions.builder().seeds("a:1", "[::1]:6379").build();
        assertEquals(List.of("a:1", "[::1]:6379"), ok.seeds());
        assertEquals("::1", Address.parse("[::1]:6379").host);
        assertTrue(RuvioOptions.builder().build().seeds().isEmpty());
        assertEquals(ok.seeds(), ok.toBuilder().build().seeds());
    }

    // ---- cluster ------------------------------------------------------------------------------------

    private FakeNode clusterNode(Supplier<Integer> ownPort, String value) throws IOException {
        return node(a -> {
            switch (a.get(0).toUpperCase()) {
                case "CLUSTER":
                    return FakeNode.slots(new int[] {0, 16383, ownPort.get()});
                case "GET":
                    return FakeNode.bulk(value);
                case "PING":
                    return "+PONG\r\n";
                default:
                    return FakeNode.ok();
            }
        });
    }

    private FakeNode[] goneAndLive() throws IOException {
        FakeNode[] n = new FakeNode[2];
        n[0] = clusterNode(() -> n[0].port, "gone");
        n[1] = clusterNode(() -> n[1].port, "value");
        return n;
    }

    private static <T> T eventually(Supplier<T> call) throws InterruptedException {
        RuntimeException last = null;
        for (int i = 0; i < 50; i++) {
            try {
                return call.get();
            } catch (RuvioConnectionException e) {
                last = e; // the command racing the drop fails; the next one reloads
                Thread.sleep(50);
            }
        }
        throw last;
    }

    @Test
    void aLostNodeReloadsTheSlotMapFromASeed() throws Exception {
        FakeNode[] n = goneAndLive();
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(n[0].address(), n[1].address()).cluster(true).build())) {
            assertEquals("gone", c.get("key"));
            assertEquals(List.of(n[0].address()), c.nodeAddresses());
            n[0].close();
            assertEquals("value", eventually(() -> c.get("key")));
            assertEquals(List.of(n[1].address()), c.nodeAddresses(), "the command went to the owner the new map names");
        }
    }

    @Test
    void aKeylessCommandWhoseNodeIsGoneGoesToTheFirstReachableNodeOfTheNewMap() throws Exception {
        FakeNode[] n = goneAndLive();
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(n[0].address(), n[1].address()).cluster(true).build())) {
            n[0].close();
            assertEquals("PONG", eventually(() -> c.ping()));
            assertEquals(1, n[1].commands("PING").size());
            assertEquals(List.of(n[1].address()), c.nodeAddresses());
        }
    }

    @Test
    void aFailedReloadLeavesNoSeedEndpointsBehind() throws Exception {
        FakeNode[] holder = new FakeNode[1];
        FakeNode gone = clusterNode(() -> holder[0].port, "gone");
        holder[0] = gone;
        String dead = "127.0.0.1:" + closedPort();
        RuvioOptions options = seeds(gone.address(), dead).cluster(true).build();
        Link link = Link.openSeed(options);
        ClusterRouter router = ClusterRouter.detect(link, options);
        toClose.add(router);
        assertEquals(1, router.nodeCount());
        int known = router.nodeCount();
        gone.close();
        assertThrows(RuvioException.class, router::refresh);
        assertEquals(known, router.nodeCount(), "the seed that could not be opened was dropped again");
        assertNull(router.node(dead));
    }
}
