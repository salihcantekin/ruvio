// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.Transaction;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A standalone client with several seeds (or a coordinator name) finds the primary again after a failover, against
 * scripted nodes that answer {@code ROLE}, refuse writes with {@code READONLY} when they are replicas and, for the
 * coordinator case, name a primary with {@code SENTINEL get-master-addr-by-name}. FailoverLiveTest does the same
 * against real servers.
 */
class FollowFailoverFakeTest {
    private static final long PATIENCE_NANOS = TimeUnit.SECONDS.toNanos(10);

    private enum Kind { MASTER, REPLICA, LIAR, NAMES, PROMOTED }

    /** What a scripted node is at the moment; changed while the test runs, as a failover does. */
    private static final class Mode {
        final Kind kind;
        final int port;

        Mode(Kind kind, int port) {
            this.kind = kind;
            this.port = port;
        }
    }

    private static final Mode MASTER = new Mode(Kind.MASTER, 0);
    private static final Mode LIAR = new Mode(Kind.LIAR, 0);
    private static final Mode REPLICA = new Mode(Kind.REPLICA, 0);
    /** A replica promoted right after it answers its first {@code ROLE}. */
    private static final Mode PROMOTED = new Mode(Kind.PROMOTED, 0);

    private static Mode replicaOf(FakeNode primary) {
        return new Mode(Kind.REPLICA, primary.port);
    }

    private static Mode names(FakeNode primary) {
        return new Mode(Kind.NAMES, primary.port);
    }

    private static final class Node {
        final FakeNode fake;
        volatile Mode mode;

        Node(Mode mode, FakeNode fake) {
            this.mode = mode;
            this.fake = fake;
        }

        int port() {
            return fake.port;
        }

        String address() {
            return fake.address();
        }

        int seen(String name) {
            return fake.commands(name).size();
        }

        int total() {
            return fake.received.size();
        }
    }

    private final List<AutoCloseable> toClose = new ArrayList<>();
    private long savedBase;
    private long savedMax;

    @BeforeEach
    void shortBackoff() {
        savedBase = Link.followBaseMillis;
        savedMax = Link.followMaxMillis;
        Link.followBaseMillis = 5;
        Link.followMaxMillis = 20;
    }

    @AfterEach
    void cleanup() throws Exception {
        Link.followBaseMillis = savedBase;
        Link.followMaxMillis = savedMax;
        for (AutoCloseable c : toClose) {
            c.close();
        }
        toClose.clear();
    }

    private Node node(Mode mode) throws IOException {
        Node[] self = new Node[1];
        FakeNode fake = new FakeNode(args -> {
            Mode now = self[0].mode;
            if (now.kind == Kind.PROMOTED && args.get(0).equalsIgnoreCase("ROLE")) {
                self[0].mode = MASTER;
            }
            return answer(args, now);
        });
        toClose.add(fake);
        self[0] = new Node(mode, fake);
        return self[0];
    }

    private static String answer(List<String> args, Mode mode) {
        String name = args.get(0).toUpperCase();
        boolean writes = name.equals("SET") || name.equals("INCR") || name.equals("DEL");
        boolean replica = mode.kind == Kind.REPLICA || mode.kind == Kind.LIAR || mode.kind == Kind.PROMOTED;
        switch (name) {
            case "ROLE":
                if (mode.kind == Kind.REPLICA || mode.kind == Kind.PROMOTED) {
                    return "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:" + mode.port + "\r\n$9\r\nconnected\r\n:0\r\n";
                }
                return "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n";
            case "SENTINEL":
                if (mode.kind == Kind.NAMES) {
                    return FakeNode.array(FakeNode.bulk("127.0.0.1"), FakeNode.bulk(Integer.toString(mode.port)));
                }
                return FakeNode.ok();
            case "EXEC":
                return mode.kind == Kind.REPLICA ? FakeNode.error("EXECABORT Transaction discarded because of previous errors.") : "*0\r\n";
            case "PING":
                return "+PONG\r\n";
            case "GET":
                return FakeNode.bulk(null);
            case "INCR":
                return replica ? readonly() : FakeNode.integer(1);
            default:
                return writes && replica ? readonly() : FakeNode.ok();
        }
    }

    private static String readonly() {
        return FakeNode.error("READONLY You can't write against a read only replica.");
    }

    private static int closedPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static RuvioOptions.Builder seeds(String... addresses) {
        return RuvioOptions.builder().seeds(addresses).connectTimeoutMillis(1_000).commandTimeoutMillis(5_000);
    }

    private static void assertReadonly(Runnable call) {
        RuvioServerException e = assertThrows(RuvioServerException.class, call::run);
        assertTrue(e.getMessage().startsWith("READONLY"), e.getMessage());
    }

    /** Pings until one succeeds: the first command after a dropped socket may fail on the old connection. */
    private static void pingUntilItWorks(RuvioClient<String, String> c) throws InterruptedException {
        long deadline = System.nanoTime() + PATIENCE_NANOS;
        for (;;) {
            try {
                assertEquals("PONG", c.ping());
                return;
            } catch (RuvioConnectionException e) {
                assertTrue(System.nanoTime() < deadline, "the client did not reconnect: " + e);
                Thread.sleep(5);
            }
        }
    }

    @Test
    void optionsDefaultToFollowingWithoutACoordinator() {
        RuvioOptions defaults = RuvioOptions.builder().build();
        assertTrue(defaults.followPrimary);
        assertNull(defaults.sentinelMaster);
        RuvioOptions set = defaults.toBuilder().followPrimary(false).sentinelMaster("main").build();
        assertFalse(set.followPrimary);
        assertEquals("main", set.sentinelMaster);
    }

    @Test
    void aReadonlyWriteFollowsTheNewPrimaryAndIsSentOnceMore() throws Exception {
        Node old = node(MASTER);
        Node next = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), next.address()).build())) {
            c.set("k", "1");
            assertEquals(1, old.seen("SET"));
            assertEquals(0, next.seen("SET"));

            old.mode = replicaOf(next.fake);
            next.mode = MASTER;
            c.set("k", "2");

            // The refused write was sent once to the old node, then once to the new primary.
            assertEquals(2, old.seen("SET"));
            assertEquals(1, next.seen("SET"));
            assertEquals("2", next.fake.commands("SET").get(0).get(2));

            c.set("k", "3");
            assertEquals(2, old.seen("SET"));
            assertEquals(2, next.seen("SET"));
        }
    }

    @Test
    void severalRefusedWritesInFlightAreAllSentOnceMoreOnTheNewPrimary() throws Exception {
        Node old = node(MASTER);
        Node next = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), next.address()).build())) {
            c.ping();
            old.mode = replicaOf(next.fake);
            next.mode = MASTER;
            List<CompletableFuture<Void>> all = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                all.add(c.async().set("k" + i, "v"));
            }
            for (CompletableFuture<Void> f : all) {
                f.get(10, TimeUnit.SECONDS);
            }
            assertEquals(20, next.seen("SET"));
            assertEquals(20, old.seen("SET"));
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    void aBurstOfRefusedWritesUsesFewFollowThreads() throws Exception {
        Node old = node(MASTER);
        Node next = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), next.address()).build())) {
            c.ping();
            old.mode = replicaOf(next.fake);
            next.mode = MASTER;
            int n = 500;
            List<CompletableFuture<Void>> all = new ArrayList<>();
            int peak = 0;
            for (int i = 0; i < n; i++) {
                all.add(c.async().set("k" + i, "v"));
                peak = Math.max(peak, followThreads());
            }
            for (CompletableFuture<Void> f : all) {
                f.get(30, TimeUnit.SECONDS);
                peak = Math.max(peak, followThreads());
            }
            assertEquals(n, next.seen("SET"));
            assertTrue(peak < 8, "follow threads: " + peak);
        }
    }

    private static int followThreads() {
        int count = 0;
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("ruvio-follow")) {
                count++;
            }
        }
        return count;
    }

    @Test
    void aHealthyConnectionSendsNothingExtra() throws Exception {
        Node node = node(MASTER);
        Node other = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(node.address(), other.address()).build())) {
            for (int i = 0; i < 50; i++) {
                c.set("k", "v");
            }
            assertEquals(50, node.total());
            assertEquals(0, other.total());
        }
    }

    @Test
    void aReplicaNamesItsPrimaryWhenThatIsNoSeed() throws Exception {
        Node replica = node(MASTER);
        Node hidden = node(MASTER);
        String dead = "127.0.0.1:" + closedPort();
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(replica.address(), dead).build())) {
            replica.mode = replicaOf(hidden.fake);
            c.set("k", "v");
            assertEquals(1, replica.seen("SET"));
            assertEquals(1, hidden.seen("SET"));
        }
    }

    @Test
    void followPrimaryOffReturnsReadonlyAndSendsNoRole() throws Exception {
        Node node = node(MASTER);
        Node other = node(MASTER);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(node.address(), other.address()).followPrimary(false).build())) {
            node.mode = replicaOf(other.fake);
            assertReadonly(() -> c.set("k", "v"));
            assertEquals(0, node.seen("ROLE"));
            assertEquals(0, other.total());
        }
    }

    @Test
    void aSingleSeedKeepsTheOldBehavior() throws Exception {
        Node node = node(MASTER);
        RuvioOptions options = RuvioOptions.builder().seeds(node.address()).connectTimeoutMillis(1_000).commandTimeoutMillis(5_000).build();
        try (RuvioClient<String, String> c = RuvioClient.connect(options)) {
            node.mode = REPLICA;
            assertReadonly(() -> c.set("k", "v"));
            assertEquals(0, node.seen("ROLE"));
            assertEquals(1, node.total());
        }
    }

    @Test
    void aReconnectTakesTheNodeThatSaysMasterNotTheNextSeed() throws Exception {
        Node first = node(MASTER);
        Node second = node(MASTER);
        Node third = node(MASTER);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), second.address(), third.address()).build())) {
            c.ping();
            // The second seed became a replica while the connection was down, and the first one too.
            second.mode = replicaOf(third.fake);
            first.mode = replicaOf(third.fake);
            first.fake.dropConnections();
            pingUntilItWorks(c);
            c.set("k", "v");
            assertEquals(0, first.seen("SET"));
            assertEquals(0, second.seen("SET"));
            assertEquals(1, third.seen("SET"));
        }
    }

    @Test
    void aReconnectWithoutAnyPrimaryStillConnectsToTheFirstSeedThatAnswers() throws Exception {
        Node first = node(MASTER);
        Node second = node(MASTER);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), second.address()).build())) {
            c.ping();
            first.mode = REPLICA;
            second.mode = REPLICA;
            first.fake.dropConnections();
            pingUntilItWorks(c);
        }
    }

    @Test
    void withoutAnyPrimaryTheReadonlyErrorComesBackWithinTheBackoff() throws Exception {
        Node first = node(MASTER);
        Node second = node(MASTER);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(first.address(), second.address()).build())) {
            first.mode = REPLICA;
            second.mode = REPLICA;
            long started = System.nanoTime();
            assertReadonly(() -> c.set("k", "v"));
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5));
            assertEquals(1, first.seen("SET"));
            assertEquals(0, second.seen("SET"));
            // The connection stayed where it was.
            assertReadonly(() -> c.set("k", "v"));
            assertEquals(2, first.seen("SET"));
        }
    }

    @Test
    void aWriteRefusedTwiceIsNotSentAThirdTime() throws Exception {
        Node old = node(MASTER);
        Node liar = node(MASTER);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), liar.address()).build())) {
            old.mode = REPLICA;
            liar.mode = LIAR;
            assertReadonly(() -> c.set("k", "v"));
            assertEquals(1, old.seen("SET"));
            assertEquals(1, liar.seen("SET"));
        }
    }

    @Test
    void theRefusingReplicaIsTakenOnceItIsPromoted() throws Exception {
        Node node = node(MASTER);
        Node other = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(node.address(), other.address()).build())) {
            node.mode = PROMOTED;
            // The first round finds it a replica and no other primary; the next round finds it promoted and the
            // write is sent there again.
            c.set("k", "v");
            assertEquals(2, node.seen("SET"));
            assertEquals(0, other.seen("SET"));
            assertEquals(2, node.seen("ROLE"));
        }
    }

    @Test
    void aRefusingNodeThatStillSaysMasterIsNeverTaken() throws Exception {
        Node liar = node(MASTER);
        Node other = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(liar.address(), other.address()).build())) {
            liar.mode = LIAR;
            assertReadonly(() -> c.set("k", "v"));
            assertEquals(1, liar.seen("SET"));
            assertEquals(8, liar.seen("ROLE")); // every round asked it and passed it over
        }
    }

    @Test
    void aCoordinatorNamingTheRefusingReplicaIsFollowedOnceItIsPromoted() throws Exception {
        Node coordinator = node(MASTER);
        Node node = node(MASTER);
        coordinator.mode = names(node.fake);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(coordinator.address()).sentinelMaster("main").build())) {
            c.ping();
            node.mode = PROMOTED;
            c.set("k", "v");
            assertEquals(2, node.seen("SET"));
        }
    }

    @Test
    void aWriteRefusedInATransactionFollowsButIsNotSentAgain() throws Exception {
        Node old = node(MASTER);
        Node next = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), next.address()).build())) {
            c.ping();
            try (Transaction<String, String> tx = c.multi()) {
                old.mode = replicaOf(next.fake);
                next.mode = MASTER;
                CompletableFuture<Void> queued = tx.set("k", "in-tx");
                assertThrows(RuvioServerException.class, tx::exec);
                assertTrue(queued.isCompletedExceptionally());
            }
            assertEquals(0, next.seen("SET"));

            // The client moved: the next command runs on the primary, once.
            c.set("k", "after");
            assertEquals(1, next.seen("SET"));
            assertEquals("after", next.fake.commands("SET").get(0).get(2));
        }
    }

    @Test
    void aWriteRefusedInAPipelineIsReturnedAtItsIndexAndNothingFollows() throws Exception {
        Node old = node(MASTER);
        Node next = node(REPLICA);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(old.address(), next.address()).build())) {
            c.ping();
            old.mode = replicaOf(next.fake);
            next.mode = MASTER;
            Pipeline<String, String> p = c.pipeline();
            CompletableFuture<Void> set = p.set("k", "v");
            CompletableFuture<String> ping = p.ping();
            p.flush();
            ExecutionException e = assertThrows(ExecutionException.class, () -> set.get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().startsWith("READONLY"), e.getCause().getMessage());
            assertEquals("PONG", ping.get(5, TimeUnit.SECONDS));
            assertEquals(1, old.seen("SET"));
            assertEquals(0, next.total());
        }
    }

    @Test
    void aCoordinatorNameFindsThePrimaryAtConnectAndAfterAFailover() throws Exception {
        Node coordinator = node(MASTER);
        Node one = node(MASTER);
        Node two = node(REPLICA);
        coordinator.mode = names(one.fake);
        try (RuvioClient<String, String> c = RuvioClient.connect(seeds(coordinator.address()).sentinelMaster("main").build())) {
            c.set("k", "1");
            assertEquals(1, one.seen("SET"));
            assertEquals(0, two.seen("SET"));
            assertEquals(1, coordinator.seen("SENTINEL"));
            assertEquals(List.of("SENTINEL", "get-master-addr-by-name", "main"), coordinator.fake.commands("SENTINEL").get(0));

            one.mode = REPLICA;
            two.mode = MASTER;
            coordinator.mode = names(two.fake);
            c.set("k", "2");
            assertEquals(2, one.seen("SET"));
            assertEquals(1, two.seen("SET"));

            // After a dropped socket the coordinator is asked again.
            one.mode = MASTER;
            two.mode = REPLICA;
            coordinator.mode = names(one.fake);
            two.fake.dropConnections();
            pingUntilItWorks(c);
            c.set("k", "3");
            assertEquals(3, one.seen("SET"));
            assertEquals(1, two.seen("SET"));
        }
    }

    @Test
    void aCoordinatorThatNamesNoPrimaryFailsTheConnect() throws Exception {
        Node coordinator = node(MASTER);
        RuvioException e = assertThrows(RuvioException.class,
                () -> RuvioClient.connect(seeds(coordinator.address()).sentinelMaster("main").build()));
        assertFalse(e instanceof RuvioConnectionException, e.toString());
        assertTrue(e.getMessage().contains("main"), e.getMessage());
    }

    @Test
    void aHungSeedCostsOneBoundedWait() throws Exception {
        FakeNode silent = new FakeNode(a -> FakeNode.SILENT);
        toClose.add(silent);
        Node live = node(MASTER);
        Node other = node(REPLICA);
        RuvioOptions options = RuvioOptions.builder().seeds(live.address(), silent.address(), other.address())
                .connectTimeoutMillis(300).commandTimeoutMillis(5_000).build();
        try (RuvioClient<String, String> c = RuvioClient.connect(options)) {
            live.mode = replicaOf(other.fake);
            other.mode = MASTER;
            long started = System.nanoTime();
            c.set("k", "v");
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(3));
            assertEquals(1, other.seen("SET"));
        }
    }
}
