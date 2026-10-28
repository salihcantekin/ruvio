// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.salihcantekin.ruvio.Pipeline;
import io.github.salihcantekin.ruvio.ReadPolicy;
import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.RuvioServerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The read policy against a scripted primary that lists one scripted replica in CLUSTER SLOTS. */
class ReplicaReadsFakeTest {
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

    /** One range 0-16383 owned by the primary, followed by the given raw replica entries. */
    private static String slots(int primary, String... replicaEntries) {
        String[] range = new String[3 + replicaEntries.length];
        range[0] = FakeNode.integer(0);
        range[1] = FakeNode.integer(16383);
        range[2] = FakeNode.array(FakeNode.bulk("127.0.0.1"), FakeNode.integer(primary), FakeNode.bulk("p"));
        System.arraycopy(replicaEntries, 0, range, 3, replicaEntries.length);
        return FakeNode.array(FakeNode.array(range));
    }

    private static String replicaEntry(int port) {
        return FakeNode.array(FakeNode.bulk("127.0.0.1"), FakeNode.integer(port), FakeNode.bulk("r"));
    }

    /** A primary whose CLUSTER SLOTS names itself and the given raw replica entries. */
    private FakeNode primary(String... replicaEntries) throws IOException {
        FakeNode[] self = new FakeNode[1];
        self[0] = node(a -> primaryReply(a, slots(self[0].port, replicaEntries)));
        return self[0];
    }

    private static String primaryReply(List<String> a, String slots) {
        switch (a.get(0).toUpperCase()) {
            case "CLUSTER":
                return slots;
            case "GET":
                return FakeNode.bulk("from-primary");
            case "DEL":
                return FakeNode.integer(1);
            case "STRLEN":
                return FakeNode.integer(11);
            default:
                return FakeNode.ok();
        }
    }

    private static String replicaReply(List<String> a) {
        switch (a.get(0).toUpperCase()) {
            case "GET":
                return FakeNode.bulk("from-replica");
            case "STRLEN":
                return FakeNode.integer(12);
            case "MGET":
                String[] items = new String[a.size() - 1];
                java.util.Arrays.fill(items, FakeNode.bulk("from-replica"));
                return FakeNode.array(items);
            default:
                return FakeNode.ok();
        }
    }

    /** A primary that lists {@code replica}, and a client with the policy on it. */
    private RuvioClient<String, String> client(FakeNode primary, ReadPolicy policy) {
        RuvioClient<String, String> c = RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1")
                .port(primary.port).cluster(true).readPolicy(policy).commandTimeoutMillis(5_000)
                .connectTimeoutMillis(500).build());
        toClose.add(c);
        return c;
    }

    private static boolean saw(FakeNode n, String command) {
        return !n.commands(command).isEmpty();
    }

    @Test
    void readsGoToTheReplicaWritesToThePrimaryAndReadonlyComesFirst() throws Exception {
        FakeNode replica = node(ReplicaReadsFakeTest::replicaReply);
        FakeNode primary = primary(replicaEntry(replica.port));
        RuvioClient<String, String> c = client(primary, ReadPolicy.PREFER_REPLICA);
        assertEquals("from-replica", c.get("k"));
        c.set("k", "v");
        assertEquals(12L, c.strlen("k"));
        assertEquals(List.of("READONLY"), replica.received.get(0));
        assertFalse(saw(replica, "SET"));
        assertFalse(saw(primary, "GET"));
        assertTrue(saw(primary, "SET"));
        assertEquals(1, replica.commands("READONLY").size());
    }

    @Test
    void thePrimaryPolicyNeverOpensAReplica() throws Exception {
        FakeNode replica = node(ReplicaReadsFakeTest::replicaReply);
        FakeNode primary = primary(replicaEntry(replica.port));
        RuvioClient<String, String> c = client(primary, ReadPolicy.PRIMARY);
        assertEquals("from-primary", c.get("k"));
        assertTrue(replica.received.isEmpty());
    }

    @Test
    void aPipelineGoesToTheReplicaOnlyWhenEveryCommandReads() throws Exception {
        FakeNode replica = node(ReplicaReadsFakeTest::replicaReply);
        FakeNode primary = primary(replicaEntry(replica.port));
        RuvioClient<String, String> c = client(primary, ReadPolicy.PREFER_REPLICA);

        Pipeline<String, String> mixed = c.pipeline();
        mixed.set("a", "1");
        CompletableFuture<String> read = mixed.get("a");
        mixed.sync();
        assertEquals("from-primary", read.join());
        assertTrue(replica.received.isEmpty(), "a pipeline with a write stays on the primary");

        Pipeline<String, String> reads = c.pipeline();
        CompletableFuture<String> one = reads.get("a");
        CompletableFuture<Long> two = reads.strlen("a");
        reads.sync();
        assertEquals("from-replica", one.join());
        assertEquals(12L, two.join());
        assertEquals(1, primary.commands("GET").size());
        assertEquals(1, replica.commands("GET").size());
        assertEquals("READONLY", replica.received.get(0).get(0));
    }

    @Test
    void splitPartsFollowTheirOwnKindOfCommand() throws Exception {
        FakeNode replica = node(ReplicaReadsFakeTest::replicaReply);
        FakeNode primary = primary(replicaEntry(replica.port));
        RuvioClient<String, String> c = client(primary, ReadPolicy.PREFER_REPLICA);
        assertEquals(List.of("from-replica", "from-replica"), c.mget("a", "b"));
        assertEquals(2, replica.commands("MGET").size(), "one part per slot");
        assertEquals(2L, c.del("a", "b"));
        assertEquals(2, primary.commands("DEL").size());
        assertFalse(saw(replica, "DEL"));
    }

    @Test
    void anUnreachableReplicaFallsBackToThePrimaryOrFails() throws Exception {
        FakeNode gone = node(ReplicaReadsFakeTest::replicaReply);
        int port = gone.port;
        gone.close();
        FakeNode primary = primary(replicaEntry(port));
        RuvioClient<String, String> prefer = client(primary, ReadPolicy.PREFER_REPLICA);
        assertEquals("from-primary", prefer.get("k"));
        assertEquals("from-primary", prefer.get("k"));

        RuvioClient<String, String> only = client(primary, ReadPolicy.REPLICA);
        assertThrows(RuvioException.class, () -> only.get("k"));
        only.set("k", "v");
        assertTrue(saw(primary, "SET"), "writes still go to the primary");
    }

    @Test
    void aLoadingReplicaSendsTheReadOnAndIsSkippedAfterwards() throws Exception {
        FakeNode replica = node(a -> a.get(0).equalsIgnoreCase("GET")
                ? FakeNode.error("LOADING Ruvio is loading the dataset in memory") : FakeNode.ok());
        FakeNode primary = primary(replicaEntry(replica.port));
        RuvioClient<String, String> prefer = client(primary, ReadPolicy.PREFER_REPLICA);
        assertEquals("from-primary", prefer.get("k"));
        assertEquals("from-primary", prefer.get("k"));
        assertEquals(1, replica.commands("GET").size(), "skipped until the next slot map load");
        assertEquals(2, primary.commands("GET").size());

        RuvioClient<String, String> only = client(primary, ReadPolicy.REPLICA);
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> only.get("k"));
        assertEquals("LOADING", e.code());
        assertEquals(2, primary.commands("GET").size(), "the replica policy does not fall back");
    }

    @Test
    void theReplicaPolicyWithNoReplicaFailsClientSide() throws Exception {
        FakeNode primary = primary();
        RuvioClient<String, String> c = client(primary, ReadPolicy.REPLICA);
        RuvioException e = assertThrows(RuvioException.class, () -> c.get("k"));
        assertTrue(e.getMessage().contains("no replica"), e.getMessage());
        assertFalse(saw(primary, "GET"));
        c.set("k", "v");
    }

    @Test
    void slotsParsingKeepsReplicasAndSkipsMalformedEntries() throws Exception {
        FakeNode replica = node(ReplicaReadsFakeTest::replicaReply);
        String bad = FakeNode.integer(7);
        String noPort = FakeNode.array(FakeNode.bulk("127.0.0.1"));
        String badPort = FakeNode.array(FakeNode.bulk("127.0.0.1"), FakeNode.integer(70000));
        FakeNode primary = primary(bad, noPort, badPort, replicaEntry(replica.port));
        RuvioClient<String, String> c = client(primary, ReadPolicy.REPLICA);
        assertEquals("from-replica", c.get("k"));
    }
}
