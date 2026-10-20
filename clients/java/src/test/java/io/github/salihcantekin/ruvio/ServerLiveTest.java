// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.ClientKillArgs;
import io.github.salihcantekin.ruvio.internal.Connection;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Connection, ACL, introspection, cluster and replication commands. */
class ServerLiveTest {
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
    void durabilityNeedsReplicationOnAStandaloneServer() {
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> c.clientDurability(1));
        assertTrue(e.getMessage().contains("needs replication"), e.getMessage());
        assertThrows(RuvioServerException.class, () -> c.clientDurability(1, 1000));
    }

    @Test
    void connection() {
        assertEquals("PONG", c.ping());
        assertEquals("hi", c.ping("hi"));
        assertEquals("échø", c.echo("échø"));
        assertEquals("PONG is taken", c.ruvio());
        assertNotNull(c.ruvio("OWNER"));
        assertThrows(RuvioServerException.class, () -> c.ruvio("BOGUS"));
        Map<String, Object> hello = c.hello(2);
        assertEquals("ruvio", hello.get("server"));
        assertTrue(c.commandCount() > 200);
        assertFalse(c.command().isEmpty());
        assertEquals(1, c.commandInfo("get").size());
        assertNotNull(c.commandDocs("get"));
        assertNull(c.clientGetname());
        c.clientSetname("java-test");
        assertEquals("java-test", c.clientGetname());
        assertTrue(c.clientId() > 0);
        assertTrue(c.clientList().contains("name=java-test"));
        assertTrue(c.clientInfo().contains("name=java-test"));
        assertTrue(c.slowlogLen() >= 0);
        assertNotNull(c.slowlogGet());
        assertNotNull(c.slowlogGet(5));
        c.slowlogReset();
        assertTrue(c.latencyLatest().isEmpty());
        assertThrows(RuvioServerException.class, () -> c.clientTracking(true));
        c.clientTracking(false);
    }

    @Test
    void clientKill() {
        try (RuvioClient<String, String> victim = RuvioClient.connect(Live.options(Live.DB))) {
            long id = victim.clientId();
            assertEquals(1L, c.clientKill(ClientKillArgs.id(id).skipMe(true)));
            // The drop is seen by the reader; a call racing it fails, the next one reconnects.
            long newId = -1;
            for (int i = 0; i < 3 && newId < 0; i++) {
                try {
                    newId = victim.clientId();
                } catch (RuvioConnectionException dropped) {
                    // expected at most once
                }
            }
            assertNotEquals(id, newId);
            String addr = null;
            for (String line : c.clientList().split("\n")) {
                if (line.contains("id=" + victim.clientId() + " ")) {
                    addr = line.replaceAll("^.*? addr=([^ ]+).*$", "$1");
                }
            }
            assertNotNull(addr);
            c.clientKill(addr);
            assertEquals(0L, c.clientKill(ClientKillArgs.addr("1.2.3.4:5")));
        }
    }

    @Test
    void resp3AndTracking() throws Exception {
        List<Object> pushes = new CopyOnWriteArrayList<>();
        RuvioOptions o = Live.options(Live.DB).toBuilder().pushListener(pushes::add).build();
        try (RuvioClient<String, String> t = RuvioClient.connect(o)) {
            Map<String, Object> hello = t.hello(3);
            assertEquals(3L, hello.get("proto"));
            t.clientTracking(true);
            c.set("trk", "1");
            assertEquals("1", t.get("trk"));
            assertEquals(java.util.Arrays.asList("1", null), t.mget("trk", "trk:none"));
            assertEquals(java.util.Collections.singletonMap("f", "v").size(), 1);
            c.set("trk", "2");
            for (int i = 0; i < 100 && pushes.isEmpty(); i++) {
                Thread.sleep(10);
            }
            assertFalse(pushes.isEmpty(), "invalidation push received");
            t.hset("trk:h", "f", "v");
            assertEquals("v", t.hgetall("trk:h").get("f"));
            t.zadd("trk:z", 1, "m");
            assertEquals(1.0, t.zrangeWithScores("trk:z", 0, -1).get(0).score());
            assertEquals(1.0, t.zscore("trk:z", "m"));
            t.clientTracking(false);
            t.hello(2);
        }
    }

    @Test
    void acl() {
        assertEquals("default", c.aclWhoami());
        assertTrue(c.aclUsers().contains("default"));
        assertFalse(c.aclList().isEmpty());
        assertFalse(c.aclGetuser("default").isEmpty());
        assertFalse(c.aclCat().isEmpty());
        assertFalse(c.aclCat("string").isEmpty());
        c.aclSetuser("javauser", "on", ">secret", "~*", "+@all");
        assertTrue(c.aclUsers().contains("javauser"));
        try (RuvioClient<String, String> u = RuvioClient.connect(Live.options(Live.DB).toBuilder().credentials("javauser", "secret").build())) {
            assertEquals("javauser", u.aclWhoami());
        }
        try (RuvioClient<String, String> u = RuvioClient.connect(Live.options(Live.DB))) {
            assertThrows(RuvioServerException.class, () -> u.auth("javauser", "wrong"));
            u.auth("javauser", "secret");
            assertEquals("javauser", u.aclWhoami());
            assertEquals(3L, ((Number) u.hello(2, "javauser", "secret").get("proto")).longValue() + 1);
        }
        // no acl_file configured: LOAD and SAVE answer an error
        assertThrows(RuvioServerException.class, () -> c.aclLoad());
        assertThrows(RuvioServerException.class, () -> c.aclSave());
    }

    @Test
    void introspection() {
        assertTrue(c.info().contains("ruvio") || c.info().contains("redis_version"));
        assertTrue(c.info("prometheus").contains("ruvio"));
        Map<String, String> cfg = c.configGet("maxmemory*");
        assertTrue(cfg.containsKey("maxmemory"));
        c.configSet(Map.of("slowlog-max-len", "128"));
        assertEquals("128", c.configGet("slowlog-max-len").get("slowlog-max-len"));
        assertThrows(RuvioServerException.class, () -> c.configSet(Map.of("shards", "2")));
        assertEquals("master", String.valueOf(c.role().get(0)));
        // The live server keeps no WAL, so it cannot replicate: both forms are refused unchanged.
        assertThrows(RuvioServerException.class, () -> c.replicaOf("127.0.0.1", 1));
        assertThrows(RuvioServerException.class, () -> c.replicaOfNoOne(false));
        c.save();
        assertNotNull(c.bgsave());
        assertEquals(0L, c.waitReplicas(0, 10));
    }

    @Test
    void cluster() {
        assertEquals(12182L, c.clusterKeyslot("foo"));
        assertEquals(12182, RuvioClient.hashSlot("foo"));
        assertEquals(RuvioClient.hashSlot("{user1}.a"), RuvioClient.hashSlot("user1"));
        assertEquals((long) RuvioClient.hashSlot("{user1}.a"), c.clusterKeyslot("{user1}.a"));
        // Topology commands need a multi-port cluster deployment. On a standalone server they must
        // reach the server and come back as a typed error; with RUVIO_TEST_CLUSTER=1 they must succeed.
        boolean cluster = "1".equals(System.getenv("RUVIO_TEST_CLUSTER"));
        java.util.List<Runnable> calls = java.util.Arrays.asList(
                () -> assertFalse(c.clusterSlots().isEmpty()),
                () -> assertFalse(c.clusterNodes().isEmpty()),
                () -> assertFalse(c.clusterShards().isEmpty()),
                () -> assertTrue(c.clusterInfo().contains("cluster_state:ok")),
                () -> assertEquals(40, c.clusterMyid().length()),
                () -> c.readonly(),
                () -> c.readwrite());
        for (Runnable call : calls) {
            if (cluster) {
                call.run();
            } else {
                RuvioServerException e = assertThrows(RuvioServerException.class, call::run);
                assertTrue(e.getMessage().contains("cluster mode is disabled"), e.getMessage());
            }
        }
    }
}
