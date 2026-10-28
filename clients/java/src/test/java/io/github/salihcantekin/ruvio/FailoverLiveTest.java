// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A client with two seeds follows a failover on real servers. scripts/run_with_servers.sh starts a pair that can each be
 * primary or replica (RUVIO_FAILOVER_ADDRS, their replication addresses in RUVIO_FAILOVER_REPLICATION, both
 * comma separated and primary first); no coordinator runs, the test swaps the roles itself with REPLICAOF, as a
 * coordinator does. Skipped without the variables.
 */
class FailoverLiveTest {
    private static final long PATIENCE_NANOS = TimeUnit.SECONDS.toNanos(10);

    private static String[] pair(String name) {
        String value = System.getenv(name);
        assumeTrue(value != null && !value.isEmpty(), name + " not set; failover live test skipped");
        String[] parts = value.split(",");
        assumeTrue(parts.length == 2, name + " must name two addresses");
        return parts;
    }

    private static RuvioClient<String, String> direct(String address) {
        RuvioOptions base = RuvioOptions.of(address);
        return RuvioClient.connect(RuvioOptions.builder().host(base.host()).port(base.port()).commandTimeoutMillis(5_000).build());
    }

    private static boolean isMaster(String address) {
        try (RuvioClient<String, String> c = direct(address)) {
            return "master".equals(text(c.role().get(0)));
        }
    }

    private static String text(Object o) {
        return o instanceof byte[] ? new String((byte[]) o, StandardCharsets.UTF_8) : String.valueOf(o);
    }

    /** Sends one command over a plain socket and returns the first line of the reply. */
    private static String raw(String address, String... args) throws IOException {
        RuvioOptions base = RuvioOptions.of(address);
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(base.host(), base.port()), 5_000);
            s.setSoTimeout(5_000);
            StringBuilder frame = new StringBuilder("*").append(args.length).append("\r\n");
            for (String a : args) {
                frame.append('$').append(a.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(a).append("\r\n");
            }
            OutputStream out = s.getOutputStream();
            out.write(frame.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            InputStream in = s.getInputStream();
            StringBuilder line = new StringBuilder();
            for (int c = in.read(); c >= 0 && c != '\n'; c = in.read()) {
                if (c != '\r') {
                    line.append((char) c);
                }
            }
            return line.toString();
        }
    }

    private static void waitUntil(String what, java.util.function.BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + PATIENCE_NANOS;
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "gave up waiting for " + what);
            Thread.sleep(20);
        }
    }

    @Test
    void aClientFollowsThePrimaryAfterTheRolesAreSwapped() throws Exception {
        String[] clients = pair("RUVIO_FAILOVER_ADDRS");
        String[] replication = pair("RUVIO_FAILOVER_REPLICATION");
        // Whichever node is the primary now, so the test can run again on the same servers.
        int old = isMaster(clients[0]) ? 0 : 1;
        int next = 1 - old;
        RuvioOptions options = RuvioOptions.builder().seeds(clients[old], clients[next]).commandTimeoutMillis(5_000).build();
        String key = "failover:java:" + ProcessHandle.current().pid();
        try (RuvioClient<String, String> client = RuvioClient.connect(options);
                RuvioClient<String, String> onNew = direct(clients[next])) {
            client.set(key, "before");
            // A replica that still copies refuses to be promoted without FORCE.
            waitUntil("the replica to hold the first write", () -> "before".equals(onNew.get(key)));

            assertEquals("+OK", raw(clients[next], "REPLICAOF", "NO", "ONE"));
            RuvioOptions toNew = RuvioOptions.of(replication[next]);
            assertEquals("+OK", raw(clients[old], "REPLICAOF", toNew.host(), Integer.toString(toNew.port())));

            long started = System.nanoTime();
            client.set(key, "after");
            assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5), "the write took " + (System.nanoTime() - started) / 1_000_000 + " ms");
            assertEquals("after", onNew.get(key));

            // Without the option the same situation is the READONLY error.
            try (RuvioClient<String, String> stuck = RuvioClient.connect(options.toBuilder().followPrimary(false).build())) {
                RuvioServerException e = assertThrows(RuvioServerException.class, () -> stuck.set(key, "never"));
                assertTrue(e.getMessage().startsWith("READONLY"), e.getMessage());
            }

            // A dropped socket: the reconnect starts at the old primary (now a replica) but takes the node that
            // answers master, before any write.
            long id = client.clientId();
            onNew.execute("CLIENT", "KILL", "ID", Long.toString(id));
            waitUntil("the client to reconnect", () -> {
                try {
                    return client.clientId() != id;
                } catch (RuvioConnectionException e) {
                    return false; // the command that meets the dropped socket fails; the next one reconnects
                }
            });
            List<Object> role = client.role();
            assertEquals("master", text(role.get(0)), role.toString());
            client.set(key, "again");
            assertEquals("again", onNew.get(key));
        }
    }
}
