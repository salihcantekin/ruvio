// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.salihcantekin.ruvio.model.PubSubMessage;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * TLS against the servers {@code scripts/run_with_servers.sh} starts: RUVIO_TLS_ADDR, RUVIO_TLS_CLIENT_AUTH_ADDR and the PEM
 * files RUVIO_TLS_CA, RUVIO_TLS_CLIENT_CERT and RUVIO_TLS_CLIENT_KEY. Skipped without them.
 */
class TlsLiveTest {
    private static String env(String name) {
        String value = System.getenv(name);
        assumeTrue(value != null && !value.isEmpty(), name + " not set; TLS test skipped");
        return value;
    }

    private static RuvioOptions.Builder to(String addrVariable) {
        RuvioOptions base = RuvioOptions.of(env(addrVariable));
        return RuvioOptions.builder().host(base.host()).port(base.port()).connectTimeoutMillis(5_000).commandTimeoutMillis(10_000);
    }

    private static Path ca() {
        return Path.of(env("RUVIO_TLS_CA"));
    }

    @Test
    void aVerifiedConnectionRunsCommands() {
        try (RuvioClient<String, String> c = RuvioClient.connect(to("RUVIO_TLS_ADDR").tls(RuvioTls.context(ca(), null, null)).build())) {
            c.set("tls:key", "value");
            assertEquals("value", c.get("tls:key"));
            String big = "x".repeat(300_000);
            c.set("tls:big", big);
            assertEquals(big, c.get("tls:big"));
        }
    }

    @Test
    void pubSubRunsOverTls() throws Exception {
        try (RuvioClient<String, String> c = RuvioClient.connect(to("RUVIO_TLS_ADDR").tls(RuvioTls.context(ca(), null, null)).build())) {
            BlockingQueue<PubSubMessage> got = new LinkedBlockingQueue<>();
            try (RuvioPubSub sub = c.pubSub(got::add)) {
                sub.subscribe("tls-ch");
                sub.ping();
                assertEquals(1L, c.publish("tls-ch", "hi"));
                assertEquals("hi", got.poll(5, TimeUnit.SECONDS).payloadString());
            }
        }
    }

    @Test
    void anUntrustedCertificateIsRefused() {
        env("RUVIO_TLS_ADDR");
        // The JVM's default certificates do not know the test CA.
        assertThrows(RuvioException.class, () -> RuvioClient.connect(to("RUVIO_TLS_ADDR").tls().build()));
    }

    @Test
    void aNameTheCertificateLacksIsRefused() {
        Path ca = ca();
        assertThrows(RuvioException.class, () -> RuvioClient.connect(
                to("RUVIO_TLS_ADDR").tls(RuvioTls.context(ca, null, null)).tlsServerName("elsewhere.example").build()));
    }

    @Test
    void aDnsNameCanBeCheckedWhileConnectingByAddress() {
        try (RuvioClient<String, String> c = RuvioClient.connect(
                to("RUVIO_TLS_ADDR").tls(RuvioTls.context(ca(), null, null)).tlsServerName("localhost").build())) {
            assertEquals("PONG", c.ping());
        }
    }

    @Test
    void skippingVerificationIsAnExplicitChoice() {
        try (RuvioClient<String, String> c = RuvioClient.connect(to("RUVIO_TLS_ADDR").tlsInsecure().build())) {
            assertEquals("PONG", c.ping());
        }
    }

    @Test
    void aServerThatWantsAClientCertificateGetsOne() {
        Path cert = Path.of(env("RUVIO_TLS_CLIENT_CERT"));
        Path key = Path.of(env("RUVIO_TLS_CLIENT_KEY"));
        try (RuvioClient<String, String> c = RuvioClient.connect(
                to("RUVIO_TLS_CLIENT_AUTH_ADDR").tls(RuvioTls.context(ca(), cert, key)).build())) {
            assertEquals("PONG", c.ping());
        }
    }

    @Test
    void aMissingClientCertificateIsRefused() {
        Path ca = ca();
        env("RUVIO_TLS_CLIENT_AUTH_ADDR");
        assertThrows(RuntimeException.class, () -> {
            try (RuvioClient<String, String> c = RuvioClient.connect(
                    to("RUVIO_TLS_CLIENT_AUTH_ADDR").tls(RuvioTls.context(ca, null, null)).build())) {
                // With TLS 1.3 the refusal can arrive with the first read.
                c.ping();
            }
        });
    }

    @Test
    void plainTcpToATlsPortFails() {
        env("RUVIO_TLS_ADDR");
        assertThrows(RuntimeException.class, () -> {
            try (RuvioClient<String, String> c = RuvioClient.connect(to("RUVIO_TLS_ADDR").commandTimeoutMillis(2_000).build())) {
                c.ping();
            }
        });
    }
}
