// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioOptions;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** The wire form and reply mapping of IDEM ABORT and the helper flag, against a scripted server. */
class IdemAbortFakeTest {
    private static RuvioClient<String, String> open(FakeNode n) {
        return RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1").port(n.port).connectTimeoutMillis(2000).build());
    }

    private static List<String> last(FakeNode n) {
        return n.received.get(n.received.size() - 1);
    }

    private static boolean sawAbort(FakeNode n) {
        synchronized (n.received) {
            return n.received.stream().anyMatch(a -> a.size() > 2 && a.get(2).equals("ABORT"));
        }
    }

    @Test
    void abortEncodesAndMapsTheReply() throws Exception {
        AtomicReference<String> abortReply = new AtomicReference<>(":1\r\n");
        try (FakeNode n = new FakeNode(a -> a.size() > 2 && a.get(2).equals("ABORT") ? abortReply.get() : ":1\r\n");
                RuvioClient<String, String> c = open(n)) {
            assertTrue(c.idemAbort("k", "fp", "me"));
            assertEquals(Arrays.asList("IDEM", "k", "ABORT", "fp", "me"), last(n));
            abortReply.set(":0\r\n");
            assertFalse(c.idemAbort("k", "fp", "me"));
        }
    }

    @Test
    void helperAbortsOnlyWhenAskedAndKeepsTheActionError() throws Exception {
        for (boolean abort : new boolean[] {false, true}) {
            try (FakeNode n = new FakeNode(a -> a.size() > 2 && a.get(2).equals("BEGIN") ? "*1\r\n$8\r\nacquired\r\n" : ":1\r\n");
                    RuvioClient<String, String> c = open(n)) {
                IllegalStateException boom = new IllegalStateException("boom");
                IllegalStateException got = assertThrows(IllegalStateException.class,
                        () -> c.idempotent("k", "fp", 1000, 100, () -> {
                            throw boom;
                        }, abort));
                assertEquals(boom, got);
                assertEquals(abort, sawAbort(n));
            }
        }
    }

    @Test
    void failedAbortDoesNotHideTheActionError() throws Exception {
        try (FakeNode n = new FakeNode(a -> a.size() > 2 && a.get(2).equals("BEGIN") ? "*1\r\n$8\r\nacquired\r\n"
                : "-ERR INPUT bad\r\n"); RuvioClient<String, String> c = open(n)) {
            IllegalStateException boom = new IllegalStateException("boom");
            assertEquals(boom, assertThrows(IllegalStateException.class, () -> c.idempotent("k", "fp", 1000, 100, () -> {
                throw boom;
            }, true)));
        }
    }
}
