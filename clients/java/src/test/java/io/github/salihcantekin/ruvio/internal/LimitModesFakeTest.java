// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.model.LimitOutcome;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The LIMIT SLIDING / TOKENS commands against a scripted server (no live server needed). */
class LimitModesFakeTest {
    private static RuvioClient connect(FakeNode node) {
        return RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1").port(node.port).connectTimeoutMillis(2000).build());
    }

    @Test
    void modesSendTheirKeywordsAndReadTheirReplies() throws Exception {
        String[] reply = {":3\r\n"};
        try (FakeNode node = new FakeNode(args -> reply[0]); RuvioClient c = connect(node)) {
            assertEquals(3L, c.limitSliding("k", 5, 1000));
            assertEquals(List.of("LIMIT", "k", "5", "1000", "SLIDING"), node.received.get(node.received.size() - 1));
            assertEquals(3L, c.limitTokens("k", 10, 2000, 2));
            assertEquals(List.of("LIMIT", "k", "10", "2000", "TOKENS", "COST", "2"), node.received.get(node.received.size() - 1));
            reply[0] = "$-1\r\n";
            assertNull(c.limitSliding("k", 5, 1000));

            reply[0] = "*2\r\n:2\r\n:0\r\n";
            assertEquals(new LimitOutcome(true, 2, 0), c.limitSlidingWithRetry("k", 5, 1000));
            assertEquals(List.of("LIMIT", "k", "5", "1000", "SLIDING", "WITHRETRY"), node.received.get(node.received.size() - 1));
            reply[0] = "*2\r\n:-1\r\n:250\r\n";
            assertEquals(new LimitOutcome(false, 0, 250), c.limitTokensWithRetry("k", 10, 2000, 1));
            assertEquals(List.of("LIMIT", "k", "10", "2000", "TOKENS", "COST", "1", "WITHRETRY"),
                    node.received.get(node.received.size() - 1));
        }
    }
}
