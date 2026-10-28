// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.salihcantekin.ruvio.RuvioClient;
import io.github.salihcantekin.ruvio.RuvioOptions;
import io.github.salihcantekin.ruvio.args.BfInsertArgs;
import io.github.salihcantekin.ruvio.args.ExpireCondition;
import io.github.salihcantekin.ruvio.args.XAddArgs;
import io.github.salihcantekin.ruvio.args.ZAddArgs;
import io.github.salihcantekin.ruvio.model.GeoValue;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The exact arguments of the TTL-on-create overloads and the EXPIRE conditions, against a scripted server. */
class TtlOnCreateFakeTest {
    private static String reply(List<String> a) {
        switch (a.get(0).toUpperCase()) {
            case "INCRBYFLOAT":
            case "ZINCRBY":
                return "$1\r\n1\r\n";
            case "XADD":
                return "$3\r\n1-0\r\n";
            case "JSON.SET":
                return "+OK\r\n";
            case "BF.INSERT":
                return "*1\r\n:1\r\n";
            default:
                return ":1\r\n";
        }
    }

    private static List<String> last(FakeNode n) {
        return n.received.get(n.received.size() - 1);
    }

    private static RuvioClient<String, String> open(FakeNode n) {
        return RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1").port(n.port).connectTimeoutMillis(2000).build());
    }

    private static final Duration SEC = Duration.ofSeconds(30);
    private static final Duration MS = Duration.ofMillis(1500);

    @Test
    void counterOverloadsSendExForWholeSecondsAndPxOtherwise() throws Exception {
        try (FakeNode n = new FakeNode(TtlOnCreateFakeTest::reply); RuvioClient<String, String> c = open(n)) {
            c.incr("k", SEC);
            assertEquals(Arrays.asList("INCR", "k", "EX", "30"), last(n));
            c.incr("k", MS);
            assertEquals(Arrays.asList("INCR", "k", "PX", "1500"), last(n));
            c.decr("k", SEC);
            assertEquals(Arrays.asList("DECR", "k", "EX", "30"), last(n));
            c.incrby("k", 5, SEC);
            assertEquals(Arrays.asList("INCRBY", "k", "5", "EX", "30"), last(n));
            c.decrby("k", 5, MS);
            assertEquals(Arrays.asList("DECRBY", "k", "5", "PX", "1500"), last(n));
            c.incrbyMax("k", 1, 100, SEC);
            assertEquals(Arrays.asList("INCRBY", "k", "1", "EX", "30", "MAX", "100"), last(n));
            c.incrbyfloat("k", 1.5, MS);
            assertEquals(Arrays.asList("INCRBYFLOAT", "k", "1.5", "PX", "1500"), last(n));
            c.hincrby("h", "f", 2, SEC);
            assertEquals(Arrays.asList("HINCRBY", "h", "f", "2", "EX", "30"), last(n));
            c.zincrby("z", 2.5, "m", MS);
            assertEquals(Arrays.asList("ZINCRBY", "z", "2.5", "m", "PX", "1500"), last(n));
            c.setbit("b", 7, true, SEC);
            assertEquals(Arrays.asList("SETBIT", "b", "7", "1", "EX", "30"), last(n));
            c.setbit("b", 7, false, Duration.ofNanos(1));
            assertEquals(Arrays.asList("SETBIT", "b", "7", "0", "PX", "1"), last(n));
        }
    }

    @Test
    void optionObjectsAndOverloadsPutTheTtlAmongTheOptions() throws Exception {
        try (FakeNode n = new FakeNode(TtlOnCreateFakeTest::reply); RuvioClient<String, String> c = open(n)) {
            c.zadd("z", ZAddArgs.builder().ttl(SEC).nx().ch(), Map.of("m", 1.0));
            assertEquals(Arrays.asList("ZADD", "z", "EX", "30", "NX", "CH", "1", "m"), last(n));
            c.zaddIncr("z", ZAddArgs.builder().ttl(MS), 2.0, "m");
            assertEquals(Arrays.asList("ZADD", "z", "PX", "1500", "INCR", "2", "m"), last(n));
            c.geoadd("g", false, true, true, SEC, GeoValue.of(13.5, 38.5, "p"));
            assertEquals(Arrays.asList("GEOADD", "g", "EX", "30", "XX", "CH", "13.5", "38.5", "p"), last(n));
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("f", "v");
            c.xadd("s", XAddArgs.builder().ttl(SEC).noMkStream().maxLen(10).delay(5), fields);
            assertEquals(Arrays.asList("XADD", "s", "EX", "30", "NOMKSTREAM", "DELAY", "5", "MAXLEN", "10", "*", "f", "v"), last(n));
            c.jsonSet("j", "$", "1", SEC);
            assertEquals(Arrays.asList("JSON.SET", "j", "$", "1", "EX", "30"), last(n));
            c.jsonSetNx("j", "$", "1", MS);
            assertEquals(Arrays.asList("JSON.SET", "j", "$", "1", "NX", "PX", "1500"), last(n));
            c.jsonSetXx("j", "$", "1", SEC);
            assertEquals(Arrays.asList("JSON.SET", "j", "$", "1", "XX", "EX", "30"), last(n));
            c.bfInsert("f", BfInsertArgs.builder().ttl(SEC).capacity(100).error(0.01), "a", "b");
            assertEquals(Arrays.asList("BF.INSERT", "f", "EX", "30", "CAPACITY", "100", "ERROR", "0.01", "ITEMS", "a", "b"), last(n));
        }
    }

    @Test
    void expireOverloadsSendTheConditionAndAlwaysSendsNone() throws Exception {
        try (FakeNode n = new FakeNode(TtlOnCreateFakeTest::reply); RuvioClient<String, String> c = open(n)) {
            c.expire("k", 10, ExpireCondition.NX);
            assertEquals(Arrays.asList("EXPIRE", "k", "10", "NX"), last(n));
            c.pexpire("k", 10, ExpireCondition.XX);
            assertEquals(Arrays.asList("PEXPIRE", "k", "10", "XX"), last(n));
            c.expireat("k", 99, ExpireCondition.GT);
            assertEquals(Arrays.asList("EXPIREAT", "k", "99", "GT"), last(n));
            c.pexpireat("k", 99, ExpireCondition.LT);
            assertEquals(Arrays.asList("PEXPIREAT", "k", "99", "LT"), last(n));
            c.expire("k", 10, ExpireCondition.ALWAYS);
            assertEquals(Arrays.asList("EXPIRE", "k", "10"), last(n));
            c.expire("k", 10, null);
            assertEquals(Arrays.asList("EXPIRE", "k", "10"), last(n));
        }
    }

    @Test
    void badTtlsAreRejectedBeforeAnythingIsSent() throws Exception {
        try (FakeNode n = new FakeNode(TtlOnCreateFakeTest::reply); RuvioClient<String, String> c = open(n)) {
            assertThrows(IllegalArgumentException.class, () -> c.incr("k", Duration.ZERO));
            assertThrows(IllegalArgumentException.class, () -> c.incr("k", Duration.ofSeconds(-1)));
            assertThrows(IllegalArgumentException.class, () -> c.incr("k", Duration.ofSeconds(Long.MAX_VALUE)));
            assertThrows(IllegalArgumentException.class, () -> c.incr("k", null));
            assertThrows(IllegalArgumentException.class, () -> c.zadd("z", ZAddArgs.builder().ttl(Duration.ZERO), Map.of("m", 1.0)));
            assertEquals(0, n.received.stream().filter(a -> a.get(0).equals("INCR") || a.get(0).equals("ZADD")).count());
        }
    }
}
