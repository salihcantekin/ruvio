// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio;

import io.github.salihcantekin.ruvio.args.GeoSearchArgs;
import io.github.salihcantekin.ruvio.args.GeoUnit;
import io.github.salihcantekin.ruvio.args.ScanArgs;
import io.github.salihcantekin.ruvio.args.ZAddArgs;
import io.github.salihcantekin.ruvio.args.ZRangeArgs;
import io.github.salihcantekin.ruvio.args.ZStoreArgs;
import io.github.salihcantekin.ruvio.model.GeoCoordinates;
import io.github.salihcantekin.ruvio.model.GeoResult;
import io.github.salihcantekin.ruvio.model.GeoValue;
import io.github.salihcantekin.ruvio.model.KeyValue;
import io.github.salihcantekin.ruvio.model.ScoredValue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SortedSetGeoLiveTest {
    static RuvioClient<String, String> c;

    @BeforeAll
    static void open() {
        c = Live.client();
    }

    @AfterAll
    static void close() {
        if (c != null) c.close();
    }

    static Map<String, Double> scores(Object... kv) {
        Map<String, Double> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], ((Number) kv[i + 1]).doubleValue());
        }
        return m;
    }

    @Test
    void zsets() {
        assertEquals(1L, c.zadd("z", 1, "a"));
        assertEquals(2L, c.zadd("z", scores("b", 2, "c", 3)));
        assertEquals(0L, c.zadd("z", ZAddArgs.builder().nx(), scores("a", 10)));
        assertEquals(1L, c.zadd("z", ZAddArgs.builder().xx().ch(), scores("a", 1.5)));
        assertEquals(0L, c.zadd("z", ZAddArgs.builder().gt(), scores("a", 1)));
        assertEquals(0L, c.zadd("z", ZAddArgs.builder().lt(), scores("a", 2)));
        assertEquals(2.5, c.zaddIncr("z", 1, "a"));
        assertNull(c.zaddIncr("z", ZAddArgs.builder().nx(), 1, "a"));
        assertEquals(1.5, c.zincrby("z", -1, "a"));
        assertEquals(3L, c.zcard("z"));
        assertEquals(1.5, c.zscore("z", "a"));
        assertNull(c.zscore("z", "nope"));
        assertEquals(Arrays.asList(1.5, null), c.zmscore("z", "a", "nope"));
        assertEquals(0L, c.zrank("z", "a"));
        assertEquals(2L, c.zrevrank("z", "a"));
        assertNull(c.zrank("z", "nope"));
        assertEquals(Arrays.asList("a", "b", "c"), c.zrange("z", 0, -1));
        assertEquals(new ScoredValue<>("a", 1.5), c.zrangeWithScores("z", 0, 0).get(0));
        assertEquals(Arrays.asList("c", "b"), c.zrange("z", "+inf", "(1.5", ZRangeArgs.builder().byScore().rev()) /* REV: high bound first */);
        assertEquals(Arrays.asList("b"), c.zrange("z", "-inf", "+inf", ZRangeArgs.builder().byScore().limit(1, 1)));
        assertEquals(2, c.zrangeWithScores("z", "[a", "[b", ZRangeArgs.builder().byLex()).size());
        assertEquals(Arrays.asList("c", "b", "a"), c.zrevrange("z", 0, -1));
        assertEquals(3.0, c.zrevrangeWithScores("z", 0, 0).get(0).score());
        assertEquals(Arrays.asList("b", "c"), c.zrangebyscore("z", "2", "+inf"));
        assertEquals(Arrays.asList("c"), c.zrangebyscore("z", "2", "+inf", 1, 1));
        assertEquals(2, c.zrangebyscoreWithScores("z", "(1.5", "3").size());
        assertEquals(1, c.zrangebyscoreWithScores("z", "-inf", "3", 0, 1).size());
        c.zadd("lex", scores("a", 0, "b", 0, "c", 0, "d", 0));
        assertEquals(Arrays.asList("b", "c"), c.zrangebylex("lex", "[b", "(d"));
        assertEquals(Arrays.asList("c"), c.zrangebylex("lex", "-", "+", 2, 1));
        assertEquals(Arrays.asList("d", "c"), c.zrevrangebylex("lex", "+", "[c"));
        assertEquals(Arrays.asList("c"), c.zrevrangebylex("lex", "+", "-", 1, 1));
        assertEquals(4L, c.zlexcount("lex", "-", "+"));
        assertEquals(2L, c.zcount("z", "2", "3"));
        assertEquals(1L, c.zremrangebylex("lex", "[a", "[a"));
        assertEquals(1L, c.zremrangebyrank("lex", 0, 0));
        assertEquals(2L, c.zremrangebyscore("lex", "-inf", "0"));
        assertEquals(0L, c.zrem("lex", "d", "zz"));
        // pops
        c.zadd("pz", scores("x", 1, "y", 2, "w", 3));
        assertEquals(Arrays.asList(new ScoredValue<>("x", 1.0)), c.zpopmin("pz"));
        assertEquals(Arrays.asList(new ScoredValue<>("w", 3.0)), c.zpopmax("pz", 1));
        assertEquals(1, c.zpopmin("pz", 5).size());
        assertTrue(c.zpopmax("pz").isEmpty());
        // due members
        c.zadd("due", scores("a", 1, "b", 2, "c", 3, "d", 9));
        assertEquals(Arrays.asList(new ScoredValue<>("a", 1.0)), c.zpopbyscore("due", "-inf", "1", 5));
        assertEquals(2L, c.zmovedue("due", "ready", "3", 5));
        assertEquals(Arrays.asList("b", "c"), c.lrange("ready", 0, -1));
        assertTrue(c.zpopbyscore("due", "-inf", "3", 5).isEmpty());
        // algebra
        c.zadd("z1", scores("a", 1, "b", 2));
        c.zadd("z2", scores("b", 3, "c", 4));
        assertEquals(3L, c.zunionstore("zu", "z1", "z2"));
        assertEquals(5.0, c.zscore("zu", "b"));
        assertEquals(1L, c.zinterstore("zi", ZStoreArgs.weights(2, 1).withAggregate(ZStoreArgs.Aggregate.MAX), "z1", "z2"));
        assertEquals(4.0, c.zscore("zi", "b"));
        assertEquals(1L, c.zinterstore("zi2", "z1", "z2"));
        assertEquals(3L, c.zunionstore("zu2", ZStoreArgs.aggregate(ZStoreArgs.Aggregate.MIN), "z1", "z2"));
        assertEquals(1L, c.zdiffstore("zd", "z1", "z2"));
        assertEquals(Arrays.asList("a", "c", "b"), c.zunion("z1", "z2"));
        assertEquals(Arrays.asList("a", "b", "c"), c.zunion(ZStoreArgs.aggregate(ZStoreArgs.Aggregate.MIN), "z1", "z2"));
        assertEquals(new ScoredValue<>("b", 5.0), c.zunionWithScores(null, "z1", "z2").get(2));
        assertEquals(Arrays.asList("b"), c.zinter("z1", "z2"));
        assertEquals(Arrays.asList("b"), c.zinter(ZStoreArgs.weights(1, 1), "z1", "z2"));
        assertEquals(5.0, c.zinterWithScores(null, "z1", "z2").get(0).score());
        assertEquals(Arrays.asList("a"), c.zdiff("z1", "z2"));
        assertEquals(1.0, c.zdiffWithScores("z1", "z2").get(0).score());
        assertNotNull(c.zrandmember("zu"));
        assertEquals(2, c.zrandmember("zu", 2).size());
        assertEquals(2, c.zrandmemberWithScores("zu", 2).size());
        for (int i = 0; i < 40; i++) {
            c.zadd("zbig", i, "m" + i);
        }
        Set<String> seen = new HashSet<>();
        for (ScoredValue<String> sv : c.zscanIterable("zbig", ScanArgs.count(9))) {
            seen.add(sv.value());
        }
        assertEquals(40, seen.size());
        assertEquals(40, c.zscanStream("zbig", null).count());
        assertEquals(40, c.zscan("zbig", "0", ScanArgs.count(100)).items().size());
    }

    @Test
    void blockingZpop() {
        try (RuvioClient<String, String> d = c.dedicated()) {
            assertNull(d.bzpopmin(0.05, "bz:none"));
            c.zadd("bz", scores("a", 1, "b", 2));
            KeyValue<String, ScoredValue<String>> min = d.bzpopmin(1, "bz");
            assertEquals("bz", min.key());
            assertEquals(new ScoredValue<>("a", 1.0), min.value());
            assertEquals("b", d.bzpopmax(1, "bz").value().value());
        }
    }

    @Test
    void geo() {
        assertEquals(1L, c.geoadd("g", 13.361389, 38.115556, "Palermo"));
        assertEquals(1L, c.geoadd("g", GeoValue.of(15.087269, 37.502669, "Catania")));
        assertEquals(0L, c.geoadd("g", true, false, false, GeoValue.of(1, 1, "Catania")));
        assertEquals(1L, c.geoadd("g", false, true, true, GeoValue.of(15.0, 37.502669, "Catania"), GeoValue.of(0, 0, "Nope")));
        assertNull(c.zscore("g", "Nope"));
        c.geoadd("g", 15.087269, 37.502669, "Catania");
        double km = c.geodist("g", "Palermo", "Catania", GeoUnit.KM);
        assertEquals(166.27, km, 0.5);
        assertEquals(166274, c.geodist("g", "Palermo", "Catania"), 500);
        assertNull(c.geodist("g", "Palermo", "nope"));
        List<String> hashes = c.geohash("g", "Palermo", "nope");
        assertEquals(11, hashes.get(0).length());
        assertNull(hashes.get(1));
        List<GeoCoordinates> pos = c.geopos("g", "Palermo", "nope");
        assertEquals(13.361389, pos.get(0).longitude(), 0.001);
        assertNull(pos.get(1));
        List<GeoResult<String>> near = c.geosearch("g", GeoSearchArgs.fromLonLat(15, 37).byRadius(200, GeoUnit.KM).asc().withDist().withCoord().withHash());
        assertEquals("Catania", near.get(0).member());
        assertNotNull(near.get(0).distance());
        assertNotNull(near.get(0).coordinates());
        assertNotNull(near.get(0).hash());
        List<GeoResult<String>> plain = c.geosearch("g", GeoSearchArgs.fromMember("Palermo").byBox(400, 400, GeoUnit.KM).desc().count(1));
        assertEquals(1, plain.size());
        assertNull(plain.get(0).distance());
        assertEquals(2L, c.geosearchstore("gs", "g", GeoSearchArgs.fromLonLat(15, 37).byRadius(300, GeoUnit.KM), false));
        assertEquals(2L, c.geosearchstore("gs2", "g", GeoSearchArgs.fromLonLat(15, 37).byRadius(300, GeoUnit.KM).count(5, true), true));
        assertTrue(c.zscore("gs2", "Catania") < 100_000);
        assertEquals(2, c.georadius("g", 15, 37, 200, GeoUnit.KM).size());
        assertEquals("Catania", c.georadius("g", 15, 37, 200, GeoUnit.KM, GeoSearchArgs.options().withDist().asc().count(1)).get(0).member());
        assertEquals(2, c.georadiusbymember("g", "Palermo", 200, GeoUnit.KM).size());
        assertEquals(1, c.georadiusbymember("g", "Palermo", 200, GeoUnit.KM, GeoSearchArgs.options().count(1).withCoord()).size());
        assertEquals(2L, c.georadiusStore("g", 15, 37, 200, GeoUnit.KM, "gr", false));
        assertEquals(2L, c.georadiusbymemberStore("g", "Palermo", 200, GeoUnit.KM, "gr2", true));
    }

    @Test
    void ttlOnCreateSortedSetAndGeo() {
        assertEquals(2.5, c.zincrby("ttl:z", 2.5, "m", LONG_TTL));
        assertEquals(5.0, c.zincrby("ttl:z", 2.5, "m", LONGER_TTL));
        assertDeadlineKept("ttl:z");

        assertEquals(1L, c.zadd("ttl:za", ZAddArgs.builder().ttl(LONG_TTL), Map.of("a", 1.0)));
        assertEquals(1L, c.zadd("ttl:za", ZAddArgs.builder().ttl(LONGER_TTL).nx(), Map.of("b", 2.0)));
        assertDeadlineKept("ttl:za");
        assertEquals(3.0, c.zaddIncr("ttl:zi", ZAddArgs.builder().ttl(LONG_TTL), 3.0, "a"));
        assertDeadlineKept("ttl:zi");

        assertEquals(1L, c.geoadd("ttl:g", false, false, false, LONG_TTL, GeoValue.of(13.36, 38.11, "p1")));
        assertEquals(1L, c.geoadd("ttl:g", false, false, false, LONGER_TTL, GeoValue.of(15.08, 37.50, "p2")));
        assertDeadlineKept("ttl:g");
    }

    private static final java.time.Duration LONG_TTL = java.time.Duration.ofSeconds(100);
    private static final java.time.Duration LONGER_TTL = java.time.Duration.ofSeconds(1000);

    /** The key got the first call's deadline (at most 100 s), not the second call's 1000 s. */
    private void assertDeadlineKept(String key) {
        long ms = c.pttl(key);
        assertTrue(ms > 0 && ms <= 100_000, "pttl " + ms);
    }
}
