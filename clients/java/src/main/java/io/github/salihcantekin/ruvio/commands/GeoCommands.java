// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.commands;

import io.github.salihcantekin.ruvio.RuvioCodec;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.args.GeoOrder;
import io.github.salihcantekin.ruvio.args.GeoSearchArgs;
import io.github.salihcantekin.ruvio.args.GeoUnit;
import io.github.salihcantekin.ruvio.internal.Args;
import io.github.salihcantekin.ruvio.internal.Cmd;
import io.github.salihcantekin.ruvio.internal.Reply;
import io.github.salihcantekin.ruvio.model.GeoCoordinates;
import io.github.salihcantekin.ruvio.model.GeoResult;
import io.github.salihcantekin.ruvio.model.GeoValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * GEO commands (a sorted set scored by a 52-bit geohash).
 *
 * @param <K> key type (also hash fields and stream field names)
 * @param <V> value type (also members, list elements and stream field values)
 */
public abstract class GeoCommands<K, V> extends SortedSetCommands<K, V> {
    /**
     * Creates the command family with the given codecs.
     *
     * @param keys the key codec
     * @param values the value codec
     */
    protected GeoCommands(RuvioCodec<K> keys, RuvioCodec<V> values) {
        super(keys, values);
    }

    static final Reply<List<GeoCoordinates>> GEOPOS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<GeoCoordinates> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (item == null) {
                out.add(null);
            } else {
                List<Object> p = Reply.list(item);
                out.add(new GeoCoordinates(Reply.dbl(p.get(0)), Reply.dbl(p.get(1))));
            }
        }
        return out;
    });

    /** Matches: plain members, or [member, dist?, hash?, [lon, lat]?] in that order. */
    static final Reply<List<GeoResult<Object>>> GEO_RESULTS = Reply.of((tree, q) -> {
        List<Object> items = Reply.list(tree);
        List<GeoResult<Object>> out = new ArrayList<>(items.size());
        for (Object item : items) {
            if (!(item instanceof List)) {
                out.add(new GeoResult<>(Reply.decodeWith(q.values, item), null, null, null));
                continue;
            }
            List<Object> parts = Reply.list(item);
            Object member = Reply.decodeWith(q.values, parts.get(0));
            Double dist = null;
            Long hash = null;
            GeoCoordinates coord = null;
            for (int i = 1; i < parts.size(); i++) {
                Object p = parts.get(i);
                if (p instanceof Long) {
                    hash = (Long) p;
                } else if (p instanceof List) {
                    List<Object> c = Reply.list(p);
                    coord = new GeoCoordinates(Reply.dbl(c.get(0)), Reply.dbl(c.get(1)));
                } else {
                    dist = Reply.dbl(p);
                }
            }
            out.add(new GeoResult<>(member, dist, hash, coord));
        }
        return out;
    });

    /**
     * Sends {@code GEOADD key longitude latitude member}: stores a place. Ruvio keeps it in a sorted set whose score is a 52-bit geohash, so {@code TYPE} reports {@code zset} and the sorted-set commands also work on the key. Adding an existing member moves it.
     *
     * <pre>{@code
     * client.geoadd("cities", 13.361389, 38.115556, "Palermo");
     * }</pre>
     *
     * @param key the geo key
     * @param longitude degrees east, from -180 to 180
     * @param latitude degrees north, from -85.05112878 to 85.05112878 (the Web Mercator limit)
     * @param member the place name
     * @return {@code 1} when the member is new, {@code 0} when an existing member was moved
     * @throws RuvioServerException {@code ERR invalid longitude,latitude pair} when a coordinate is outside the ranges; {@code WRONGTYPE} when the key holds a non-sorted-set
     */
    public CompletableFuture<Long> geoadd(K key, double longitude, double latitude, V member) {
        return send(args().w(Cmd.GEOADD).k(key).d(longitude).d(latitude).v(member), Reply.LONG);
    }

    /**
     * Sends {@code GEOADD key longitude latitude member [longitude latitude member ...]}: stores several places in one command.
     *
     * <pre>{@code
     * client.geoadd("cities", GeoValue.of(13.36, 38.11, "Palermo"), GeoValue.of(15.08, 37.50, "Catania"));
     * }</pre>
     *
     * @param key the geo key
     * @param members the places, built with {@link GeoValue#of}; longitude -180 to 180, latitude -85.05112878 to 85.05112878
     * @return how many members were new
     * @throws RuvioServerException {@code ERR invalid longitude,latitude pair} for an out-of-range coordinate (nothing is written); {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> geoadd(K key, GeoValue<V>... members) {
        return geoadd(key, false, false, false, members);
    }

    /**
     * Sends {@code GEOADD key [NX|XX] [CH] longitude latitude member ...}.
     *
     * @param key the geo key
     * @param nx {@code true} to add only new members (existing ones are not moved)
     * @param xx {@code true} to update only existing members (new ones are not added)
     * @param ch {@code true} to count members whose position changed as well as new ones
     * @param members the places; longitude -180 to 180, latitude -85.05112878 to 85.05112878
     * @return how many members were new, or with {@code ch} new plus changed
     * @throws RuvioServerException {@code ERR} for an out-of-range coordinate or {@code nx} together with {@code xx}; {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> geoadd(K key, boolean nx, boolean xx, boolean ch, GeoValue<V>... members) {
        Args a = args().w(Cmd.GEOADD).k(key);
        if (nx) a.s("NX");
        if (xx) a.s("XX");
        if (ch) a.s("CH");
        for (GeoValue<V> m : members) {
            a.d(m.longitude()).d(m.latitude()).v(m.member());
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code GEOADD key EX|PX ttl [NX|XX] [CH] longitude latitude member ...}: like the overload without {@code ttl}, and a geo
     * key this call creates gets a time to live. An existing key keeps its deadline.
     *
     * <pre>{@code
     * client.geoadd("drivers", false, false, false, Duration.ofMinutes(5), GeoValue.of(13.36, 38.11, "d1"));
     * }</pre>
     *
     * @param key the geo key
     * @param nx {@code true} to add only new members (existing ones are not moved)
     * @param xx {@code true} to update only existing members (new ones are not added)
     * @param ch {@code true} to count members whose position changed as well as new ones
     * @param ttl time to live of a newly created key: whole seconds are sent as {@code EX}, anything finer as {@code PX}
     * @param members the places; longitude -180 to 180, latitude -85.05112878 to 85.05112878
     * @return how many members were new, or with {@code ch} new plus changed
     * @throws IllegalArgumentException when {@code ttl} is null, zero, negative or too large
     * @throws RuvioServerException {@code ERR} for an out-of-range coordinate or {@code nx} together with {@code xx}; {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<Long> geoadd(K key, boolean nx, boolean xx, boolean ch, Duration ttl, GeoValue<V>... members) {
        Args a = args().w(Cmd.GEOADD).k(key).ttl(ttl);
        if (nx) a.s("NX");
        if (xx) a.s("XX");
        if (ch) a.s("CH");
        for (GeoValue<V> m : members) {
            a.d(m.longitude()).d(m.latitude()).v(m.member());
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code GEODIST key member1 member2}: the distance between two stored places, in meters.
     *
     * <pre>{@code
     * Double meters = client.geodist("cities", "Palermo", "Catania");
     * }</pre>
     *
     * @param key the geo key
     * @param member1 the first place
     * @param member2 the second place
     * @return the distance in meters, or {@code null} when either member or the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> geodist(K key, V member1, V member2) {
        return send(args().w(Cmd.GEODIST).k(key).v(member1).v(member2), Reply.DOUBLE);
    }

    /**
     * Sends {@code GEODIST key member1 member2 unit}.
     *
     * @param key the geo key
     * @param member1 the first place
     * @param member2 the second place
     * @param unit a {@link GeoUnit}: {@code M} meters, {@code KM} kilometers, {@code FT} feet, {@code MI} miles
     * @return the distance in {@code unit}, or {@code null} when either member or the key does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<Double> geodist(K key, V member1, V member2, GeoUnit unit) {
        return send(args().w(Cmd.GEODIST).k(key).v(member1).v(member2).w(unit.fragment()), Reply.DOUBLE);
    }

    /**
     * Sends {@code GEOHASH key member [member ...]}: the standard 11-character geohash text of each place.
     *
     * @param key the geo key
     * @param members one or more places
     * @return one string per member in order; {@code null} for a member that does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<List<String>> geohash(K key, V... members) {
        return kvs(Cmd.GEOHASH, key, Reply.STRING_LIST, members);
    }

    /**
     * Sends {@code GEOPOS key member [member ...]}: the stored position of each place. Values are the position after rounding to the 52-bit geohash, so they can differ slightly from the input.
     *
     * @param key the geo key
     * @param members one or more places
     * @return one entry per member in order (longitude, latitude in degrees); {@code null} for a member that does not exist
     * @throws RuvioServerException {@code WRONGTYPE} for a non-sorted-set
     */
    @SafeVarargs
    public final CompletableFuture<List<GeoCoordinates>> geopos(K key, V... members) {
        return kvs(Cmd.GEOPOS, key, GEOPOS, members);
    }

    /**
     * Sends {@code GEOSEARCH key FROMMEMBER|FROMLONLAT ... BYRADIUS|BYBOX ... [ASC|DESC] [COUNT n [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]}: finds places inside a circle or a box.
     * Build the options with {@link GeoSearchArgs}: one origin ({@code fromMember} or {@code fromLonLat}) and one shape ({@code byRadius} or {@code byBox}) are required.
     *
     * <pre>{@code
     * List<GeoResult<String>> near = client.geosearch("cities",
     *         GeoSearchArgs.fromLonLat(15, 37).byRadius(200, GeoUnit.KM).order(GeoOrder.ASC).count(5).withDist());
     * for (GeoResult<String> r : near) {
     *     System.out.println(r.member() + " " + r.distance() + " km");
     * }
     * }</pre>
     *
     * @param key the geo key
     * @param search origin, shape, order (a {@link GeoOrder}, via {@code order(GeoOrder)}, {@code asc()} or {@code desc()}), count and which extra fields to return
     * @return the matches; {@code distance()}, {@code hash()} and {@code coordinates()} are filled only for the {@code WITH*} flags you set; empty when the key does not exist
     * @throws RuvioServerException {@code ERR} when the origin or the shape is missing, {@code ANY} has no {@code COUNT}, {@code COUNT} is zero, or a size is negative; also when {@code fromMember} names a member that is not in the set; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<GeoResult<V>>> geosearch(K key, GeoSearchArgs search) {
        Args a = args().w(Cmd.GEOSEARCH).k(key);
        search.writeSearch(a, true);
        return send(a, r(GEO_RESULTS));
    }

    /**
     * Sends {@code GEOSEARCHSTORE destination source ... [STOREDIST]}: like {@link #geosearch} but writes the matches into the sorted set {@code destination} (replacing it). The {@code WITH*} flags are not allowed here. In a cluster both keys must hash to one slot.
     *
     * @param destination the key that receives the matches
     * @param source the geo key to search
     * @param search origin, shape, order and count (no {@code WITH*} flags)
     * @param storeDist {@code false} stores each match with its geohash as score (a geo set); {@code true} stores the distance from the origin, in the shape's unit, as score
     * @return the number of members stored
     * @throws RuvioServerException {@code ERR} for incomplete options or {@code WITH*} flags; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Long> geosearchstore(K destination, K source, GeoSearchArgs search, boolean storeDist) {
        Args a = args().w(Cmd.GEOSEARCHSTORE).k(destination).k(source);
        search.writeSearch(a, false);
        if (storeDist) {
            a.s("STOREDIST");
        }
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code GEORADIUS key longitude latitude radius unit}: places within a circle around a point. The newer {@link #geosearch} replaces it.
     *
     * @param key the geo key
     * @param longitude centre longitude, -180 to 180
     * @param latitude centre latitude, -85.05112878 to 85.05112878
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @return the member names, unsorted; empty when none or the key does not exist
     * @throws RuvioServerException {@code ERR radius must be non-negative}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<GeoResult<V>>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit) {
        return georadius(key, longitude, latitude, radius, unit, GeoSearchArgs.options());
    }

    /**
     * Sends {@code GEORADIUS key longitude latitude radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n [ANY]] [ASC|DESC]}.
     *
     * @param key the geo key
     * @param longitude centre longitude, -180 to 180
     * @param latitude centre latitude, -85.05112878 to 85.05112878
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @param options order (a {@link GeoOrder}), count and {@code WITH*} flags, from {@link GeoSearchArgs#options()}; the origin and shape in it are ignored
     * @return the matches with the extra fields you asked for
     * @throws RuvioServerException {@code ERR} for a negative radius or {@code ANY} without {@code COUNT}; {@code WRONGTYPE} for a non-sorted-set
     */
    public CompletableFuture<List<GeoResult<V>>> georadius(K key, double longitude, double latitude, double radius, GeoUnit unit, GeoSearchArgs options) {
        Args a = args().w(Cmd.GEORADIUS).k(key).d(longitude).d(latitude).d(radius).w(unit.fragment());
        options.writeTail(a, true);
        return send(a, r(GEO_RESULTS));
    }

    /**
     * Sends {@code GEORADIUS key longitude latitude radius unit STORE|STOREDIST destination}: stores the matches in {@code destination}. In a cluster both keys must hash to one slot.
     *
     * @param key the geo key to search
     * @param longitude centre longitude
     * @param latitude centre latitude
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @param destination the key that receives the matches (replaced)
     * @param storeDist {@code false} for {@code STORE} (geohash scores), {@code true} for {@code STOREDIST} (distance scores)
     * @return the number of members stored
     * @throws RuvioServerException {@code ERR radius must be non-negative}; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Long> georadiusStore(K key, double longitude, double latitude, double radius, GeoUnit unit, K destination, boolean storeDist) {
        Args a = args().w(Cmd.GEORADIUS).k(key).d(longitude).d(latitude).d(radius).w(unit.fragment());
        a.s(storeDist ? "STOREDIST" : "STORE").k(destination);
        return send(a, Reply.LONG);
    }

    /**
     * Sends {@code GEORADIUSBYMEMBER key member radius unit}: places within a circle around a stored member. The newer {@link #geosearch} replaces it.
     *
     * @param key the geo key
     * @param member the stored place used as the centre
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @return the matches (the centre member included), unsorted
     * @throws RuvioServerException {@code ERR} when {@code member} is not in the set or the radius is negative; {@code WRONGTYPE}
     */
    public CompletableFuture<List<GeoResult<V>>> georadiusbymember(K key, V member, double radius, GeoUnit unit) {
        return georadiusbymember(key, member, radius, unit, GeoSearchArgs.options());
    }

    /**
     * Sends {@code GEORADIUSBYMEMBER key member radius unit [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT n [ANY]] [ASC|DESC]}.
     *
     * @param key the geo key
     * @param member the stored place used as the centre
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @param options order, count and {@code WITH*} flags, from {@link GeoSearchArgs#options()}
     * @return the matches with the extra fields you asked for
     * @throws RuvioServerException {@code ERR} when {@code member} is not in the set, the radius is negative or {@code ANY} has no {@code COUNT}; {@code WRONGTYPE}
     */
    public CompletableFuture<List<GeoResult<V>>> georadiusbymember(K key, V member, double radius, GeoUnit unit, GeoSearchArgs options) {
        Args a = args().w(Cmd.GEORADIUSBYMEMBER).k(key).v(member).d(radius).w(unit.fragment());
        options.writeTail(a, true);
        return send(a, r(GEO_RESULTS));
    }

    /**
     * Sends {@code GEORADIUSBYMEMBER key member radius unit STORE|STOREDIST destination}. In a cluster both keys must hash to one slot.
     *
     * @param key the geo key to search
     * @param member the stored place used as the centre
     * @param radius the radius, zero or greater, in {@code unit}
     * @param unit {@code M}, {@code KM}, {@code FT} or {@code MI}
     * @param destination the key that receives the matches (replaced)
     * @param storeDist {@code false} for {@code STORE} (geohash scores), {@code true} for {@code STOREDIST} (distance scores)
     * @return the number of members stored
     * @throws RuvioServerException {@code ERR} when {@code member} is not in the set or the radius is negative; {@code WRONGTYPE}; {@code CROSSSLOT} in a cluster
     */
    public CompletableFuture<Long> georadiusbymemberStore(K key, V member, double radius, GeoUnit unit, K destination, boolean storeDist) {
        Args a = args().w(Cmd.GEORADIUSBYMEMBER).k(key).v(member).d(radius).w(unit.fragment());
        a.s(storeDist ? "STOREDIST" : "STORE").k(destination);
        return send(a, Reply.LONG);
    }
}
