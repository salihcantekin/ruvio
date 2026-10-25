// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import static org.junit.jupiter.api.Assertions.*;

import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Reply parser unit tests: every reply type, nil forms, attributes, partial input and malformed input. */
class RespReaderTest {
    private static RespReader reader(String s) {
        byte[] b = s.getBytes(StandardCharsets.ISO_8859_1);
        RespReader r = new RespReader(b);
        r.wrap(b, 0, b.length);
        return r;
    }

    @Test
    void oversizedHeadersAreRefusedBeforeAnythingIsAllocated() {
        assertThrows(RuvioException.class, () -> reader("$67108865\r\n").readString());
        assertThrows(RuvioException.class, () -> reader("*1048577\r\n").arrayLength());
        assertThrows(RuvioException.class, () -> reader("%1048577\r\n").arrayLength());
        assertThrows(RuvioException.class, () -> reader("*1048577\r\n").readObject());
        assertEquals(1048576, reader("*1048576\r\n").arrayLength());
    }

    @Test
    void stringLikeReplies() {
        assertEquals("hello", reader("$5\r\nhello\r\n").readString());
        assertEquals("", reader("$0\r\n\r\n").readString());
        assertEquals("OK", reader("+OK\r\n").readString());
        assertEquals("42", reader(":42\r\n").readString());
        assertEquals("3.5", reader(",3.5\r\n").readString());
        assertEquals("123456789012345678901234567890", reader("(123456789012345678901234567890\r\n").readString());
        assertEquals("abc", reader("=7\r\ntxt:abc\r\n").readString(), "verbatim drops the 4 byte format prefix");
        assertEquals("ab", reader("=2\r\nab\r\n").readString(), "a verbatim shorter than the prefix is kept whole");
        assertNull(reader("$-1\r\n").readString());
        assertNull(reader("_\r\n").readString());
        assertNull(reader("*-1\r\n").readString());
        assertArrayEquals(new byte[] {1, 0, 2}, reader("$3\r\n\u0001\u0000\u0002\r\n").readBytes());
        assertNull(reader("$-1\r\n").readBytes());
        assertEquals("é", reader("$2\r\nÃ©\r\n").readString());
        RuvioServerException e = assertThrows(RuvioServerException.class, () -> reader("-WRONGTYPE bad\r\n").readString());
        assertTrue(e.getMessage().contains("WRONGTYPE"));
        assertThrows(RuvioException.class, () -> reader("*1\r\n:1\r\n").readString());
        assertThrows(RuvioException.class, () -> reader("%0\r\n").readString());
    }

    @Test
    void integerAndBooleanReplies() {
        assertEquals(42L, reader(":42\r\n").readLongOrNull());
        assertEquals(-7L, reader(":-7\r\n").readLongOrNull());
        assertEquals(Long.MAX_VALUE, reader(":9223372036854775807\r\n").readLongOrNull());
        assertEquals(Long.MIN_VALUE, reader(":-9223372036854775808\r\n").readLongOrNull());
        assertEquals(5L, reader(":+5\r\n").readLongOrNull());
        assertEquals(1L, reader("#t\r\n").readLongOrNull());
        assertEquals(0L, reader("#f\r\n").readLongOrNull());
        assertEquals(12L, reader("$2\r\n12\r\n").readLongOrNull(), "a bulk holding digits is accepted");
        assertNull(reader("$-1\r\n").readLongOrNull());
        assertNull(reader("_\r\n").readLongOrNull());
        assertThrows(RuvioException.class, () -> reader(":\r\n").readLongOrNull());
        assertThrows(RuvioException.class, () -> reader(":12x\r\n").readLongOrNull());
        assertThrows(RuvioException.class, () -> reader("$2\r\nab\r\n").readLongOrNull());
        assertThrows(RuvioServerException.class, () -> reader("-ERR no\r\n").readLongOrNull());
    }

    @Test
    void doubles() {
        assertEquals(1.5, reader("$3\r\n1.5\r\n").readDoubleOrNull());
        assertEquals(-2.0, reader(",-2\r\n").readDoubleOrNull());
        assertEquals(7.0, reader(":7\r\n").readDoubleOrNull());
        assertEquals(Double.POSITIVE_INFINITY, reader(",inf\r\n").readDoubleOrNull());
        assertEquals(Double.NEGATIVE_INFINITY, reader(",-inf\r\n").readDoubleOrNull());
        assertTrue(Double.isNaN(reader(",nan\r\n").readDoubleOrNull()));
        assertNull(reader("_\r\n").readDoubleOrNull());
        assertNull(reader("$-1\r\n").readDoubleOrNull());
        assertThrows(NumberFormatException.class, () -> reader("$3\r\nabc\r\n").readDoubleOrNull());

        for (String s : new String[] {"0", "1", "-1", "+1", "123456789012345678", "1e3", "-1.25e-2", "9007199254740993", ".5", "inf", "+inf", "Infinity", "-Infinity", "NaN"}) {
            byte[] b = s.getBytes(StandardCharsets.US_ASCII);
            double got = RespReader.parseDouble(b, 0, b.length);
            double want = switch (s) {
                case "inf", "+inf", "Infinity" -> Double.POSITIVE_INFINITY;
                case "-Infinity" -> Double.NEGATIVE_INFINITY;
                case "NaN" -> Double.NaN;
                default -> Double.parseDouble(s);
            };
            assertEquals(want, got, s);
        }
        byte[] sign = "-".getBytes(StandardCharsets.US_ASCII);
        assertThrows(NumberFormatException.class, () -> RespReader.parseDouble(sign, 0, 1));
    }

    @Test
    void aggregateHeaders() {
        assertEquals(3, reader("*3\r\n").arrayLength());
        assertEquals(0, reader("*0\r\n").arrayLength());
        assertEquals(2, reader("~2\r\n").arrayLength());
        assertEquals(2, reader(">2\r\n").arrayLength());
        assertEquals(4, reader("%2\r\n").arrayLength(), "a map reports twice its entries");
        assertEquals(-1, reader("*-1\r\n").arrayLength());
        assertEquals(-1, reader("_\r\n").arrayLength());
        assertEquals(-1, reader("$-1\r\n").arrayLength());
        assertThrows(RuvioServerException.class, () -> reader("-ERR nope\r\n").arrayLength());
        assertThrows(RuvioException.class, () -> reader(":5\r\n").arrayLength());
        assertThrows(RuvioException.class, () -> reader("$3\r\nabc\r\n").arrayLength());
    }

    @Test
    void attributesAreSkippedBeforeTheReply() {
        assertEquals("v", reader("|1\r\n+key\r\n+val\r\n$1\r\nv\r\n").readString());
        assertEquals(9L, reader("|2\r\n+a\r\n:1\r\n+b\r\n*2\r\n:1\r\n:2\r\n:9\r\n").readLongOrNull());
        assertEquals(1, reader("|1\r\n+k\r\n+v\r\n*1\r\n:1\r\n").arrayLength());
    }

    @Test
    void incompleteInputAlwaysReportsIncomplete() {
        String[] frames = {
            "$5\r\nhel", "$5\r\nhello", "$5\r\nhello\r", "$5", "$5\r", "+OK", "+OK\r", ":12", ":12\r", "*2\r\n$1\r\na\r\n", "*2\r\n", "%1\r\n+a\r\n", "=7\r\ntxt:a", "_", "#", ",1.", "(12",
        };
        for (String f : frames) {
            assertSame(RespReader.INCOMPLETE, assertThrows(RespReader.Incomplete.class, () -> reader(f).readObject()), f);
        }
        assertThrows(RespReader.Incomplete.class, () -> reader("").peek());
        assertThrows(RespReader.Incomplete.class, () -> reader("$3\r\nab").readString());
        assertThrows(RespReader.Incomplete.class, () -> reader("=7\r\ntxt:a").readString());
        assertThrows(RespReader.Incomplete.class, () -> reader("|1\r\n+k\r\n").skip());
    }

    @Test
    void skipConsumesExactlyOneElementOfEveryType() {
        String[] elements = {
            "+OK\r\n", "-ERR x\r\n", ":5\r\n", ",1.5\r\n", "(123\r\n", "#t\r\n", "_\r\n", "$3\r\nabc\r\n", "$-1\r\n", "=5\r\ntxt:a\r\n", "!3\r\nerr\r\n",
            "*2\r\n:1\r\n*1\r\n:2\r\n", "~1\r\n+a\r\n", ">1\r\n+a\r\n", "%1\r\n+k\r\n:1\r\n", "|1\r\n+k\r\n+v\r\n:1\r\n",
        };
        for (String e : elements) {
            RespReader r = reader(e + ":99\r\n");
            r.skip();
            assertEquals(e.length(), r.pos, e);
            assertEquals(99L, r.readLongOrNull(), e);
        }
        assertThrows(RuvioException.class, () -> reader("?x\r\n").skip());
    }

    @Test
    void readObjectBuildsTheGenericTree() {
        assertArrayEquals("abc".getBytes(), (byte[]) reader("$3\r\nabc\r\n").readObject());
        assertEquals("OK", reader("+OK\r\n").readObject());
        assertEquals("123", reader("(123\r\n").readObject());
        assertEquals(5L, reader(":5\r\n").readObject());
        assertEquals(2.5, reader(",2.5\r\n").readObject());
        assertEquals(Boolean.TRUE, reader("#t\r\n").readObject());
        assertEquals(Boolean.FALSE, reader("#f\r\n").readObject());
        assertNull(reader("_\r\n").readObject());
        assertNull(reader("*-1\r\n").readObject());
        assertInstanceOf(RuvioServerException.class, reader("-ERR x\r\n").readObject());

        Object list = reader("*3\r\n:1\r\n$1\r\na\r\n*0\r\n").readObject();
        List<?> items = assertInstanceOf(List.class, list);
        assertEquals(3, items.size());
        assertEquals(1L, items.get(0));
        assertEquals(List.of(), items.get(2));

        assertInstanceOf(RespReader.Push.class, reader(">2\r\n+message\r\n+x\r\n").readObject());
        assertEquals(List.of("a"), reader("~1\r\n+a\r\n").readObject());

        Map<?, ?> map = assertInstanceOf(Map.class, reader("%2\r\n$1\r\nk\r\n:1\r\n+j\r\n$1\r\nv\r\n").readObject());
        assertEquals(1L, map.get("k"), "bulk keys become strings");
        assertArrayEquals("v".getBytes(), (byte[]) map.get("j"));

        assertEquals(7L, reader("|1\r\n+k\r\n+v\r\n:7\r\n").readObject());
        assertThrows(RuvioException.class, () -> reader("?x\r\n").readObject());
    }

    @Test
    void blobErrorsBecomeServerExceptionObjects() {
        // RESP3 blob error: !<len>\r\n<message>\r\n
        Object o = reader("!10\r\nERR broken\r\n").readObject();
        RuvioServerException e = assertInstanceOf(RuvioServerException.class, o);
        assertTrue(e.getMessage().contains("ERR broken"));

        RuvioServerException thrown = assertThrows(RuvioServerException.class, () -> reader("!10\r\nERR broken\r\n").readString());
        assertTrue(thrown.getMessage().contains("ERR broken"));
        assertThrows(RespReader.Incomplete.class, () -> reader("!10\r\nERR bro").readString());
        assertThrows(RuvioException.class, () -> reader("!-1\r\n").readString());
    }

    @Test
    void tryNullOnlyConsumesNils() {
        RespReader r = reader("$-1\r\n$1\r\na\r\n_\r\n*-1\r\n");
        assertTrue(r.tryNull());
        assertFalse(r.tryNull());
        assertEquals("a", r.readString());
        assertTrue(r.tryNull());
        assertTrue(r.tryNull());
        assertThrows(RespReader.Incomplete.class, r::tryNull);
    }

    @Test
    void lineReadersAndErrors() {
        assertEquals("WRONGTYPE wrong kind", reader("-WRONGTYPE wrong kind\r\n").readError().getMessage());
        assertEquals("hello world", reader("+hello world\r\n").readLine());
        assertTrue(reader(":5\r\n").unexpected("array").getMessage().contains("':'"));
        assertTrue(reader("").unexpected("string").getMessage().contains("wanted string"));
    }

    @Test
    void wrapReusesTheReaderForTheNextBuffer() {
        RespReader r = reader("+first\r\n");
        assertEquals("first", r.readLine());
        byte[] next = "+second\r\n".getBytes(StandardCharsets.US_ASCII);
        r.wrap(next, 0, next.length);
        assertEquals("second", r.readLine());
        byte[] both = "xx+a\r\n+b\r\n".getBytes(StandardCharsets.US_ASCII);
        r.wrap(both, 2, both.length);
        assertEquals("a", r.readLine());
        assertEquals("b", r.readLine());
    }
}
