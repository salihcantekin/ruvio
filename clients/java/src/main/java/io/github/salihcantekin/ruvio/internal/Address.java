// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import java.util.ArrayList;
import java.util.List;

/** A parsed {@code host:port} address. */
public final class Address {
    /** The host name or address (without the brackets of an IPv6 literal). */
    public final String host;
    /** The TCP port. */
    public final int port;

    /**
     * Creates an address.
     *
     * @param host the host name or address
     * @param port the TCP port
     */
    public Address(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Parses {@code host:port}; an IPv6 address is written {@code [::1]:6379}.
     *
     * @param text the address
     * @return the parsed address
     * @throws IllegalArgumentException when the text is not {@code host:port}, the host is empty or the port is not 1 to 65535
     */
    public static Address parse(String text) {
        if (text == null) {
            throw new IllegalArgumentException("seed address is null");
        }
        String host;
        String portText;
        if (text.startsWith("[")) {
            int close = text.indexOf(']');
            if (close < 0 || close + 1 >= text.length() || text.charAt(close + 1) != ':') {
                throw new IllegalArgumentException("seed address '" + text + "' is not [host]:port");
            }
            host = text.substring(1, close);
            portText = text.substring(close + 2);
        } else {
            int colon = text.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException("seed address '" + text + "' is not host:port");
            }
            host = text.substring(0, colon);
            portText = text.substring(colon + 1);
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("seed address '" + text + "' has an empty host");
        }
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("seed address '" + text + "' has a bad port", e);
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("seed address '" + text + "' has a port outside 1 to 65535");
        }
        return new Address(host, port);
    }

    /**
     * Parses a list of {@code host:port} strings.
     *
     * @param texts the addresses
     * @return the parsed addresses, in order
     * @throws IllegalArgumentException when one of them is not valid
     */
    public static List<Address> parseAll(List<String> texts) {
        List<Address> out = new ArrayList<>(texts.size());
        for (String t : texts) {
            out.add(parse(t));
        }
        return out;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Address && ((Address) o).port == port && ((Address) o).host.equals(host);
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString() {
        return (host.indexOf(':') >= 0 ? "[" + host + "]" : host) + ":" + port;
    }
}
