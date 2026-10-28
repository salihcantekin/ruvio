// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

/** One cluster node: its address, the lazily opened shared connection and its transaction pool. */
public final class ClusterNode {
    /** The host name or address. */
    public final String host;
    /** The TCP port. */
    public final int port;
    /** {@code host:port}. */
    public final String address;
    /** The shared connection to this node, opened on first use. */
    public final Link link;
    /** The pool of transaction connections to this node. */
    public final TxPool txPool;
    /** Position in the router's node list (0 for the seed); lets a pipeline find the node's group by index. */
    public int index;
    /** A primary's replica that serves its reads, picked when the slot map loads; null when none or unusable. */
    public volatile ClusterNode reader;
    /** A replica's primary as of the last slot map load. */
    public volatile ClusterNode primary;

    /**
     * Creates a node.
     *
     * @param host the host name or address
     * @param port the TCP port
     * @param link the shared connection to the node
     */
    public ClusterNode(String host, int port, Link link) {
        this.host = host;
        this.port = port;
        this.address = address(host, port);
        this.link = link;
        this.txPool = new TxPool(link);
    }

    /**
     * Formats an address.
     *
     * @param host the host
     * @param port the port
     * @return {@code host:port}
     */
    public static String address(String host, int port) {
        return host + ":" + port;
    }

    /** Closes the node's connections. */
    public void close() {
        txPool.close();
        link.close();
    }

    @Override
    public String toString() {
        return address;
    }
}
