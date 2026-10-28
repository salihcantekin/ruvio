// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;

import java.util.ArrayDeque;

/** A few idle dedicated connections for transactions and dedicated clients, reused after use. */
public final class TxPool implements AutoCloseable {
    private static final int MAX_IDLE = 4;

    private final Link link;
    private final ArrayDeque<Connection> idle = new ArrayDeque<>();
    private boolean closed;

    /**
     * Creates a pool.
     *
     * @param link the link used to open extra connections
     */
    public TxPool(Link link) {
        this.link = link;
        if (link.follows()) {
            link.onMove(this::dropIdle);
        }
    }

    /** A moved link: the idle sockets point to the old node. */
    private void dropIdle() {
        synchronized (this) {
            for (Connection c : idle) {
                c.close();
            }
            idle.clear();
        }
    }

    /**
     * A transaction's connection was refused with {@code READONLY}: the link looks for the primary, so the next
     * transaction is opened there. Does nothing for a link that does not follow.
     *
     * @param c the connection that answered
     */
    public void refused(Connection c) {
        if (link.follows() && c != null) {
            link.followAfterRefusal(c);
        }
    }

    /**
     * Takes an idle connection or opens a new one.
     *
     * @return a connection that the caller owns until {@link #giveBack}
     * @throws RuvioConnectionException when the client is closed
     */
    public Connection borrow() {
        synchronized (this) {
            if (closed) {
                throw new RuvioConnectionException("client is closed");
            }
            while (!idle.isEmpty()) {
                Connection c = idle.pollFirst();
                if (c.isOpen()) {
                    return c;
                }
            }
        }
        return link.openExtra("tx");
    }

    /**
     * Returns a connection; it is kept for reuse only when healthy, idle and the pool has room, otherwise closed.
     *
     * @param c the connection
     * @param healthy whether it is in a clean state
     */
    public void giveBack(Connection c, boolean healthy) {
        if (healthy && c.isOpen() && c.inFlight() == 0) {
            synchronized (this) {
                if (!closed && idle.size() < MAX_IDLE) {
                    idle.addLast(c);
                    return;
                }
            }
        }
        c.close();
    }

    /** Closes every idle connection and refuses further borrows. */
    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            for (Connection c : idle) {
                c.close();
            }
            idle.clear();
        }
    }
}
