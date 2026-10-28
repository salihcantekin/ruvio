// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** A scripted RESP server for unit tests: records every command and answers with whatever the handler returns. */
final class FakeNode implements AutoCloseable {
    /** Returned by a handler to drop the connection without answering. */
    static final String CLOSE = "\0close";
    /** Returned by a handler to send nothing (the command stays unanswered). */
    static final String SILENT = "\0silent";

    final ServerSocket server;
    final int port;
    final List<List<String>> received = Collections.synchronizedList(new ArrayList<>());
    final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
    volatile Function<List<String>, String> handler;
    private volatile boolean closed;

    FakeNode(Function<List<String>, String> handler) throws IOException {
        this.handler = handler;
        this.server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
        this.port = server.getLocalPort();
        Thread t = new Thread(this::acceptLoop, "fake-node-" + port);
        t.setDaemon(true);
        t.start();
    }

    String address() {
        return "127.0.0.1:" + port;
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                // A connection accepted while close() runs must not outlive it: close() drops
                // the sockets it finds under this lock, and one added after that sees closed.
                synchronized (sockets) {
                    if (closed) {
                        s.close();
                        return;
                    }
                    sockets.add(s);
                }
                Thread t = new Thread(() -> serve(s), "fake-conn-" + port);
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Socket s) {
        try (InputStream in = new BufferedInputStream(s.getInputStream()); OutputStream out = s.getOutputStream()) {
            for (;;) {
                List<String> args = readCommand(in);
                if (args == null) {
                    return;
                }
                received.add(args);
                String reply = handler.apply(args);
                if (CLOSE.equals(reply)) {
                    s.close();
                    return;
                }
                if (SILENT.equals(reply)) {
                    continue;
                }
                out.write(reply.getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException ignored) {
            // client went away
        }
    }

    private static List<String> readCommand(InputStream in) throws IOException {
        String head = line(in);
        if (head == null) {
            return null;
        }
        int n = Integer.parseInt(head.substring(1));
        List<String> args = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int len = Integer.parseInt(line(in).substring(1));
            byte[] b = in.readNBytes(len);
            in.readNBytes(2);
            args.add(new String(b, StandardCharsets.UTF_8));
        }
        return args;
    }

    private static String line(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) >= 0) {
            if (c == '\r') {
                in.read();
                return sb.toString();
            }
            sb.append((char) c);
        }
        return null;
    }

    /** Commands received so far whose first word equals {@code name} (case-insensitive). */
    List<List<String>> commands(String name) {
        List<List<String>> out = new ArrayList<>();
        synchronized (received) {
            for (List<String> c : received) {
                if (c.get(0).equalsIgnoreCase(name)) {
                    out.add(c);
                }
            }
        }
        return out;
    }

    /**
     * The number of accepted sockets once it reaches {@code count}, or after two seconds. The
     * client's connect returns before this node's accept loop records the socket, so a count
     * read straight after a connect can still be one short.
     */
    int awaitSockets(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (sockets.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        return sockets.size();
    }

    void dropConnections() {
        synchronized (sockets) {
            for (Socket s : sockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // already closed
                }
            }
            sockets.clear();
        }
    }

    @Override
    public void close() {
        synchronized (sockets) {
            closed = true;
        }
        try {
            server.close();
        } catch (IOException ignored) {
            // closing
        }
        dropConnections();
    }

    // ---- reply builders ---------------------------------------------------------------------

    static String ok() {
        return "+OK\r\n";
    }

    static String integer(long v) {
        return ":" + v + "\r\n";
    }

    static String bulk(String s) {
        return s == null ? "$-1\r\n" : "$" + s.getBytes(StandardCharsets.UTF_8).length + "\r\n" + s + "\r\n";
    }

    static String error(String message) {
        return "-" + message + "\r\n";
    }

    static String array(String... rawItems) {
        StringBuilder sb = new StringBuilder("*").append(rawItems.length).append("\r\n");
        for (String i : rawItems) {
            sb.append(i);
        }
        return sb.toString();
    }

    /** CLUSTER SLOTS reply for ranges {start, end, port}. */
    static String slots(int[]... ranges) {
        String[] items = new String[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            items[i] = array(integer(ranges[i][0]), integer(ranges[i][1]),
                    array(bulk("127.0.0.1"), integer(ranges[i][2]), bulk("node-" + ranges[i][2])));
        }
        return array(items);
    }
}
