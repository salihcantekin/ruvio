// SPDX-License-Identifier: MIT
package io.github.salihcantekin.ruvio.internal;

import io.github.salihcantekin.ruvio.RuvioConnectionException;
import io.github.salihcantekin.ruvio.RuvioException;
import io.github.salihcantekin.ruvio.RuvioServerException;
import io.github.salihcantekin.ruvio.RuvioTimeoutException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * One multiplexed socket shared by every caller.
 *
 * <p>Write side: callers encode their frame straight into the shared output buffer under a short
 * lock and push their {@link Request} onto a FIFO ring in the same critical section, so byte order
 * and reply order always match. When nothing else is in flight the caller writes the buffer itself
 * (no thread hand-off for serial use); otherwise the writer thread drains everything queued in one
 * {@code write} (no timers, no batch-fill wait), swapping between two buffers.
 *
 * <p>Read side: the reader thread reads into one reusable buffer, decodes each reply in place with
 * the request's decoder and completes the futures in FIFO order.
 */
public class Connection implements AutoCloseable {
    private static final int READ_BUFFER = 64 * 1024;
    private static final int WRITE_BUFFER = 16 * 1024;
    private static final int SHRINK_ABOVE = 1024 * 1024;

    /** The server host. */
    public final String host;
    /** The server port. */
    public final int port;
    /** Set on the shared connection of a link that follows a failover: the link, and the writes kept until their reply. */
    private Link followLink;
    private static final int KEEP_BYTES = 128 * 1024;
    /** The ring of kept bytes, per ring slot where the command sits in it ({@code -1}: in {@link #spill}) and its length (0: none kept). */
    private byte[] kept;
    private int[] keptOff;
    private int[] keptLen;
    private byte[][] spill;
    /** Bytes ever put in the ring (padding included; lock held) and bytes released by the reader, in absolute positions. */
    private long keptWritten;
    private volatile long keptReleased;
    private final SocketChannel channel;
    /** With TLS, the streams of the TLS socket the reader and writer use instead of {@link #channel}; all null otherwise. */
    private final InputStream tlsIn;
    private final OutputStream tlsOut;
    private final SSLSocket tlsSocket;
    private final ReentrantLock lock = new ReentrantLock();
    private final Thread writer;
    private final Thread reader;
    private volatile boolean writerSignal;

    // guarded by lock
    private RespOut out = new RespOut(WRITE_BUFFER);
    private RespOut spare = new RespOut(WRITE_BUFFER);
    private boolean writing;
    private int mark;

    // FIFO of pending replies: producers append under lock, the reader consumes.
    private final Request<?>[] ring;
    private final int mask;
    private volatile long tail;
    private volatile long head;

    private volatile long timeoutMillis;
    private volatile Thread watchdog;

    private volatile Throwable closed;
    private volatile Consumer<Object> pushHandler;
    private ByteBuffer writeView;
    private byte[] writeViewArray;

    /**
     * Opens the socket and starts the reader and writer threads.
     *
     * @param host the server host
     * @param port the server port
     * @param connectTimeoutMs the connect timeout in milliseconds
     * @param maxInFlight the most replies outstanding at once (rounded up to a power of two)
     * @param name the name used for the threads
     * @throws IOException when the socket cannot be opened
     */
    @SuppressWarnings("this-escape") // the writer and reader threads start here on purpose
    public Connection(String host, int port, int connectTimeoutMs, int maxInFlight, String name) throws IOException {
        this(host, port, connectTimeoutMs, maxInFlight, name, null, null);
    }

    /**
     * Opens the socket, wrapped in TLS when {@code tls} is given, and starts the reader and writer threads. The TLS
     * handshake is part of the connect, and the server certificate must carry {@code serverName} (the host when null).
     *
     * @param host the server host
     * @param port the server port
     * @param connectTimeoutMs the connect timeout in milliseconds (the handshake gets the same)
     * @param maxInFlight the most replies outstanding at once (rounded up to a power of two)
     * @param name the name used for the threads
     * @param tls the TLS context, or null for plain TCP
     * @param serverName the name the certificate must carry, or null for {@code host}
     * @throws IOException when the socket cannot be opened or the handshake fails
     */
    @SuppressWarnings("this-escape") // the writer and reader threads start here on purpose
    public Connection(String host, int port, int connectTimeoutMs, int maxInFlight, String name, SSLContext tls,
            String serverName) throws IOException {
        this.host = host;
        this.port = port;
        int cap = Integer.highestOneBit(Math.max(16, maxInFlight - 1)) << 1;
        this.ring = new Request<?>[cap];
        this.mask = cap - 1;
        SocketChannel ch = SocketChannel.open();
        SSLSocket secure = null;
        try {
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            ch.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            ch.socket().connect(new InetSocketAddress(host, port), connectTimeoutMs);
            if (tls != null) {
                secure = handshake(ch.socket(), tls, serverName == null ? host : serverName, port, connectTimeoutMs);
            }
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
        this.channel = ch;
        if (secure != null) {
            this.tlsIn = secure.getInputStream();
            this.tlsOut = secure.getOutputStream();
            this.tlsSocket = secure;
        } else {
            this.tlsIn = null;
            this.tlsOut = null;
            this.tlsSocket = null;
        }
        out.owner = this;
        spare.owner = this;
        writer = new Thread(this::writeLoop, name + "-writer");
        writer.setDaemon(true);
        reader = new Thread(this::readLoop, name + "-reader");
        reader.setDaemon(true);
        writer.start();
        reader.start();
    }

    /** Wraps the connected socket in TLS and finishes the handshake, with the host name checked against {@code name}. */
    private static SSLSocket handshake(Socket plain, SSLContext tls, String name, int port, int timeoutMs) throws IOException {
        SSLSocket socket = (SSLSocket) tls.getSocketFactory().createSocket(plain, name, port, true);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        if (!isIpAddress(name)) {
            parameters.setServerNames(List.of(new SNIHostName(name)));
        }
        socket.setSSLParameters(parameters);
        socket.setSoTimeout(timeoutMs);
        socket.startHandshake();
        socket.setSoTimeout(0);
        return socket;
    }

    private static boolean isIpAddress(String name) {
        return name.indexOf(':') >= 0 || (!name.isEmpty() && name.chars().allMatch(c -> c == '.' || (c >= '0' && c <= '9')));
    }

    /**
     * Sets the receiver of RESP3 push frames; it runs on the reader thread.
     *
     * @param handler the receiver, or null
     */
    public void setPushHandler(Consumer<Object> handler) {
        this.pushHandler = handler;
    }

    /**
     * Turns on the command timeout for futures handed to callers: each request gets a deadline when it is
     * queued (the timeout plus a blocking command's own wait, none for one that waits forever), and one
     * daemon thread per connection fails overdue requests with {@link RuvioException}. The late reply is
     * still read and dropped, so the connection stays in use. Call once, before the first command.
     *
     * @param millis the command timeout in milliseconds; 0 or less leaves it off
     */
    public void setCommandTimeout(long millis) {
        if (millis <= 0 || watchdog != null || closed != null) {
            return;
        }
        timeoutMillis = millis;
        long period = Math.max(10, Math.min(1000, millis / 4)) * 1_000_000L;
        Thread t = new Thread(() -> watchLoop(millis, period), writer.getName().replace("-writer", "-watchdog"));
        t.setDaemon(true);
        watchdog = t;
        t.start();
    }

    private void watchLoop(long millis, long period) {
        while (closed == null) {
            LockSupport.parkNanos(this, period);
            long now = System.nanoTime();
            long t = tail; // the ring slots below t are published by this volatile read
            for (long i = head; i < t; i++) {
                Request<?> q = ring[(int) (i & mask)];
                if (q != null && q.deadline != 0 && now - q.deadline >= 0) {
                    q.deadline = 0;
                    q.fail(new RuvioTimeoutException("no reply within " + millis + " ms (command timeout)"));
                }
            }
        }
    }

    /**
     * Tells whether the connection is still usable.
     *
     * @return {@code true} until it fails or is closed
     */
    public boolean isOpen() {
        return closed == null;
    }

    /**
     * Returns how many requests wait for a reply.
     *
     * @return the count
     */
    public int inFlight() {
        return (int) (tail - head);
    }

    // ---- write side -----------------------------------------------------------------------

    /**
     * Locks the output buffer for one frame, waiting while the ring of outstanding requests is full. Must be followed by {@link #end} or {@link #abort}.
     *
     * @return the buffer to encode into
     * @throws RuvioConnectionException when the connection is closed
     */
    public RespOut begin() {
        lock.lock();
        for (;;) {
            if (closed != null) {
                lock.unlock();
                throw closedException();
            }
            if (tail - head < ring.length) {
                mark = out.pos;
                return out;
            }
            // Ring full: let the writer and reader make progress without holding the lock.
            lock.unlock();
            signalWriter();
            LockSupport.parkNanos(20_000);
            lock.lock();
        }
    }

    /**
     * Makes this connection keep each write sent with {@link #endKeeping} until its reply arrives, so that a
     * {@code READONLY} refusal can be sent again by {@code link}. The bytes are copied into a fixed ring of bytes that
     * is freed in reply order, so the healthy path allocates nothing; a write that does not fit (very many writes in
     * flight) gets its own copy. Call before the first command.
     *
     * @param link the link to tell about a refusal
     */
    void keepWrites(Link link) {
        kept = new byte[KEEP_BYTES];
        keptOff = new int[ring.length];
        keptLen = new int[ring.length];
        followLink = link;
    }

    /**
     * Like {@link #end}, and keeps the encoded command (unless it is a read, which a replica never refuses) for a
     * {@code READONLY} reply.
     *
     * @param request the request that will receive the reply
     */
    public void endKeeping(Request<?> request) {
        end(request, true);
    }

    /** Keeps the command encoded at {@code out.buf[mark, out.pos)} for ring slot {@code slot}. Lock held. */
    private void keep(int slot) {
        byte[] b = out.buf;
        if (Link.isRead(b, mark)) {
            keptLen[slot] = 0;
            return;
        }
        int n = out.pos - mark;
        byte[] arena = kept;
        int size = arena.length;
        long w = keptWritten;
        int o = (int) (w % size);
        if (o + n > size) {
            w += size - o; // the command does not fit before the end: start again at the front
            o = 0;
        }
        if (n <= size && w + n - keptReleased <= size) {
            System.arraycopy(b, mark, arena, o, n);
            keptWritten = w + n;
            keptOff[slot] = o;
        } else {
            if (spill == null) {
                spill = new byte[ring.length][];
            }
            spill[slot] = java.util.Arrays.copyOfRange(b, mark, mark + n);
            keptOff[slot] = -1;
        }
        keptLen[slot] = n;
    }

    /** Reader thread: the request in ring slot {@code slot} was answered; its kept bytes may be overwritten. */
    private void release(int slot) {
        int n = keptLen[slot];
        if (n == 0) {
            return;
        }
        keptLen[slot] = 0;
        int o = keptOff[slot];
        if (o < 0) {
            spill[slot] = null;
            return;
        }
        long r = keptReleased;
        long start = r - r % kept.length + o;
        if (start < r) {
            start += kept.length;
        }
        keptReleased = start + n;
    }

    /** Drops the partly encoded frame and releases the lock. */
    public void abort() {
        out.pos = mark;
        lock.unlock();
    }

    /**
     * Enqueues {@code request} for the frame just encoded and releases the lock.
     *
     * @param request the request that will receive the reply
     */
    public void end(Request<?> request) {
        end(request, false);
    }

    private void end(Request<?> request, boolean keep) {
        long t = tail;
        if (kept != null) {
            if (keep) {
                keep((int) (t & mask));
            } else {
                keptLen[(int) (t & mask)] = 0;
            }
        }
        boolean serial = t == head;
        long tm = timeoutMillis;
        if (tm > 0) {
            request.arm(tm);
        }
        ring[(int) (t & mask)] = request;
        tail = t + 1;
        submitLocked(serial);
    }

    /**
     * Enqueues several requests whose frames were appended in order (pipelines).
     *
     * @param requests the requests, in frame order
     * @param count how many entries of {@code requests} to use
     * @throws RuvioException when {@code count} exceeds the ring size
     */
    public void endMany(Request<?>[] requests, int count) {
        if (tail - head + count > ring.length) {
            // Too many for the ring at once: fail fast rather than deadlock.
            abort();
            throw new RuvioException("pipeline of " + count + " exceeds maxInFlight " + ring.length);
        }
        long t = tail;
        boolean serial = t == head;
        long tm = timeoutMillis;
        for (int i = 0; i < count; i++) {
            if (tm > 0) {
                requests[i].arm(tm);
            }
            ring[(int) ((t + i) & mask)] = requests[i];
            if (kept != null) {
                keptLen[(int) ((t + i) & mask)] = 0;
            }
        }
        tail = t + count;
        submitLocked(serial);
    }

    /**
     * Returns the size of the ring of outstanding requests.
     *
     * @return the most replies that can be outstanding
     */
    public int capacity() {
        return ring.length;
    }

    private void submitLocked(boolean serial) {
        if (writing) {
            lock.unlock(); // the active writer picks this frame up
            return;
        }
        writing = true;
        if (!serial) {
            lock.unlock();
            signalWriter();
            return;
        }
        RespOut b = out;
        out = spare;
        spare = null;
        lock.unlock();
        // Nothing else in flight: write on the caller's thread, no hand-off.
        if (writeBuffer(b)) {
            afterInlineWrite(b);
        }
    }

    private void afterInlineWrite(RespOut b) {
        recycle(b);
        lock.lock();
        spare = b;
        if (out.pos > 0) {
            lock.unlock();
            signalWriter(); // writing stays true; ownership moves to the writer thread
        } else {
            writing = false;
            lock.unlock();
        }
    }

    private static void recycle(RespOut b) {
        b.reset();
        if (b.buf.length > SHRINK_ABOVE) {
            b.buf = new byte[WRITE_BUFFER];
        }
    }

    private void signalWriter() {
        writerSignal = true;
        LockSupport.unpark(writer);
    }

    private void writeLoop() {
        while (closed == null) {
            while (!writerSignal) {
                LockSupport.park(this);
                if (closed != null) {
                    return;
                }
            }
            writerSignal = false;
            for (;;) {
                lock.lock();
                if (out.pos == 0) {
                    writing = false;
                    lock.unlock();
                    break;
                }
                writing = true;
                RespOut b = out;
                out = spare;
                spare = null;
                lock.unlock();
                if (!writeBuffer(b)) {
                    return;
                }
                recycle(b);
                lock.lock();
                spare = b;
                lock.unlock();
            }
        }
    }

    /** Writes the whole buffer. Returns false (and fails the connection) on an I/O error. */
    private boolean writeBuffer(RespOut b) {
        try {
            if (tlsOut != null) {
                tlsOut.write(b.buf, 0, b.pos);
                tlsOut.flush();
                return true;
            }
            ByteBuffer bb = view(b.buf);
            bb.limit(b.pos).position(0);
            while (bb.hasRemaining()) {
                channel.write(bb);
            }
            return true;
        } catch (IOException | RuntimeException e) {
            fail(new RuvioConnectionException("write failed: " + e.getMessage(), e));
            return false;
        }
    }

    // Only one thread writes at a time (the "writing" flag), so the cached view is safe.
    private ByteBuffer view(byte[] array) {
        if (writeViewArray != array) {
            writeViewArray = array;
            writeView = ByteBuffer.wrap(array);
        }
        writeView.clear();
        return writeView;
    }

    // ---- read side ------------------------------------------------------------------------

    private void readLoop() {
        byte[] buf = new byte[READ_BUFFER];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        RespReader r = new RespReader(buf);
        int lim = 0;
        try {
            for (;;) {
                int n;
                if (tlsIn != null) {
                    n = tlsIn.read(buf, lim, buf.length - lim);
                } else {
                    bb.limit(buf.length).position(lim);
                    n = channel.read(bb);
                }
                if (n < 0) {
                    throw new RuvioConnectionException("connection closed by server");
                }
                lim += n;
                r.wrap(buf, 0, lim);
                while (r.pos < lim) {
                    int start = r.pos;
                    try {
                        dispatch(r);
                    } catch (RespReader.Incomplete e) {
                        r.pos = start;
                        break;
                    }
                }
                int rest = lim - r.pos;
                if (rest > 0 && r.pos > 0) {
                    System.arraycopy(buf, r.pos, buf, 0, rest);
                }
                lim = rest;
                if (lim == buf.length) {
                    byte[] bigger = new byte[buf.length << 1];
                    System.arraycopy(buf, 0, bigger, 0, lim);
                    buf = bigger;
                    bb = ByteBuffer.wrap(buf);
                } else if (lim == 0 && buf.length > SHRINK_ABOVE) {
                    buf = new byte[READ_BUFFER];
                    bb = ByteBuffer.wrap(buf);
                }
            }
        } catch (Throwable e) {
            fail(e instanceof RuvioException ? e : new RuvioConnectionException("read failed: " + e.getMessage(), e));
        }
    }

    /**
     * Returns the pending request at the head of the FIFO. Reader thread only.
     *
     * @return the request, or null when none is pending
     */
    public final Request<?> peekRequest() {
        long h = head;
        if (h == tail) {
            return null;
        }
        return ring[(int) (h & mask)];
    }

    /** Removes the request at the head of the FIFO. Reader thread only. */
    public final void popRequest() {
        long h = head;
        int slot = (int) (h & mask);
        ring[slot] = null;
        if (kept != null) {
            release(slot);
        }
        head = h + 1;
    }

    /**
     * Decodes one complete reply at the reader position. Throws Incomplete to wait for bytes.
     *
     * @param r the reader
     */
    public void dispatch(RespReader r) {
        byte t = r.peek();
        if (t == '>') {
            Object push = r.readObject();
            Consumer<Object> h = pushHandler;
            if (h != null) {
                try {
                    h.accept(push);
                } catch (RuntimeException ignored) {
                    // listener failures must not kill the reader
                }
            }
            return;
        }
        Request<?> q = peekRequest();
        if (q == null) {
            throw new RuvioException("protocol error: reply without a pending request");
        }
        if (q.isDone()) {
            // Failed by the command timeout (or cancelled): read the late reply and drop it.
            r.skip();
            popRequest();
            return;
        }
        if (t == '-') {
            RuvioServerException err = r.readError();
            Link l = followLink;
            if (l != null && Link.isReadonly(err)) {
                int slot = (int) (head & mask);
                int n = keptLen[slot];
                if (n > 0) {
                    int o = keptOff[slot];
                    byte[] command = o < 0 ? spill[slot] : java.util.Arrays.copyOfRange(kept, o, o + n);
                    popRequest();
                    l.followReadonly(q, command, this, err);
                    return;
                }
            }
            popRequest();
            q.fail(err);
            return;
        }
        int start = r.pos;
        Object value;
        try {
            value = q.decode(r);
        } catch (RespReader.Incomplete e) {
            throw e;
        } catch (RuntimeException e) {
            r.pos = start;
            r.skip(); // may throw Incomplete; the decode is retried then
            popRequest();
            q.fail(e);
            return;
        }
        popRequest();
        q.deliver(value);
    }

    // ---- shutdown -------------------------------------------------------------------------

    private RuvioConnectionException closedException() {
        Throwable c = closed;
        if (c instanceof RuvioConnectionException) {
            return new RuvioConnectionException(c.getMessage(), c);
        }
        return new RuvioConnectionException("connection closed", c);
    }

    /**
     * Closes the connection and fails every pending request with this cause.
     *
     * @param cause why the connection failed
     */
    public void fail(Throwable cause) {
        lock.lock();
        try {
            if (closed != null) {
                return;
            }
            closed = cause;
        } finally {
            lock.unlock();
        }
        try {
            // The plain socket first: closing a TLS socket sends a close_notify, which can wait behind a blocked write.
            channel.close();
            if (tlsSocket != null) {
                tlsSocket.close();
            }
        } catch (IOException ignored) {
            // closing anyway
        }
        LockSupport.unpark(writer);
        Thread w = watchdog;
        if (w != null) {
            LockSupport.unpark(w);
        }
        // Producers can no longer push (they check "closed" under the lock), so drain the ring.
        // Wait for the reader to stop touching head before draining from another thread.
        if (Thread.currentThread() != reader) {
            try {
                reader.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        RuvioConnectionException ex = closedException();
        long h = head;
        long t = tail;
        for (long i = h; i < t; i++) {
            Request<?> q = ring[(int) (i & mask)];
            ring[(int) (i & mask)] = null;
            if (q != null) {
                q.fail(ex);
            }
        }
        head = t;
        onClosed(cause);
    }

    /**
     * Hook for subclasses (pub/sub reconnect), called after the connection failed.
     *
     * @param cause why it failed
     */
    public void onClosed(Throwable cause) {
    }

    @Override
    public void close() {
        fail(new RuvioConnectionException("connection closed by client"));
    }
}
