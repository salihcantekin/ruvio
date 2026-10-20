use super::*;

/// Data directories whose clients served while parked resume only after the serving write's
/// commit (a test can then fail that write's sync before the served client commits).
#[cfg(test)]
pub(super) static DELAY_SERVED_POPS: std::sync::Mutex<Vec<std::path::PathBuf>> =
    std::sync::Mutex::new(Vec::new());

/// Input an unauthenticated client may have buffered.
const UNAUTHENTICATED_BUFFER_BYTES: usize = 16 * 1024;
/// How long `SAVE` waits for the other shards to put their files in its round.
const SAVE_ROUND_TIMEOUT: Duration = Duration::from_secs(120);

/// The reply to a write whose WAL record is larger than a whole segment. The write is
/// refused before it is applied and nothing is logged; the log stays healthy.
pub(super) const OVERSIZED_WRITE_ERROR: &[u8] = b"ERR write batch exceeds wal_segment_bytes";
/// How often a refused oversized write is logged.
const OVERSIZED_WRITE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// A hot key named inside `MULTI`: the hot store cannot join the transaction.
const HOT_KEY_IN_MULTI_REPLY: &[u8] = b"-ERR hot keys cannot be used inside MULTI\r\n";

pub(super) type Outbound = Rc<PubSubOut>;

/// How a hot command ended: its reply is written, the log refused its record (with the
/// error to reply), or writing the reply failed.
enum HotStatus {
    Done,
    Refused(&'static [u8]),
    Failed,
}

/// The error reply (without the leading `-`) for a write the WAL refused: a record over
/// `wal_segment_bytes` gets its own error, logged at most once a minute; any other
/// failure is a persistence error.
#[cold]
pub(super) fn refused_write(error: &crate::PersistenceError) -> &'static [u8] {
    static LOGGED: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

    let crate::PersistenceError::Failed(message) = error else {
        return b"MISCONF persistence error";
    };

    if !message.starts_with("encoded WAL record exceeds configured segment size") {
        return b"MISCONF persistence error";
    }

    let mut logged = LOGGED.lock().unwrap_or_else(|error| error.into_inner());

    if logged.is_none_or(|at| at.elapsed() >= OVERSIZED_WRITE_LOG_INTERVAL) {
        *logged = Some(Instant::now());
        warn!(
            %error,
            "write refused: its WAL record is larger than wal_segment_bytes; raise \
             wal_segment_bytes or split the write or transaction (logged at most once a minute)"
        );
    }

    OVERSIZED_WRITE_ERROR
}

pub(super) struct PubSubOut {
    pub(super) queue: RefCell<VecDeque<std::sync::Arc<[u8]>>>,
    pub(super) queue_bytes: Cell<usize>,
    pub(super) waker: RefCell<Option<Waker>>,
    pub(super) closed: Cell<bool>,
    pub(super) peak_queue_frames: Rc<Cell<usize>>,
    pub(super) peak_queue_bytes: Rc<Cell<usize>>,
}

impl PubSubOut {
    pub(super) fn new(
        peak_queue_frames: Rc<Cell<usize>>,
        peak_queue_bytes: Rc<Cell<usize>>,
    ) -> Self {
        Self {
            queue: RefCell::new(VecDeque::new()),
            queue_bytes: Cell::new(0),
            waker: RefCell::new(None),
            closed: Cell::new(false),
            peak_queue_frames,
            peak_queue_bytes,
        }
    }

    pub(super) fn try_push(&self, frame: std::sync::Arc<[u8]>) -> bool {
        if self.closed.get() {
            return false;
        }

        let mut queue = self.queue.borrow_mut();

        if self.queue_bytes.get().saturating_add(frame.len()) > crate::pubsub::PUBSUB_QUEUE_BYTES {
            return false;
        }

        self.queue_bytes
            .set(self.queue_bytes.get().saturating_add(frame.len()));
        queue.push_back(frame);
        self.peak_queue_frames
            .set(self.peak_queue_frames.get().max(queue.len()));
        self.peak_queue_bytes
            .set(self.peak_queue_bytes.get().max(self.queue_bytes.get()));
        drop(queue);
        self.wake();
        true
    }

    pub(super) fn pop(&self) -> Option<std::sync::Arc<[u8]>> {
        let frame = self.queue.borrow_mut().pop_front()?;
        self.queue_bytes.set(self.queue_bytes.get() - frame.len());
        Some(frame)
    }

    pub(super) fn close(&self) {
        self.closed.set(true);
        self.wake();
    }

    pub(super) fn wake(&self) {
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    pub(super) fn wait(&self) -> OutboundWait<'_> {
        OutboundWait { outbound: self }
    }
}

pub(super) struct OutboundWait<'a> {
    pub(super) outbound: &'a PubSubOut,
}

impl Future for OutboundWait<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        {
            let mut slot = self.outbound.waker.borrow_mut();

            if slot
                .as_ref()
                .is_none_or(|waker| !waker.will_wake(cx.waker()))
            {
                *slot = Some(cx.waker().clone());
            }
        }

        if self.outbound.closed.get() || !self.outbound.queue.borrow().is_empty() {
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

pub(super) enum ClientStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

pub(super) enum SocketWait {
    Plain,
    /// Bytes already copied into the connection's read buffer.
    Bytes(usize),
}

impl ClientStream {
    pub(super) async fn wait_readable(&mut self, chunk: &mut [u8]) -> io::Result<SocketWait> {
        match self {
            Self::Plain(stream) => {
                stream.readable().await?;

                Ok(SocketWait::Plain)
            }

            Self::Tls(stream) => {
                let read = stream.read(chunk).await?;

                super::net::note_input(read);
                Ok(SocketWait::Bytes(read))
            }
        }
    }

    /// Reads straight into `input` for plain sockets. Returns true at end of stream.
    pub(super) fn read_ready(&mut self, input: &mut InputBuffer, limit: usize) -> io::Result<bool> {
        match self {
            Self::Plain(stream) => input.read_from(stream, limit),
            Self::Tls(_) => Ok(false),
        }
    }

    /// The socket underneath, for watching it while a command blocks.
    pub(super) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref().0,
        }
    }

    pub(super) async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.write_all(buf).await?,
            Self::Tls(stream) => stream.write_all(buf).await?,
        }
        super::net::note_output(buf.len());
        Ok(())
    }
}

/// First probe after five idle minutes, then every 30 s; nine unanswered probes close it.
pub(super) fn client_keepalive() -> socket2::TcpKeepalive {
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(300));

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    let keepalive = keepalive
        .with_interval(Duration::from_secs(30))
        .with_retries(9);

    keepalive
}

/// A listening socket with the backlog Redis uses (511) instead of std's 128, so a burst
/// of connects is not refused before the acceptor runs.
pub(super) fn bind_listener(address: SocketAddr) -> io::Result<std::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    // As std does on Unix: a restart can bind while old connections sit in TIME_WAIT.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.bind(&address.into())?;
    socket.listen(LISTEN_BACKLOG)?;
    Ok(socket.into())
}

const LISTEN_BACKLOG: i32 = 511;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_client(
    stream: TcpStream,
    state: Rc<ShardState>,
    limits: ProtocolLimits,
    timeouts: ConnectionTimeouts,
    request_limits: RequestLimits,
    shutdown: ShutdownToken,
    mut permit: ConnectionPermit,
    peer_ip: std::net::IpAddr,
) -> io::Result<()> {
    // Replies are small and latency-bound, so Nagle's delay would cost more than it saves.
    let _ = stream.set_nodelay(true);
    // Subscribers have no idle timeout; keepalive finds a peer that vanished without a FIN
    // instead of holding its queue forever.
    let _ = socket2::SockRef::from(&stream).set_tcp_keepalive(&client_keepalive());

    state
        .active_connections
        .set(state.active_connections.get().saturating_add(1));
    let _connection_guard = ConnectionGuard {
        state: Rc::clone(&state),
    };

    let addr = stream
        .peer_addr()
        .map_or_else(|_| String::new(), |addr| addr.to_string());
    let laddr = stream
        .local_addr()
        .map_or_else(|_| String::new(), |addr| addr.to_string());
    let mut stream = wrap_stream(stream, &state).await?;
    // Plain sockets read straight into `input`; only TLS needs a bounce buffer.
    let mut read_chunk = match stream {
        ClientStream::Plain(_) => Vec::new(),
        ClientStream::Tls(_) => vec![0; READ_CHUNK_BYTES],
    };
    let conn = state.next_conn_id.get();

    state.next_conn_id.set(conn.saturating_add(1));
    let outbound = Rc::new(PubSubOut::new(
        Rc::clone(&state.peak_pubsub_queue_frames),
        Rc::clone(&state.peak_pubsub_queue_bytes),
    ));

    state
        .subscribers
        .borrow_mut()
        .insert(conn, Rc::clone(&outbound));
    let _watch_guard = WatchGuard {
        conn,
        state: Rc::clone(&state),
        outbound: Rc::clone(&outbound),
    };

    let mut auth = crate::acl::ConnectionAuth::from_security(&state.security);

    if crate::security::protected_mode_denies(&state.security, &auth, peer_ip) {
        stream
            .write_all(crate::security::PROTECTED_MODE_ERROR)
            .await?;

        return Ok(());
    }

    state.clients.borrow_mut().insert(
        conn,
        ClientEntry {
            addr,
            laddr,
            created: Instant::now(),
            name: Vec::new(),
            db: 0,
            resp: 2,
            user: auth.username.to_vec(),
        },
    );

    if auth.authenticated && !auth.username.is_empty() && !permit.bind_user(&auth.username) {
        stream
            .write_all(b"-ERR max number of connections for this user reached\r\n")
            .await?;

        return Ok(());
    }

    let mut rate = CommandRateLimiter::new(
        state
            .live
            .with(|live| live.connections.max_commands_per_second),
    );
    let mut session = TxSession::new(conn);
    let fwd_entry = state
        .forwarding
        .as_ref()
        .map(|forwarding| forwarding.register(conn));
    let _fwd_guard = FwdGuard {
        state: Rc::clone(&state),
        conn,
    };
    let mut forwarder = Forwarder::new(
        state
            .forwarding
            .as_ref()
            .map_or(0, |forwarding| forwarding.fabric.shard_count()),
    );
    let mut multi_log = MultiLog::default();
    let mut input = InputBuffer::default();
    // A frame refused for an oversized argument, dropped as it arrives.
    let mut skipping: Option<FrameSkip> = None;
    // Set when the parse that started a skip left input to drop before the next read.
    let mut skip_now = false;
    // End of stream seen by the read that started a skip.
    let mut eof_pending = false;
    let mut output = BoundedOutput::with_capacity(0, request_limits.max_output_buffer_bytes);

    loop {
        if shutdown.is_requested() {
            return Ok(());
        }
        if session.in_pubsub && input.is_empty() {
            input.release_all();
            output.release_idle();
        }

        let read_timeout = if input.is_empty() {
            // A subscriber is silent by design; it is not idle.
            if session.in_pubsub || session.tracking {
                Duration::from_secs(365 * 24 * 60 * 60)
            } else {
                timeouts.idle
            }
        } else {
            timeouts.read
        };

        if let Some(frame) = outbound.pop() {
            if state.published_config.persistence.sync_worker_active {
                OpenWalGroup::new(&state).commit().await?;
            }
            timeout(timeouts.write, stream.write_all(&frame))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))??;

            continue;
        }

        if outbound.closed.get() {
            return Ok(());
        }

        let notified = outbound.wait();

        tokio::pin!(notified);

        if let Some(frame) = outbound.pop() {
            if state.published_config.persistence.sync_worker_active {
                OpenWalGroup::new(&state).commit().await?;
            }
            timeout(timeouts.write, stream.write_all(&frame))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))??;

            continue;
        }

        if outbound.closed.get() {
            return Ok(());
        }

        // Before AUTH only a small login frame is expected; do not buffer megabytes for a
        // client that has not proven who it is.
        let buffer_limit = if auth.authenticated {
            limits.max_connection_buffer_bytes
        } else {
            limits
                .max_connection_buffer_bytes
                .min(UNAUTHENTICATED_BUFFER_BYTES)
        };

        let mut close_after_parse = std::mem::take(&mut eof_pending);

        if !std::mem::take(&mut skip_now) {
            tokio::select! {
                _ = &mut notified => continue,

                ready = timeout(read_timeout, stream.wait_readable(&mut read_chunk)) => {
                    match ready {
                        Err(_) if !input.is_empty() => {
                            crate::admission::note_protocol_error();
                            let _ = timeout(
                                timeouts.write,
                                stream.write_all(b"-ERR protocol error\r\n"),
                            )
                            .await;

                            return Ok(());
                        }

                        Err(_) => return Ok(()),
                        Ok(Err(error)) => return Err(error),
                        Ok(Ok(SocketWait::Plain)) => {
                            let before = input.len();
                            let eof =
                                stream.read_ready(&mut input, buffer_limit.saturating_add(1))?;

                            // A refused frame cut short still gets its reply.
                            if eof && input.len() == before && skipping.is_none() {
                                return Ok(());
                            }

                            if input.len() == before && !eof {
                                continue;
                            }

                            close_after_parse = eof;
                        }

                        Ok(Ok(SocketWait::Bytes(0))) if skipping.is_none() => return Ok(()),
                        Ok(Ok(SocketWait::Bytes(0))) => close_after_parse = true,
                        Ok(Ok(SocketWait::Bytes(read))) => {
                            input.extend_from_slice(&read_chunk[..read]);
                        }
                    }
                }
            };
        }

        state
            .peak_input_buffer_bytes
            .set(state.peak_input_buffer_bytes.get().max(input.len()));

        if input.len() > buffer_limit {
            stream
                .write_all(b"-ERR connection buffer limit exceeded\r\n")
                .await?;

            return Ok(());
        }

        if let Some(skip) = skipping.as_mut() {
            match skip.advance(&mut input) {
                Ok(false) => {
                    if close_after_parse {
                        let _ = write_response(
                            &mut output,
                            Response::OwnedError(command_rejection(skip.message)),
                        );
                        flush_output(&mut stream, &mut output, timeouts.write, &state).await?;

                        return Ok(());
                    }

                    continue;
                }
                Ok(true) => {
                    write_response(
                        &mut output,
                        Response::OwnedError(command_rejection(skip.message)),
                    )?;
                    skipping = None;
                    input.set_need(0);

                    if input.is_empty() {
                        flush_output(&mut stream, &mut output, timeouts.write, &state).await?;

                        if close_after_parse {
                            return Ok(());
                        }

                        continue;
                    }
                }
                Err(_) => {
                    crate::admission::note_protocol_error();
                    stream.write_all(b"-ERR protocol error\r\n").await?;

                    return Ok(());
                }
            }
        }

        if input.waiting_for_frame() && !close_after_parse {
            continue;
        }

        if state.replica_incomplete.get() {
            let mut consumed = 0;
            let mut commands = 0;
            while consumed < input.len() {
                let frame_length = match parse_command_with_limits(&input[consumed..], limits) {
                    Ok(Some((_, length))) => length,
                    Ok(None) => break,
                    Err(ProtocolError::Rejected { consumed, .. }) => consumed,
                    Err(_) => {
                        crate::admission::note_protocol_error();
                        stream.write_all(b"-ERR protocol error\r\n").await?;
                        return Ok(());
                    }
                };
                consumed += frame_length;
                commands += 1;
                let error = if state.catchup_wait_started.get().elapsed() > Duration::from_secs(30)
                {
                    b"MASTERDOWN incomplete replica; waiting for a recoverable primary".as_slice()
                } else {
                    b"LOADING replica catching up".as_slice()
                };
                write_response(&mut output, Response::Error(error))?;
                if commands == limits.max_pipeline_commands {
                    flush_output(&mut stream, &mut output, timeouts.write, &state).await?;
                    commands = 0;
                }
            }
            input.consume(consumed);
            flush_output(&mut stream, &mut output, timeouts.write, &state).await?;
            if close_after_parse {
                return Ok(());
            }
            continue;
        }

        let mut consumed = 0;
        let mut batch_commands = 0;
        let mut flush_at = PIPELINE_FLUSH_BYTES;
        let mut wal_group = OpenWalGroup::new(&state);

        state.store.borrow().sample_time();
        let _time_sample = TimeSample { state: &state };

        loop {
            // Bound bytes withheld by one open group; `dirty_bytes` tracks the
            // whole unsynced file and stays high while the worker owns syncs.
            if state.published_config.persistence.sync_worker_active
                && state
                    .persistence
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .group_bytes()
                    >= 64 * 1024
            {
                wal_group.commit().await?;
            }
            // Every wait on another shard starts with the group committed: an open group
            // stages every record this shard writes meanwhile, other clients' too, and their
            // `everysec` replies do not wait for the write.
            // Under `always` other clients' replies wait for a sync that publishes the
            // staged records anyway, so the commit would only add a sync.
            if forwarder.len() >= MAX_PENDING_FORWARDS {
                if !wal_group.always {
                    wal_group.commit().await?;
                }
                settle(
                    &state,
                    &mut forwarder,
                    fwd_entry.as_ref(),
                    conn,
                    &session,
                    &auth,
                    &mut output,
                )
                .await?;
            }
            if batch_commands == limits.max_pipeline_commands {
                wal_group.commit().await?;
                settle(
                    &state,
                    &mut forwarder,
                    fwd_entry.as_ref(),
                    conn,
                    &session,
                    &auth,
                    &mut output,
                )
                .await?;
                if session.durability.is_some() {
                    super::durable::hold_until_durable(&state, &mut session, &mut output, &shutdown)
                        .await?;
                }
                flush_output(&mut stream, &mut output, timeouts.write, &state).await?;
                batch_commands = 0;
            } else if output.len() >= flush_at {
                // A pipeline whose replies pile up sends what the socket takes now, so the
                // output limit counts only bytes the client has not taken yet.
                wal_group.commit().await?;
                settle(
                    &state,
                    &mut forwarder,
                    fwd_entry.as_ref(),
                    conn,
                    &session,
                    &auth,
                    &mut output,
                )
                .await?;
                if session.durability.is_some() {
                    super::durable::hold_until_durable(&state, &mut session, &mut output, &shutdown)
                        .await?;
                }
                let unsent = flush_ready(&mut stream, &mut output, timeouts.write, &state).await?;

                // A client that is not reading is retried at doubling sizes, so the unsent
                // tail is not moved once per command on its way to the limit.
                flush_at = PIPELINE_FLUSH_BYTES.max(unsent.saturating_mul(2));
            }

            match if session.is_multi() || session.in_pubsub {
                Ok(HotParse::Fallback)
            } else {
                match parse_hot_command_with_limits(&input[consumed..], limits) {
                    Ok(HotParse::Complete(command, _))
                        if !crate::database::hot_path_allowed(session.database, &command) =>
                    {
                        Ok(HotParse::Fallback)
                    }

                    other => other,
                }
            } {
                Ok(HotParse::Complete(command, frame_length)) => {
                    let command_timing =
                        CommandTiming::new(state.clone(), hot_command_group(&command), || {
                            hot_command_name(&command)
                        });
                    command_timing.observe_key(
                        || hot_command_key(&command).map(<[u8]>::to_vec),
                        hot_command_write_bytes(&command),
                    );
                    consumed += frame_length;
                    batch_commands += 1;

                    if !rate.allow() {
                        write_response(
                            &mut output,
                            Response::Error(crate::admission::RATE_LIMIT_ERROR),
                        )?;
                        continue;
                    }

                    if !auth.authenticated {
                        write_response(
                            &mut output,
                            Response::Error(b"NOAUTH Authentication required."),
                        )?;
                        continue;
                    }

                    if let Err(deny) = auth.rules.authorize_hot(&command) {
                        write_response(&mut output, crate::acl::noperm_response(deny))?;
                        continue;
                    }

                    if let Some(refusal) = state.write_gate.get().refusal()
                        && hot_command_mutates_store(&command)
                    {
                        write_response(&mut output, Response::Error(refusal))?;
                        continue;
                    }

                    if state.hot_store.is_some()
                        && !state.published_config.hot_keys.is_empty()
                        && let Some(full) = hot_to_command(&command)
                        && reply_hot_store(
                            &state,
                            &full,
                            &mut output,
                            session.resp,
                            conn,
                            session.tracking,
                        )?
                    {
                        continue;
                    }

                    if let Some(forwarding) = state.forwarding.as_ref()
                        && let Some((owner, slot)) = hot_remote_owner(forwarding, &command)
                    {
                        if redirect_to_node(forwarding, &mut output, slot, owner)? {
                            continue;
                        }
                        forwarder.push(
                            owner,
                            &input[consumed - frame_length..consumed],
                            output.len(),
                            hot_command_mutates_store(&command),
                        );
                        continue;
                    }

                    if hot_command_mutates_store(&command) {
                        bump_mutations(&state);
                    }

                    if wal_group.uses_wal {
                        if wal_group_eligible(WalGroupCommand::Hot(&command), &session) {
                            wal_group.begin();
                        } else {
                            wal_group.commit().await?;
                        }
                    }

                    let track_plan = hot_track_plan(&state, session.tracking, &command);
                    let null = session.resp.null_bulk();
                    let status = if wal_group.uses_wal {
                        let mut store = state.store.borrow_mut();
                        let mut persistence = state
                            .persistence
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());

                        match execute_hot_into_with_sink(
                            &mut store,
                            command,
                            &mut output,
                            &mut *persistence,
                            null,
                        ) {
                            Ok(()) => HotStatus::Done,
                            Err(ExecuteError::Mutation(error)) => {
                                HotStatus::Refused(refused_write(&error))
                            }
                            Err(ExecuteError::Io(_)) => HotStatus::Failed,
                        }
                    } else {
                        let mut store = state.store.borrow_mut();
                        let mut sink = NoopMutationSink;

                        match execute_hot_into_with_sink(
                            &mut store,
                            command,
                            &mut output,
                            &mut sink,
                            null,
                        ) {
                            Ok(()) => HotStatus::Done,
                            Err(ExecuteError::Mutation(error)) => match error {},
                            Err(ExecuteError::Io(_)) => HotStatus::Failed,
                        }
                    };

                    match status {
                        HotStatus::Done => {
                            if let Some(plan) = track_plan {
                                observe_tracking(&state, conn, session.tracking, plan, true);
                            }
                        }
                        HotStatus::Refused(error) => {
                            write_response(&mut output, Response::Error(error))?;
                        }
                        HotStatus::Failed => {}
                    }

                    flush_key_notices(&state);
                }

                Ok(HotParse::Incomplete) => break,
                Ok(HotParse::Fallback) => {
                    match parse_command_with_limits(&input[consumed..], limits) {
                        Ok(Some((command, frame_length))) => {
                            // Every key in one slot (true without a fabric): the lean path
                            // may skip the `CROSSSLOT` check of the generic one.
                            let mut one_slot = true;
                            // Routed, then awaited before it runs here (see the re-check below).
                            let mut waited = false;

                            // A subscribed client may only run (local) pub/sub commands;
                            // `gate` refuses the rest, so nothing is forwarded.
                            if let Some(forwarding) = state.forwarding.as_ref()
                                && !session.in_pubsub
                            {
                                let simple =
                                    wal_group_eligible(WalGroupCommand::Parsed(&command), &session);
                                let (owner, slot_ok) = command_owner(&state, forwarding, &command);

                                one_slot = slot_ok;
                                let fan = fan_for(&state, &command).is_some();
                                // Commands that leave this shard (or that SCAN answers for
                                // every shard) are checked here: `gate` never sees them. Fan
                                // commands also run locally through `gate`, which counts the
                                // rate there; inside MULTI the command is queued locally.
                                let leaves = !matches!(owner, Owner::Local)
                                    || (state.single_port
                                        && matches!(command, Command::Scan { .. }));

                                if leaves || fan {
                                    let counted = !session.is_multi();
                                    let refusal = forward_refusal(
                                        &auth,
                                        counted.then_some(&mut rate),
                                        &command,
                                    );

                                    if let Some(refusal) = refusal {
                                        consumed += frame_length;
                                        batch_commands += 1;
                                        // Inside MULTI the refused command is not queued.
                                        session.mark_queue_error();
                                        write_response(&mut output, refusal)?;
                                        continue;
                                    }

                                    // Another node of the cluster owns the slot. Inside MULTI
                                    // the transaction is aborted, as Redis Cluster does.
                                    if let Owner::Node(slot) = owner {
                                        consumed += frame_length;
                                        batch_commands += 1;
                                        session.mark_queue_error();
                                        redirect_to_node(
                                            forwarding,
                                            &mut output,
                                            slot,
                                            forwarding.fabric.slot_owner(slot),
                                        )?;
                                        continue;
                                    }
                                }

                                // SWAPDB would swap the home shard's databases only, and a
                                // queued fan command would run on the other shards at once
                                // and on this one at EXEC. A queued PUBLISH is sent at EXEC,
                                // after the batch is recorded.
                                let unsupported: Option<&[u8]> =
                                    if matches!(command, Command::SwapDb(..)) {
                                        Some(SWAPDB_SHARDS_REPLY)
                                    } else if fan && session.is_multi() {
                                        Some(FAN_IN_MULTI_REPLY)
                                    } else {
                                        None
                                    };

                                if let Some(reply) = unsupported {
                                    consumed += frame_length;
                                    batch_commands += 1;
                                    session.mark_queue_error();
                                    output.write_all(reply)?;
                                    continue;
                                }

                                if simple {
                                    match owner {
                                        Owner::Local => {}
                                        Owner::Remote(owner) => {
                                            // Counted here, as the hot path counts what it
                                            // forwards: the owner shard does not count it.
                                            let _timing = forwarded_timing(&state, &command);

                                            forwarder.push(
                                                owner,
                                                &input[consumed..consumed + frame_length],
                                                output.len(),
                                                command_may_write(&command),
                                            );
                                            consumed += frame_length;
                                            batch_commands += 1;
                                            continue;
                                        }
                                        Owner::Node(_) | Owner::Split => {
                                            consumed += frame_length;
                                            batch_commands += 1;
                                            output.write_all(SPLIT_REPLY)?;
                                            continue;
                                        }
                                    }
                                } else {
                                    waited = true;
                                    // Neither a plain command nor this shard's alone: it
                                    // may wait on another shard (see `MAX_PENDING_FORWARDS`).
                                    if !wal_group.always {
                                        wal_group.commit().await?;
                                    }
                                    settle(
                                        &state,
                                        &mut forwarder,
                                        fwd_entry.as_ref(),
                                        conn,
                                        &session,
                                        &auth,
                                        &mut output,
                                    )
                                    .await?;

                                    if let Some(fan) = fan_for(&state, &command) {
                                        let frame = &input[consumed..consumed + frame_length];
                                        let mut local = BoundedOutput::with_capacity(
                                            0,
                                            request_limits.max_output_buffer_bytes,
                                        );

                                        // Counted above, before any shard ran it.
                                        let mut counted = CommandRateLimiter::new(0);
                                        let _timing = forwarded_timing(&state, &command);

                                        wal_group.commit().await?;
                                        dispatch_command(
                                            &state,
                                            &mut local,
                                            &mut session,
                                            &mut auth,
                                            &mut permit,
                                            &mut counted,
                                            command,
                                            frame,
                                            conn,
                                            &shutdown,
                                            Some(Peer::Socket(stream.tcp())),
                                            None,
                                        )
                                        .await?;
                                        let remote = match fwd_entry.as_ref() {
                                            Some(entry) => {
                                                forwarder
                                                    .broadcast(
                                                        forwarding, entry, conn, &session, &auth,
                                                        frame,
                                                    )
                                                    .await?
                                            }
                                            None => Vec::new(),
                                        };

                                        consumed += frame_length;
                                        batch_commands += 1;
                                        merge_fan(fan, &local, &remote, &mut output)?;
                                        continue;
                                    }

                                    // A blocking command may wait on its owner shard: the
                                    // replies queued ahead of it leave first, recorded.
                                    if matches!(owner, Owner::Remote(_))
                                        && !session.is_multi()
                                        && waits_on_owner(&command)
                                        && !output.is_empty()
                                    {
                                        wal_group.commit().await?;
                                        if session.durability.is_some() {
                                            super::durable::hold_until_durable(
                                                &state,
                                                &mut session,
                                                &mut output,
                                                &shutdown,
                                            )
                                            .await?;
                                        }
                                        flush_output(
                                            &mut stream,
                                            &mut output,
                                            timeouts.write,
                                            &state,
                                        )
                                        .await?;
                                    }

                                    let timing = forwarded_timing(&state, &command);

                                    if let Some(entry) = fwd_entry.as_ref()
                                        && forward_special(
                                            &state,
                                            &mut forwarder,
                                            entry,
                                            conn,
                                            &mut session,
                                            &auth,
                                            &mut rate,
                                            &mut output,
                                            &command,
                                            &input[consumed..consumed + frame_length],
                                            &owner,
                                            &mut multi_log,
                                            stream.tcp(),
                                        )
                                        .await?
                                    {
                                        consumed += frame_length;
                                        batch_commands += 1;
                                        continue;
                                    }
                                    // Run here after all: counted below like any command.
                                    timing.discard();
                                }
                            }
                            // Replies or forwards queued ahead of this command. Only then can a
                            // blocking command have something to send first: with nothing
                            // ahead it parks on its first run, outside the group.
                            let ahead = !output.is_empty() || !forwarder.is_empty();

                            if wal_group.uses_wal {
                                if wal_group_eligible(WalGroupCommand::Parsed(&command), &session)
                                    // Scripts and EXEC run with no await before their
                                    // reply, so the group they join never stays open across
                                    // one (see `transaction_joins_group`).
                                    || (state.forwarding.is_none()
                                        && transaction_joins_group(&command, &session))
                                    || (ahead && blocking_joins_group(&command, &session))
                                {
                                    wal_group.begin();
                                } else {
                                    waited = true;
                                    wal_group.commit().await?;
                                }
                            }
                            // A command that waited above may find its slot taken over by
                            // another shard meanwhile: it goes through routing again.
                            if waited
                                && let Some(forwarding) = state.forwarding.as_ref()
                                && forwarding.fabric.mover.moved()
                                && !session.in_pubsub
                                && super::moveslots::runs_elsewhere(&state, &session, &command)
                            {
                                continue;
                            }
                            let command_timing =
                                CommandTiming::new(state.clone(), command_group(&command), || {
                                    crate::acl::command_acl_name(&command)
                                });
                            command_timing.observe_key(
                                || {
                                    crate::routing::command_keys(&command)
                                        .first()
                                        .map(|key| key.to_vec())
                                },
                                command_write_bytes(&command),
                            );
                            let record = state.forwarding.is_some()
                                && session.is_multi()
                                && !matches!(
                                    command,
                                    Command::Multi
                                        | Command::Exec
                                        | Command::Discard
                                        | Command::Watch(_)
                                        | Command::Unwatch
                                );

                            if matches!(command, Command::Multi) {
                                multi_log.clear();
                            }

                            consumed += frame_length;
                            batch_commands += 1;

                            if plain_command(&command)
                                && lean_session(&state, &session, &auth)
                                && one_slot
                            {
                                dispatch_plain(
                                    &state,
                                    &mut output,
                                    &session,
                                    &mut rate,
                                    command,
                                    &input[consumed - frame_length..consumed],
                                )?;
                                continue;
                            }

                            let mut parking = None;
                            // A classic PUBLISH reaches the other shards only after this
                            // shard ran it, so the auth, ACL and rate checks of `gate` and
                            // the pub/sub session rules decided first; its reply (an integer)
                            // says it ran.
                            // Borrowed from the input, which stays put until the batch ends (a
                            // message the parser had to own is cloned): `publish_everywhere` copies
                            // it once, and only if another shard has room for it.
                            let publish = match (&command, state.forwarding.as_ref()) {
                                (Command::Publish { channel, message }, Some(_)) => {
                                    Some((channel.clone(), message.clone()))
                                }
                                _ => None,
                            };
                            let reply_at = output.len();

                            // Shards this connection sent writes to so far (a WAIT settled them first).
                            session.forwarded_shards |= forwarder.wrote();
                            dispatch_command(
                                &state,
                                &mut output,
                                &mut session,
                                &mut auth,
                                &mut permit,
                                &mut rate,
                                command,
                                &input[consumed - frame_length..consumed],
                                conn,
                                &shutdown,
                                Some(Peer::Socket(stream.tcp())),
                                ahead.then_some(&mut parking),
                            )
                            .await?;

                            if let Some((channel, message)) = publish
                                && let Some(forwarding) = state.forwarding.as_ref()
                                && output.get(reply_at) == Some(&b':')
                            {
                                publish_everywhere(forwarding, &channel, &message);
                            }

                            // The command is about to wait: the group it joined is committed,
                            // and the replies queued ahead of it leave first (recorded before
                            // they do), so the client is not kept waiting for answers it is
                            // owed. A blocking command that did not wait never gets here.
                            if let Some(parking) = parking {
                                wal_group.commit().await?;

                                if !output.is_empty() {
                                    settle(
                                        &state,
                                        &mut forwarder,
                                        fwd_entry.as_ref(),
                                        conn,
                                        &session,
                                        &auth,
                                        &mut output,
                                    )
                                    .await?;
                                    // Forwarded replies may have added records.
                                    wal_group.commit().await?;
                                    if session.durability.is_some() {
                                        super::durable::hold_until_durable(
                                            &state,
                                            &mut session,
                                            &mut output,
                                            &shutdown,
                                        )
                                        .await?;
                                    }
                                    flush_output(&mut stream, &mut output, timeouts.write, &state)
                                        .await?;
                                }

                                match parking {
                                    Parking::Again(resolved) => {
                                        let frame = &input[consumed - frame_length..consumed];
                                        let Ok(Some((mut command, _))) =
                                            parse_command_with_limits(frame, limits)
                                        else {
                                            return Err(io::Error::other(
                                                "a parsed blocking command failed to parse again",
                                            ));
                                        };

                                        // `$` keeps the end it had on the first run.
                                        if let (Some(resolved), Command::StreamRead { keys, .. }) =
                                            (resolved, &mut command)
                                        {
                                            for ((_, cursor), (millis, sequence)) in
                                                keys.iter_mut().zip(resolved)
                                            {
                                                *cursor = crate::stream::StreamCursor::After(
                                                    millis, sequence,
                                                );
                                            }
                                        }

                                        // Counted when it first ran.
                                        let mut counted = CommandRateLimiter::new(0);

                                        dispatch_command(
                                            &state,
                                            &mut output,
                                            &mut session,
                                            &mut auth,
                                            &mut permit,
                                            &mut counted,
                                            command,
                                            frame,
                                            conn,
                                            &shutdown,
                                            Some(Peer::Socket(stream.tcp())),
                                            None,
                                        )
                                        .await?;
                                    }
                                    Parking::Wait(wait) => {
                                        finish_parking(
                                            &state,
                                            wait,
                                            &mut session,
                                            &mut output,
                                            &shutdown,
                                            Some(Peer::Socket(stream.tcp())),
                                            conn,
                                        )
                                        .await?;
                                    }
                                }
                            }

                            if let Some(served_by) = session.take_served_after_park() {
                                wal_group.skip_records_written_while_parked(served_by);
                            }

                            if record {
                                multi_log
                                    .record(&input[consumed - frame_length..consumed], &output);
                            }
                        }

                        Ok(None) => break,
                        Err(ProtocolError::Rejected {
                            message,
                            consumed: frame,
                        }) => {
                            consumed += frame;
                            batch_commands += 1;
                            session.mark_queue_error();
                            write_response(
                                &mut output,
                                Response::OwnedError(command_rejection(message)),
                            )?;
                        }

                        // An argument over the limit: drop the rest of the frame as it
                        // arrives and refuse it. A client that has not authenticated is
                        // not worth reading megabytes for.
                        Err(ProtocolError::Oversized {
                            message,
                            parsed,
                            skip,
                            args_left,
                        }) if auth.authenticated => {
                            consumed += parsed;
                            session.mark_queue_error();
                            skipping = Some(FrameSkip::new(
                                message,
                                parsed,
                                skip,
                                args_left,
                                limits.max_command_bytes,
                            ));
                            skip_now = true;
                            break;
                        }

                        Err(_) => {
                            protocol_error(&mut stream, &mut wal_group, timeouts.write).await?;

                            return Ok(());
                        }
                    }
                }

                Err(_) => {
                    protocol_error(&mut stream, &mut wal_group, timeouts.write).await?;

                    return Ok(());
                }
            }
        }

        if consumed > 0 {
            input.consume(consumed);
        }

        if skipping.is_none() && input.len() >= LARGE_FRAME_HINT_BYTES {
            input.set_need(crate::protocol::incomplete_frame_need(&input, limits));
        } else {
            input.set_need(0);
        }

        // The local group goes to the sync worker before waiting for the owners of forwarded
        // commands, so the two durability waits overlap instead of running one after the other.
        let durability = wal_group.commit_deferred().await?;

        settle(
            &state,
            &mut forwarder,
            fwd_entry.as_ref(),
            conn,
            &session,
            &auth,
            &mut output,
        )
        .await?;

        if let Some(durability) = durability {
            durability.wait(&state).await?;
        }

        if session.durability.is_some() {
            super::durable::hold_until_durable(&state, &mut session, &mut output, &shutdown).await?;
        }
        flush_output(&mut stream, &mut output, timeouts.write, &state).await?;
        input.release_idle();
        output.release_oversized(IDLE_SHRINK_ABOVE_BYTES, IDLE_KEEP_BYTES);

        if close_after_parse {
            // The client is gone, but the frame it was refused for still gets its reply.
            if skipping.is_some() {
                eof_pending = true;
                continue;
            }

            return Ok(());
        }
    }
}

pub(super) async fn wrap_stream(stream: TcpStream, state: &ShardState) -> io::Result<ClientStream> {
    let Some(tls) = &state.tls else {
        return Ok(ClientStream::Plain(stream));
    };

    let config = tls.current_config()?;
    let acceptor = tokio_rustls::TlsAcceptor::from(config);

    match timeout(Duration::from_secs(5), acceptor.accept(stream)).await {
        Ok(Ok(tls_stream)) => Ok(ClientStream::Tls(Box::new(tls_stream))),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "tls handshake timed out",
        )),
    }
}

/// Replies a pipeline holds before it sends what the socket accepts.
const PIPELINE_FLUSH_BYTES: usize = 64 * 1024;

/// Answers a frame that cannot be parsed, ending the connection. The batch's group is
/// committed first: a client that does not read would otherwise keep it open, and every
/// record of the shard staged, for as long as the write waits.
async fn protocol_error(
    stream: &mut ClientStream,
    wal_group: &mut OpenWalGroup<'_>,
    write_timeout: Duration,
) -> io::Result<()> {
    crate::admission::note_protocol_error();
    wal_group.commit().await?;
    timeout(write_timeout, stream.write_all(b"-ERR protocol error\r\n"))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))?
}

/// Sends what the socket accepts without waiting and drops it from `output`; returns the
/// bytes still unsent. TLS sends everything, waiting as `flush_output` does.
async fn flush_ready(
    stream: &mut ClientStream,
    output: &mut BoundedOutput,
    write_timeout: Duration,
    state: &ShardState,
) -> io::Result<usize> {
    let ClientStream::Plain(socket) = stream else {
        flush_output(stream, output, write_timeout, state).await?;

        return Ok(0);
    };

    state
        .peak_output_buffer_bytes
        .set(state.peak_output_buffer_bytes.get().max(output.len()));

    if output.exceeded() {
        return flush_output(stream, output, write_timeout, state)
            .await
            .map(|()| 0);
    }

    let mut sent = 0;

    while sent < output.len() {
        match socket.try_write(&output[sent..]) {
            Ok(0) => break,
            Ok(written) => sent += written,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error),
        }
    }

    super::net::note_output(sent);
    output.consume_front(sent);

    // A client reading slower than the pipeline answers is waited for once half of the
    // limit is unsent, so the limit bounds memory without cutting it off; one that does
    // not take a byte for the whole write timeout is cut.
    if output.len() > output.maximum() / 2 {
        match timeout(write_timeout, socket.write_all(output)).await {
            Ok(result) => result?,
            Err(_) => return Err(io::Error::other("output buffer limit exceeded")),
        }
        super::net::note_output(output.len());

        output.clear();
    }

    Ok(output.len())
}

pub(super) async fn flush_output(
    stream: &mut ClientStream,
    output: &mut BoundedOutput,
    write_timeout: Duration,
    state: &ShardState,
) -> io::Result<()> {
    state
        .peak_output_buffer_bytes
        .set(state.peak_output_buffer_bytes.get().max(output.len()));
    if output.exceeded() {
        let _ = timeout(
            write_timeout,
            stream.write_all(b"-ERR output buffer limit exceeded\r\n"),
        )
        .await;

        return Err(io::Error::other("output buffer limit exceeded"));
    }

    if output.is_empty() {
        return Ok(());
    }

    timeout(write_timeout, stream.write_all(&*output))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))??;
    output.clear();
    Ok(())
}

/// Commands that `dispatch_inner` hands straight to `execute_in_database`: none of them
/// touches session state, is answered by the connection layer (cluster, admin, client
/// registry), uses pub/sub, scripts or transactions, or can block.
fn plain_command(command: &Command<'_>) -> bool {
    !crate::protocol::command_may_block(command)
        && !matches!(
            command,
            Command::Wait { .. }
                | Command::Ruvio(_)
                | Command::Hello { .. }
                | Command::Auth { .. }
                | Command::Acl(_)
                | Command::ClientGetName
                | Command::ClientId
                | Command::ClientList { .. }
                | Command::ClientInfo
                | Command::ClientKill { .. }
                | Command::ClientSetName(_)
                | Command::ClientTracking(_)
                | Command::ClientDurability { .. }
                | Command::Slowlog(_)
                | Command::LatencyLatest
                | Command::ShardPublish { .. }
                | Command::ShardSubscribe(_)
                | Command::ShardUnsubscribe(_)
                | Command::Publish { .. }
                | Command::Subscribe(_)
                | Command::Unsubscribe(_)
                | Command::PSubscribe(_)
                | Command::PUnsubscribe(_)
                | Command::ClusterSlots
                | Command::ClusterNodes
                | Command::ClusterInfo
                | Command::ClusterShards
                | Command::ClusterMyId
                | Command::ClusterKeySlot(_)
                | Command::ReadOnly
                | Command::ReadWrite
                | Command::ConfigGet(_)
                | Command::Info(_)
                | Command::Save
                | Command::ConfigSet(_)
                | Command::ConfigRewrite
                | Command::ReplicaOf { .. }
                | Command::MoveSlots(_)
                | Command::Topology(_)
                | Command::Migrate(_)
                | Command::Role
                | Command::Bgsave
                | Command::ChangesSubscribe(_)
                | Command::Multi
                | Command::Exec
                | Command::Discard
                | Command::Watch(_)
                | Command::Unwatch
                | Command::Eval { .. }
                | Command::EvalSha { .. }
                | Command::ScriptLoad(_)
                | Command::ScriptExists(_)
                | Command::ScriptFlush
                | Command::ScriptKill
                | Command::FunctionLoad { .. }
                | Command::FunctionList
                | Command::FunctionDelete(_)
                | Command::FCall { .. }
                | Command::Select(_)
                | Command::FlushDb
                | Command::FlushAll
                | Command::SwapDb(..)
                | Command::Move { .. }
        )
}

/// Nothing `gate` or `dispatch_inner` checks can change a plain command's outcome on this
/// connection: authenticated with unrestricted ACL rules, no transaction, pub/sub, client
/// tracking, replication or hot store. In cluster mode the gate only refuses `SELECT`,
/// `SWAPDB` and `MOVE` (never plain commands) and `dispatch_inner` only adds the `CROSSSLOT`
/// check, which the caller does with `command_slot` for a command on several keys.
#[inline]
fn lean_session(
    state: &ShardState,
    session: &TxSession,
    auth: &crate::acl::ConnectionAuth,
) -> bool {
    !session.is_multi()
        && !session.in_pubsub
        && !session.tracking
        && auth.authenticated
        && auth.rules.all_commands
        && auth.rules.all_keys
        && state.role.get() == crate::config::ReplicationRole::Off
        && (state.hot_store.is_none() || state.published_config.hot_keys.is_empty())
        && state.tracker.borrow().is_empty()
}

/// `dispatch_command` for a [`plain_command`] on a [`lean_session`]: the same steps without
/// the session gate, the connection-layer intercepts and the async frames. Generic commands
/// pay for code size in instruction-cache misses, so this path stays short.
#[inline(never)]
fn dispatch_plain(
    state: &Rc<ShardState>,
    output: &mut BoundedOutput,
    session: &TxSession,
    rate: &mut CommandRateLimiter,
    command: Command<'_>,
    frame: &[u8],
) -> io::Result<()> {
    let threshold = state.live.slowlog_threshold_micros.get();
    let started = (threshold >= 0).then(crate::time::Stamp::now);

    if !rate.allow() {
        return write_response(output, Response::Error(crate::admission::RATE_LIMIT_ERROR));
    }

    if !state.waiters.borrow().is_empty() {
        expire_waiters(state);
    }

    if command_mutates_store(&command) {
        bump_mutations(state);
    }

    {
        let mut store = state.store.borrow_mut();
        let mut waiters = state.waiters.borrow_mut();
        let result = if state.published_config.persistence.profile.uses_wal() {
            let mut persistence = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            execute_in_database(
                &mut store,
                command,
                &mut *persistence,
                &mut *waiters,
                session.database,
            )
            .map_err(|error| refused_write(&error))
        } else {
            execute_in_database(
                &mut store,
                command,
                &mut NoopMutationSink,
                &mut *waiters,
                session.database,
            )
            .map_err(|never| match never {})
        };

        match result {
            Ok(response) => write_response_in(output, response, session.resp)?,
            Err(error) => write_response(output, Response::Error(error))?,
        }
    }

    flush_key_notices(state);

    if let Some(started) = started {
        let micros = started.elapsed_micros();

        if micros >= threshold as u64 {
            clients::SlowSample::plain(frame).record(state, micros, session);
        }
    }

    Ok(())
}

/// Times the command for `SLOWLOG` and keeps the client registry current, then runs it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn dispatch_command<'s>(
    state: &'s Rc<ShardState>,
    output: &mut BoundedOutput,
    session: &mut TxSession,
    auth: &mut crate::acl::ConnectionAuth,
    permit: &mut ConnectionPermit,
    rate: &mut CommandRateLimiter,
    command: Command<'_>,
    frame: &[u8],
    conn: u64,
    shutdown: &ShutdownToken,
    peer: Option<Peer<'_>>,
    deferred: Option<&mut Option<Parking<'s>>>,
) -> io::Result<()> {
    let threshold = state.live.slowlog_threshold_micros.get();
    let sample = (threshold >= 0).then(|| clients::SlowSample::of(&command, frame));
    let identity = clients::is_identity_change(&command);
    let started = sample.as_ref().map(|_| crate::time::Stamp::now());
    let result = dispatch_inner(
        state, output, session, auth, permit, rate, command, conn, shutdown, peer, deferred,
    )
    .await;

    if identity {
        clients::refresh(state, conn, session, auth);
    }

    if let (Some(sample), Some(started)) = (sample, started)
        && !sample.blocking
    {
        let micros = started.elapsed_micros();

        if micros >= threshold as u64 {
            sample.record(state, micros, session);
        }
    }

    result
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_inner<'s>(
    state: &'s Rc<ShardState>,
    output: &mut BoundedOutput,
    session: &mut TxSession,
    auth: &mut crate::acl::ConnectionAuth,
    permit: &mut ConnectionPermit,
    rate: &mut CommandRateLimiter,
    command: Command<'_>,
    conn: u64,
    shutdown: &ShutdownToken,
    peer: Option<Peer<'_>>,
    mut deferred: Option<&mut Option<Parking<'s>>>,
) -> io::Result<()> {
    drain_replicated(state);

    match gate(
        &mut SessionCtx {
            auth,
            tx: session,
            permit,
            rate,
            security: &state.security,
            cluster: state.cluster_view().is_some(),
            write_refusal: state.write_gate.get().refusal(),
            replication: state.role.get() != crate::config::ReplicationRole::Off,
            databases: state.published_config.protocol.databases,
        },
        &command,
    ) {
        SessionAction::Reply(response) => {
            if matches!(command, Command::ClientTracking(false))
                && matches!(response, Response::Simple(b"OK"))
            {
                state.tracker.borrow_mut().forget(conn);
            }

            write_response_in(output, response, session.resp)?;

            return Ok(());
        }

        SessionAction::Encoded(bytes) => {
            output.write_all(bytes)?;

            return Ok(());
        }

        SessionAction::Close => return Ok(()),
        SessionAction::NeedStore => {
            if let Command::Wait {
                replicas,
                timeout_ms,
            } = &command
            {
                if let Some(slot) = deferred.as_deref_mut()
                    && state.role.get() == crate::config::ReplicationRole::Primary
                {
                    *slot = Some(Parking::Wait(Wait::Replicas {
                        replicas: *replicas,
                        timeout_ms: *timeout_ms,
                    }));

                    return Ok(());
                }

                let reached = wait_for_durable_replicas(
                    state,
                    session.forwarded_shards,
                    *replicas,
                    *timeout_ms,
                    shutdown,
                )
                .await;

                write_response(output, Response::Integer(reached as i64))?;

                return Ok(());
            }
        }
    }

    // A hot key lives in the shared hot store, outside the shard's transaction and its undo
    // log: queueing it would run it ahead of EXEC as its own record. Refuse it, so EXEC
    // aborts like after any command refused while queueing.
    if session.is_multi()
        && session.database == 0
        && state.hot_store.is_some()
        && crate::hot_store::dispatch(&state.published_config.hot_keys, &command)
            != crate::hot_store::HotDispatch::PassThrough
    {
        session.mark_queue_error();
        output.write_all(HOT_KEY_IN_MULTI_REPLY)?;

        return Ok(());
    }

    if session.database == 0
        && reply_hot_store(
            state,
            &command,
            output,
            session.resp,
            conn,
            session.tracking,
        )?
    {
        return Ok(());
    }

    if !session.is_multi() {
        match &command {
            Command::ClusterSlots => {
                write_cluster_slots(output, state.cluster_view(), &state.shard_peers())?;

                return Ok(());
            }

            Command::ClusterNodes => {
                write_cluster_nodes(output, state.cluster_view(), &state.shard_peers())?;

                return Ok(());
            }

            Command::ClusterShards => {
                write_cluster_shards(output, state.cluster_view(), &state.shard_peers())?;

                return Ok(());
            }

            Command::ClusterInfo => {
                write_cluster_info(output, state.cluster_view(), &state.shard_peers())?;

                return Ok(());
            }

            Command::ClusterMyId => {
                write_cluster_myid(output, state.cluster_view())?;

                return Ok(());
            }

            Command::ClusterKeySlot(key) => {
                write_key_slot(output, key.as_ref())?;

                return Ok(());
            }

            Command::ReadOnly | Command::ReadWrite => {
                write_read_mode(output, state.cluster_view())?;

                return Ok(());
            }

            Command::ConfigGet(pattern) => {
                write_response(
                    output,
                    crate::engine::config_get_response(&state.live.current(), pattern),
                )?;

                return Ok(());
            }

            Command::ConfigSet(pairs) => {
                let pairs: Vec<(&[u8], &[u8])> = pairs
                    .iter()
                    .map(|(name, value)| (name.as_ref(), value.as_ref()))
                    .collect();

                write_response(output, super::live::config_set(state, &pairs).await)?;

                return Ok(());
            }

            Command::ConfigRewrite => {
                write_response(output, super::live::config_rewrite(state))?;

                return Ok(());
            }

            Command::MoveSlots(target) => {
                write_response(output, super::moveslots::command(state, *target))?;

                return Ok(());
            }

            Command::Topology(map) => {
                super::topology::command(state, map.as_deref(), output)?;

                return Ok(());
            }

            Command::Migrate(args) => {
                let args: Vec<&[u8]> = args.iter().map(AsRef::as_ref).collect();

                write_response(output, super::migrate::command(state, &args))?;

                return Ok(());
            }

            Command::ReplicaOf { primary, force } => {
                let primary = primary
                    .as_ref()
                    .map(|(host, port)| (host.as_ref(), port.as_ref()));

                write_response(
                    output,
                    super::replica::replicaof(state, primary, *force).await,
                )?;

                return Ok(());
            }

            Command::Role => {
                write_response(output, super::replica::role_response(state))?;

                return Ok(());
            }

            Command::Save | Command::Bgsave => {
                let response = if matches!(command, Command::Bgsave) {
                    match super::checkpoint::start_background_checkpoint(state) {
                        Ok(super::checkpoint::BackgroundSave::Started) => {
                            Response::Simple(b"Background saving started")
                        }
                        Ok(super::checkpoint::BackgroundSave::AlreadyRunning) => {
                            Response::Error(b"ERR Background save already in progress")
                        }
                        Err(_) => Response::Error(b"MISCONF persistence error"),
                    }
                } else {
                    match super::checkpoint::save_every_shard(state) {
                        Ok((requested, mut waiting)) => {
                            // With several shards the save is done once a round taken after
                            // the request is published. Once shutdown begins, shards stop
                            // taking requested checkpoints and only the final round, written
                            // as the server stops, may publish: the save fails at once
                            // instead of waiting out its timeout. So does one whose round
                            // failed or whose checkpoint failed on any shard, with the error
                            // a single shard's SAVE gives.
                            // `waiting` makes shards start this request's files past their
                            // throttles. It is dropped on every way out (an answer, the
                            // timeout, shutdown) and as soon as the client closes its side:
                            // a half-closed client still gets its answer, without priority.
                            let deadline = Instant::now() + SAVE_ROUND_TIMEOUT;
                            let closed = peer_closed(peer);

                            tokio::pin!(closed);
                            while !super::checkpoint::save_published(state, requested)
                                && !super::checkpoint::save_failed(state, requested)
                                && !shutdown.is_requested()
                                && Instant::now() < deadline
                            {
                                tokio::select! {
                                    () = tokio::time::sleep(Duration::from_millis(5)) => {}
                                    () = &mut closed, if waiting.is_some() => waiting = None,
                                }
                            }
                            drop(waiting);
                            if super::checkpoint::save_published(state, requested) {
                                Response::Simple(b"OK")
                            } else if super::checkpoint::save_failed(state, requested) {
                                Response::Error(b"MISCONF persistence error")
                            } else if shutdown.is_requested() {
                                Response::Error(b"ERR SAVE: the server is shutting down")
                            } else {
                                Response::Error(
                                    b"ERR SAVE: not every shard finished its checkpoint in time",
                                )
                            }
                        }
                        Err(_) => Response::Error(b"MISCONF persistence error"),
                    }
                };

                write_response(output, response)?;

                return Ok(());
            }

            Command::ChangesSubscribe(subscription) => {
                let outbound = state.subscribers.borrow().get(&conn).cloned();
                let started = match outbound {
                    Some(outbound) => cdc::start(
                        state,
                        outbound,
                        subscription.clone(),
                        session.resp == crate::engine::RespVersion::V3,
                        shutdown.clone(),
                    ),
                    None => Err(&b"ERR connection cannot receive pushes"[..]),
                };

                match started {
                    Ok(cursor) => {
                        write_response_in(
                            output,
                            Response::Bulk(Some(std::borrow::Cow::Owned(
                                cursor.text().into_bytes(),
                            ))),
                            session.resp,
                        )?;
                    }
                    Err(message) => {
                        output.write_all(b"-")?;
                        output.write_all(message)?;
                        output.write_all(b"\r\n")?;
                    }
                }

                return Ok(());
            }

            Command::Ruvio(Some(word))
                if word.eq_ignore_ascii_case(b"hotkeys")
                    || word.eq_ignore_ascii_case(b"bigkeys") =>
            {
                let stats = state.key_stats.borrow();
                let pairs: Vec<(std::borrow::Cow<'_, [u8]>, Response<'_>)> =
                    if word.eq_ignore_ascii_case(b"hotkeys") {
                        stats
                            .hot_keys()
                            .into_iter()
                            .map(|(key, count, _)| {
                                (
                                    std::borrow::Cow::Borrowed(key),
                                    Response::Integer(count as i64),
                                )
                            })
                            .collect()
                    } else {
                        stats
                            .big_keys()
                            .into_iter()
                            .map(|(key, bytes)| {
                                (
                                    std::borrow::Cow::Borrowed(key),
                                    Response::Integer(bytes as i64),
                                )
                            })
                            .collect()
                    };
                write_response(output, Response::Map(pairs))?;

                return Ok(());
            }

            Command::Info(section) => {
                write_evented_info(output, state, section.as_deref())?;

                return Ok(());
            }

            Command::ClientList { .. }
            | Command::ClientInfo
            | Command::ClientKill { .. }
            | Command::Slowlog(_)
            | Command::LatencyLatest => {
                clients::answer_client(state, session, auth, conn, &command, output)?;

                return Ok(());
            }

            _ => {}
        }
    }

    if ((session.is_multi() && state.enforces_slots()) || state.cluster_view().is_some())
        && !matches!(
            command,
            Command::Multi
                | Command::Exec
                | Command::Discard
                | Command::Watch(_)
                | Command::Unwatch
        )
    {
        // A command refused here inside MULTI is not queued: the transaction fails.
        match command_slot(&command) {
            Err(_) => {
                session.mark_queue_error();
                output.write_all(CROSSSLOT_REPLY)?;

                return Ok(());
            }

            Ok(Some(slot)) if session.watch_slot().is_some_and(|watched| watched != slot) => {
                session.mark_queue_error();
                output.write_all(CROSSSLOT_REPLY)?;

                return Ok(());
            }

            // A key of another shard is forwarded there, never redirected. Only a sharded
            // channel keeps `MOVED`: a subscription cannot be forwarded, so a cluster client
            // subscribes on the owner, as with Redis Cluster.
            Ok(Some(slot))
                if matches!(
                    command,
                    Command::ShardPublish { .. }
                        | Command::ShardSubscribe(_)
                        | Command::ShardUnsubscribe(_)
                ) && redirect_if_remote(output, state.cluster_view(), slot)? =>
            {
                session.mark_queue_error();

                return Ok(());
            }

            _ => {}
        }
    }

    if apply_sharded_pubsub(state, session, output, &command)? {
        return Ok(());
    }

    match session.prepare(command) {
        PrepareResult::Immediate(command)
            if matches!(
                command,
                Command::Eval { .. }
                    | Command::EvalSha { .. }
                    | Command::ScriptLoad(_)
                    | Command::ScriptExists(_)
                    | Command::ScriptFlush
                    | Command::ScriptKill
                    | Command::FunctionLoad { .. }
                    | Command::FunctionList
                    | Command::FunctionDelete(_)
                    | Command::FCall { .. }
            ) =>
        {
            expire_waiters(state);
            let mutates = matches!(
                command,
                Command::Eval { .. } | Command::EvalSha { .. } | Command::FCall { .. }
            );
            let track_plan = crate::tracking::plan(&command, session.database);
            let record_reads = session.tracking;

            let response = {
                let mut store = state.store.borrow_mut();
                let mut persistence = state
                    .persistence
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let mut waiters = state.waiters.borrow_mut();
                let mut scripts = state.scripts.borrow_mut();
                let rules = &auth.rules;

                scripts.restrict_next_script(
                    (!(rules.all_commands && rules.all_keys)).then(|| std::sync::Arc::clone(rules)),
                );
                match crate::script::execute_script_command(
                    &mut store,
                    &mut *persistence,
                    &mut *waiters,
                    &mut scripts,
                    state.live.with(|live| live.scripts),
                    command,
                    session.database,
                ) {
                    Ok(response) => response,
                    Err(error) => Response::Error(refused_write(&error)),
                }
            };

            flush_key_notices(state);

            if mutates && !matches!(response, Response::Error(_) | Response::OwnedError(_)) {
                bump_mutations(state);
            }

            let applied = crate::tracking::response_applied(&response);

            write_response_in(output, response, session.resp)?;
            observe_tracking(state, conn, record_reads, track_plan, applied);
        }

        PrepareResult::Immediate(command) => {
            expire_waiters(state);
            let record_reads = session.tracking;
            let track_plan = if record_reads || !state.tracker.borrow().is_empty() {
                crate::tracking::plan(&command, session.database)
            } else {
                crate::tracking::TrackPlan::default()
            };

            if command_mutates_store(&command) {
                bump_mutations(state);
            }

            if flushes_hot_store(&command, session.database) && !flush_hot_store(state) {
                write_response(output, Response::Error(b"MISCONF persistence error"))?;

                return Ok(());
            }

            let retry_copy = match &command {
                Command::ListMove { block: Some(_), .. }
                | Command::ListMPop { block: Some(_), .. }
                | Command::FillGet {
                    wait_ms: Some(_), ..
                } => Some(command.clone().into_owned()),
                _ => None,
            };
            // A waiter is registered only once the loop has flushed the replies ahead of
            // it: the command runs again then, so a failed flush leaves nothing parked.
            let again = deferred.is_some()
                && matches!(
                    command,
                    Command::ListBlock { .. }
                        | Command::SortedSetBlockPop { .. }
                        | Command::StreamRead { block: Some(_), .. }
                        | Command::StreamGroupRead { block: Some(_), .. }
                );
            let mut retry_timeout = None;

            let blocked = {
                let mut store = state.store.borrow_mut();
                let mut waiters = state.waiters.borrow_mut();
                let result = if state.published_config.persistence.profile.uses_wal() {
                    let mut persistence = state
                        .persistence
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());

                    execute_in_database(
                        &mut store,
                        command,
                        &mut *persistence,
                        &mut *waiters,
                        session.database,
                    )
                    .map_err(|error| refused_write(&error))
                } else {
                    let mut sink = NoopMutationSink;

                    execute_in_database(
                        &mut store,
                        command,
                        &mut sink,
                        &mut *waiters,
                        session.database,
                    )
                    .map_err(|never| match never {})
                };

                match result {
                    Ok(Response::BlockList {
                        keys,
                        timeout_ms,
                        sorted,
                        ..
                    }) => Some(ParkRequest::List {
                        keys: keys
                            .into_iter()
                            .map(|key| key.into_owned())
                            .collect::<Vec<_>>(),
                        timeout_ms,
                        sorted,
                    }),
                    Ok(Response::BlockStream {
                        keys,
                        cursors,
                        timeout_ms,
                    }) => Some(ParkRequest::Stream {
                        keys: keys
                            .into_iter()
                            .map(|key| key.into_owned())
                            .collect::<Vec<_>>(),
                        cursors,
                        timeout_ms,
                    }),
                    Ok(Response::BlockGroup {
                        key,
                        group,
                        consumer,
                        cursor,
                        timeout_ms,
                        noack,
                    }) => Some(ParkRequest::Group {
                        key: key.into_owned(),
                        group: group.into_owned(),
                        consumer: consumer.into_owned(),
                        cursor,
                        noack,
                        timeout_ms,
                    }),
                    Ok(Response::BlockRetry { timeout_ms }) => {
                        retry_timeout = Some(timeout_ms);
                        None
                    }
                    Ok(response) => {
                        let applied = crate::tracking::response_applied(&response);

                        write_response_in(output, response, session.resp)?;
                        observe_tracking(state, conn, record_reads, track_plan, applied);
                        None
                    }

                    Err(error) => {
                        write_response(output, Response::Error(error))?;
                        None
                    }
                }
            };

            // `observe_tracking` ran while the store was held above.
            if record_reads && !state.tracker.borrow().is_empty() {
                state.store.borrow_mut().set_tracking_active(true);
            }
            flush_key_notices(state);

            if let (Some(timeout_ms), Some(command)) = (retry_timeout, retry_copy) {
                let Some(retry) = begin_retry(
                    state,
                    session,
                    output,
                    command,
                    timeout_ms,
                    conn,
                    record_reads,
                )?
                else {
                    return Ok(());
                };

                match deferred.as_deref_mut() {
                    Some(slot) => *slot = Some(Parking::Wait(Wait::Retry(Box::new(retry)))),
                    None => {
                        finish_retry(state, session, output, shutdown, peer, conn, retry).await?
                    }
                }
            }

            if let Some(request) = blocked {
                if let (Some(deferred), true) = (deferred, again) {
                    let resolved = match request {
                        ParkRequest::Stream { cursors, .. } => Some(cursors),
                        _ => None,
                    };

                    *deferred = Some(Parking::Again(resolved));

                    return Ok(());
                }

                let slot = EncodedReplySlot::default();
                let parked = state.waiters.borrow_mut().park(
                    request,
                    Rc::clone(&slot),
                    session.resp.null_aggregate().to_vec(),
                );
                let (waiter_id, deadline) = match parked {
                    Ok(parked) => parked,
                    Err(_) => {
                        output.write_all(b"-ERR max number of blocking clients reached\r\n")?;

                        return Ok(());
                    }
                };
                let served = Rc::clone(&slot);
                let encoded = await_block(
                    state,
                    waiter_id,
                    slot,
                    deadline,
                    shutdown,
                    peer,
                    session.resp.null_aggregate(),
                )
                .await
                .ok_or_else(closed_while_blocked)?;

                #[cfg(test)]
                if DELAY_SERVED_POPS
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .contains(&state.published_config.persistence.data_dir)
                {
                    // Let the serving write's commit (its yield, then its sync) run first.
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                }

                session.note_served_after_park(served.served_by());
                output.write_all(&encoded)?;
            }
        }

        PrepareResult::Reply(response) => write_response(output, response)?,
        PrepareResult::Watch(keys) => {
            match common_slot(keys.iter().map(Vec::as_slice)).or_else(|crossed| {
                if state.enforces_slots() {
                    Err(crossed)
                } else {
                    Ok(keys.first().map_or(0, |key| crate::routing::hash_slot(key)))
                }
            }) {
                Ok(slot) => {
                    state
                        .store
                        .borrow_mut()
                        .watch(conn, session.database, &keys);
                    session.accept_watch(
                        slot,
                        keys.len(),
                        state
                            .forwarding
                            .as_ref()
                            .map_or(0, |forwarding| forwarding.me),
                    );
                    write_response(output, Response::Simple(b"OK"))?;
                }

                Err(_) => {
                    output.write_all(CROSSSLOT_REPLY)?;
                }
            }
        }
        PrepareResult::Unwatch => {
            state.store.borrow_mut().unwatch(conn);
            session.clear_watches();
            write_response(output, Response::Simple(b"OK"))?;
        }

        PrepareResult::Exec => {
            if session.is_aborted() {
                state.store.borrow_mut().unwatch(conn);
                session.finish_exec();
                session.clear_watches();
                output.write_all(
                    b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                )?;

                return Ok(());
            }

            let slot = transaction_slot(session.queued());

            if state.enforces_slots()
                && (slot.is_err()
                    || session.watch_slot().is_some_and(|watched| {
                        slot.ok().flatten().is_some_and(|slot| slot != watched)
                    }))
            {
                state.store.borrow_mut().unwatch(conn);
                session.finish_exec();
                session.clear_watches();
                output.write_all(CROSSSLOT_REPLY)?;

                return Ok(());
            }

            let refused = session.take_exec_refusals();

            if session.queued().is_empty() && session.watch_slot().is_none() {
                session.finish_exec();
                write_response_in(
                    output,
                    crate::transaction::splice_refusals(Response::Exec(Vec::new()), refused),
                    session.resp,
                )?;

                return Ok(());
            }

            let mut commands = session.take_exec_commands();
            // The engine does not see the running settings or the server's counters: these
            // are answered here, in their place among the replies.
            // PUBLISH is set aside the same way and sent once the batch is recorded, so a
            // subscriber never hears of a transaction that DISCARD or a refused write undid.
            let server_queries: Vec<(usize, Command<'static>)> = commands
                .iter_mut()
                .enumerate()
                .filter(|(_, command)| {
                    matches!(
                        command,
                        Command::ConfigGet(_) | Command::Info(_) | Command::Publish { .. }
                    )
                })
                .map(|(index, command)| (index, std::mem::replace(command, Command::Ping(None))))
                .collect();
            let scripted = commands.iter().any(|command| {
                matches!(
                    command,
                    Command::Eval { .. } | Command::EvalSha { .. } | Command::FCall { .. }
                )
            });
            let track_plan = crate::tracking::TrackPlan::batch(&commands, session.database);
            let flushes_hot = commands
                .iter()
                .any(|command| flushes_hot_store(command, session.database));

            session.clear_watches();
            expire_waiters(state);
            let mutates = commands.iter().any(command_mutates_store);
            let response = {
                let mut store = state.store.borrow_mut();
                let mut persistence = state
                    .persistence
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let mut waiters = state.waiters.borrow_mut();
                let mut cache = scripted.then(|| state.scripts.borrow_mut());
                let scripts = cache.as_deref_mut().map(|cache| {
                    let rules = &auth.rules;

                    crate::engine::TransactionScripts {
                        cache,
                        limits: state.live.with(|live| live.scripts),
                        rules: (!(rules.all_commands && rules.all_keys))
                            .then(|| std::sync::Arc::clone(rules)),
                    }
                });

                crate::engine::execute_transaction_in(
                    &mut store,
                    &mut *persistence,
                    &mut *waiters,
                    conn,
                    commands,
                    session.database,
                    state.enforces_slots(),
                    scripts,
                )
                .unwrap_or_else(|error| Response::Error(refused_write(&error)))
            };
            let response = match response {
                Response::Exec(mut replies) if !server_queries.is_empty() => {
                    for (index, query) in server_queries {
                        let answer = match &query {
                            Command::ConfigGet(pattern) => {
                                crate::engine::config_get_response(&state.live.current(), pattern)
                            }
                            Command::Info(section) => Response::Bulk(Some(
                                super::info::evented_info(state, section.as_deref())?.into(),
                            )),
                            Command::Publish { channel, message } => {
                                let delivered = publish_classic(state, channel, message);

                                if let Some(forwarding) = state.forwarding.as_ref() {
                                    super::forward::publish_everywhere(
                                        forwarding, channel, message,
                                    );
                                }
                                Response::Integer(delivered)
                            }
                            _ => unreachable!("only CONFIG GET, INFO and PUBLISH are set aside"),
                        };

                        if let Some(reply) = replies.get_mut(index) {
                            *reply = answer;
                        }
                    }
                    Response::Exec(replies)
                }
                response => response,
            };
            let response = crate::transaction::splice_refusals(response, refused);

            flush_key_notices(state);

            if mutates && !matches!(response, Response::NullArray | Response::Error(_)) {
                bump_mutations(state);
            }

            if flushes_hot && matches!(response, Response::Exec(_)) && !flush_hot_store(state) {
                write_response(output, Response::Error(b"MISCONF persistence error"))?;

                return Ok(());
            }

            let applied = crate::tracking::response_applied(&response);

            write_response_in(output, response, session.resp)?;
            observe_tracking(state, conn, false, track_plan, applied);
        }
    }

    Ok(())
}

/// Parked blocking clients wait on their reply slot from their own task: a delivery wakes
/// it, and so do its timeout, a server shutdown or the client hanging up. Returns `None`
/// when the client is gone; its waiter is cancelled, so nothing is popped for it later.
pub(super) async fn await_block(
    state: &ShardState,
    waiter_id: u64,
    slot: EncodedReplySlot,
    deadline: Option<Instant>,
    shutdown: &ShutdownToken,
    peer: Option<Peer<'_>>,
    null: &'static [u8],
) -> Option<Vec<u8>> {
    if shutdown.is_requested() {
        let _ = state.waiters.borrow_mut().cancel(waiter_id);

        return Some(slot.take().unwrap_or_else(|| null.to_vec()));
    }

    let timeout = async {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending().await,
        }
    };

    tokio::select! {
        biased;
        payload = slot.filled() => {
            // Shutdown fills the slot of every parked client with its timeout reply.
            Some(payload)
        }
        _ = timeout => {
            let _ = state.waiters.borrow_mut().cancel(waiter_id);

            Some(slot.take().unwrap_or_else(|| null.to_vec()))
        }
        () = peer_closed(peer) => {
            let _ = state.waiters.borrow_mut().cancel(waiter_id);

            None
        }
    }
}

/// Where a blocked command learns that its client left: the client's own socket, or, for a
/// command forwarded from the client's home shard, the cancel that shard sends when it sees
/// the client go.
#[derive(Clone, Copy)]
pub(super) enum Peer<'a> {
    Socket(&'a TcpStream),
    Forwarded(&'a super::forward::ForwardCancel),
}

/// Resolves when the client has closed its side of the connection. Bytes sent while
/// blocked are left for the request loop; with bytes queued (a TLS close notification,
/// or pipelined input) a close cannot be read without consuming them, so the socket is
/// asked about the peer's shutdown on a slow timer instead.
pub(super) async fn peer_closed(peer: Option<Peer<'_>>) {
    let peer = match peer {
        None => return std::future::pending().await,
        Some(Peer::Forwarded(cancel)) => return cancel.cancelled().await,
        Some(Peer::Socket(peer)) => peer,
    };
    let mut probe = [0u8; 1];

    match peer.peek(&mut probe).await {
        Ok(0) | Err(_) => return,
        Ok(_) => {}
    }

    loop {
        tokio::time::sleep(PEER_SHUTDOWN_CHECK).await;
        if peer_shut_down(peer) {
            return;
        }
    }
}

/// How often a blocked client with unread input is checked for a hang-up.
const PEER_SHUTDOWN_CHECK: Duration = Duration::from_millis(250);

/// True once the peer shut its side down, even with its last bytes still unread:
/// `POLLRDHUP` reports the FIN wherever it is in the receive queue.
#[cfg(target_os = "linux")]
fn peer_shut_down(peer: &TcpStream) -> bool {
    use std::os::fd::AsRawFd;

    let mut poll = libc::pollfd {
        fd: peer.as_raw_fd(),
        events: libc::POLLRDHUP,
        revents: 0,
    };

    // SAFETY: `poll` is one initialised pollfd that lives across the call, the count is
    // 1, and a zero timeout never blocks.
    #[allow(unsafe_code)]
    let ready = unsafe { libc::poll(&mut poll, 1, 0) };

    ready > 0 && poll.revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR) != 0
}

/// macOS has no `POLLRDHUP`; the TCP state says whether the peer's FIN (or a reset) arrived,
/// whatever is still unread.
#[cfg(target_vendor = "apple")]
fn peer_shut_down(peer: &TcpStream) -> bool {
    use std::os::fd::AsRawFd;

    // netinet/tcp_fsm.h: CLOSED, and the states reached after the peer's FIN.
    const TCPS_CLOSED: u8 = 0;
    const TCPS_CLOSE_WAIT: u8 = 5;
    const TCPS_CLOSING: u8 = 7;
    const TCPS_LAST_ACK: u8 = 8;
    const TCPS_TIME_WAIT: u8 = 10;

    let mut info = std::mem::MaybeUninit::<libc::tcp_connection_info>::zeroed();
    let mut length = std::mem::size_of::<libc::tcp_connection_info>() as libc::socklen_t;

    // SAFETY: `info` is a writable buffer of `length` bytes that lives across the call, and
    // `length` is its exact size; the kernel writes at most that much.
    #[allow(unsafe_code)]
    let result = unsafe {
        libc::getsockopt(
            peer.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_CONNECTION_INFO,
            info.as_mut_ptr().cast(),
            &mut length,
        )
    };

    if result != 0 {
        return false;
    }

    // SAFETY: zero-initialised, then filled by the kernel: every field is a plain integer.
    #[allow(unsafe_code)]
    let state = unsafe { info.assume_init() }.tcpi_state;

    matches!(
        state,
        TCPS_CLOSED | TCPS_CLOSE_WAIT | TCPS_CLOSING | TCPS_LAST_ACK | TCPS_TIME_WAIT
    )
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
fn peer_shut_down(_peer: &TcpStream) -> bool {
    false
}

/// Error a connection ends with when its client hung up while blocked.
pub(super) fn closed_while_blocked() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "client closed the connection while blocked",
    )
}

/// The watch a retried blocking command keeps on its keys while it waits, under its own
/// id so the connection's `WATCH` is left alone. Dropping it releases both.
struct RetryWatch<'a> {
    state: &'a ShardState,
    id: u64,
}

impl Drop for RetryWatch<'_> {
    fn drop(&mut self) {
        self.state.store.borrow_mut().unwatch(self.id);
        self.state
            .retry_blockers
            .set(self.state.retry_blockers.get() - 1);
    }
}

/// Retried blocking commands watch under the connection id with this bit set.
const RETRY_WATCH_BIT: u64 = 1 << 63;

/// Re-runs a blocking `BLMOVE`/`BLMPOP` or `FILLGET ... WAIT` whenever one of its keys
/// changes (the same signal `WATCH` uses), until it gets an answer, the timeout passes or
/// the server shuts down. `FILLGET` also retries when the pending fill lapses, as the
/// caller then becomes the filler. The command is applied atomically on each attempt, so no
/// waiter state has to survive between attempts. An idle waiter costs nothing.
///
/// `begin_retry` watches the keys before anything can yield, so a change made while the
/// replies ahead are flushed still wakes the retry; `finish_retry` waits and answers.
fn begin_retry<'s>(
    state: &'s ShardState,
    session: &TxSession,
    output: &mut BoundedOutput,
    command: Command<'static>,
    timeout_ms: Option<u64>,
    conn: u64,
    record_reads: bool,
) -> io::Result<Option<RetryPark<'s>>> {
    let track_plan = crate::tracking::plan(&command, session.database);

    if state.retry_blockers.get() >= crate::list_waiters::DEFAULT_MAX_LIST_WAITERS {
        std::io::Write::write_all(output, b"-ERR max number of blocking clients reached\r\n")?;

        return Ok(None);
    }

    state.retry_blockers.set(state.retry_blockers.get() + 1);

    let watch = RetryWatch {
        state,
        id: conn | RETRY_WATCH_BIT,
    };
    let fill_key = match &command {
        Command::ListMove { source, .. } => {
            let keys = [source.as_ref()];

            state
                .store
                .borrow_mut()
                .watch(watch.id, session.database, &keys);
            None
        }
        Command::ListMPop { keys, .. } => {
            state
                .store
                .borrow_mut()
                .watch(watch.id, session.database, keys);
            None
        }
        Command::FillGet { key, .. } => {
            state
                .store
                .borrow_mut()
                .watch(watch.id, session.database, &[key.as_ref()]);
            Some(key.clone().into_owned())
        }
        _ => None,
    };
    let deadline = timeout_ms
        .filter(|timeout| *timeout > 0)
        .map(|timeout| Instant::now() + Duration::from_millis(timeout));

    Ok(Some(RetryPark {
        watch,
        command,
        fill_key,
        deadline,
        track_plan,
        record_reads,
    }))
}

/// A retried blocking command whose keys are watched, waiting to run again.
pub(super) struct RetryPark<'s> {
    watch: RetryWatch<'s>,
    command: Command<'static>,
    fill_key: Option<Vec<u8>>,
    deadline: Option<Instant>,
    track_plan: crate::tracking::TrackPlan,
    record_reads: bool,
}

/// A command that is about to wait. The caller may flush the replies queued ahead of it
/// before calling `finish_parking`. Nothing that happens meanwhile is missed: a retry
/// already watches its keys, and a list or stream pop runs again (`Again`) before it
/// registers a waiter. Dropping a parking, as a failed flush does, leaves nothing behind.
pub(super) enum Parking<'s> {
    /// A blocking pop or read that found nothing, to parse from its frame and run again
    /// through `dispatch_command`; `XREAD` carries the ids its `$` resolved to.
    Again(Option<Vec<(u64, u64)>>),
    Wait(Wait<'s>),
}

/// Commands that may wait on the shard that runs them: blocking pops and reads, retried
/// blocking commands and `WAIT`.
fn waits_on_owner(command: &Command<'_>) -> bool {
    crate::protocol::command_may_block(command) || matches!(command, Command::Wait { .. })
}

/// A parked command `finish_parking` waits for.
pub(super) enum Wait<'s> {
    Retry(Box<RetryPark<'s>>),
    Replicas { replicas: u64, timeout_ms: u64 },
}

/// Waits for a parked command and writes its reply.
pub(super) async fn finish_parking(
    state: &ShardState,
    parking: Wait<'_>,
    session: &mut TxSession,
    output: &mut BoundedOutput,
    shutdown: &ShutdownToken,
    peer: Option<Peer<'_>>,
    conn: u64,
) -> io::Result<()> {
    match parking {
        Wait::Retry(retry) => {
            finish_retry(state, session, output, shutdown, peer, conn, *retry).await
        }
        Wait::Replicas {
            replicas,
            timeout_ms,
        } => {
            let reached = wait_for_durable_replicas(
                state,
                session.forwarded_shards,
                replicas,
                timeout_ms,
                shutdown,
            )
            .await;

            write_response(output, Response::Integer(reached as i64))
        }
    }
}

async fn finish_retry(
    state: &ShardState,
    session: &mut TxSession,
    output: &mut BoundedOutput,
    shutdown: &ShutdownToken,
    peer: Option<Peer<'_>>,
    conn: u64,
    retry: RetryPark<'_>,
) -> io::Result<()> {
    let RetryPark {
        watch,
        command,
        fill_key,
        deadline,
        track_plan,
        record_reads,
    } = retry;
    let outcome = loop {
        if shutdown.is_requested() {
            break None;
        }

        let lapse = fill_key.as_ref().and_then(|key| {
            state
                .store
                .borrow()
                .fill_lapses_in(session.database, key)
                .map(|millis| Instant::now() + Duration::from_millis(millis))
        });
        let wake_at = match (deadline, lapse) {
            (Some(deadline), Some(lapse)) => Some(deadline.min(lapse)),
            (deadline, lapse) => deadline.or(lapse),
        };
        let changed = std::future::poll_fn(|context| {
            let mut store = state.store.borrow_mut();

            if store.take_watch_change(watch.id) || shutdown.is_requested() {
                return std::task::Poll::Ready(());
            }

            store.wake_on_watch(watch.id, context.waker());
            std::task::Poll::Pending
        });
        let timer = async {
            match wake_at {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            biased;
            () = changed => {}
            () = timer => {}
            () = peer_closed(peer) => return Err(closed_while_blocked()),
        }

        if shutdown.is_requested() {
            break None;
        }

        let timed_out = deadline.is_some_and(|deadline| Instant::now() >= deadline);

        // The key's slot moved to another shard while the command waited here.
        if state
            .forwarding
            .as_ref()
            .is_some_and(|forwarding| forwarding.fabric.mover.moved())
            && super::moveslots::runs_elsewhere(state, session, &command)
        {
            break Some(Response::Error(super::moveslots::UNBLOCKED_ERROR));
        }

        // The batch that parked this command sampled the clock once; a retry must see time pass.
        state.store.borrow().sample_time();

        let attempt = {
            let mut store = state.store.borrow_mut();
            let mut waiters = state.waiters.borrow_mut();
            let mut persistence = state
                .persistence
                .lock()
                .unwrap_or_else(|error| error.into_inner());

            execute_in_database(
                &mut store,
                command.clone(),
                &mut *persistence,
                &mut *waiters,
                session.database,
            )
            .map(|response| match response {
                Response::BlockRetry { .. } => None,
                response => Some(response.into_owned()),
            })
        };

        match attempt {
            Ok(None) if timed_out => break None,
            Ok(None) => continue,
            Ok(Some(response)) => break Some(response),
            Err(error) => break Some(Response::Error(refused_write(&error))),
        }
    };

    drop(watch);
    flush_key_notices(state);

    match outcome {
        Some(response) => {
            bump_mutations(state);

            let applied = crate::tracking::response_applied(&response);

            write_response_in(output, response, session.resp)?;
            observe_tracking(state, conn, record_reads, track_plan, applied);
        }
        None if matches!(command, Command::FillGet { .. }) => write_response_in(
            output,
            Response::Array(vec![Some(std::borrow::Cow::Borrowed(b"BUSY")), None]),
            session.resp,
        )?,
        None => std::io::Write::write_all(output, session.resp.null_aggregate())?,
    }

    Ok(())
}

/// Whether the command table marks `command` as one that writes (unknown names count as writes).
fn command_may_write(command: &Command<'_>) -> bool {
    crate::protocol::command_spec(crate::acl::command_acl_name(command).as_bytes())
        .is_none_or(|spec| spec.flags & crate::protocol::flags::WRITE != 0)
}
