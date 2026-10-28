use super::*;

#[test]
fn evented_serves_ping_on_the_owning_thread() {
    let (address, stop, server) = serve_on_ephemeral();

    assert_eq!(request(address, b"*1\r\n$4\r\nPING\r\n"), b"+PONG\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_exec_aborts_after_a_command_refused_while_queueing() {
    let (address, stop, server) = serve_on_ephemeral();
    let replies = request(
        address,
        &resp(&[
            &[b"MULTI"],
            &[b"NOSUCHCOMMAND"],
            &[b"SET", b"{abort}key", b"discarded"],
            &[b"EXEC"],
            &[b"GET", b"{abort}key"],
            &[b"MULTI"],
            &[b"SET", b"{abort}key", b"kept"],
            &[b"EXEC"],
        ]),
    );

    assert_eq!(
        replies,
        b"+OK\r\n-ERR unknown command\r\n+QUEUED\r\n\
          -EXECABORT Transaction discarded because of previous errors.\r\n\
          $-1\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_exec_aborts_after_a_command_refused_by_acl_or_arity_while_queueing() {
    let (address, stop, server) = serve_with(|config| {
        config.security.requirepass = "secret".into();
        config.security.users =
            vec![crate::acl::AclUser::parse("app", "on >app-secret ~app:* +@all -config").unwrap()];
    });
    let refused: [&[&[u8]]; 3] = [
        // A key outside the user's patterns.
        &[b"SET", b"other", b"v"],
        // A command the user may not run.
        &[b"CONFIG", b"GET", b"*"],
        // A wrong number of arguments.
        &[b"GET"],
    ];

    for command in refused {
        let text = String::from_utf8(request(
            address,
            &resp(&[
                &[b"AUTH", b"app", b"app-secret"],
                &[b"MULTI"],
                &[b"SET", b"app:1", b"queued"],
                command,
                &[b"EXEC"],
                &[b"EXISTS", b"app:1"],
            ]),
        ))
        .unwrap();
        let replies: Vec<&str> = text.split_terminator("\r\n").collect();

        assert_eq!(replies[..3], ["+OK", "+OK", "+QUEUED"], "{text}");
        assert!(
            replies[3].starts_with("-NOPERM") || replies[3].starts_with("-ERR"),
            "{text}"
        );
        assert_eq!(
            replies[4..],
            [
                "-EXECABORT Transaction discarded because of previous errors.",
                ":0"
            ],
            "{text}"
        );
    }
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_exec_runs_the_others_when_a_queued_command_has_invalid_arguments() {
    let (address, stop, server) = serve_on_ephemeral();
    let replies = request(
        address,
        &resp(&[
            &[b"MULTI"],
            &[b"SET", b"a", b"1"],
            &[b"MOVE", b"a", b"0"],
            &[b"MOVE", b"a", b"99"],
            &[b"SWAPDB", b"0", b"99"],
            &[b"INCR", b"a"],
            &[b"EXEC"],
            &[b"MULTI"],
            &[b"MOVE", b"a", b"0"],
            &[b"EXEC"],
        ]),
    );

    assert_eq!(
        String::from_utf8(replies).unwrap(),
        "+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n\
         *5\r\n+OK\r\n-ERR source and destination objects are the same\r\n\
         -ERR DB index is out of range\r\n-ERR DB index is out of range\r\n:2\r\n\
         +OK\r\n+QUEUED\r\n*1\r\n-ERR source and destination objects are the same\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_exec_crossslot_empty_and_reused_multi() {
    let (address, stop, server) = serve_on_ephemeral();
    let replies = request(
        address,
        &resp(&[
            &[b"MULTI"],
            &[b"SET", b"{first}key", b"discarded"],
            &[b"SET", b"{second}key", b"discarded"],
            &[b"EXEC"],
            &[b"MULTI"],
            &[b"EXEC"],
            &[b"MULTI"],
            &[b"SET", b"{first}key", b"value"],
            &[b"GET", b"{first}key"],
            &[b"EXEC"],
            &[b"MULTI"],
            &[b"GET", b"{first}key"],
            &[b"EXEC"],
        ]),
    );

    assert_eq!(
        replies,
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n\
              +OK\r\n*0\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$5\r\nvalue\r\n\
              +OK\r\n+QUEUED\r\n*1\r\n$5\r\nvalue\r\n"
    );
    // One shard owns every slot, so a transaction may span hash slots.
    assert_eq!(
        request(address, &resp(&[&[b"GET", b"{second}key"]])),
        b"$9\r\ndiscarded\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_memory_pressure_pipeline_and_slow_reader_recover() {
    let data_dir = tempfile::tempdir().unwrap();
    let (address, stop, server) = serve_with(|config| {
        config.persistence.data_dir = data_dir.path().to_path_buf();
        config.memory.max_shard_bytes = 96 * 1024;
        config.memory.reserved_headroom_bytes = 0;
        config.requests.max_output_buffer_bytes = 20 * 1024;
    });

    let value = vec![b'x'; 8 * 1024];
    let mut writes = Vec::new();
    for index in 0..32 {
        let key = format!("pressure:{index}");
        writes.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${}\r\n",
                key.len(),
                value.len()
            )
            .as_bytes(),
        );
        writes.extend_from_slice(&value);
        writes.extend_from_slice(b"\r\n");
    }
    let replies = request(address, &writes);
    let success = replies
        .windows(5)
        .filter(|part| *part == b"+OK\r\n")
        .count();
    let rejected = replies.windows(5).filter(|part| *part == b"-OOM ").count();
    assert!(
        success > 0 && rejected > 0,
        "unexpected replies: {replies:?}"
    );
    assert_eq!(success + rejected, 32);

    let mut slow = StdTcpStream::connect(address).unwrap();
    slow.set_read_timeout(Some(PATIENCE)).unwrap();
    // One reply of three values is over the limit on its own, so it is refused whole;
    // replies the socket takes as they come do not count against the limit.
    slow.write_all(
        b"*4\r\n$4\r\nMGET\r\n$10\r\npressure:0\r\n$10\r\npressure:0\r\n$10\r\npressure:0\r\n",
    )
    .unwrap();
    slow.shutdown(Shutdown::Write).unwrap();

    assert_eq!(
        request_within(address, b"*1\r\n$4\r\nPING\r\n", PATIENCE),
        b"+PONG\r\n"
    );
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$10\r\npressure:0\r\n"),
        [format!("${}\r\n", value.len()).as_bytes(), &value, b"\r\n"].concat()
    );
    let mut slow_reply = Vec::new();
    slow.read_to_end(&mut slow_reply).unwrap();
    assert_eq!(slow_reply, b"-ERR output buffer limit exceeded\r\n");
    let info = String::from_utf8(request(address, b"*1\r\n$4\r\nINFO\r\n")).unwrap();
    let peak = info
        .lines()
        .find_map(|line| line.strip_prefix("output_buffer_peak_bytes:"))
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(peak >= value.len() && peak <= 20 * 1024, "{info}");

    let mut new_write = b"*3\r\n$3\r\nSET\r\n$5\r\nextra\r\n$8192\r\n".to_vec();
    new_write.extend_from_slice(&value);
    new_write.extend_from_slice(b"\r\n");
    assert!(request(address, &new_write).starts_with(b"-OOM "));
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$5\r\nextra\r\n"),
        b"$-1\r\n"
    );
    for index in 0..32 {
        let key = format!("pressure:{index}");
        let deletion = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{key}\r\n", key.len());
        let deleted = request(address, deletion.as_bytes());
        assert!(deleted == b":1\r\n" || deleted == b":0\r\n", "{deleted:?}");
    }
    assert_eq!(request(address, &new_write), b"+OK\r\n");
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$5\r\nextra\r\n"),
        [format!("${}\r\n", value.len()).as_bytes(), &value, b"\r\n"].concat()
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_standalone_deletes_keys_in_different_hash_slots() {
    let (address, stop, server) = serve_on_ephemeral();

    assert_eq!(
        request(
            address,
            b"*3\r\n$3\r\nSET\r\n$4\r\n\x00key\r\n$6\r\nvalue1\r\n\
                  *3\r\n$3\r\nSET\r\n$7\r\ncounter\r\n$2\r\n41\r\n\
                  *3\r\n$3\r\nDEL\r\n$4\r\n\x00key\r\n$7\r\ncounter\r\n"
        ),
        b"+OK\r\n+OK\r\n:2\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_multi_accepts_keys_in_different_hash_slots_on_one_shard() {
    let (address, stop, server) = serve_on_ephemeral();

    assert_eq!(
        request(
            address,
            b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nDEL\r\n$4\r\n\x00key\r\n$7\r\ncounter\r\n"
        ),
        b"+OK\r\n+QUEUED\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_pipeline_set_get_and_info_on_the_owning_thread() {
    let (address, stop, server) = serve_on_ephemeral();
    let text = String::from_utf8(request(
            address,
            b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$4\r\nINCR\r\n$3\r\nkey\r\n*1\r\n$4\r\nINFO\r\n",
        ))
        .unwrap();

    assert!(
        text.starts_with("+PONG\r\n+OK\r\n$5\r\nvalue\r\n-ERR"),
        "{text:?}"
    );
    assert!(text.contains("ruvio_backend:evented"), "{text}");
    // Every command ahead of INFO in the same batch is counted, the refused INCR too.
    for field in [
        "total_commands_processed:4\r\n",
        "get_commands:1\r\n",
        "set_commands:1\r\n",
        "increment_commands:1\r\n",
        "other_commands:1\r\n",
        "index_memory_bytes:",
        "pooled_value_bytes:",
    ] {
        assert!(text.contains(field), "Missing {field}: {text}");
    }
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_auth_rejects_commands_until_the_password_matches() {
    let (address, stop, server) = serve_with(|config| {
        config.security.requirepass = "secret".into();
    });

    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
        .unwrap();
    let mut denied = [0; 34];

    client.read_exact(&mut denied).unwrap();
    assert_eq!(&denied, b"-NOAUTH Authentication required.\r\n");
    client
        .write_all(
            b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"+OK\r\n+OK\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_acl_named_user_limits_commands_and_keys() {
    let (address, stop, server) = serve_with(|config| {
        config.security.requirepass = "secret".into();
        config.security.users = vec![
            crate::acl::AclUser::parse("app", "on >app-secret ~app:* +get +set +ping +auth +acl")
                .unwrap(),
        ];
    });

    let text = String::from_utf8(request(
        address,
        b"*3\r\n$4\r\nAUTH\r\n$3\r\napp\r\n$10\r\napp-secret\r\n\
              *2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n\
              *3\r\n$3\r\nSET\r\n$5\r\napp:1\r\n$1\r\nv\r\n\
              *2\r\n$3\r\nGET\r\n$5\r\napp:1\r\n\
              *3\r\n$3\r\nSET\r\n$5\r\nother\r\n$1\r\nv\r\n\
              *3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$1\r\n*\r\n",
    ))
    .unwrap();

    assert!(
        text.starts_with("+OK\r\n$3\r\napp\r\n+OK\r\n$1\r\nv\r\n"),
        "{text}"
    );
    assert!(text.contains("NOPERM this user has no permissions to access one or more keys"));
    assert!(text.contains("NOPERM this user has no permissions to run the 'config|get' command"));
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_rate_limits_commands_on_one_connection() {
    let (address, stop, server) = serve_with(|config| {
        config.connections.max_commands_per_second = 2;
    });

    assert_eq!(
        request(
            address,
            b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"
        ),
        b"+PONG\r\n+PONG\r\n-ERR max commands per second exceeded\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_caps_authenticated_connections_per_acl_user() {
    let (address, stop, server) = serve_with(|config| {
        config.security.requirepass = "secret".into();
        config.connections = ConnectionLimits {
            max_global: 8,
            max_per_ip: 8,
            max_per_user: 1,
            ..ConnectionLimits::default()
        };
    });

    let mut first = StdTcpStream::connect(address).unwrap();

    first.set_read_timeout(Some(PATIENCE)).unwrap();
    first
        .write_all(b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n")
        .unwrap();
    let mut ok = [0; 5];

    first.read_exact(&mut ok).unwrap();
    assert_eq!(&ok, b"+OK\r\n");
    assert_eq!(
        request(address, b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n"),
        b"-ERR max number of connections for this user reached\r\n"
    );
    drop(first);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_applies_configured_protocol_limits() {
    let (address, stop, server) = serve_with(|config| {
        config.protocol = ProtocolLimits {
            max_key_bytes: 3,
            ..ProtocolLimits::default()
        };
    });

    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$4\r\nkeys\r\n"),
        b"-ERR key is too large\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_rejects_connections_over_the_per_ip_limit() {
    let (address, stop, server) = serve_with(|config| {
        config.connections = ConnectionLimits {
            max_global: 2,
            max_per_ip: 1,
            ..ConnectionLimits::default()
        };
    });

    let mut first = StdTcpStream::connect(address).unwrap();

    first.set_read_timeout(Some(PATIENCE)).unwrap();
    first.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut pong = [0; 7];

    first.read_exact(&mut pong).unwrap();
    assert_eq!(&pong, b"+PONG\r\n");
    let mut second = StdTcpStream::connect(address).unwrap();

    second.set_read_timeout(Some(PATIENCE)).unwrap();
    let mut response = Vec::new();

    second.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"-ERR max number of clients reached\r\n");
    drop(first);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_closes_idle_connections_after_the_configured_timeout() {
    let (address, stop, server) = serve_with(|config| {
        config.timeouts = ConnectionTimeouts {
            idle: Duration::from_millis(50),
            read: Duration::from_secs(1),
            write: Duration::from_secs(1),
        };
    });

    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    assert!(response.is_empty());
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_limits_inactive_partial_frame_reads() {
    let (address, stop, server) = serve_with(|config| {
        config.timeouts = ConnectionTimeouts {
            idle: Duration::from_secs(1),
            read: Duration::from_millis(50),
            write: Duration::from_secs(1),
        };
    });

    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    client.write_all(b"*2\r\n").unwrap();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"-ERR protocol error\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// The idle timeout counts from the last request, not from the connect: a client that
/// keeps talking for longer than the timeout stays, and is closed once it goes quiet.
#[test]
fn evented_idle_timeout_restarts_with_each_request() {
    const IDLE: Duration = Duration::from_secs(2);
    let (address, stop, server) = serve_with(|config| {
        config.timeouts = ConnectionTimeouts {
            idle: IDLE,
            read: Duration::from_secs(1),
            write: Duration::from_secs(1),
        };
    });
    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    // Twelve requests 250 ms apart span 3 s, past the 2 s idle timeout.
    for _ in 0..12 {
        thread::sleep(Duration::from_millis(250));
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut reply = [0; 7];

        client.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"+PONG\r\n");
    }

    let quiet = std::time::Instant::now();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    assert!(response.is_empty());
    let waited = quiet.elapsed();

    assert!(
        waited >= IDLE - Duration::from_millis(100),
        "closed after {waited:?}"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

/// A partial frame after an idle stretch gets the short read timeout, not what is left of
/// the long idle one.
#[test]
fn evented_read_timeout_after_requests_is_not_the_idle_timeout() {
    let (address, stop, server) = serve_with(|config| {
        config.timeouts = ConnectionTimeouts {
            idle: Duration::from_secs(60),
            read: Duration::from_millis(100),
            write: Duration::from_secs(1),
        };
    });
    let mut client = StdTcpStream::connect(address).unwrap();

    client.set_read_timeout(Some(PATIENCE)).unwrap();
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut reply = [0; 7];

    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
    let started = std::time::Instant::now();

    client.write_all(b"*2\r\n").unwrap();
    let mut response = Vec::new();

    client.read_to_end(&mut response).unwrap();
    assert_eq!(response, b"-ERR protocol error\r\n");
    assert!(started.elapsed() < Duration::from_secs(20));
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn pubsub_queue_peak_tracks_enqueued_frames() {
    let peak_frames = Rc::new(Cell::new(0));
    let peak_bytes = Rc::new(Cell::new(0));
    let outbound = PubSubOut::new(Rc::clone(&peak_frames), Rc::clone(&peak_bytes));
    let frame = Arc::from(&b"message"[..]);
    assert!(outbound.try_push(Arc::clone(&frame)));
    assert!(outbound.try_push(Arc::clone(&frame)));
    assert_eq!(peak_frames.get(), 2);
    assert_eq!(peak_bytes.get(), 14);
    outbound.pop();
    assert!(outbound.try_push(frame));
    assert_eq!(peak_frames.get(), 2);
    assert_eq!(peak_bytes.get(), 14);
    let large: Arc<[u8]> = Arc::from(vec![0u8; crate::pubsub::PUBSUB_QUEUE_BYTES / 2]);
    assert!(outbound.try_push(Arc::clone(&large)));
    assert!(!outbound.try_push(large));
    for _ in 0..1000 {
        outbound.pop();
    }
    for _ in 0..1000 {
        assert!(outbound.try_push(Arc::from(&b"message"[..])));
    }
}

#[test]
fn evented_oversized_and_truncated_frames_do_not_poison_reconnects() {
    let (address, stop, server) = serve_with(|config| {
        config.protocol.max_key_bytes = 8;
        config.protocol.max_value_bytes = 8;
        config.protocol.max_pipeline_commands = 2;
    });

    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$9\r\n"),
        OVERSIZED_REPLY
    );
    assert_eq!(
        request(address, b"*2\r\n$3\r\nGET\r\n$99999999999\r\n"),
        b"-ERR protocol error\r\n"
    );
    assert_eq!(request(address, b"*2\r\n$3\r\nGET\r\n$2\r\nk"), b"");
    // A line that does not start with `*` is an inline command.
    assert_eq!(request(address, b"!\r\n"), b"-ERR unknown command\r\n");
    assert_eq!(
        request(address, b"PING\r\nPING \"x\r\n"),
        b"+PONG\r\n-ERR unbalanced quotes in request\r\n"
    );
    for _ in 0..128 {
        assert_eq!(
            request(address, &b"*1\r\n$4\r\nPING\r\n".repeat(5)),
            b"+PONG\r\n".repeat(5)
        );
    }
    assert_eq!(request(address, b"*1\r\n$4\r\nPING\r\n"), b"+PONG\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_unknown_command_replies_and_keeps_the_connection() {
    let (address, stop, server) = serve_with(|_| {});

    assert_eq!(
        request(address, b"*1\r\n$4\r\nNOPE\r\n*1\r\n$4\r\nPING\r\n"),
        b"-ERR unknown command\r\n+PONG\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_rejected_admin_verbs_reply_and_keep_the_connection() {
    let (address, stop, server) = serve_with(|_| {});

    assert_eq!(
        request(
            address,
            b"*3\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$11\r\nrequirepass\r\n\
                  *1\r\n$8\r\nSHUTDOWN\r\n\
                  *2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n\
                  *3\r\n$7\r\nCLUSTER\r\n$4\r\nMEET\r\n$9\r\n127.0.0.1\r\n\
                  *1\r\n$4\r\nPING\r\n",
        ),
        b"-ERR wrong number of arguments for 'config|set' command\r\n\
              -ERR unknown command\r\n\
              -ERR unknown command\r\n\
              -ERR only CLUSTER SLOTS, NODES, SHARDS, INFO, MYID, and KEYSLOT are supported\r\n\
              +PONG\r\n"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_tls_serves_ping_and_rejects_plaintext() {
    let directory = tempfile::tempdir().unwrap();
    let (cert, key) = crate::tls::write_self_signed_pair(directory.path(), "one");
    let cert_for_client = cert.clone();
    let key_path = key.clone();
    let (address, stop, server) = serve_with(|config| {
        config.security = SecurityConfig {
            tls_cert: cert,
            tls_key: key,
            ..SecurityConfig::default()
        };
    });

    ping_over_tls(address, &cert_for_client);

    let mut plain = StdTcpStream::connect(address).unwrap();

    plain.set_read_timeout(Some(PATIENCE)).unwrap();
    let _ = plain.write_all(b"*1\r\n$4\r\nPING\r\n");
    let mut response = Vec::new();
    let _ = plain.read_to_end(&mut response);

    assert_ne!(response, b"+PONG\r\n");

    let loaded_at = std::fs::metadata(&cert_for_client)
        .unwrap()
        .modified()
        .unwrap();
    let (cert2, key2) = crate::tls::write_self_signed_pair(directory.path(), "two");

    std::fs::copy(&cert2, &cert_for_client).unwrap();
    std::fs::copy(&key2, &key_path).unwrap();
    let mut touch = std::fs::OpenOptions::new()
        .append(true)
        .open(&cert_for_client)
        .unwrap();

    writeln!(touch, "# reloaded").unwrap();
    // Writes within one tick of a file system's clock can keep the old modification time:
    // give the file one the server tells apart whatever the clock's granularity.
    touch
        .set_modified(loaded_at + Duration::from_secs(60))
        .unwrap();
    drop(touch);
    ping_over_tls(address, &cert2);

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_replica_refuses_hot_path_writes_but_serves_reads() {
    let directory = tempfile::tempdir().unwrap();
    let data_dir = directory.path().to_path_buf();
    let (address, stop, server) = serve_with(move |config| {
        config.persistence.profile = crate::DurabilityProfile::Always;
        config.persistence.data_dir = data_dir;
        config.replication.role = crate::config::ReplicationRole::Replica;
        config.replication.primary = Some("127.0.0.1:1".parse().unwrap());
    });
    let reply = String::from_utf8(request(
        address,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
    ))
    .unwrap();

    assert_eq!(reply.matches("-READONLY").count(), 3, "{reply}");
    assert!(reply.ends_with("$-1\r\n"), "{reply}");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

const OVERSIZED_REPLY: &[u8] =
    b"-ERR argument exceeds max_key_bytes/max_value_bytes; command not executed\r\n";

#[test]
fn evented_frame_with_too_many_arguments_is_refused_and_the_connection_kept() {
    let (address, stop, server) = serve_with(|config| {
        config.protocol.max_arguments = 64;
    });
    let mut frame: Vec<&[u8]> = vec![b"ZADD", b"z"];
    frame.extend(std::iter::repeat_n([b"1".as_slice(), b"m"], 200).flatten());
    let mut payload = resp(&[&frame]);
    payload.extend_from_slice(&resp(&[&[b"EXISTS", b"z"], &[b"PING"]]));

    assert_eq!(
        request(address, &payload),
        [
            b"-ERR too many arguments (max_arguments); command not executed\r\n".as_slice(),
            b":0\r\n+PONG\r\n",
        ]
        .concat()
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_oversized_argument_is_refused_and_the_connection_kept() {
    let (address, stop, server) = serve_with(|config| {
        config.protocol.max_key_bytes = 16;
        config.protocol.max_value_bytes = 1024;
        config.protocol.max_command_bytes = 64 * 1024;
    });
    let big = vec![b'v'; 20_000];
    let mut payload = resp(&[&[b"SET", b"a", b"1"]]);

    // The oversized value sits between two arguments, and the frame arrives in pieces.
    payload.extend_from_slice(b"*4\r\n$3\r\nSET\r\n$1\r\nb\r\n");
    payload.extend_from_slice(format!("${}\r\n", big.len()).as_bytes());
    payload.extend_from_slice(&big);
    payload.extend_from_slice(b"\r\n$2\r\nPX\r\n");
    payload.extend_from_slice(&resp(&[&[b"GET", b"a"], &[b"EXISTS", b"b"]]));

    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    for piece in payload.chunks(3_000) {
        client.write_all(piece).unwrap();
        thread::sleep(Duration::from_millis(2));
    }

    let expected = [
        b"+OK\r\n".as_slice(),
        OVERSIZED_REPLY,
        b"$1\r\n1\r\n",
        b":0\r\n",
    ]
    .concat();
    let mut response = vec![0; expected.len()];

    client.read_exact(&mut response).unwrap();
    assert_eq!(response, expected);

    // A frame past `max_command_bytes` is not read through: the connection closes.
    client.write_all(b"*2\r\n$3\r\nGET\r\n$70000\r\n").unwrap();
    let mut rest = Vec::new();

    client.read_to_end(&mut rest).unwrap();
    assert_eq!(rest, b"-ERR protocol error\r\n");
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_pipeline_sends_before_queued_replies_can_reach_the_output_limit() {
    use super::super::connection::flush_threshold;

    // Whatever is left unsent, the next send comes by half the limit, so a reply of up to
    // half the limit queued before it never overruns the limit.
    for maximum in [4 * 1024, 100 * 1024, 1024 * 1024, 512 * 1024 * 1024] {
        for unsent in [0, 1, maximum / 4, maximum / 2 - 1, maximum / 2] {
            let threshold = flush_threshold(unsent, maximum);

            assert!(
                threshold <= maximum / 2,
                "{unsent} of {maximum}: {threshold}"
            );
        }
    }
    // Below that, a client that is not reading is retried at doubling sizes.
    assert_eq!(flush_threshold(0, 1024 * 1024), 64 * 1024);
    assert_eq!(flush_threshold(100 * 1024, 1024 * 1024), 200 * 1024);
    assert_eq!(flush_threshold(524_241, 1024 * 1024), 512 * 1024);
}

#[test]
fn evented_large_pipeline_replies_stream_under_a_small_output_limit() {
    let (address, stop, server) = serve_with(|config| {
        config.requests.max_output_buffer_bytes = 1024 * 1024;
        config.timeouts.write = Duration::from_millis(300);
    });
    let value = vec![b'v'; 32 * 1024];

    assert_eq!(
        request(address, &resp(&[&[b"SET", b"big", &value]])),
        b"+OK\r\n"
    );

    // 600 replies of 32 KiB are 19 MiB, far over the limit, but the client reads them.
    let gets = resp(&[&[b"GET", b"big"]]).repeat(600);
    let reply = [format!("${}\r\n", value.len()).as_bytes(), &value, b"\r\n"].concat();
    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let writer = {
        let mut client = client.try_clone().unwrap();

        thread::spawn(move || client.write_all(&gets).unwrap())
    };
    let mut response = vec![0; reply.len() * 600];

    client.read_exact(&mut response).unwrap();
    join_within(writer, PATIENCE).unwrap();
    assert!(response.chunks(reply.len()).all(|chunk| chunk == reply));
    drop(client);

    // A client that takes nothing for the write timeout while half the limit is unsent
    // is cut; what it was sent is bounded by the limit plus the socket buffers.
    let mut idle = StdTcpStream::connect(address).unwrap();

    idle.set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    idle.write_all(&resp(&[&[b"GET", b"big"]]).repeat(2_000))
        .unwrap();
    // Cut: only the INFO request's own connection is left.
    wait_for_info(address, "connected_clients", |clients| clients == 1);
    let mut received = Vec::new();
    let mut chunk = vec![0; 1 << 20];

    while let Ok(read @ 1..) = idle.read(&mut chunk) {
        received.extend_from_slice(&chunk[..read]);
    }

    assert!(
        received.len() < reply.len() * 1_000,
        "received {} bytes",
        received.len()
    );
    assert_eq!(
        request(address, b"*1\r\n$4\r\nPING\r\n"),
        b"+PONG\r\n",
        "the shard kept serving"
    );
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_shutdown_closes_idle_and_subscribed_clients_at_once() {
    let (address, stop, server) = serve_with(|config| {
        config.shutdown_grace = Duration::from_secs(10);
    });
    let mut idle = StdTcpStream::connect(address).unwrap();
    let mut subscriber = StdTcpStream::connect(address).unwrap();

    for client in [&mut idle, &mut subscriber] {
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }

    idle.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut pong = [0; 7];

    idle.read_exact(&mut pong).unwrap();
    subscriber
        .write_all(&resp(&[&[b"SUBSCRIBE", b"news"]]))
        .unwrap();
    let mut confirm = [0; 32];
    let _ = subscriber.read(&mut confirm).unwrap();

    let started = Instant::now();

    stop.request();
    join_within(server, PATIENCE).unwrap();
    // A shutdown that waits for them takes the whole 10 s grace.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown waited {:?} for idle clients",
        started.elapsed()
    );

    let mut rest = Vec::new();

    assert_eq!(idle.read_to_end(&mut rest).unwrap(), 0);
}

#[test]
fn client_sockets_get_keepalive_and_listeners_a_long_backlog() {
    let listener = super::bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
    let client = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let accepted = super::accept_bounded(&listener);
    let socket = socket2::SockRef::from(&accepted);

    socket
        .set_tcp_keepalive(&super::client_keepalive())
        .unwrap();
    assert!(socket.keepalive().unwrap());
    #[cfg(target_os = "linux")]
    {
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), 9);
    }
    drop(client);
}

/// Like `request`, but keeps its side open: a blocked client that half-closes has left.
fn exchange(address: SocketAddr, payload: &[u8], reply_len: usize) -> Vec<u8> {
    let mut client = StdTcpStream::connect(address).unwrap();

    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client.write_all(payload).unwrap();
    let mut reply = vec![0; reply_len];

    client.read_exact(&mut reply).unwrap();
    reply
}

#[test]
fn evented_blocked_client_that_hangs_up_gives_up_its_waiter() {
    let (address, stop, server) = serve_on_ephemeral();

    for command in [
        &[b"BLPOP".as_slice(), b"q", b"0"][..],
        &[b"BLMOVE", b"q", b"moved", b"LEFT", b"LEFT", b"0"],
    ] {
        let mut gone = connect_bounded(address);

        gone.write_all(&resp(&[command])).unwrap();
        wait_for_parked(address, 1);
        drop(gone);
        wait_for_parked(address, 0);

        assert_eq!(
            request(address, &resp(&[&[b"RPUSH", b"q", b"x"], &[b"LLEN", b"q"]])),
            b":1\r\n:1\r\n",
            "{:?} served a client that had left",
            String::from_utf8_lossy(command[0])
        );
        assert_eq!(request(address, &resp(&[&[b"DEL", b"q"]])), b":1\r\n");
    }

    // More hung-up waiters than the shard may park leave room for a live one.
    for _ in 0..crate::list_waiters::DEFAULT_MAX_LIST_WAITERS + 50 {
        let mut gone = connect_bounded(address);

        gone.write_all(&resp(&[&[b"BLPOP", b"zz", b"0"]])).unwrap();
        let mut ping = connect_bounded(address);
        ping.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut pong = [0; 7];
        ping.read_exact(&mut pong).unwrap();
        drop(gone);
    }
    wait_for_parked(address, 0);
    assert_eq!(
        exchange(address, &resp(&[&[b"BLPOP", b"zz", b"0.1"]]), 5),
        b"*-1\r\n"
    );

    stop.request();
    join_within(server, PATIENCE).unwrap();
}

// A hang-up behind unread bytes is seen only where the OS reports the FIN (Linux, macOS).
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[test]
fn evented_blocked_client_that_hangs_up_with_unread_input_gives_up_its_waiter() {
    // Bytes the parked connection has not read yet (pipelined input, or a TLS
    // close_notify record) hide the FIN from a zero-byte peek.
    let (address, stop, server) = serve_on_ephemeral();
    let mut gone = StdTcpStream::connect(address).unwrap();

    gone.write_all(&resp(&[&[b"BLPOP", b"q", b"0"]])).unwrap();
    wait_for_parked(address, 1);
    gone.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    thread::sleep(Duration::from_millis(50));
    gone.shutdown(std::net::Shutdown::Write).unwrap();
    wait_for_parked(address, 0);

    assert_eq!(
        request(address, &resp(&[&[b"RPUSH", b"q", b"x"], &[b"LLEN", b"q"]])),
        b":1\r\n:1\r\n",
        "the waiter outlived a client that had hung up"
    );
    drop(gone);
    stop.request();
    join_within(server, PATIENCE).unwrap();
}

#[test]
fn evented_parked_clients_wake_on_data_timeout_and_shutdown() {
    let (address, stop, server) = serve_with(|config| {
        config.shutdown_grace = Duration::from_secs(10);
    });
    let started = Instant::now();

    assert_eq!(
        exchange(
            address,
            &resp(&[&[b"BLMOVE", b"none", b"d", b"LEFT", b"LEFT", b"0.15"]]),
            5
        ),
        b"*-1\r\n"
    );
    assert!(started.elapsed() >= Duration::from_millis(150));

    let mut list = connect_bounded(address);
    let mut moved = connect_bounded(address);

    list.write_all(&resp(&[&[b"BLPOP", b"a", b"5"]])).unwrap();
    moved
        .write_all(&resp(&[&[b"BLMOVE", b"b", b"d", b"LEFT", b"LEFT", b"5"]]))
        .unwrap();
    wait_for_parked(address, 2);
    assert_eq!(
        request(
            address,
            &resp(&[&[b"RPUSH", b"a", b"1"], &[b"RPUSH", b"b", b"2"]])
        ),
        b":1\r\n:1\r\n",
        "the parked BLPOP takes its element straight from the push; the push still counts it"
    );
    let mut reply = [0; 18];
    list.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"*2\r\n$1\r\na\r\n$1\r\n1\r\n");
    let mut reply = [0; 7];
    moved.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"$1\r\n2\r\n");

    let mut parked = connect_bounded(address);
    let mut retried = connect_bounded(address);

    parked
        .write_all(&resp(&[&[b"BLPOP", b"never", b"0"]]))
        .unwrap();
    retried
        .write_all(&resp(&[&[
            b"BLMOVE", b"never", b"d", b"LEFT", b"LEFT", b"0",
        ]]))
        .unwrap();
    wait_for_parked(address, 2);

    let stopping = Instant::now();

    stop.request();
    join_within(server, PATIENCE).unwrap();
    // A shutdown that waits for them takes the whole 10 s grace.
    assert!(
        stopping.elapsed() < Duration::from_secs(5),
        "shutdown waited {:?} for blocked clients",
        stopping.elapsed()
    );
    for client in [&mut parked, &mut retried] {
        let mut rest = Vec::new();

        client.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, b"*-1\r\n");
    }
}

/// The admission counters in `INFO prometheus` move: a refused accept, a rate-limited
/// command and an unreadable frame each count. They are process-wide, so other tests can
/// only raise them; each is checked for at least one.
#[test]
fn admission_counters_reach_the_prometheus_reply() {
    let (address, stop, server) = serve_with(|config| {
        config.connections = ConnectionLimits {
            max_global: 2,
            max_per_ip: 1,
            max_commands_per_second: 1,
            ..ConnectionLimits::default()
        };
    });
    let connect = || {
        let stream = StdTcpStream::connect(address).unwrap();

        stream.set_read_timeout(Some(PATIENCE)).unwrap();
        stream
    };
    let mut first = connect();
    let mut refused = Vec::new();

    connect().read_to_end(&mut refused).unwrap();
    assert_eq!(refused, b"-ERR max number of clients reached\r\n");
    first
        .write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
        .unwrap();
    let mut replies = vec![0; 7];

    first.read_exact(&mut replies).unwrap();
    assert_eq!(replies, b"+PONG\r\n");
    first.write_all(b"*x\r\n").unwrap();
    let mut rest = Vec::new();
    let _ = first.read_to_end(&mut rest);
    assert!(
        String::from_utf8_lossy(&rest).contains("max commands per second")
            && rest.ends_with(b"-ERR protocol error\r\n"),
        "{rest:?}"
    );
    drop(first);

    let deadline = Instant::now() + Duration::from_secs(5);
    let metrics = loop {
        let mut client = connect();
        let mut reply = Vec::new();

        client
            .write_all(&resp(&[&[b"INFO", b"prometheus"]]))
            .unwrap();
        client.shutdown(Shutdown::Write).unwrap();
        let _ = client.read_to_end(&mut reply);

        if reply.starts_with(b"$") || Instant::now() > deadline {
            break String::from_utf8(reply).unwrap();
        }
        thread::sleep(Duration::from_millis(20));
    };

    for name in [
        "ruvio_connections_total",
        "ruvio_rejected_connections_total",
        "ruvio_rate_limited_commands_total",
        "ruvio_protocol_errors_total",
        "ruvio_net_input_bytes_total",
        "ruvio_net_output_bytes_total",
    ] {
        let value = metrics
            .lines()
            .find(|line| line.starts_with(&format!("{name}{{")))
            .and_then(|line| line.rsplit(' ').next()?.parse::<u64>().ok());

        assert!(value.is_some_and(|value| value > 0), "{name}: {metrics}");
    }
    stop.request();
    join_within(server, PATIENCE).unwrap();
}
