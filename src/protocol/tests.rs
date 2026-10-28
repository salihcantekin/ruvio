use super::*;
#[cfg(not(miri))]
use proptest::prelude::*;
use std::io::Cursor;

fn owned(value: &[u8]) -> Bytes<'static> {
    Cow::Owned(value.to_vec())
}

#[test]
fn every_supported_command_name_is_recognized() {
    let matrix = include_str!("../../docs/compatibility-matrix.json");
    let marker = "\"name\": \"";
    let mut rest = matrix;
    let mut names = Vec::new();

    while let Some(start) = rest.find(marker) {
        rest = &rest[start + marker.len()..];
        let end = rest.find('"').expect("command name");

        names.push(&rest[..end]);
        rest = &rest[end..];
    }

    assert!(names.len() > 150, "matrix names {}", names.len());

    for name in names {
        let arguments = name.split(' ').map(|part| owned(part.as_bytes())).collect();
        let parsed = parse_arguments(arguments);

        if let Err(error) = parsed {
            let text = error.to_string();

            assert!(
                !text.contains("unknown command"),
                "{name} is no longer accepted: {text}"
            );
        }
    }
}

#[test]
fn json_commands_parse_and_validate_arguments() {
    assert_eq!(
        parse_arguments(vec![owned(b"JSON.GET"), owned(b"doc")]).unwrap(),
        Command::JsonGet {
            key: owned(b"doc"),
            path: owned(b"$")
        }
    );
    assert_eq!(
        parse_arguments(vec![owned(b"JSON.DEL"), owned(b"doc"), owned(b"$.a")]).unwrap(),
        Command::JsonDel {
            key: owned(b"doc"),
            path: owned(b"$.a")
        }
    );
    assert_eq!(
        parse_arguments(vec![
            owned(b"JSON.SET"),
            owned(b"doc"),
            owned(b"$"),
            owned(br#"{"a":1}"#)
        ])
        .unwrap(),
        Command::JsonSet {
            key: owned(b"doc"),
            path: owned(b"$"),
            value: owned(br#"{"a":1}"#)
        }
    );
    for name in [b"JSON.MERGE".as_slice(), b"jmerge".as_slice()] {
        assert_eq!(
            parse_arguments(vec![
                owned(name),
                owned(b"doc"),
                owned(b"$.a"),
                owned(b"{}")
            ])
            .unwrap(),
            Command::JsonMerge {
                key: owned(b"doc"),
                path: owned(b"$.a"),
                value: owned(b"{}"),
            }
        );
        assert!(parse_arguments(vec![owned(name), owned(b"doc"), owned(b"$")]).is_err());
    }
    assert!(parse_arguments(vec![owned(b"JSON.SET"), owned(b"doc"), owned(b"$")]).is_err());
    assert!(
        parse_arguments(vec![
            owned(b"JSON.GET"),
            owned(b"doc"),
            owned(b"$"),
            owned(b"x")
        ])
        .is_err()
    );
    let limits = ProtocolLimits {
        max_value_bytes: 3,
        ..ProtocolLimits::default()
    };
    let command = Command::JsonSet {
        key: owned(b"doc"),
        path: owned(b"$"),
        value: owned(b"1234"),
    };
    assert!(validate_command_payloads(&command, limits).is_err());
    assert!(command_mutates_store(&command));
    let merge = Command::JsonMerge {
        key: owned(b"doc"),
        path: owned(b"$"),
        value: owned(b"1234"),
    };
    assert!(validate_command_payloads(&merge, limits).is_err());
    assert!(command_mutates_store(&merge));
    assert!(command_mutates_store(&Command::JsonDel {
        key: owned(b"doc"),
        path: owned(b"$")
    }));
    assert!(!command_mutates_store(&Command::JsonGet {
        key: owned(b"doc"),
        path: owned(b"$")
    }));
}

#[test]
fn wait_parses_a_count_and_rejects_a_negative_timeout() {
    let parsed = parse_arguments(vec![owned(b"WAIT"), owned(b"2"), owned(b"0")]).unwrap();

    assert_eq!(
        parsed,
        Command::Wait {
            replicas: 2,
            timeout_ms: 0
        }
    );
    assert!(parse_arguments(vec![owned(b"WAIT"), owned(b"1"), owned(b"-1")]).is_err());
    assert!(parse_arguments(vec![owned(b"WAIT"), owned(b"1")]).is_err());
}

fn test_limits() -> ProtocolLimits {
    ProtocolLimits {
        max_arguments: 4,
        max_key_bytes: 3,
        max_value_bytes: 5,
        max_command_bytes: 128,
        max_pipeline_commands: 2,
        max_connection_buffer_bytes: 128,
        databases: crate::database::DEFAULT_DATABASES,
    }
}

#[test]
fn parses_supported_commands_and_binary_values() {
    let input = b"*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n\0\xffx\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Ping(None))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::set(owned(b"key"), Cow::Owned(vec![0, 0xff, b'x'])))
    );
    assert_eq!(read_command(&mut reader).unwrap(), None);
}

#[test]
fn xread_pairs_keys_with_ids_after_every_key() {
    let command = parse_arguments(vec![
        owned(b"XREAD"),
        owned(b"STREAMS"),
        owned(b"{t}a"),
        owned(b"{t}b"),
        owned(b"0"),
        owned(b"1-0"),
    ])
    .unwrap();

    match command {
        Command::StreamRead { keys, .. } => {
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0].0.as_ref(), b"{t}a");
            assert_eq!(keys[1].0.as_ref(), b"{t}b");
            assert_eq!(keys[1].1, crate::stream::StreamCursor::After(1, 0));
        }

        other => panic!("{other:?}"),
    }
}

#[test]
fn parses_xpending_range_and_rejects_both_xclaim_clocks() {
    let command = parse_arguments(vec![
        owned(b"XPENDING"),
        owned(b"events"),
        owned(b"workers"),
        owned(b"IDLE"),
        owned(b"5"),
        owned(b"-"),
        owned(b"+"),
        owned(b"2"),
        owned(b"c1"),
    ])
    .unwrap();

    assert!(matches!(
        command,
        Command::StreamPendingRange {
            min_idle: Some(5),
            count: 2,
            ..
        }
    ));
    let rejected = parse_arguments(vec![
        owned(b"XCLAIM"),
        owned(b"events"),
        owned(b"workers"),
        owned(b"c2"),
        owned(b"0"),
        owned(b"1-0"),
        owned(b"IDLE"),
        owned(b"1"),
        owned(b"TIME"),
        owned(b"2"),
    ]);

    assert!(rejected.is_err());
}

#[test]
fn unknown_command_consumes_its_frame_and_leaves_the_next_one() {
    let frame = b"*1\r\n$4\r\nNOPE\r\n*1\r\n$4\r\nPING\r\n";
    let error = parse_command_with_limits(frame, ProtocolLimits::default()).unwrap_err();
    let ProtocolError::Rejected { message, consumed } = error else {
        panic!("expected a rejected command");
    };

    assert_eq!(message, "unknown command");

    let (command, next) = parse_command_with_limits(&frame[consumed..], ProtocolLimits::default())
        .unwrap()
        .unwrap();

    assert!(matches!(command, Command::Ping(None)));
    assert_eq!(consumed + next, frame.len());
}

#[test]
fn rejects_oversized_bulk_strings_before_allocating() {
    let input = format!("*2\r\n$3\r\nGET\r\n${}\r\n", DEFAULT_MAX_VALUE_BYTES + 1);
    let error = read_command(&mut Cursor::new(input)).unwrap_err();

    assert!(matches!(error, ProtocolError::LimitExceeded(_)));
}

#[test]
fn enforces_key_and_value_limits_in_every_parser() {
    let oversized_key = b"*2\r\n$3\r\nGET\r\n$4\r\nkeys\r\n";
    let oversized_value = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\nvalues\r\n";
    let fallback_key = b"*2\r\n$4\r\nTYPE\r\n$4\r\nkeys\r\n";
    let oversized_delete = b"*2\r\n$3\r\nDEL\r\n$4\r\nkeys\r\n";

    for frame in [
        oversized_key.as_slice(),
        oversized_value.as_slice(),
        oversized_delete.as_slice(),
    ] {
        assert!(matches!(
            read_command_with_limits(&mut Cursor::new(frame), test_limits()),
            Err(ProtocolError::LimitExceeded(_))
        ));
        assert!(matches!(
            parse_hot_command_with_limits(frame, test_limits()),
            Ok(HotParse::Fallback)
        ));
        assert!(matches!(
            parse_command_with_limits(frame, test_limits()),
            Err(ProtocolError::LimitExceeded(_)
                | ProtocolError::Rejected { .. }
                | ProtocolError::Oversized { .. })
        ));
    }

    assert_eq!(
        parse_hot_command_with_limits(fallback_key, test_limits()).unwrap(),
        HotParse::Fallback
    );
    assert!(matches!(
        read_command_with_limits(&mut Cursor::new(fallback_key), test_limits()),
        Err(ProtocolError::LimitExceeded(_))
    ));
    assert!(matches!(
        parse_command_with_limits(fallback_key, test_limits()),
        Err(ProtocolError::LimitExceeded(_) | ProtocolError::Rejected { .. })
    ));
}

#[test]
fn oversized_argument_within_the_frame_limit_reports_what_to_skip() {
    let limits = ProtocolLimits {
        max_key_bytes: 8,
        max_value_bytes: 16,
        max_command_bytes: 1024,
        ..ProtocolLimits::default()
    };
    let header = b"*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$100\r\n";

    assert!(matches!(
        parse_command_with_limits(header, limits),
        Err(ProtocolError::Oversized {
            parsed,
            skip: 102,
            args_left: 1,
            ..
        }) if parsed == header.len()
    ));
    // A length the frame limit cannot hold is not skipped.
    assert!(matches!(
        parse_command_with_limits(b"*2\r\n$3\r\nGET\r\n$5000\r\n", limits),
        Err(ProtocolError::LimitExceeded(_))
    ));
    // Too many arguments: skipped and refused while the smallest such frame fits.
    let limits = ProtocolLimits {
        max_arguments: 4,
        ..limits
    };
    assert!(matches!(
        parse_command_with_limits(b"*9\r\n$3\r\nDEL\r\n", limits),
        Err(ProtocolError::Oversized {
            parsed: 4,
            skip: 0,
            args_left: 9,
            ..
        })
    ));
    assert!(matches!(
        parse_command_with_limits(b"*999\r\n$3\r\nDEL\r\n", limits),
        Err(ProtocolError::LimitExceeded(_))
    ));
    assert_eq!(bulk_header(b"$12\r\nabc").unwrap(), Some((5, 12)));
    assert_eq!(bulk_header(b"$12").unwrap(), None);
    assert!(bulk_header(b"x").is_err());
}

#[test]
fn enforces_command_and_argument_limits_before_execution() {
    let frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let mut command_limited = test_limits();

    command_limited.max_command_bytes = frame.len() - 1;
    command_limited.max_connection_buffer_bytes = frame.len() - 1;

    assert!(matches!(
        read_command_with_limits(&mut Cursor::new(frame), command_limited),
        Err(ProtocolError::LimitExceeded(_))
    ));
    assert!(matches!(
        parse_hot_command_with_limits(frame, command_limited),
        Ok(HotParse::Fallback)
    ));
    assert!(matches!(
        parse_command_with_limits(frame, command_limited),
        Err(ProtocolError::LimitExceeded(_))
    ));

    let mut argument_limited = test_limits();

    argument_limited.max_arguments = 2;
    assert!(matches!(
        parse_hot_command_with_limits(frame, argument_limited),
        Ok(HotParse::Fallback)
    ));
    // Refused without running, and skipped so the connection survives.
    assert!(matches!(
        parse_command_with_limits(frame, argument_limited),
        Err(ProtocolError::Oversized { args_left: 3, .. })
    ));
}

#[test]
fn rejects_invalid_limit_relationships() {
    let mut limits = ProtocolLimits::default();

    limits.max_connection_buffer_bytes = limits.max_command_bytes - 1;
    assert_eq!(
        limits.validate(),
        Err("max_connection_buffer_bytes must be at least max_command_bytes")
    );

    limits = ProtocolLimits::default();
    limits.max_pipeline_commands = 0;
    assert_eq!(
        limits.validate(),
        Err("max_pipeline_commands must be greater than zero")
    );
}

#[test]
fn rejects_wrong_command_arity() {
    let input = b"*1\r\n$3\r\nGET\r\n";
    let error = read_command(&mut Cursor::new(input)).unwrap_err();

    assert!(matches!(
        error,
        ProtocolError::Rejected {
            message: "GET expects one argument",
            ..
        }
    ));
}

#[test]
fn parses_acl_commands() {
    let input = b"*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n*2\r\n$3\r\nACL\r\n$5\r\nUSERS\r\n*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$3\r\napp\r\n*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n*5\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$3\r\napp\r\n$2\r\non\r\n$6\r\nnopass\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Acl(AclCommand::WhoAmI))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Acl(AclCommand::Users))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Acl(AclCommand::GetUser(owned(b"app"))))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Acl(AclCommand::Cat(None)))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Acl(AclCommand::SetUser {
            name: owned(b"app"),
            rules: vec![owned(b"on"), owned(b"nopass")],
        }))
    );
}

#[test]
fn parses_the_primary_lease_commands() {
    let frame = |args: &[&str]| {
        let mut text = format!("*{}\r\n", args.len());

        for arg in args {
            text.push_str(&format!("${}\r\n{arg}\r\n", arg.len()));
        }
        text
    };
    let parse = |args: &[&str]| read_command(&mut Cursor::new(frame(args).into_bytes()));

    assert_eq!(
        parse(&["RUVIO", "lease", "begin"]).unwrap(),
        Some(Command::PrimaryLease(None))
    );
    assert_eq!(
        parse(&["RUVIO", "LEASE", "GRANT", "7", "2", "3", "4", "2000"]).unwrap(),
        Some(Command::PrimaryLease(Some(Box::new(
            crate::protocol::LeaseGrant {
                token: 7,
                coordinator: 2,
                voters: 3,
                epoch: 4,
                lease_ms: 2000,
            }
        ))))
    );
    for args in [
        &["RUVIO", "LEASE"][..],
        &["RUVIO", "LEASE", "BEGIN", "1"],
        &["RUVIO", "LEASE", "GRANT", "7", "2", "3", "4"],
        &["RUVIO", "LEASE", "GRANT", "7", "2", "x", "4", "2000"],
        &[
            "RUVIO",
            "LEASE",
            "GRANT",
            "7",
            "2",
            "4294967296",
            "4",
            "2000",
        ],
        &["RUVIO", "LEASE", "RENEW"],
    ] {
        assert!(parse(args).is_err(), "{args:?}");
    }
}

#[test]
fn parses_transaction_commands() {
    let input = b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nEXEC\r\n*1\r\n$7\r\nDISCARD\r\n*2\r\n$5\r\nWATCH\r\n$1\r\nk\r\n*1\r\n$7\r\nUNWATCH\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(read_command(&mut reader).unwrap(), Some(Command::Multi));
    assert_eq!(read_command(&mut reader).unwrap(), Some(Command::Exec));
    assert_eq!(read_command(&mut reader).unwrap(), Some(Command::Discard));
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Watch(vec![owned(b"k")]))
    );
    assert_eq!(read_command(&mut reader).unwrap(), Some(Command::Unwatch));
}

#[test]
fn parses_eval_and_script_commands() {
    let input = b"*5\r\n$4\r\nEVAL\r\n$42\r\nreturn ruvio.call('SET', KEYS[1], ARGV[1])\r\n$1\r\n1\r\n$1\r\nk\r\n$1\r\nv\r\n*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::Eval {
            script: owned(b"return ruvio.call('SET', KEYS[1], ARGV[1])"),
            keys: vec![owned(b"k")],
            args: vec![owned(b"v")],
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::ScriptLoad(owned(b"return 1")))
    );
}

#[test]
fn parses_collection_commands() {
    let input = b"*3\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n$1\r\na\r\n\
                      *4\r\n$4\r\nZADD\r\n$6\r\nscores\r\n$3\r\n1.5\r\n$3\r\nbob\r\n\
                      *4\r\n$6\r\nZRANGE\r\n$6\r\nscores\r\n$1\r\n0\r\n$2\r\n-1\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::ListPush(owned(b"list"), vec![owned(b"a")].into()))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetAdd {
            key: owned(b"scores"),
            pairs: vec![(1.5, owned(b"bob"))].into(),
            options: SortedSetAddOptions::default(),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRange {
            key: owned(b"scores"),
            start: 0,
            stop: -1,
            with_scores: false,
            reverse: false,
        })
    );
}

#[test]
fn parses_set_family_commands() {
    let input = b"*2\r\n$5\r\nSCARD\r\n$3\r\nset\r\n\
                      *2\r\n$8\r\nSMEMBERS\r\n$3\r\nset\r\n\
                      *3\r\n$6\r\nSINTER\r\n$1\r\na\r\n$1\r\nb\r\n\
                      *4\r\n$11\r\nSUNIONSTORE\r\n$3\r\nout\r\n$1\r\na\r\n$1\r\nb\r\n\
                      *4\r\n$5\r\nSMOVE\r\n$4\r\nfrom\r\n$2\r\nto\r\n$1\r\nm\r\n\
                      *2\r\n$11\r\nSRANDMEMBER\r\n$3\r\nset\r\n\
                      *3\r\n$11\r\nSRANDMEMBER\r\n$3\r\nset\r\n$2\r\n-2\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetCard(owned(b"set")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetMembers(owned(b"set")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetAlgebra {
            keys: vec![owned(b"a"), owned(b"b")],
            op: SetOp::Inter,
            destination: None,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetAlgebra {
            keys: vec![owned(b"a"), owned(b"b")],
            op: SetOp::Union,
            destination: Some(owned(b"out")),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetMove {
            source: owned(b"from"),
            destination: owned(b"to"),
            member: owned(b"m"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetRandom {
            key: owned(b"set"),
            count: None,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SetRandom {
            key: owned(b"set"),
            count: Some(-2),
        })
    );
}

#[test]
fn parses_hash_family_commands() {
    let input = b"*4\r\n$4\r\nHSET\r\n$4\r\nuser\r\n$4\r\nname\r\n$3\r\nada\r\n\
                      *3\r\n$4\r\nHGET\r\n$4\r\nuser\r\n$4\r\nname\r\n\
                      *2\r\n$4\r\nHLEN\r\n$4\r\nuser\r\n\
                      *2\r\n$7\r\nHGETALL\r\n$4\r\nuser\r\n\
                      *4\r\n$5\r\nHMGET\r\n$4\r\nuser\r\n$4\r\nname\r\n$4\r\ncity\r\n\
                      *3\r\n$7\r\nHEXISTS\r\n$4\r\nuser\r\n$4\r\nname\r\n\
                      *2\r\n$5\r\nHKEYS\r\n$4\r\nuser\r\n\
                      *2\r\n$5\r\nHVALS\r\n$4\r\nuser\r\n\
                      *3\r\n$4\r\nHDEL\r\n$4\r\nuser\r\n$4\r\nname\r\n\
                      *4\r\n$6\r\nHSETNX\r\n$4\r\nuser\r\n$4\r\nname\r\n$3\r\nada\r\n\
                      *3\r\n$7\r\nHSTRLEN\r\n$4\r\nuser\r\n$4\r\nname\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashSet {
            key: owned(b"user"),
            pairs: vec![(owned(b"name"), owned(b"ada"))],
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashGet {
            key: owned(b"user"),
            field: owned(b"name"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashLen(owned(b"user")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashGetAll(owned(b"user")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashMGet {
            key: owned(b"user"),
            fields: vec![owned(b"name"), owned(b"city")],
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashExists {
            key: owned(b"user"),
            field: owned(b"name"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashKeys(owned(b"user")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashValues(owned(b"user")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashDelete {
            key: owned(b"user"),
            fields: vec![owned(b"name")],
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashSetNx {
            key: owned(b"user"),
            field: owned(b"name"),
            value: owned(b"ada"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::HashStrLen {
            key: owned(b"user"),
            field: owned(b"name"),
        })
    );
}

#[test]
fn parses_sorted_set_family_commands() {
    let input = b"*6\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n\
                      *5\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n-1\r\n$10\r\nWITHSCORES\r\n\
                      *4\r\n$13\r\nZRANGEBYSCORE\r\n$1\r\nz\r\n$4\r\n-inf\r\n$2\r\n(2\r\n\
                      *3\r\n$4\r\nZREM\r\n$1\r\nz\r\n$1\r\na\r\n\
                      *4\r\n$7\r\nZINCRBY\r\n$1\r\nz\r\n$2\r\n.5\r\n$1\r\nb\r\n\
                      *2\r\n$5\r\nZCARD\r\n$1\r\nz\r\n\
                      *3\r\n$6\r\nZSCORE\r\n$1\r\nz\r\n$1\r\nb\r\n\
                      *3\r\n$5\r\nZRANK\r\n$1\r\nz\r\n$1\r\nb\r\n\
                      *3\r\n$8\r\nZREVRANK\r\n$1\r\nz\r\n$1\r\nb\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetAdd {
            key: owned(b"z"),
            pairs: vec![(1.0, owned(b"a")), (2.0, owned(b"b"))].into(),
            options: SortedSetAddOptions::default(),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRange {
            key: owned(b"z"),
            start: 0,
            stop: -1,
            with_scores: true,
            reverse: false,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRangeByScore {
            key: owned(b"z"),
            min: ScoreBound::NegInf,
            max: ScoreBound::Exclusive(2.0),
            with_scores: false,
            reverse: false,
            limit: None,
            via_zrange: false,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRemove {
            key: owned(b"z"),
            members: vec![owned(b"a")],
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetIncr {
            key: owned(b"z"),
            delta: 0.5,
            member: owned(b"b"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetCard(owned(b"z")))
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetScore {
            key: owned(b"z"),
            member: owned(b"b"),
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRank {
            key: owned(b"z"),
            member: owned(b"b"),
            reverse: false,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRank {
            key: owned(b"z"),
            member: owned(b"b"),
            reverse: true,
        })
    );
}

#[test]
fn parses_sorted_set_lex_and_limit() {
    let input = b"*8\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$2\r\n[b\r\n$1\r\n+\r\n$5\r\nBYLEX\r\n$5\r\nLIMIT\r\n$1\r\n1\r\n$2\r\n-1\r\n\
                      *7\r\n$13\r\nZRANGEBYSCORE\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\n2\r\n$5\r\nLIMIT\r\n$1\r\n0\r\n$1\r\n1\r\n\
                      *4\r\n$14\r\nZREVRANGEBYLEX\r\n$1\r\nz\r\n$1\r\n+\r\n$2\r\n[b\r\n";
    let mut reader = Cursor::new(input);

    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRangeByLex {
            key: owned(b"z"),
            min: LexBound::Inclusive(owned(b"b")),
            max: LexBound::PosInf,
            with_scores: false,
            reverse: false,
            limit: Some(RangeLimit {
                offset: 1,
                count: None,
            }),
            invocation: LexInvocation::ZRange,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRangeByScore {
            key: owned(b"z"),
            min: ScoreBound::Inclusive(1.0),
            max: ScoreBound::Inclusive(2.0),
            with_scores: false,
            reverse: false,
            limit: Some(RangeLimit {
                offset: 0,
                count: Some(1),
            }),
            via_zrange: false,
        })
    );
    assert_eq!(
        read_command(&mut reader).unwrap(),
        Some(Command::SortedSetRangeByLex {
            key: owned(b"z"),
            min: LexBound::Inclusive(owned(b"b")),
            max: LexBound::PosInf,
            with_scores: false,
            reverse: true,
            limit: None,
            invocation: LexInvocation::ZRevRangeByLex,
        })
    );
}

#[test]
fn incrementally_parses_one_frame_without_consuming_the_next() {
    let first = b"*1\r\n$4\r\nPING\r\n";
    let second = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
    let mut pipelined = Vec::from(first.as_slice());

    pipelined.extend_from_slice(second);

    let (command, consumed) = parse_command(&pipelined).unwrap().unwrap();

    assert_eq!(command, Command::Ping(None));
    assert_eq!(consumed, first.len());
    assert_eq!(
        parse_command(&pipelined[consumed..]).unwrap(),
        Some((Command::Get(owned(b"key")), second.len()))
    );
    assert_eq!(parse_command(&first[..first.len() - 1]).unwrap(), None);
}

#[test]
fn borrows_fallback_command_arguments_from_the_input_buffer() {
    let frame = b"*4\r\n$4\r\nSADD\r\n$3\r\nset\r\n$5\r\nalpha\r\n$4\r\nbeta\r\n";
    let (command, consumed) = parse_command(frame).unwrap().unwrap();
    let Command::SetAdd(key, members) = command else {
        panic!("expected borrowed SADD command");
    };

    assert_eq!(consumed, frame.len());
    assert!(matches!(key, Cow::Borrowed(_)));
    assert!(
        members
            .iter()
            .all(|member| matches!(member, Cow::Borrowed(_)))
    );
    assert!(key.as_ptr() >= frame.as_ptr());
    assert!(members[1].as_ptr() < frame[frame.len()..].as_ptr());
}

#[test]
fn borrows_hot_command_arguments_from_the_input_buffer() {
    let frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let HotParse::Complete(HotCommand::Set(key, value, None), consumed) =
        parse_hot_command(frame).unwrap()
    else {
        panic!("expected borrowed SET command");
    };

    assert_eq!(consumed, frame.len());
    assert_eq!(key, b"key");
    assert_eq!(value, b"value");
    assert!(key.as_ptr() >= frame.as_ptr());
    assert!(value.as_ptr() < frame[frame.len()..].as_ptr());
    assert_eq!(
        parse_hot_command(&frame[..frame.len() - 1]).unwrap(),
        HotParse::Incomplete
    );

    let increment = b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n";

    assert_eq!(
        parse_hot_command(increment).unwrap(),
        HotParse::Complete(HotCommand::Increment(b"counter", 1), increment.len())
    );

    let increment_by = b"*3\r\n$6\r\nINCRBY\r\n$7\r\ncounter\r\n$2\r\n10\r\n";

    assert_eq!(
        parse_hot_command(increment_by).unwrap(),
        HotParse::Complete(HotCommand::Increment(b"counter", 10), increment_by.len())
    );

    let expiring = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$5\r\n30000\r\n";
    let HotParse::Complete(HotCommand::Set(key, value, Some(30_000)), consumed) =
        parse_hot_command(expiring).unwrap()
    else {
        panic!("expected borrowed SET PX command");
    };

    assert_eq!(consumed, expiring.len());
    assert_eq!(key, b"key");
    assert_eq!(value, b"value");
    assert!(key.as_ptr() >= expiring.as_ptr());

    let deleted = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";

    assert_eq!(
        parse_hot_command(deleted).unwrap(),
        HotParse::Complete(HotCommand::Delete(b"key"), deleted.len())
    );
    assert_eq!(
        parse_hot_command(b"*3\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap(),
        HotParse::Fallback
    );
    assert_eq!(
        parse_hot_command(
            b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n"
        )
        .unwrap(),
        HotParse::Complete(
            HotCommand::Set(b"key", b"value", Some(1_000)),
            b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$1\r\n1\r\n".len(),
        )
    );

    let setex = b"*4\r\n$5\r\nsetex\r\n$3\r\nkey\r\n$2\r\n30\r\n$5\r\nvalue\r\n";
    assert_eq!(
        parse_hot_command(setex).unwrap(),
        HotParse::Complete(HotCommand::SetEx(b"key", b"value", 30_000), setex.len())
    );

    // The full parser words the errors for a bad expiry.
    for seconds in [&b"0"[..], b"-1", b"x", b"9223372036854775807"] {
        let mut frame = b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n".to_vec();
        frame.extend_from_slice(format!("${}\r\n", seconds.len()).as_bytes());
        frame.extend_from_slice(seconds);
        frame.extend_from_slice(b"\r\n$1\r\nv\r\n");
        assert_eq!(parse_hot_command(&frame).unwrap(), HotParse::Fallback);
    }
}

#[test]
fn treats_every_truncated_hot_frame_as_incomplete() {
    let frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";

    for split in 0..frame.len() {
        assert_eq!(
            parse_hot_command(&frame[..split]).unwrap(),
            HotParse::Incomplete,
            "split at byte {split}"
        );
    }
}

#[test]
fn parses_cluster_discovery_commands() {
    let slots = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
    let nodes = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";

    assert_eq!(
        parse_command(slots).unwrap(),
        Some((Command::ClusterSlots, slots.len()))
    );
    assert_eq!(
        parse_command(nodes).unwrap(),
        Some((Command::ClusterNodes, nodes.len()))
    );

    for (frame, command) in [
        (
            &b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n"[..],
            Command::ClusterInfo,
        ),
        (
            &b"*2\r\n$7\r\ncluster\r\n$6\r\nshards\r\n"[..],
            Command::ClusterShards,
        ),
        (
            &b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"[..],
            Command::ClusterMyId,
        ),
        (
            &b"*3\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n$1\r\nk\r\n"[..],
            Command::ClusterKeySlot(owned(b"k")),
        ),
        (&b"*1\r\n$8\r\nREADONLY\r\n"[..], Command::ReadOnly),
        (&b"*1\r\n$9\r\nREADWRITE\r\n"[..], Command::ReadWrite),
    ] {
        assert_eq!(parse_command(frame).unwrap(), Some((command, frame.len())));
    }

    assert!(parse_command(b"*2\r\n$8\r\nREADONLY\r\n$1\r\nx\r\n").is_err());
    assert!(parse_command(b"*2\r\n$7\r\nCLUSTER\r\n$7\r\nKEYSLOT\r\n").is_err());

    let info = b"*2\r\n$4\r\nINFO\r\n$5\r\nstats\r\n";

    assert_eq!(
        parse_command(info).unwrap(),
        Some((Command::Info(Some(owned(b"stats"))), info.len()))
    );

    let save = b"*1\r\n$4\r\nSAVE\r\n";
    let bgsave = b"*1\r\n$6\r\nBGSAVE\r\n";

    assert_eq!(
        parse_command(save).unwrap(),
        Some((Command::Save, save.len()))
    );
    assert_eq!(
        parse_command(bgsave).unwrap(),
        Some((Command::Bgsave, bgsave.len()))
    );
}

#[test]
fn rejects_mutating_admin_and_cluster_verbs() {
    let frames: &[(&[u8], &str)] = &[
        (
            b"*3\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$11\r\nrequirepass\r\n",
            "wrong number of arguments for 'config|set' command",
        ),
        (
            b"*3\r\n$6\r\nCONFIG\r\n$7\r\nREWRITE\r\n$1\r\nx\r\n",
            "wrong number of arguments for 'config|rewrite' command",
        ),
        (
            b"*2\r\n$6\r\nCONFIG\r\n$9\r\nRESETSTAT\r\n",
            "only CONFIG GET, SET and REWRITE are supported",
        ),
        (
            b"*3\r\n$7\r\nCLUSTER\r\n$4\r\nMEET\r\n$9\r\n127.0.0.1\r\n",
            "only CLUSTER SLOTS, NODES, SHARDS, INFO, MYID, and KEYSLOT are supported",
        ),
        (b"*1\r\n$8\r\nSHUTDOWN\r\n", "unknown command"),
        (b"*1\r\n$7\r\nMONITOR\r\n", "unknown command"),
        (
            b"*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n",
            "unknown command",
        ),
    ];

    for (frame, message) in frames {
        let error = parse_command(frame).unwrap_err();
        let ProtocolError::Rejected {
            message: got,
            consumed,
        } = error
        else {
            panic!("frame {frame:?} should be rejected, got {error:?}");
        };

        assert_eq!(got, *message);
        assert_eq!(consumed, frame.len());
    }
}

#[test]
fn parses_set_expiry_and_condition_options() {
    let frame = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n60\r\n";

    assert_eq!(
        parse_command(frame).unwrap(),
        Some((
            Command::Set(
                owned(b"key"),
                owned(b"value"),
                SetOptions {
                    expiry: SetExpiry::RelativeMs(60_000),
                    condition: SetCondition::Always,
                    get: false,
                }
            ),
            frame.len()
        ))
    );

    let keep = b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n";

    assert_eq!(
        parse_command(keep).unwrap(),
        Some((
            Command::Set(
                owned(b"k"),
                owned(b"v"),
                SetOptions {
                    expiry: SetExpiry::KeepTtl,
                    condition: SetCondition::NotExists,
                    get: false,
                }
            ),
            keep.len()
        ))
    );

    let invalid = b"*6\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n1\r\n$2\r\nPX\r\n";

    let error = parse_command(invalid).unwrap_err();
    let ProtocolError::Rejected {
        message: "syntax error",
        consumed,
    } = error
    else {
        panic!("conflicting SET expiry should be rejected, got {error:?}");
    };

    assert_eq!(consumed, invalid.len());
}

#[test]
fn parses_string_family_commands() {
    let mget = b"*3\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n";

    assert_eq!(
        parse_command(mget).unwrap(),
        Some((Command::MGet(vec![owned(b"a"), owned(b"b")]), mget.len()))
    );

    let mset = b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n";

    assert_eq!(
        parse_command(mset).unwrap(),
        Some((
            Command::MSet(vec![(owned(b"a"), owned(b"1")), (owned(b"b"), owned(b"2"))]),
            mset.len()
        ))
    );

    let getset = b"*3\r\n$6\r\nGETSET\r\n$1\r\nk\r\n$1\r\nv\r\n";

    assert_eq!(
        parse_command(getset).unwrap(),
        Some((Command::GetSet(owned(b"k"), owned(b"v")), getset.len()))
    );

    let append = b"*3\r\n$6\r\nAPPEND\r\n$1\r\nk\r\n$1\r\nx\r\n";

    assert_eq!(
        parse_command(append).unwrap(),
        Some((Command::Append(owned(b"k"), owned(b"x")), append.len()))
    );

    let strlen = b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n";

    assert_eq!(
        parse_command(strlen).unwrap(),
        Some((Command::StrLen(owned(b"k")), strlen.len()))
    );

    let incrby = b"*3\r\n$6\r\nINCRBY\r\n$1\r\nk\r\n$2\r\n10\r\n";

    assert_eq!(
        parse_command(incrby).unwrap(),
        Some((Command::Increment(owned(b"k"), 10), incrby.len()))
    );

    let decrby = b"*3\r\n$6\r\nDECRBY\r\n$1\r\nk\r\n$1\r\n3\r\n";

    assert_eq!(
        parse_command(decrby).unwrap(),
        Some((Command::Increment(owned(b"k"), -3), decrby.len()))
    );

    let mget = b"*1\r\n$4\r\nMGET\r\n";
    let mget_error = parse_command(mget).unwrap_err();

    assert!(matches!(
        mget_error,
        ProtocolError::Rejected {
            message: "MGET expects at least one key",
            consumed,
        } if consumed == mget.len()
    ));

    let mset = b"*2\r\n$4\r\nMSET\r\n$1\r\na\r\n";
    let mset_error = parse_command(mset).unwrap_err();

    assert!(matches!(
        mset_error,
        ProtocolError::Rejected {
            message: "MSET expects key/value pairs",
            consumed,
        } if consumed == mset.len()
    ));
}

#[test]
fn parses_keyspace_commands() {
    let exists = b"*3\r\n$6\r\nEXISTS\r\n$1\r\na\r\n$1\r\nb\r\n";

    assert_eq!(
        parse_command(exists).unwrap(),
        Some((
            Command::Exists(vec![owned(b"a"), owned(b"b")].into()),
            exists.len()
        ))
    );

    let type_cmd = b"*2\r\n$4\r\nTYPE\r\n$1\r\na\r\n";

    assert_eq!(
        parse_command(type_cmd).unwrap(),
        Some((Command::Type(owned(b"a")), type_cmd.len()))
    );

    let unlink = b"*2\r\n$6\r\nUNLINK\r\n$1\r\na\r\n";

    assert_eq!(
        parse_command(unlink).unwrap(),
        Some((Command::Del(vec![owned(b"a")].into()), unlink.len()))
    );

    let rename = b"*3\r\n$6\r\nRENAME\r\n$1\r\na\r\n$1\r\nb\r\n";

    assert_eq!(
        parse_command(rename).unwrap(),
        Some((
            Command::Rename {
                source: owned(b"a"),
                destination: owned(b"b"),
            },
            rename.len()
        ))
    );

    let scan =
        b"*6\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$2\r\na*\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n";

    assert_eq!(
        parse_command(scan).unwrap(),
        Some((
            Command::Scan {
                cursor: 0,
                pattern: Some(owned(b"a*")),
                count: 2,
                type_name: None,
                slot: None,
            },
            scan.len()
        ))
    );

    let dbsize = b"*1\r\n$6\r\nDBSIZE\r\n";

    assert_eq!(
        parse_command(dbsize).unwrap(),
        Some((Command::DbSize, dbsize.len()))
    );

    assert!(glob_match(b"a*", b"abc"));
    assert!(glob_match(b"a?", b"ab"));
    assert!(!glob_match(b"a?", b"abc"));
    assert!(glob_match(b"*", b""));
}

#[test]
fn parses_select() {
    let select0 = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n";

    assert_eq!(
        parse_command(select0).unwrap(),
        Some((Command::Select(0), select0.len()))
    );
}

#[test]
fn parses_logical_database_commands() {
    let flushdb = b"*1\r\n$7\r\nFLUSHDB\r\n";
    let flushall = b"*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n";
    let swapdb = b"*3\r\n$6\r\nSWAPDB\r\n$1\r\n0\r\n$1\r\n1\r\n";
    let movekey = b"*3\r\n$4\r\nMOVE\r\n$1\r\nk\r\n$1\r\n2\r\n";
    let badflush = b"*2\r\n$7\r\nFLUSHDB\r\n$4\r\nFAST\r\n";

    assert_eq!(
        parse_command(flushdb).unwrap(),
        Some((Command::FlushDb, flushdb.len()))
    );
    assert_eq!(
        parse_command(flushall).unwrap(),
        Some((Command::FlushAll, flushall.len()))
    );
    assert_eq!(
        parse_command(swapdb).unwrap(),
        Some((Command::SwapDb(0, 1), swapdb.len()))
    );
    assert_eq!(
        parse_command(movekey).unwrap(),
        Some((
            Command::Move {
                key: owned(b"k"),
                db: 2
            },
            movekey.len()
        ))
    );
    assert!(parse_command(badflush).is_err());
}

#[test]
fn parses_list_family_commands() {
    let rpush = b"*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\na\r\n";

    assert_eq!(
        parse_command(rpush).unwrap(),
        Some((
            Command::ListPushBack(owned(b"l"), vec![owned(b"a")].into()),
            rpush.len()
        ))
    );
    let rpop = b"*2\r\n$4\r\nRPOP\r\n$1\r\nl\r\n";

    assert_eq!(
        parse_command(rpop).unwrap(),
        Some((
            Command::ListPopBack {
                key: owned(b"l"),
                count: None,
            },
            rpop.len(),
        ))
    );
    let llen = b"*2\r\n$4\r\nLLEN\r\n$1\r\nl\r\n";

    assert_eq!(
        parse_command(llen).unwrap(),
        Some((Command::ListLen(owned(b"l")), llen.len()))
    );
    let lindex = b"*3\r\n$6\r\nLINDEX\r\n$1\r\nl\r\n$2\r\n-1\r\n";

    assert_eq!(
        parse_command(lindex).unwrap(),
        Some((Command::ListIndex(owned(b"l"), -1), lindex.len()))
    );
    let ltrim = b"*4\r\n$5\r\nLTRIM\r\n$1\r\nl\r\n$1\r\n0\r\n$1\r\n1\r\n";

    assert_eq!(
        parse_command(ltrim).unwrap(),
        Some((Command::ListTrim(owned(b"l"), 0, 1), ltrim.len()))
    );
    let blpop = b"*4\r\n$5\r\nBLPOP\r\n$1\r\na\r\n$1\r\nb\r\n$3\r\n0.5\r\n";

    assert_eq!(
        parse_command(blpop).unwrap(),
        Some((
            Command::ListBlock {
                keys: vec![owned(b"a"), owned(b"b")],
                timeout_ms: Some(500),
                back: false,
            },
            blpop.len()
        ))
    );
    let brpop = b"*3\r\n$5\r\nBRPOP\r\n$1\r\na\r\n$1\r\n0\r\n";

    assert_eq!(
        parse_command(brpop).unwrap(),
        Some((
            Command::ListBlock {
                keys: vec![owned(b"a")],
                timeout_ms: None,
                back: true,
            },
            brpop.len()
        ))
    );
}

// cargo-fuzz (`resp_parsers`) already explores this space. Under Miri the
// default 256 × 512-byte cases do not finish in a useful time.
#[cfg(not(miri))]
proptest! {
    #[test]
    fn parsers_never_panic_or_consume_past_arbitrary_input(
        input in proptest::collection::vec(any::<u8>(), 0..512)
    ) {
        if let Ok(HotParse::Complete(_, consumed)) = parse_hot_command(&input) {
            prop_assert!(consumed <= input.len());
        }

        if let Ok(Some((_, consumed))) = parse_command(&input) {
            prop_assert!(consumed <= input.len());
        }
    }
}

#[test]
fn parsers_bound_consume_on_small_garbage_frames() {
    for input in [
        &b""[..],
        &b"\0"[..],
        &b"*999\r\n"[..],
        &b"$999\r\n"[..],
        &[0xff; 32][..],
    ] {
        if let Ok(HotParse::Complete(_, consumed)) = parse_hot_command(input) {
            assert!(consumed <= input.len());
        }

        if let Ok(Some((_, consumed))) = parse_command(input) {
            assert!(consumed <= input.len());
        }
    }
}

#[test]
fn every_truncated_fallback_frame_is_incomplete() {
    let frame = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";

    for length in 0..frame.len() {
        assert_eq!(parse_command(&frame[..length]).unwrap(), None);
    }
}

#[test]
fn incomplete_frame_need_reports_the_missing_payload_size() {
    let limits = ProtocolLimits::default();
    let mut frame = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\n".to_vec();

    frame.extend_from_slice(&[b'x'; 10]);

    let need = incomplete_frame_need(&frame, limits);

    assert_eq!(need, frame.len() - 10 + 100_000 + 2);
    assert!(matches!(
        parse_command_with_limits(&frame, limits),
        Ok(None)
    ));

    // Header-only prefixes and garbage fall back to "one more byte".
    assert_eq!(incomplete_frame_need(b"*3\r", limits), 4);
    assert_eq!(incomplete_frame_need(b"+OK", limits), 4);
}

#[test]
fn glob_supports_classes_ranges_and_escapes() {
    use crate::protocol::glob_match;

    assert!(glob_match(b"k[ab]", b"ka"));
    assert!(!glob_match(b"k[ab]", b"kc"));
    assert!(glob_match(b"k[^ab]", b"kc"));
    assert!(glob_match(b"k[a-c]x*", b"kbxyz"));
    assert!(glob_match(b"a\\*b", b"a*b"));
    assert!(!glob_match(b"a\\*b", b"aXb"));
    assert!(glob_match(b"h?llo*", b"hello world"));
    assert!(!glob_match(b"k[ab", b"kc"));
}

#[test]
fn glob_handles_stars_at_every_position() {
    use crate::protocol::glob_match;

    assert!(glob_match(b"", b""));
    assert!(!glob_match(b"", b"a"));
    assert!(glob_match(b"**", b"abc"));
    assert!(glob_match(b"*c", b"abcabc"));
    assert!(!glob_match(b"*c", b"abcab"));
    assert!(glob_match(b"a*b*c", b"aXbYbZc"));
    assert!(!glob_match(b"a*b*c", b"aXcYb"));
    assert!(glob_match(b"news.*", b"news.tech"));
    assert!(!glob_match(b"news.*", b"new.tech"));
    assert!(glob_match(b"*?", b"x"));
    assert!(!glob_match(b"*?", b""));
}

#[test]
fn glob_with_many_stars_stays_fast_on_a_mismatch() {
    use crate::protocol::glob_match;

    // A recursive backtracking matcher takes seconds here; this one does at most
    // pattern length times value length steps.
    let pattern = b"*a*a*a*a*a*a*a*a*a*a*a*a*b";
    let value = [b'a'; 4096];
    let started = std::time::Instant::now();

    for _ in 0..100 {
        assert!(!glob_match(pattern, &value));
    }

    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();

    for part in parts {
        out.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
        out.extend_from_slice(part);
        out.extend_from_slice(b"\r\n");
    }

    out
}

#[test]
fn parses_client_registry_commands() {
    let parsed = |parts: &[&[u8]]| {
        parse_command(&frame(parts)).map(|found| found.map(|(c, _)| c.into_owned()))
    };

    assert_eq!(
        parsed(&[b"CLIENT", b"LIST", b"ID", b"3", b"7"]).unwrap(),
        Some(Command::ClientList { ids: vec![3, 7] })
    );
    assert_eq!(
        parsed(&[b"CLIENT", b"KILL", b"1.2.3.4:5"]).unwrap(),
        Some(Command::ClientKill {
            id: None,
            addr: Some(owned(b"1.2.3.4:5")),
            skip_me: false,
            legacy: true,
        })
    );
    assert_eq!(
        parsed(&[b"CLIENT", b"KILL", b"ID", b"9", b"SKIPME", b"no"]).unwrap(),
        Some(Command::ClientKill {
            id: Some(9),
            addr: None,
            skip_me: false,
            legacy: false,
        })
    );
    assert!(parsed(&[b"CLIENT", b"KILL", b"ID", b"0"]).is_err());
    assert!(parsed(&[b"CLIENT", b"LIST", b"TYPE", b"bogus"]).is_err());
}

#[test]
fn parses_slowlog_and_latency() {
    let parsed = |parts: &[&[u8]]| {
        parse_command(&frame(parts)).map(|found| found.map(|(c, _)| c.into_owned()))
    };

    assert_eq!(
        parsed(&[b"SLOWLOG", b"GET"]).unwrap(),
        Some(Command::Slowlog(SlowlogOp::Get(10)))
    );
    assert_eq!(
        parsed(&[b"slowlog", b"get", b"-1"]).unwrap(),
        Some(Command::Slowlog(SlowlogOp::Get(usize::MAX)))
    );
    assert_eq!(
        parsed(&[b"SLOWLOG", b"LEN"]).unwrap(),
        Some(Command::Slowlog(SlowlogOp::Len))
    );
    assert!(parsed(&[b"SLOWLOG", b"GET", b"-2"]).is_err());
    assert!(parsed(&[b"SLOWLOG", b"LEN", b"x"]).is_err());
    assert_eq!(
        parsed(&[b"LATENCY", b"LATEST"]).unwrap(),
        Some(Command::LatencyLatest)
    );
}

#[test]
fn xadd_maxlen_accepts_exact_and_approximate_markers() {
    for marker in ["=", "~"] {
        let parsed = parse_words(&["XADD", "s", "MAXLEN", marker, "5", "*", "f", "v"]).unwrap();

        assert!(matches!(
            parsed,
            Command::StreamAdd {
                max_len: Some(5),
                ..
            }
        ));
    }

    assert!(parse_words(&["XADD", "s", "MAXLEN", "5", "*", "f", "v"]).is_ok());
}

fn parse_words(words: &[&str]) -> Result<Command<'static>, ProtocolError> {
    parse_arguments(words.iter().map(|word| owned(word.as_bytes())).collect())
}

#[test]
fn bitfield_folds_overflow_into_later_writes_and_scales_hash_offsets() {
    let Command::BitField {
        key,
        ops,
        read_only_name,
    } = parse_words(&[
        "BITFIELD", "k", "GET", "u8", "#2", "OVERFLOW", "SAT", "INCRBY", "i5", "3", "-4",
        "overflow", "fail", "set", "u63", "0", "9",
    ])
    .unwrap()
    else {
        panic!("not a BITFIELD");
    };
    let u8_type = BitFieldType {
        signed: false,
        bits: 8,
    };
    let i5_type = BitFieldType {
        signed: true,
        bits: 5,
    };

    assert_eq!(key.as_ref(), b"k");
    assert!(!read_only_name);
    assert_eq!(
        ops,
        vec![
            BitFieldOp::Get {
                ty: u8_type,
                offset: 16
            },
            BitFieldOp::IncrBy {
                ty: i5_type,
                offset: 3,
                delta: -4,
                overflow: BitFieldOverflow::Sat
            },
            BitFieldOp::Set {
                ty: BitFieldType {
                    signed: false,
                    bits: 63
                },
                offset: 0,
                value: 9,
                overflow: BitFieldOverflow::Fail,
            },
        ]
    );
    assert!(
        matches!(parse_words(&["BITFIELD", "k"]).unwrap(), Command::BitField { ops, .. } if ops.is_empty())
    );
}

#[test]
fn bitfield_rejects_bad_types_offsets_and_read_only_writes() {
    for words in [
        &["BITFIELD", "k", "GET", "u64", "0"][..],
        &["BITFIELD", "k", "GET", "i65", "0"],
        &["BITFIELD", "k", "GET", "I8", "0"],
        &["BITFIELD", "k", "GET", "u0", "0"],
        &["BITFIELD", "k", "GET", "u8", "-1"],
        &["BITFIELD", "k", "GET", "u8", "134217721"],
        &["BITFIELD", "k", "GET", "u8"],
        &["BITFIELD", "k", "SET", "u8", "0"],
        &["BITFIELD", "k", "SET", "u8", "0", "x"],
        &["BITFIELD", "k", "OVERFLOW", "BAD"],
        &["BITFIELD", "k", "BOGUS"],
        &["BITFIELD_RO", "k", "SET", "u8", "0", "1"],
        &["BITFIELD_RO", "k", "INCRBY", "u8", "0", "1"],
    ] {
        assert!(parse_words(words).is_err(), "{words:?} should be refused");
    }

    assert!(parse_words(&["BITFIELD", "k", "GET", "u8", "134217720"]).is_ok());
    assert!(matches!(
        parse_words(&["BITFIELD_RO", "k", "GET", "i8", "0"]).unwrap(),
        Command::BitField {
            read_only_name: true,
            ..
        }
    ));
}

#[test]
fn zrange_rev_takes_the_high_bound_first() {
    let input = b"*6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$4\r\n+inf\r\n$2\r\n(1\r\n$7\r\nBYSCORE\r\n$3\r\nREV\r\n\
                  *6\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$2\r\n[c\r\n$1\r\n-\r\n$5\r\nBYLEX\r\n$3\r\nREV\r\n";
    let mut reader = Cursor::new(input);

    match read_command(&mut reader).unwrap() {
        Some(Command::SortedSetRangeByScore {
            min, max, reverse, ..
        }) => {
            assert_eq!(min, ScoreBound::Exclusive(1.0));
            assert_eq!(max, ScoreBound::PosInf);
            assert!(reverse);
        }
        other => panic!("unexpected {other:?}"),
    }
    match read_command(&mut reader).unwrap() {
        Some(Command::SortedSetRangeByLex {
            min, max, reverse, ..
        }) => {
            assert_eq!(min, LexBound::NegInf);
            assert_eq!(max, LexBound::Inclusive(owned(b"c")));
            assert!(reverse);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn slowlog_arguments_never_keep_passwords() {
    let frame = |args: &[&[u8]]| {
        let mut bytes = format!("*{}\r\n", args.len()).into_bytes();

        for arg in args {
            bytes.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            bytes.extend_from_slice(arg);
            bytes.extend_from_slice(b"\r\n");
        }

        bytes
    };
    let shown = |args: &[&[u8]]| {
        crate::protocol::slowlog_arguments(&frame(args), 128)
            .into_iter()
            .map(|arg| String::from_utf8(arg).unwrap())
            .collect::<Vec<_>>()
    };

    assert_eq!(shown(&[b"AUTH", b"hunter2"]), ["AUTH", "(redacted)"]);
    assert_eq!(
        shown(&[
            b"RUVIO",
            b"MIGRATE",
            b"EXPORT",
            b"m1",
            b"0",
            b"99",
            b"10.0.0.2",
            b"6379",
            b"AUTH",
            b"mover",
            b"hunter2"
        ])[8..],
        ["AUTH", "(redacted)", "(redacted)"]
    );
    assert_eq!(
        shown(&[b"AUTH", b"bob", b"hunter2"]),
        ["AUTH", "(redacted)", "(redacted)"]
    );
    assert_eq!(
        shown(&[
            b"HELLO", b"3", b"AUTH", b"bob", b"hunter2", b"SETNAME", b"app"
        ]),
        [
            "HELLO",
            "3",
            "AUTH",
            "(redacted)",
            "(redacted)",
            "SETNAME",
            "app"
        ]
    );
    assert_eq!(
        shown(&[
            b"ACL",
            b"SETUSER",
            b"bob",
            b"on",
            b">hunter2",
            b"~app:*",
            b"<old"
        ]),
        [
            "ACL",
            "SETUSER",
            "bob",
            "on",
            "(redacted)",
            "~app:*",
            "(redacted)"
        ]
    );
    assert_eq!(
        shown(&[
            b"CONFIG",
            b"SET",
            b"requirepass",
            b"hunter2",
            b"maxmemory",
            b"1"
        ]),
        [
            "CONFIG",
            "SET",
            "requirepass",
            "(redacted)",
            "maxmemory",
            "1"
        ]
    );
    // Inline frames go through the same filter.
    assert_eq!(
        crate::protocol::slowlog_arguments(b"AUTH hunter2\r\n", 128),
        [b"AUTH".to_vec(), b"(redacted)".to_vec()]
    );
    assert_eq!(shown(&[b"GET", b"hunter2"]), ["GET", "hunter2"]);
}

#[test]
fn protocol_limits_above_two_gib_are_rejected() {
    let base = ProtocolLimits::default();

    assert!(base.validate().is_ok());

    for limits in [
        ProtocolLimits {
            max_command_bytes: MAX_FRAME_LIMIT_BYTES + 1,
            max_connection_buffer_bytes: MAX_FRAME_LIMIT_BYTES + 1,
            ..base
        },
        ProtocolLimits {
            max_value_bytes: u32::MAX as usize + 10,
            ..base
        },
        ProtocolLimits {
            max_connection_buffer_bytes: usize::MAX,
            ..base
        },
    ] {
        assert!(limits.validate().is_err(), "{limits:?}");
    }

    let largest = ProtocolLimits {
        max_command_bytes: MAX_FRAME_LIMIT_BYTES,
        max_value_bytes: MAX_FRAME_LIMIT_BYTES,
        max_connection_buffer_bytes: MAX_FRAME_LIMIT_BYTES,
        ..base
    };

    assert!(largest.validate().is_ok());
}

#[test]
fn random_draw_counts_past_the_limit_are_refused() {
    let limit = super::table::MAX_RANDOM_COUNT as i64;

    for name in ["SRANDMEMBER", "HRANDFIELD", "ZRANDMEMBER"] {
        for count in [i64::MIN, i64::MIN + 1, -(limit + 1)] {
            let count = count.to_string();
            let frame = format!(
                "*3\r\n${}\r\n{name}\r\n$1\r\nk\r\n${}\r\n{count}\r\n",
                name.len(),
                count.len()
            );

            assert!(
                parse_command(frame.as_bytes()).is_err(),
                "{name} {count} was accepted"
            );
        }

        for count in [-limit, i64::MAX] {
            let count = count.to_string();
            let frame = format!(
                "*3\r\n${}\r\n{name}\r\n$1\r\nk\r\n${}\r\n{count}\r\n",
                name.len(),
                count.len()
            );

            assert!(parse_command(frame.as_bytes()).unwrap().is_some());
        }
    }
}

fn mset_frame(pairs: usize) -> (Vec<u8>, Vec<(Bytes<'static>, Bytes<'static>)>) {
    let mut frame = format!("*{}\r\n$4\r\nMSET\r\n", 1 + pairs * 2).into_bytes();
    let mut expected = Vec::new();

    for index in 0..pairs {
        let (key, value) = (format!("key:{index}"), format!("value-{index}"));

        for part in [&key, &value] {
            frame.extend_from_slice(format!("${}\r\n{part}\r\n", part.len()).as_bytes());
        }

        expected.push((owned(key.as_bytes()), owned(value.as_bytes())));
    }

    (frame, expected)
}

/// Argument lists around and past the inline span space parse the same, and every prefix
/// of a long frame reads as incomplete.
#[test]
fn mset_frames_around_the_inline_argument_space_parse_every_pair() {
    for pairs in [1, 7, 8, 15, 16, 17, 40] {
        let (frame, expected) = mset_frame(pairs);

        assert_eq!(
            parse_command(&frame).unwrap(),
            Some((Command::MSet(expected), frame.len())),
            "{pairs} pairs"
        );
    }

    let (frame, _) = mset_frame(40);

    for cut in [1, 5, 40, frame.len() / 2, frame.len() - 1] {
        assert_eq!(parse_command(&frame[..cut]).unwrap(), None, "cut at {cut}");
    }
}

/// A count header far larger than the bytes behind it reserves no more than those bytes
/// could hold, and still reads as incomplete.
#[test]
fn a_huge_argument_count_with_few_bytes_reads_as_incomplete() {
    let limits = ProtocolLimits {
        max_arguments: usize::MAX,
        ..ProtocolLimits::default()
    };
    let frame = b"*100000000\r\n$4\r\nMSET\r\n$1\r\na\r\n";

    assert_eq!(parse_command_with_limits(frame, limits).unwrap(), None);
}

fn del_frame(keys: usize) -> (Vec<u8>, Vec<Bytes<'static>>) {
    let mut frame = format!("*{}\r\n$3\r\nDEL\r\n", 1 + keys).into_bytes();
    let mut expected = Vec::new();

    for index in 0..keys {
        let key = format!("key:{index}");

        frame.extend_from_slice(format!("${}\r\n{key}\r\n", key.len()).as_bytes());
        expected.push(owned(key.as_bytes()));
    }

    (frame, expected)
}

/// DEL frames that exactly fill the inline span space (31 keys, 32 arguments) and that
/// pass it by one (32 keys, 33 arguments) parse every key, in order, and read as
/// incomplete when cut short.
#[test]
fn del_frames_at_and_one_past_the_inline_argument_space_parse_every_key() {
    for keys in [15, 16, 30, 31, 32, 33] {
        let (frame, expected) = del_frame(keys);

        assert_eq!(
            parse_command(&frame).unwrap(),
            Some((Command::Del(expected.into()), frame.len())),
            "{keys} keys"
        );

        for cut in [frame.len() / 2, frame.len() - 3, frame.len() - 1] {
            assert_eq!(
                parse_command(&frame[..cut]).unwrap(),
                None,
                "{keys} keys cut at {cut}"
            );
        }
    }
}

/// Integer arguments follow Redis's `string2ll`; timeouts refuse NaN and infinity.
#[test]
fn integer_and_timeout_arguments_follow_redis_rules() {
    use super::parse::redis_integer;

    for (text, expected) in [
        ("0", Some(0)),
        ("5", Some(5)),
        ("-5", Some(-5)),
        ("9223372036854775807", Some(i64::MAX)),
        ("-9223372036854775808", Some(i64::MIN)),
        ("9223372036854775808", None),
        ("-9223372036854775809", None),
        ("+5", None),
        (" 5", None),
        ("5 ", None),
        ("05", None),
        ("-0", None),
        ("-", None),
        ("", None),
        ("0x10", None),
        ("1e3", None),
    ] {
        assert_eq!(redis_integer(text.as_bytes()), expected, "{text:?}");
    }

    for timeout in ["nan", "inf", "-inf"] {
        let frame = format!(
            "*3\r\n$5\r\nBLPOP\r\n$1\r\nk\r\n${}\r\n{timeout}\r\n",
            timeout.len()
        );

        assert!(
            parse_command(frame.as_bytes()).is_err(),
            "BLPOP k {timeout}"
        );
    }
}

#[test]
fn parses_config_set_pairs_and_config_rewrite() {
    let set = b"*6\r\n$6\r\nconfig\r\n$3\r\nset\r\n$9\r\nmaxmemory\r\n$3\r\n100\r\n$15\r\nslowlog-max-len\r\n$1\r\n5\r\n";
    let (command, used) = parse_command(set).unwrap().unwrap();

    assert_eq!(used, set.len());
    let Command::ConfigSet(pairs) = command else {
        panic!("{command:?}");
    };
    let pairs: Vec<(&[u8], &[u8])> = pairs
        .iter()
        .map(|(name, value)| (name.as_ref(), value.as_ref()))
        .collect();
    assert_eq!(
        pairs,
        [
            (&b"maxmemory"[..], &b"100"[..]),
            (&b"slowlog-max-len"[..], &b"5"[..])
        ]
    );

    let rewrite = b"*2\r\n$6\r\nCONFIG\r\n$7\r\nREWRITE\r\n";
    assert_eq!(
        parse_command(rewrite).unwrap(),
        Some((Command::ConfigRewrite, rewrite.len()))
    );
}

#[test]
fn client_setinfo_takes_a_known_attribute_and_a_value() {
    let frame = |args: &[&str]| {
        let mut frame = format!("*{}\r\n", args.len()).into_bytes();

        for arg in args {
            frame.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        frame
    };
    let parse =
        |args: &[&str]| parse_command(&frame(args)).map(|parsed| parsed.unwrap().0.into_owned());

    assert!(matches!(
        parse(&["CLIENT", "SETINFO", "lib-name", "redis-py"]),
        Ok(Command::ClientSetInfo)
    ));
    assert!(matches!(
        parse(&["CLIENT", "SETINFO", "LIB-VER", "5.0"]),
        Ok(Command::ClientSetInfo)
    ));
    assert!(parse(&["CLIENT", "SETINFO", "OTHER", "x"]).is_err());
    assert!(parse(&["CLIENT", "SETINFO", "LIB-NAME"]).is_err());
}

#[test]
fn scan_count_is_capped() {
    let frame = b"*4\r\n$4\r\nSCAN\r\n$1\r\n0\r\n$5\r\nCOUNT\r\n$10\r\n9999999999\r\n";
    let Command::Scan { count, .. } = parse_command(frame).unwrap().unwrap().0.into_owned() else {
        panic!("not a scan");
    };

    assert_eq!(count, crate::protocol::MAX_SCAN_COUNT);
    assert_eq!(count, 10_000);
}

#[test]
fn scan_takes_a_slot_and_routes_to_it() {
    fn frame(args: &[&[u8]]) -> Vec<u8> {
        let mut frame = format!("*{}\r\n", args.len()).into_bytes();

        for arg in args {
            frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            frame.extend_from_slice(arg);
            frame.extend_from_slice(b"\r\n");
        }
        frame
    }
    let parse =
        |args: &[&[u8]]| parse_command(&frame(args)).map(|parsed| parsed.unwrap().0.into_owned());

    let Command::Scan { slot, .. } = parse(&[b"SCAN", b"0", b"SLOT", b"16383"]).unwrap() else {
        panic!("not a scan");
    };

    assert_eq!(slot, Some(16383));
    assert_eq!(
        crate::routing::command_slot(&parse(&[b"SCAN", b"7", b"SLOT", b"42"]).unwrap()),
        Ok(Some(42))
    );
    assert_eq!(
        crate::routing::command_slot(&parse(&[b"SCAN", b"0"]).unwrap()),
        Ok(None)
    );
    for bad in [&b"16384"[..], b"-1", b"x"] {
        assert!(parse(&[b"SCAN", b"0", b"SLOT", bad]).is_err());
    }
}

#[test]
fn stream_trim_limit_is_read_with_an_approximate_trim_and_ignored() {
    let frame = |args: &[&str]| {
        let mut frame = format!("*{}\r\n", args.len()).into_bytes();

        for arg in args {
            frame.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        frame
    };
    let parse =
        |args: &[&str]| parse_command(&frame(args)).map(|parsed| parsed.unwrap().0.into_owned());

    assert!(matches!(
        parse(&[
            "XADD", "s", "MAXLEN", "~", "10", "LIMIT", "5", "*", "f", "v"
        ]),
        Ok(Command::StreamAdd {
            max_len: Some(10),
            ..
        })
    ));
    assert!(matches!(
        parse(&[
            "XADD", "s", "MINID", "~", "1-1", "LIMIT", "5", "*", "f", "v"
        ]),
        Ok(Command::StreamAdd {
            min_id: Some((1, 1)),
            ..
        })
    ));
    assert!(matches!(
        parse(&["XTRIM", "s", "MAXLEN", "~", "10", "LIMIT", "5"]),
        Ok(Command::StreamTrim { max_len: 10, .. })
    ));
    assert!(matches!(
        parse(&["XTRIM", "s", "MINID", "~", "1-1", "LIMIT", "5"]),
        Ok(Command::StreamTrimMinId { id: (1, 1), .. })
    ));
    // As in Redis, LIMIT needs `~`, a count, and nothing after it.
    assert!(parse(&["XTRIM", "s", "MAXLEN", "10", "LIMIT", "5"]).is_err());
    assert!(parse(&["XTRIM", "s", "MAXLEN", "=", "10", "LIMIT", "5"]).is_err());
    assert!(parse(&["XTRIM", "s", "MAXLEN", "~", "10", "LIMIT"]).is_err());
    assert!(parse(&["XTRIM", "s", "MAXLEN", "~", "10", "LIMIT", "x"]).is_err());
    assert!(parse(&["XTRIM", "s", "MAXLEN", "~", "10", "LIMIT", "5", "extra"]).is_err());
    assert!(matches!(
        parse(&["XTRIM", "s", "MAXLEN", "~", "10"]),
        Ok(Command::StreamTrim { max_len: 10, .. })
    ));
}
