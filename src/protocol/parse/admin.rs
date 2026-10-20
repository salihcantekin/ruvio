use super::*;

pub(in crate::protocol) fn parse_hello<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Ok(Command::Hello {
            protocol: None,
            auth: None,
            client_name: None,
        });
    }

    let protocol = Some(parse_i64(args.bytes(0))?);
    let mut auth = None;
    let mut client_name = None;
    let mut index = 1;

    while index < args.len() {
        let option = args.bytes(index);

        if option.eq_ignore_ascii_case(b"AUTH") {
            if index + 3 > args.len() {
                return Err(ProtocolError::Invalid(
                    "HELLO AUTH expects a username and a password",
                ));
            }

            auth = Some((args.arg(index + 1), args.arg(index + 2)));
            index += 3;
        } else if option.eq_ignore_ascii_case(b"SETNAME") {
            if index + 2 > args.len() {
                return Err(ProtocolError::Invalid("HELLO SETNAME expects a name"));
            }

            client_name = Some(args.arg(index + 1));
            index += 2;
        } else {
            return Err(ProtocolError::Invalid(
                "HELLO only supports protocol version 2 or 3, AUTH, and SETNAME",
            ));
        }
    }

    Ok(Command::Hello {
        protocol,
        auth,
        client_name,
    })
}

pub(in crate::protocol) fn parse_client<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() == 0 {
        return Err(ProtocolError::Invalid(
            "CLIENT expects GETNAME, SETNAME, or TRACKING",
        ));
    }

    let subcommand = args.bytes(0);
    // Arguments after the subcommand.
    let rest = args.len() - 1;

    if subcommand.eq_ignore_ascii_case(b"GETNAME") {
        if rest != 0 {
            return Err(ProtocolError::Invalid(
                "CLIENT GETNAME expects no arguments",
            ));
        }

        return Ok(Command::ClientGetName);
    }

    if subcommand.eq_ignore_ascii_case(b"ID") {
        if rest != 0 {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'client|id' command",
            ));
        }

        return Ok(Command::ClientId);
    }

    if subcommand.eq_ignore_ascii_case(b"INFO") {
        if rest != 0 {
            return Err(ProtocolError::Invalid(
                "wrong number of arguments for 'client|info' command",
            ));
        }

        return Ok(Command::ClientInfo);
    }

    if subcommand.eq_ignore_ascii_case(b"LIST") {
        return parse_client_list(args);
    }

    if subcommand.eq_ignore_ascii_case(b"KILL") {
        return parse_client_kill(args);
    }

    if subcommand.eq_ignore_ascii_case(b"SETNAME") {
        if rest != 1 {
            return Err(ProtocolError::Invalid("CLIENT SETNAME expects a name"));
        }

        return Ok(Command::ClientSetName(args.arg(1)));
    }

    if subcommand.eq_ignore_ascii_case(b"TRACKING") {
        if rest == 0 {
            return Err(ProtocolError::Invalid("CLIENT TRACKING expects ON or OFF"));
        }

        if rest != 1 {
            return Err(ProtocolError::Invalid(
                "CLIENT TRACKING only supports ON or OFF",
            ));
        }

        let mode = args.bytes(1);

        if mode.eq_ignore_ascii_case(b"ON") {
            return Ok(Command::ClientTracking(true));
        }

        if mode.eq_ignore_ascii_case(b"OFF") {
            return Ok(Command::ClientTracking(false));
        }

        return Err(ProtocolError::Invalid(
            "CLIENT TRACKING only supports ON or OFF",
        ));
    }

    if subcommand.eq_ignore_ascii_case(b"DURABILITY") {
        return parse_client_durability(args);
    }

    Err(ProtocolError::Invalid(
        "CLIENT expects GETNAME, SETNAME, or TRACKING",
    ))
}

/// Replies wait this long for replicas by default.
pub(crate) const DURABILITY_TIMEOUT_MS: u64 = 5_000;

/// `CLIENT DURABILITY REPLICAS n [TIMEOUT ms]`.
fn parse_client_durability<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    const USAGE: &str = "CLIENT DURABILITY expects REPLICAS n [TIMEOUT ms]";

    if !(args.len() == 3 || args.len() == 5) || !args.bytes(1).eq_ignore_ascii_case(b"REPLICAS") {
        return Err(ProtocolError::Invalid(USAGE));
    }

    let number = |index: usize| {
        std::str::from_utf8(args.bytes(index))
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or(ProtocolError::Invalid(USAGE))
    };
    let replicas = number(2)?;
    let timeout_ms = if args.len() == 5 {
        if !args.bytes(3).eq_ignore_ascii_case(b"TIMEOUT") {
            return Err(ProtocolError::Invalid(USAGE));
        }

        number(4)?
    } else {
        DURABILITY_TIMEOUT_MS
    };

    if replicas > crate::replication::MAX_REPLICAS as u64 {
        return Err(ProtocolError::Invalid(
            "CLIENT DURABILITY asks for more replicas than a primary can have",
        ));
    }

    Ok(Command::ClientDurability {
        replicas,
        timeout_ms,
    })
}

/// `ACL LOAD` and `ACL SAVE` read and write `security.acl_file` only.
pub(in crate::protocol) fn parse_acl_command<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    if args.len() > 0 && args.bytes(0).eq_ignore_ascii_case(b"SETUSER") {
        if args.len() < 2 {
            return Err(ProtocolError::Invalid("ACL SETUSER expects a username"));
        }

        if args.bytes(1).is_empty() {
            return Err(ProtocolError::Invalid(
                "ACL SETUSER username must not be empty",
            ));
        }

        return Ok(Command::Acl(AclCommand::SetUser {
            name: args.arg(1),
            rules: args.rest(2),
        }));
    }

    let is = |subcommand: &[u8]| args.bytes(0).eq_ignore_ascii_case(subcommand);

    match args.len() {
        1 if is(b"WHOAMI") => Ok(Command::Acl(AclCommand::WhoAmI)),
        1 if is(b"USERS") => Ok(Command::Acl(AclCommand::Users)),
        1 if is(b"LIST") => Ok(Command::Acl(AclCommand::List)),
        2 if is(b"GETUSER") => Ok(Command::Acl(AclCommand::GetUser(args.arg(1)))),
        1 if is(b"CAT") => Ok(Command::Acl(AclCommand::Cat(None))),
        2 if is(b"CAT") => Ok(Command::Acl(AclCommand::Cat(Some(args.arg(1))))),
        1 if is(b"LOAD") => Ok(Command::Acl(AclCommand::Load)),
        1 if is(b"SAVE") => Ok(Command::Acl(AclCommand::Save)),
        _ => Err(ProtocolError::Invalid(
            "only ACL WHOAMI, USERS, LIST, GETUSER, CAT, SETUSER, LOAD, and SAVE are supported",
        )),
    }
}

fn client_id(bytes: &[u8]) -> Result<u64, ProtocolError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .filter(|id| *id > 0)
        .ok_or(ProtocolError::Invalid("Invalid client ID"))
}

/// `CLIENT LIST [TYPE normal|master|replica|pubsub] [ID id ...]`, options from argument 1.
fn parse_client_list<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let mut ids = Vec::new();
    let option = |word: &[u8]| args.bytes(1).eq_ignore_ascii_case(word);

    match args.len() {
        1 => {}
        len if option(b"ID") && len > 2 => {
            for index in 2..len {
                ids.push(client_id(args.bytes(index))?);
            }
        }
        3 if option(b"TYPE") => {
            let kind = args.bytes(2);

            if !["normal", "master", "replica", "slave", "pubsub"]
                .iter()
                .any(|name| kind.eq_ignore_ascii_case(name.as_bytes()))
            {
                return Err(ProtocolError::Invalid("Unknown client type"));
            }
        }
        _ => return Err(ProtocolError::Invalid("syntax error")),
    }

    Ok(Command::ClientList { ids })
}

/// `CLIENT KILL addr` or `CLIENT KILL [ID id] [ADDR addr] [SKIPME yes|no]`, from argument 1.
fn parse_client_kill<'a>(args: &Args<'a, '_>) -> Parsed<'a> {
    let options = args.len() - 1;

    if options == 1 {
        return Ok(Command::ClientKill {
            id: None,
            addr: Some(args.arg(1)),
            skip_me: false,
            legacy: true,
        });
    }

    if options == 0 || !options.is_multiple_of(2) {
        return Err(ProtocolError::Invalid("syntax error"));
    }

    let mut id = None;
    let mut addr = None;
    let mut skip_me = true;

    for index in (1..args.len()).step_by(2) {
        let name = args.bytes(index);
        let value = args.bytes(index + 1);

        if name.eq_ignore_ascii_case(b"ID") {
            id = Some(client_id(value)?);
        } else if name.eq_ignore_ascii_case(b"ADDR") {
            addr = Some(args.arg(index + 1));
        } else if name.eq_ignore_ascii_case(b"SKIPME") {
            skip_me = if value.eq_ignore_ascii_case(b"yes") {
                true
            } else if value.eq_ignore_ascii_case(b"no") {
                false
            } else {
                return Err(ProtocolError::Invalid("syntax error"));
            };
        } else {
            return Err(ProtocolError::Invalid("syntax error"));
        }
    }

    Ok(Command::ClientKill {
        id,
        addr,
        skip_me,
        legacy: false,
    })
}
