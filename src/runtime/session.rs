//! Connection-local command policy. Adapters keep I/O; this module has no sockets.

use crate::{
    SecurityConfig,
    admission::{CommandRateLimiter, ConnectionPermit},
    engine::{RespVersion, Response, hello_response},
    protocol::Command,
    transaction::TxSession,
};

pub struct SessionCtx<'a> {
    pub auth: &'a mut crate::acl::ConnectionAuth,
    pub tx: &'a mut TxSession,
    pub permit: &'a mut ConnectionPermit,
    pub rate: &'a mut CommandRateLimiter,
    pub security: &'a SecurityConfig,
    pub cluster: bool,
    /// The error a write gets on this shard (a replica, or a replaced primary).
    pub write_refusal: Option<&'static [u8]>,
    pub replication: bool,
    pub databases: usize,
}

#[derive(Debug, PartialEq)]
pub enum SessionAction<'a> {
    Reply(Response<'a>),
    #[allow(dead_code)]
    Encoded(&'a [u8]),
    NeedStore,
    #[allow(dead_code)]
    Close,
}

/// AUTH, ACL, MULTI-admin, and rate-limit decisions. Does not touch the store. A command
/// refused here inside MULTI is never queued, so the transaction is marked failed and EXEC
/// answers EXECABORT, as in Redis. Invalid arguments (`MOVE` to its own or a missing
/// database) are not refused here inside MULTI: the command is queued and fails at EXEC.
pub fn gate<'a>(ctx: &mut SessionCtx<'_>, command: &Command<'a>) -> SessionAction<'static> {
    let action = admit(ctx, command);

    if matches!(
        action,
        SessionAction::Reply(Response::Error(_) | Response::OwnedError(_))
    ) {
        ctx.tx.mark_queue_error();
    }

    action
}

fn admit<'a>(ctx: &mut SessionCtx<'_>, command: &Command<'a>) -> SessionAction<'static> {
    if ctx.tx.is_multi()
        && matches!(
            command,
            Command::Auth { .. }
                | Command::Acl(_)
                | Command::Hello { .. }
                | Command::ClientGetName
                | Command::ClientSetName(_)
                | Command::ClientTracking(_)
                | Command::ClientDurability { .. }
                | Command::Select(_)
        )
    {
        return SessionAction::Reply(Response::Error(b"ERR command not allowed inside MULTI"));
    }

    if !ctx.rate.allow() {
        return SessionAction::Reply(Response::Error(crate::admission::RATE_LIMIT_ERROR));
    }

    // Before any other refusal, so an anonymous client learns nothing about the server's
    // role; only AUTH and HELLO with AUTH may log in.
    if !ctx.auth.authenticated
        && !matches!(
            command,
            Command::Auth { .. } | Command::Hello { auth: Some(_), .. }
        )
    {
        return SessionAction::Reply(Response::Error(b"NOAUTH Authentication required."));
    }

    if let Some(refusal) = ctx.write_refusal
        && crate::protocol::command_mutates_store(command)
    {
        return SessionAction::Reply(Response::Error(refusal));
    }

    if ctx.replication && crate::protocol::command_stays_process_local(command) {
        return SessionAction::Reply(Response::Error(crate::replication::UNCOPIED_LINE));
    }

    if ctx.tx.in_pubsub {
        match command {
            Command::Ping(payload) => {
                let body = payload
                    .as_ref()
                    .map(|payload| payload.as_ref().to_vec())
                    .unwrap_or_default();

                return SessionAction::Reply(Response::Array(vec![
                    Some(std::borrow::Cow::Owned(b"pong".to_vec())),
                    Some(std::borrow::Cow::Owned(body)),
                ]));
            }

            Command::ShardSubscribe(_)
            | Command::ShardUnsubscribe(_)
            | Command::Subscribe(_)
            | Command::Unsubscribe(_)
            | Command::PSubscribe(_)
            | Command::PUnsubscribe(_) => {}
            _ => {
                return SessionAction::Reply(Response::Error(
                    b"ERR only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, and PING are allowed in this context",
                ));
            }
        }
    }

    if let Command::Hello {
        protocol,
        auth,
        client_name,
    } = command
    {
        if ctx.auth.authenticated
            && let Err(deny) = ctx.auth.rules.authorize(command)
        {
            return SessionAction::Reply(crate::acl::noperm_response(deny));
        }

        let version = match protocol {
            None => ctx.tx.resp,
            Some(2) => RespVersion::V2,
            Some(3) => RespVersion::V3,
            Some(_) => {
                return SessionAction::Reply(Response::Error(
                    b"NOPROTO unsupported protocol version",
                ));
            }
        };

        if let Some((username, password)) = auth {
            let previous = ctx.auth.clone();
            let authed = crate::engine::apply_connection_auth(
                ctx.security,
                &Command::Auth {
                    username: Some(username.clone()),
                    password: password.clone(),
                },
                ctx.auth,
            );

            if !matches!(authed, Some(Response::Simple(b"OK"))) {
                *ctx.auth = previous;

                return SessionAction::Reply(
                    authed.unwrap_or(Response::Error(b"WRONGPASS invalid username-password pair")),
                );
            }

            if !ctx.permit.bind_user(&ctx.auth.username) {
                *ctx.auth = previous;

                return SessionAction::Reply(Response::Error(
                    crate::admission::USER_CONNECTION_LIMIT_ERROR,
                ));
            }
        }

        ctx.tx.resp = version;

        if let Some(name) = client_name {
            ctx.tx.client_name = name.as_ref().to_vec();
        }

        return SessionAction::Reply(hello_response(version, ctx.tx.id, ctx.cluster));
    }

    if matches!(command, Command::Auth { .. }) {
        let previous = ctx.auth.clone();

        if let Some(response) =
            crate::engine::apply_connection_auth(ctx.security, command, ctx.auth)
        {
            if matches!(response, Response::Simple(b"OK"))
                && !ctx.permit.bind_user(&ctx.auth.username)
            {
                *ctx.auth = previous;

                return SessionAction::Reply(Response::Error(
                    crate::admission::USER_CONNECTION_LIMIT_ERROR,
                ));
            }

            return SessionAction::Reply(response);
        }
    } else if let Some(response) =
        crate::engine::apply_connection_auth(ctx.security, command, ctx.auth)
    {
        return SessionAction::Reply(response);
    }

    if let Err(deny) = ctx.auth.rules.authorize(command) {
        return SessionAction::Reply(crate::acl::noperm_response(deny));
    }

    if let Command::ClientSetName(name) = command {
        ctx.tx.client_name = name.as_ref().to_vec();

        return SessionAction::Reply(Response::Simple(b"OK"));
    }

    // CLIENT ID: the connection id, answered here because only the session knows it.
    if matches!(command, Command::ClientId) {
        return SessionAction::Reply(Response::Integer(ctx.tx.id as i64));
    }

    if matches!(command, Command::ClientGetName) {
        let name = if ctx.tx.client_name.is_empty() {
            Response::Bulk(None)
        } else {
            Response::Bulk(Some(std::borrow::Cow::Owned(ctx.tx.client_name.clone())))
        };

        return SessionAction::Reply(name);
    }

    if let Command::ClientTracking(on) = command {
        if *on && ctx.tx.resp != RespVersion::V3 {
            return SessionAction::Reply(Response::Error(b"ERR CLIENT TRACKING requires RESP3"));
        }

        ctx.tx.tracking = *on;

        return SessionAction::Reply(Response::Simple(b"OK"));
    }

    if let Command::ClientDurability {
        replicas,
        timeout_ms,
    } = *command
    {
        if replicas > 0 && !ctx.replication {
            return SessionAction::Reply(Response::Error(
                b"ERR CLIENT DURABILITY needs replication on this server",
            ));
        }

        ctx.tx.durability = (replicas > 0).then_some(crate::transaction::Durability {
            replicas,
            timeout_ms,
        });
        // The first replies under it wait for every record written before them.
        ctx.tx.durable_mark = 0;

        return SessionAction::Reply(Response::Simple(b"OK"));
    }

    if let Command::Acl(op) = command {
        return SessionAction::Reply(crate::acl::acl_response(ctx.security, ctx.auth, op));
    }

    if let Command::Select(index) = command {
        let Some(index) = database_index(*index, ctx.databases) else {
            return SessionAction::Reply(Response::Error(DB_OUT_OF_RANGE));
        };

        if ctx.cluster && index != 0 {
            return SessionAction::Reply(Response::Error(
                b"ERR SELECT is not allowed in cluster mode",
            ));
        }

        ctx.tx.database = index;

        return SessionAction::Reply(Response::Simple(b"OK"));
    }

    match database_check(ctx, command) {
        Some(error) if ctx.tx.is_multi() => SessionAction::Reply(ctx.tx.queue_refusal(error)),
        Some(error) => SessionAction::Reply(Response::Error(error)),
        None => SessionAction::NeedStore,
    }
}

/// `SWAPDB` and `MOVE` argument checks against the configured databases and cluster mode.
fn database_check(ctx: &SessionCtx<'_>, command: &Command<'_>) -> Option<&'static [u8]> {
    match command {
        Command::SwapDb(left, right) => {
            if ctx.cluster {
                return Some(b"ERR SWAPDB is not allowed in cluster mode");
            }

            if database_index(*left, ctx.databases).is_none()
                || database_index(*right, ctx.databases).is_none()
            {
                return Some(DB_OUT_OF_RANGE);
            }

            None
        }

        Command::Move { db, .. } => {
            if ctx.cluster {
                return Some(b"ERR MOVE is not allowed in cluster mode");
            }

            match database_index(*db, ctx.databases) {
                None => Some(DB_OUT_OF_RANGE),
                Some(target) if target == ctx.tx.database => {
                    Some(b"ERR source and destination objects are the same")
                }
                Some(_) => None,
            }
        }

        _ => None,
    }
}

const DB_OUT_OF_RANGE: &[u8] = b"ERR DB index is out of range";

fn database_index(index: i64, databases: usize) -> Option<u8> {
    usize::try_from(index)
        .ok()
        .filter(|index| *index < databases.min(crate::database::MAX_DATABASES))
        .map(|index| index as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectionLimits, admission::ConnectionLimiter, protocol::AclCommand,
        transaction::PrepareResult,
    };

    use std::{
        borrow::Cow,
        net::{IpAddr, Ipv4Addr},
    };

    fn permit() -> ConnectionPermit {
        ConnectionLimiter::new(ConnectionLimits {
            max_global: 8,
            max_per_ip: 8,
            max_per_user: 1,
            ..ConnectionLimits::default()
        })
        .try_acquire(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .expect("acquire")
    }

    fn ctx<'a>(
        security: &'a SecurityConfig,
        auth: &'a mut crate::acl::ConnectionAuth,
        tx: &'a mut TxSession,
        permit: &'a mut ConnectionPermit,
        rate: &'a mut CommandRateLimiter,
    ) -> SessionCtx<'a> {
        SessionCtx {
            auth,
            tx,
            permit,
            rate,
            security,
            cluster: false,
            write_refusal: None,
            replication: false,
            databases: crate::database::DEFAULT_DATABASES,
        }
    }

    #[test]
    fn anonymous_clients_get_noauth_before_hello_or_readonly() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);
        let noauth = SessionAction::Reply(Response::Error(b"NOAUTH Authentication required."));

        session.write_refusal = Some(crate::replication::READONLY_LINE);
        assert_eq!(
            gate(
                &mut session,
                &Command::Hello {
                    protocol: Some(3),
                    auth: None,
                    client_name: Some(Cow::Borrowed(b"x")),
                },
            ),
            noauth
        );
        assert_eq!(session.tx.resp, RespVersion::V2);
        assert!(session.tx.client_name.is_empty());
        assert_eq!(gate(&mut session, &Command::FlushAll), noauth);

        // HELLO with AUTH still logs in.
        assert!(matches!(
            gate(
                &mut session,
                &Command::Hello {
                    protocol: Some(3),
                    auth: Some((Cow::Borrowed(b"default"), Cow::Borrowed(b"secret"))),
                    client_name: None,
                },
            ),
            SessionAction::Reply(Response::Map(_))
        ));
        assert!(session.auth.authenticated);
    }

    #[test]
    fn rejects_auth_and_acl_inside_multi() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);

        assert!(matches!(
            tx.prepare(Command::Multi),
            PrepareResult::Reply(Response::Simple(b"OK"))
        ));
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        assert_eq!(
            gate(
                &mut session,
                &Command::Auth {
                    username: None,
                    password: Cow::Borrowed(b"secret"),
                }
            ),
            SessionAction::Reply(Response::Error(b"ERR command not allowed inside MULTI"))
        );
        assert_eq!(
            gate(&mut session, &Command::Acl(AclCommand::WhoAmI)),
            SessionAction::Reply(Response::Error(b"ERR command not allowed inside MULTI"))
        );
        assert_eq!(
            gate(&mut session, &Command::Select(0)),
            SessionAction::Reply(Response::Error(b"ERR command not allowed inside MULTI"))
        );
    }

    #[test]
    fn select_stores_the_index_and_rejects_out_of_range() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        assert_eq!(
            gate(&mut session, &Command::Select(0)),
            SessionAction::Reply(Response::Simple(b"OK"))
        );
        assert_eq!(session.tx.database, 0);
        assert_eq!(
            gate(&mut session, &Command::Select(1)),
            SessionAction::Reply(Response::Simple(b"OK"))
        );
        assert_eq!(session.tx.database, 1);
        assert_eq!(
            gate(&mut session, &Command::Select(16)),
            SessionAction::Reply(Response::Error(b"ERR DB index is out of range"))
        );
        assert_eq!(session.tx.database, 1);
        session.databases = 32;
        assert_eq!(
            gate(&mut session, &Command::Select(31)),
            SessionAction::Reply(Response::Simple(b"OK"))
        );
        assert_eq!(session.tx.database, 31);
    }

    #[test]
    fn cluster_mode_only_allows_database_zero() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        session.cluster = true;
        assert_eq!(
            gate(&mut session, &Command::Select(0)),
            SessionAction::Reply(Response::Simple(b"OK"))
        );
        assert_eq!(
            gate(&mut session, &Command::Select(1)),
            SessionAction::Reply(Response::Error(
                b"ERR SELECT is not allowed in cluster mode"
            ))
        );
        assert_eq!(
            gate(&mut session, &Command::SwapDb(0, 1)),
            SessionAction::Reply(Response::Error(
                b"ERR SWAPDB is not allowed in cluster mode"
            ))
        );
        assert_eq!(
            gate(
                &mut session,
                &Command::Move {
                    key: Cow::Borrowed(b"k"),
                    db: 1
                }
            ),
            SessionAction::Reply(Response::Error(b"ERR MOVE is not allowed in cluster mode"))
        );
        assert_eq!(
            gate(&mut session, &Command::FlushDb),
            SessionAction::NeedStore
        );
    }

    #[test]
    fn swapdb_and_move_check_the_database_indexes() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);
        let move_to = |db| Command::Move {
            key: Cow::Borrowed(b"k"),
            db,
        };

        assert_eq!(
            gate(&mut session, &Command::SwapDb(0, 15)),
            SessionAction::NeedStore
        );
        assert_eq!(
            gate(&mut session, &Command::SwapDb(0, 16)),
            SessionAction::Reply(Response::Error(b"ERR DB index is out of range"))
        );
        assert_eq!(
            gate(&mut session, &Command::SwapDb(-1, 0)),
            SessionAction::Reply(Response::Error(b"ERR DB index is out of range"))
        );
        assert_eq!(gate(&mut session, &move_to(3)), SessionAction::NeedStore);
        assert_eq!(
            gate(&mut session, &move_to(0)),
            SessionAction::Reply(Response::Error(
                b"ERR source and destination objects are the same"
            ))
        );
        assert_eq!(
            gate(&mut session, &move_to(16)),
            SessionAction::Reply(Response::Error(b"ERR DB index is out of range"))
        );
    }

    #[test]
    fn database_wide_writes_are_queued_inside_multi() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);

        assert!(matches!(
            tx.prepare(Command::Multi),
            PrepareResult::Reply(Response::Simple(b"OK"))
        ));
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        for command in [Command::FlushDb, Command::FlushAll, Command::SwapDb(0, 1)] {
            assert_eq!(gate(&mut session, &command), SessionAction::NeedStore);
        }

        // An invalid database is queued and fails at EXEC; the transaction still runs.
        assert_eq!(
            gate(&mut session, &Command::SwapDb(0, 16)),
            SessionAction::Reply(Response::Simple(b"QUEUED"))
        );
        assert!(!tx.is_aborted());
        assert_eq!(
            tx.take_exec_refusals(),
            vec![(0, &b"ERR DB index is out of range"[..])]
        );
    }

    #[test]
    fn rate_limits_before_store() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(1);

        assert!(rate.allow());
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        assert_eq!(
            gate(&mut session, &Command::Ping(None)),
            SessionAction::Reply(Response::Error(crate::admission::RATE_LIMIT_ERROR))
        );
    }

    #[test]
    fn unauthenticated_commands_need_auth() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };

        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        assert_eq!(
            gate(&mut session, &Command::Get(Cow::Borrowed(b"k"))),
            SessionAction::Reply(Response::Error(b"NOAUTH Authentication required."))
        );
    }

    #[test]
    fn authenticates_then_enforces_noperm_and_lets_allowed_commands_through() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            users: vec![
                crate::acl::AclUser::parse("app", "on >app-secret ~* +get +ping +auth").unwrap(),
            ],
            ..SecurityConfig::default()
        };

        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        assert_eq!(
            gate(
                &mut session,
                &Command::Auth {
                    username: Some(Cow::Borrowed(b"app")),
                    password: Cow::Borrowed(b"app-secret"),
                }
            ),
            SessionAction::Reply(Response::Simple(b"OK"))
        );
        assert_eq!(
            gate(&mut session, &Command::Get(Cow::Borrowed(b"k"))),
            SessionAction::NeedStore
        );
        let denied = gate(
            &mut session,
            &Command::Set(
                Cow::Borrowed(b"k"),
                Cow::Borrowed(b"v"),
                crate::protocol::SetOptions::default(),
            ),
        );

        match denied {
            SessionAction::Reply(Response::OwnedError(message)) => {
                let text = String::from_utf8(message).unwrap();

                assert!(text.contains("NOPERM"), "{text}");
                assert!(text.contains("set"), "{text}");
            }

            other => panic!("expected NOPERM, got {other:?}"),
        }
    }

    #[test]
    fn rejects_auth_when_the_user_connection_cap_is_reached() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };

        let limiter = ConnectionLimiter::new(ConnectionLimits {
            max_global: 8,
            max_per_ip: 8,
            max_per_user: 1,
            ..ConnectionLimits::default()
        });

        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mut first = limiter.try_acquire(ip).unwrap();
        let mut second = limiter.try_acquire(ip).unwrap();
        let mut auth_one = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx_one = TxSession::new(1);
        let mut rate_one = CommandRateLimiter::new(0);
        let mut session_one = ctx(
            &security,
            &mut auth_one,
            &mut tx_one,
            &mut first,
            &mut rate_one,
        );

        assert_eq!(
            gate(
                &mut session_one,
                &Command::Auth {
                    username: None,
                    password: Cow::Borrowed(b"secret"),
                }
            ),
            SessionAction::Reply(Response::Simple(b"OK"))
        );

        let mut auth_two = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx_two = TxSession::new(2);
        let mut rate_two = CommandRateLimiter::new(0);
        let mut session_two = ctx(
            &security,
            &mut auth_two,
            &mut tx_two,
            &mut second,
            &mut rate_two,
        );

        assert_eq!(
            gate(
                &mut session_two,
                &Command::Auth {
                    username: None,
                    password: Cow::Borrowed(b"secret"),
                }
            ),
            SessionAction::Reply(Response::Error(
                crate::admission::USER_CONNECTION_LIMIT_ERROR
            ))
        );
        assert!(!auth_two.authenticated);
    }

    #[test]
    fn hello_switches_the_protocol_and_rejects_an_unknown_version() {
        // No password: the connection starts logged in (anonymous HELLO is refused above).
        let security = SecurityConfig::default();

        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(7);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);
        let switched = gate(
            &mut session,
            &Command::Hello {
                protocol: Some(3),
                auth: None,
                client_name: None,
            },
        );

        assert!(matches!(switched, SessionAction::Reply(Response::Map(_))));
        assert_eq!(session.tx.resp, crate::engine::RespVersion::V3);
        assert_eq!(
            gate(
                &mut session,
                &Command::Hello {
                    protocol: Some(4),
                    auth: None,
                    client_name: None,
                },
            ),
            SessionAction::Reply(Response::Error(b"NOPROTO unsupported protocol version"))
        );
        assert_eq!(session.tx.resp, crate::engine::RespVersion::V3);
        assert!(matches!(
            session.tx.prepare(Command::Multi),
            PrepareResult::Reply(Response::Simple(b"OK"))
        ));
        assert_eq!(
            gate(
                &mut session,
                &Command::Hello {
                    protocol: Some(2),
                    auth: None,
                    client_name: None,
                },
            ),
            SessionAction::Reply(Response::Error(b"ERR command not allowed inside MULTI"))
        );
    }

    /// HELLO AUTH and HELLO SETNAME authenticate and store the client name. CLIENT GETNAME, CLIENT SETNAME, and CLIENT TRACKING use that connection.
    #[test]
    fn hello_auth_and_setname_keep_the_old_protocol_when_the_password_is_wrong() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };

        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(3);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);

        {
            let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

            assert!(matches!(
                gate(
                    &mut session,
                    &Command::Hello {
                        protocol: Some(3),
                        auth: Some((Cow::Borrowed(b"default"), Cow::Borrowed(b"nope"))),
                        client_name: Some(Cow::Borrowed(b"app")),
                    },
                ),
                SessionAction::Reply(Response::Error(b"WRONGPASS invalid username-password pair"))
            ));
            assert_eq!(session.tx.resp, crate::engine::RespVersion::V2);
            assert!(session.tx.client_name.is_empty());
        }

        assert!(!auth.authenticated);

        {
            let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

            assert!(matches!(
                gate(
                    &mut session,
                    &Command::Hello {
                        protocol: Some(3),
                        auth: Some((Cow::Borrowed(b"default"), Cow::Borrowed(b"secret"))),
                        client_name: Some(Cow::Borrowed(b"app")),
                    },
                ),
                SessionAction::Reply(Response::Map(_))
            ));
            assert_eq!(session.tx.resp, crate::engine::RespVersion::V3);
            assert_eq!(session.tx.client_name, b"app");
            assert_eq!(
                gate(&mut session, &Command::ClientGetName),
                SessionAction::Reply(Response::Bulk(Some(Cow::Owned(b"app".to_vec()))))
            );
            assert_eq!(
                gate(&mut session, &Command::ClientTracking(true)),
                SessionAction::Reply(Response::Simple(b"OK"))
            );
            assert!(session.tx.tracking);
            assert_eq!(
                gate(&mut session, &Command::ClientSetName(Cow::Borrowed(b""))),
                SessionAction::Reply(Response::Simple(b"OK"))
            );
            assert!(session.tx.client_name.is_empty());
        }

        assert!(auth.authenticated);
    }

    #[test]
    fn replica_rejects_a_write_before_the_store() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        session.write_refusal = Some(crate::replication::READONLY_LINE);
        let readonly = SessionAction::Reply(Response::Error(crate::replication::READONLY_LINE));

        assert_eq!(
            gate(
                &mut session,
                &Command::Set(
                    Cow::Borrowed(b"k"),
                    Cow::Borrowed(b"v"),
                    crate::protocol::SetOptions::default(),
                ),
            ),
            readonly,
        );
        assert_eq!(
            gate(&mut session, &Command::Get(Cow::Borrowed(b"k"))),
            SessionAction::NeedStore,
        );
        assert_eq!(
            gate(
                &mut session,
                &Command::Eval {
                    script: Cow::Borrowed(b"return 1"),
                    keys: Vec::new(),
                    args: Vec::new(),
                },
            ),
            readonly,
        );
        assert_ne!(gate(&mut session, &Command::Ping(None)), readonly);
        assert_ne!(gate(&mut session, &Command::Save), readonly);
        session.write_refusal = None;
        assert_eq!(
            gate(
                &mut session,
                &Command::Set(
                    Cow::Borrowed(b"k"),
                    Cow::Borrowed(b"v"),
                    crate::protocol::SetOptions::default(),
                ),
            ),
            SessionAction::NeedStore,
        );
    }

    #[test]
    fn replication_refuses_process_local_commands() {
        let security = SecurityConfig::default();
        let mut auth = crate::acl::ConnectionAuth::from_security(&security);
        let mut tx = TxSession::new(1);
        let mut permit = permit();
        let mut rate = CommandRateLimiter::new(0);
        let mut session = ctx(&security, &mut auth, &mut tx, &mut permit, &mut rate);

        session.replication = true;
        let refused = SessionAction::Reply(Response::Error(crate::replication::UNCOPIED_LINE));

        assert_eq!(
            gate(
                &mut session,
                &Command::Acl(AclCommand::SetUser {
                    name: Cow::Borrowed(b"ada"),
                    rules: Vec::new(),
                }),
            ),
            refused,
        );
        assert_eq!(
            gate(
                &mut session,
                &Command::FunctionLoad {
                    name: Cow::Borrowed(b"f"),
                    body: Cow::Borrowed(b"return 1"),
                },
            ),
            refused,
        );
        assert_eq!(
            gate(&mut session, &Command::Get(Cow::Borrowed(b"k"))),
            SessionAction::NeedStore,
        );
        session.replication = false;
        assert_eq!(
            gate(
                &mut session,
                &Command::FunctionLoad {
                    name: Cow::Borrowed(b"f"),
                    body: Cow::Borrowed(b"return 1"),
                },
            ),
            SessionAction::NeedStore,
        );
    }
}
