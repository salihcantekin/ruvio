use std::fmt;

/// Every way a call can fail.
///
/// Match on the variant to decide what to do: [`Error::Server`] is a normal refusal by the
/// server (the connection is still usable), [`Error::Io`] means the socket failed, and
/// [`Error::Protocol`] means the reply or an argument was malformed. The `Display` text is
/// the bare message, e.g. `WRONGTYPE Operation against a key holding the wrong kind of value`.
///
/// # Examples
///
/// ```no_run
/// # use ruvio_client::*;
/// # use std::time::Duration;
/// # fn run(client: &mut Client) -> Result<(), Error> {
/// match client.get("counter") {
///     Ok(value) => println!("{value:?}"),
///     Err(Error::Server(message)) if message.starts_with("WRONGTYPE") => {
///         println!("not a string");
///     }
///     Err(other) => return Err(other),
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub enum Error {
    /// The TCP connection failed, timed out, or was closed (connect error, reset, broken pipe,
    /// or a read timeout set with `Client::set_read_timeout`). After a failed command the
    /// outcome is unknown; with reconnect on the next command opens a new socket.
    Io(std::io::Error),
    /// The bytes on the socket were not a complete RESP2 value, a reply had an unexpected shape
    /// for the command, or the client rejected an argument before sending (for example an empty
    /// host, an empty key list for `DEL`, or a non-UTF-8 text reply read as a `String`).
    Protocol(String),
    /// Ruvio returned a RESP error reply; the text is the error message including its code
    /// (`ERR ...`, `WRONGTYPE ...`, `NOAUTH ...`, `CROSSSLOT ...`, `NOPERM ...`). The connection
    /// can still run the next command.
    Server(String),
}

/// Server replies that a later attempt can get past: the server is loading or has no primary, a slot or key
/// is moving, a failover is in progress, or too few replicas confirmed a durable write in time.
const TRANSIENT_REPLIES: [&str; 6] = [
    "LOADING",
    "MASTERDOWN",
    "CLUSTERDOWN",
    "TRYAGAIN",
    "READONLY",
    "NOREPLICAS",
];

impl Error {
    /// True when the same call can succeed later without any change on the caller's side: the socket failed
    /// or timed out ([`Error::Io`]), or the server answered `LOADING`, `MASTERDOWN`, `CLUSTERDOWN`,
    /// `TRYAGAIN`, `READONLY` or `NOREPLICAS`. False for every other server reply (`WRONGTYPE`, `OOM`, a
    /// syntax error) and for [`Error::Protocol`].
    ///
    /// The client already follows `MOVED`/`ASK` and waits out `TRYAGAIN` during a slot move; this judges
    /// what still reaches the caller, for a retry loop of your own. Retry only when this is true and the
    /// command is safe to repeat (a read, a `SET` of the same value, a command with an idempotency key) or
    /// [`may_have_run`](Error::may_have_run) is false.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ruvio_client::*;
    /// # fn run(client: &mut Client) -> Result<i64, Error> {
    /// let mut attempt = 0;
    /// loop {
    ///     match client.incr("visits") {
    ///         // INCR is not safe to repeat: retry it only when it surely did not run.
    ///         Err(error) if attempt < 3 && error.is_transient() && !error.may_have_run() => attempt += 1,
    ///         result => return result,
    ///     }
    /// }
    /// # }
    /// ```
    pub fn is_transient(&self) -> bool {
        match self {
            Error::Io(_) => true,
            Error::Server(message) => TRANSIENT_REPLIES
                .iter()
                .any(|code| reply_code(message) == *code),
            Error::Protocol(_) => false,
        }
    }

    /// True when the failed call may have changed data, so repeating a write that is not safe to repeat
    /// could apply it twice. False only for a server error reply, which the server sends instead of running
    /// the command; `NOREPLICAS` is the exception, because the write was applied on the primary and only
    /// the replica confirmation timed out. A failed or timed-out socket and a protocol error may have run.
    pub fn may_have_run(&self) -> bool {
        match self {
            Error::Server(message) => reply_code(message) == "NOREPLICAS",
            Error::Io(_) | Error::Protocol(_) => true,
        }
    }
}

fn reply_code(message: &str) -> &str {
    message.split(' ').next().unwrap_or(message)
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(formatter, "{error}"),
            Error::Protocol(message) | Error::Server(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Protocol(_) | Error::Server(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use std::error::Error as _;
    use std::io::{self, ErrorKind};

    #[test]
    fn transient_covers_io_and_the_retryable_replies_only() {
        for reply in [
            "LOADING Ruvio is loading the dataset in memory",
            "MASTERDOWN Link with MASTER is down",
            "CLUSTERDOWN The cluster is down",
            "TRYAGAIN slot is moving",
            "READONLY You can't write against a read only replica.",
            "NOREPLICAS Not enough good replicas to write.",
        ] {
            assert!(Error::Server(reply.into()).is_transient(), "{reply}");
        }
        for reply in [
            "WRONGTYPE Operation against a key",
            "OOM command not allowed",
            "ERR syntax error",
            "READONLYX x",
            "LOAD",
        ] {
            assert!(!Error::Server(reply.into()).is_transient(), "{reply}");
        }
        assert!(Error::Io(io::Error::new(ErrorKind::TimedOut, "slow")).is_transient());
        assert!(Error::Io(io::Error::new(ErrorKind::ConnectionReset, "reset")).is_transient());
        assert!(!Error::Protocol("bad frame".into()).is_transient());
    }

    #[test]
    fn only_a_refusing_server_reply_surely_did_not_run() {
        assert!(!Error::Server("WRONGTYPE Operation against a key".into()).may_have_run());
        assert!(!Error::Server("TRYAGAIN slot is moving".into()).may_have_run());
        assert!(!Error::Server("READONLY replica".into()).may_have_run());
        assert!(
            Error::Server("NOREPLICAS Not enough good replicas to write.".into()).may_have_run()
        );
        assert!(Error::Server("NOREPLICAS".into()).may_have_run());
        assert!(Error::Io(io::Error::new(ErrorKind::TimedOut, "slow")).may_have_run());
        assert!(Error::Protocol("bad frame".into()).may_have_run());
    }

    #[test]
    fn display_shows_the_message_of_every_variant() {
        assert_eq!(Error::Protocol("bad frame".into()).to_string(), "bad frame");
        assert_eq!(Error::Server("ERR nope".into()).to_string(), "ERR nope");
        assert_eq!(
            Error::Io(io::Error::new(ErrorKind::BrokenPipe, "pipe gone")).to_string(),
            "pipe gone"
        );
    }

    #[test]
    fn source_is_the_io_error_only() {
        let io_error = Error::Io(io::Error::new(ErrorKind::TimedOut, "slow"));
        assert_eq!(io_error.source().unwrap().to_string(), "slow");
        assert!(Error::Protocol("x".into()).source().is_none());
        assert!(Error::Server("x".into()).source().is_none());
    }

    #[test]
    fn io_errors_convert_with_from_and_question_mark() {
        fn run(fail: bool) -> Result<u8, Error> {
            if fail {
                Err(io::Error::new(ErrorKind::UnexpectedEof, "eof"))?;
            }

            Ok(7)
        }

        assert_eq!(run(false).unwrap(), 7);
        match run(true) {
            Err(Error::Io(error)) => assert_eq!(error.kind(), ErrorKind::UnexpectedEof),
            other => panic!("expected Io, got {other:?}"),
        }
        assert!(format!("{:?}", Error::Server("s".into())).contains("Server"));
    }
}
