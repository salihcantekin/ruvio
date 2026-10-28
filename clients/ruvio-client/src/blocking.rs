//! How long a blocking command may legitimately wait, so the command timeout can add it.
//!
//! The rule is the same in every Ruvio client: a command that carries its own wait gets
//! `wait + command timeout`; a wait of `0` (block forever) or an argument that does not parse
//! means no deadline for that call.

use std::time::Duration;

/// The wait one command (or pipeline) carries on top of the command timeout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wait {
    /// Not a blocking command: the plain command timeout applies.
    None,
    /// Blocks up to this long; the deadline is this plus the command timeout.
    For(Duration),
    /// Blocks with no bound (a `0` wait or an argument we could not parse): no deadline.
    Forever,
    /// The connection switches to push mode (`MONITOR`, `SUBSCRIBE`, ...): no deadline from
    /// here on.
    Push,
}

impl Wait {
    /// The wider of two waits, for a pipeline that holds several commands.
    #[inline]
    pub(crate) fn max(self, other: Wait) -> Wait {
        match (self, other) {
            (Wait::None, other) | (other, Wait::None) => other,
            (Wait::For(a), Wait::For(b)) => Wait::For(a.max(b)),
            _ => Wait::Forever,
        }
    }

    /// The read deadline for a command under `timeout`; `None` means wait forever.
    #[inline]
    pub(crate) fn deadline(self, timeout: Option<Duration>) -> Option<Duration> {
        match self {
            Wait::None => timeout,
            Wait::For(wait) => timeout.map(|timeout| timeout.saturating_add(wait)),
            Wait::Forever | Wait::Push => None,
        }
    }
}

/// The wait `arguments` (command name first) carry. A plain command costs a length and a
/// first-byte check, and nothing allocates.
#[inline]
pub(crate) fn blocking_wait<S: AsRef<[u8]>>(arguments: &[S]) -> Wait {
    let Some(name) = arguments.first() else {
        return Wait::None;
    };
    let name = name.as_ref();
    let Some(&first) = name.first() else {
        return Wait::None;
    };
    let first = first | 0x20;
    // Length and first letter of every name `classify` knows; GET, SET, PING, ... stop here.
    let candidate = match name.len() {
        4 => first == b'w',
        5 => first == b'b' || first == b'x',
        6 | 8 => first == b'b',
        7 => matches!(first, b'w' | b'f' | b'm'),
        9 => first == b's',
        10 => matches!(first, b'b' | b'x' | b'p' | b's'),
        _ => false,
    };

    if candidate {
        classify(name, arguments)
    } else {
        Wait::None
    }
}

/// The widest wait of a pipeline's commands.
#[inline]
pub(crate) fn pipeline_wait<S: AsRef<[u8]>>(commands: &[&[S]]) -> Wait {
    commands
        .iter()
        .fold(Wait::None, |wait, command| wait.max(blocking_wait(command)))
}

#[cold]
fn classify<S: AsRef<[u8]>>(name: &[u8], arguments: &[S]) -> Wait {
    let is = |known: &str| name.eq_ignore_ascii_case(known.as_bytes());
    let last = || arguments.last().map(AsRef::as_ref);

    if is("BLPOP")
        || is("BRPOP")
        || is("BZPOPMIN")
        || is("BZPOPMAX")
        || is("BRPOPLPUSH")
        || is("BLMOVE")
    {
        return seconds(if arguments.len() > 1 { last() } else { None });
    }

    if is("BLMPOP") || is("BZMPOP") {
        return seconds(arguments.get(1).map(AsRef::as_ref));
    }

    if is("XREAD") || is("XREADGROUP") {
        // XREADGROUP GROUP <group> <consumer> ...: skip the names, they may spell BLOCK.
        let start = if is("XREAD") { 1 } else { 4 };

        for index in start..arguments.len() {
            let argument = arguments[index].as_ref();

            if argument.eq_ignore_ascii_case(b"STREAMS") {
                break;
            }

            if argument.eq_ignore_ascii_case(b"BLOCK") {
                return millis(arguments.get(index + 1).map(AsRef::as_ref));
            }
        }

        return Wait::None;
    }

    if is("WAIT") || is("WAITAOF") {
        return millis(if arguments.len() > 1 { last() } else { None });
    }

    if is("FILLGET") {
        // FILLGET key fill_ms [WAIT wait_ms]
        return match arguments.get(3).map(AsRef::as_ref) {
            Some(option) if option.eq_ignore_ascii_case(b"WAIT") => {
                millis(arguments.get(4).map(AsRef::as_ref))
            }
            _ => Wait::None,
        };
    }

    if is("MONITOR") || is("SUBSCRIBE") || is("PSUBSCRIBE") || is("SSUBSCRIBE") {
        return Wait::Push;
    }

    Wait::None
}

/// A wait in seconds, possibly fractional (`0.5`).
fn seconds(argument: Option<&[u8]>) -> Wait {
    let parsed = argument
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse::<f64>().ok());

    match parsed {
        Some(seconds) if seconds > 0.0 && seconds.is_finite() => {
            Duration::try_from_secs_f64(seconds).map_or(Wait::Forever, Wait::For)
        }
        _ => Wait::Forever,
    }
}

/// A wait in whole milliseconds.
fn millis(argument: Option<&[u8]>) -> Wait {
    let parsed = argument
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse::<u64>().ok());

    match parsed {
        Some(ms) if ms > 0 => Wait::For(Duration::from_millis(ms)),
        _ => Wait::Forever,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(value: u64) -> Wait {
        Wait::For(Duration::from_millis(value))
    }

    #[test]
    fn blocking_wait_table() {
        let cases: &[(&[&str], Wait)] = &[
            (&["GET", "k"], Wait::None),
            (&["SET", "k", "v"], Wait::None),
            (&["blpop", "a", "b", "1"], ms(1000)),
            (&["BLPOP", "k", "0"], Wait::Forever),
            (&["BLPOP", "k", "0.5"], ms(500)),
            (&["BLPOP", "k", "soon"], Wait::Forever),
            (&["BLPOP", "k", "-1"], Wait::Forever),
            (&["BRPOP", "k", "2"], ms(2000)),
            (&["BZPOPMIN", "k", "1.5"], ms(1500)),
            (&["BZPOPMAX", "k", "3"], ms(3000)),
            (&["BRPOPLPUSH", "a", "b", "4"], ms(4000)),
            (&["BLMOVE", "a", "b", "LEFT", "RIGHT", "0.25"], ms(250)),
            (&["BLMPOP", "2", "2", "a", "b", "LEFT"], ms(2000)),
            (&["BZMPOP", "0", "1", "z", "MIN"], Wait::Forever),
            (&["XREAD", "STREAMS", "s", "$"], Wait::None),
            (
                &["XREAD", "COUNT", "1", "BLOCK", "700", "STREAMS", "s", "$"],
                ms(700),
            ),
            (&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"], Wait::Forever),
            (
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
                Wait::None,
            ),
            (
                &["XREADGROUP", "GROUP", "BLOCK", "c", "STREAMS", "s", ">"],
                Wait::None,
            ),
            (
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "c",
                    "BLOCK",
                    "50",
                    "STREAMS",
                    "s",
                    ">",
                ],
                ms(50),
            ),
            (&["WAIT", "1", "100"], ms(100)),
            (&["WAIT", "1", "0"], Wait::Forever),
            (&["WAITAOF", "1", "0", "200"], ms(200)),
            (&["FILLGET", "k", "1000"], Wait::None),
            (&["FILLGET", "k", "1000", "WAIT", "300"], ms(300)),
            (&["FILLGET", "k", "1000", "WAIT", "0"], Wait::Forever),
            (&["MONITOR"], Wait::Push),
            (&["SUBSCRIBE", "news"], Wait::Push),
            (&["PSUBSCRIBE", "n*"], Wait::Push),
            (&["SSUBSCRIBE", "news"], Wait::Push),
            (&["PUBLISH", "news", "x"], Wait::None),
            (&["BLPOP"], Wait::Forever),
        ];

        for (arguments, want) in cases {
            assert_eq!(blocking_wait(arguments), *want, "{arguments:?}");
        }

        assert_eq!(blocking_wait::<&str>(&[]), Wait::None);
        assert_eq!(blocking_wait(&[b"BLPOP".as_slice(), b"k", b"1"]), ms(1000));
    }

    #[test]
    fn deadline_adds_the_wait_and_a_pipeline_takes_the_widest() {
        let timeout = Some(Duration::from_secs(30));

        assert_eq!(Wait::None.deadline(timeout), timeout);
        assert_eq!(ms(1000).deadline(timeout), Some(Duration::from_secs(31)));
        assert_eq!(Wait::Forever.deadline(timeout), None);
        assert_eq!(ms(1000).deadline(None), None);
        assert_eq!(Wait::None.max(ms(5)).max(ms(9)), ms(9));
        assert_eq!(ms(5).max(Wait::Forever), Wait::Forever);
        assert_eq!(Wait::None.max(Wait::None), Wait::None);
        assert_eq!(
            pipeline_wait(&[
                &["GET", "k"][..],
                &["BLPOP", "k", "2"],
                &["XREAD", "BLOCK", "100", "STREAMS", "s", "$"]
            ]),
            ms(2000)
        );
        assert_eq!(
            pipeline_wait(&[&["BLPOP", "k", "2"][..], &["BLPOP", "k", "0"]]),
            Wait::Forever
        );
        assert_eq!(
            pipeline_wait(&[&["GET", "k"][..], &["SET", "k", "v"]]),
            Wait::None
        );
    }
}
