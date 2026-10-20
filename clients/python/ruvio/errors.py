# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""Exception types raised by the Ruvio client.

Everything derives from :class:`RuvioError`. Server ``-ERR`` replies raise :class:`ResponseError` (or
a subclass chosen by the reply code, for example :class:`WrongTypeError`); transport problems raise
:class:`ConnectionError` / :class:`TimeoutError`.
"""

from __future__ import annotations

__all__ = [
    "RuvioError",
    "ConnectionError",
    "TimeoutError",
    "ProtocolError",
    "ResponseError",
    "WrongTypeError",
    "NoScriptError",
    "NoGroupError",
    "BusyError",
    "ExecAbortError",
    "MovedError",
    "CrossSlotError",
    "LockError",
    "WatchError",
]


class RuvioError(Exception):
    """Base class for every error raised by this package. Catch it to handle all client errors."""


class ConnectionError(RuvioError, OSError):  # noqa: A001 - mirrors redis-py naming
    """The socket could not be opened, or it closed while a reply was pending.

    Also an :class:`OSError`. The command that hit the failure is not retried, because it may have
    run on the server. The next command reconnects.
    """


class TimeoutError(ConnectionError):  # noqa: A001
    """A socket operation timed out (connect timeout, ``socket_timeout`` on the sync client, or a helper
    waiting too long, such as ``get_or_create``).

    Subclass of :class:`ConnectionError`, not of the builtin ``TimeoutError``. After a socket timeout
    the connection is closed, since a late reply would be out of order.
    """


class ProtocolError(RuvioError):
    """The server sent bytes that are not valid RESP. The connection is closed."""


class ResponseError(RuvioError):
    """A server error reply (``-ERR ...``).

    Attributes:
        code: First word of the error (``ERR``, ``WRONGTYPE``, ``NOPERM``, ``BUSYKEY`` ...).
        message: The full error text as sent by the server.
    """

    __slots__ = ("code",)

    def __init__(self, message: str) -> None:
        """Creates the error from the full server message; ``code`` is the first word.

        Args:
            message: The error text, e.g. ``"WRONGTYPE Operation against a key holding the wrong kind of value"``.
        """
        super().__init__(message)
        space = message.find(" ")
        self.code = message if space < 0 else message[:space]

    @property
    def message(self) -> str:
        """The full error text as sent by the server."""
        return str(self.args[0])


class WrongTypeError(ResponseError):
    """``WRONGTYPE`` - the command does not fit the type of the key (for example ``lpush`` on a string)."""


class NoScriptError(ResponseError):
    """``NOSCRIPT`` - ``evalsha`` for a script that is not cached (always after a server restart).

    Load it again with ``script_load`` or run ``eval``.
    """


class NoGroupError(ResponseError):
    """``NOGROUP`` - the stream or consumer group does not exist."""


class BusyError(ResponseError):
    """``BUSY`` / ``BUSYGROUP`` - for example ``xgroup_create`` for a group that already exists."""


class ExecAbortError(ResponseError):
    """``EXECABORT`` - a queued command was refused, so the transaction did not run."""


class MovedError(ResponseError):
    """``MOVED`` / ``ASK`` cluster redirection that was not resolved (the cluster client follows ``MOVED``
    and ``ASK`` up to 3 times per command in all). ``TRYAGAIN`` is a plain :class:`ResponseError`.
    """


class CrossSlotError(ResponseError):
    """``CROSSSLOT`` - the keys of one command (or transaction) live in different hash slots.

    Raised by the cluster client before anything is sent, and also if a server answers it. Fix it by giving
    the keys a shared ``{hash tag}``, or by splitting the work into one command per slot.
    """

    def __init__(self, message: str = "CROSSSLOT Keys in request don't hash to the same slot") -> None:
        """Creates the error.

        Args:
            message: Error text (default: the standard CROSSSLOT message).
        """
        super().__init__(message)


class LockError(RuvioError):
    """A lock, lease or semaphore helper could not acquire the resource, or no longer holds it."""


_BY_CODE = {
    "WRONGTYPE": WrongTypeError,
    "NOSCRIPT": NoScriptError,
    "NOGROUP": NoGroupError,
    "BUSY": BusyError,
    "BUSYGROUP": BusyError,
    "EXECABORT": ExecAbortError,
    "MOVED": MovedError,
    "ASK": MovedError,
    "CROSSSLOT": CrossSlotError,
}


def make_error(message: str) -> ResponseError:
    """Builds the most specific :class:`ResponseError` subclass for a server error message.

    Args:
        message: The error text, starting with the code word.

    Returns:
        An exception instance (not raised).
    """
    space = message.find(" ")
    cls = _BY_CODE.get(message if space < 0 else message[:space], ResponseError)
    return cls(message)


class WatchError(RuvioError):
    """``EXEC`` returned nil: a watched key changed, so the transaction did not run. :meth:`ruvio.Ruvio.transaction`
    retries on it automatically.
    """
