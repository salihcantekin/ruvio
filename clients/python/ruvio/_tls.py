# MIT License - Copyright (c) 2026 Salih Cantekin. See LICENSE.
"""TLS settings shared by the sync and asyncio clients: one ``ssl.SSLContext`` built from the arguments."""

from __future__ import annotations

import ssl
from typing import Optional, Union


def make_context(
    tls: Union[bool, ssl.SSLContext, None],
    ca_file: Optional[str],
    cert_file: Optional[str],
    key_file: Optional[str],
    insecure: bool,
    unix_socket_path: Optional[str],
) -> Optional[ssl.SSLContext]:
    """The context every connection uses, or ``None`` for plain TCP.

    ``tls=True`` verifies the server certificate chain and host name against the system roots (or
    ``ca_file``); a ready ``ssl.SSLContext`` is used as it is. ``insecure`` accepts any certificate.

    Raises:
        ValueError: A ``tls_*`` option is set without ``tls``, ``tls`` is combined with a Unix socket,
            or ``cert_file`` and ``key_file`` are not given together with a file-based context.
    """
    extras = ca_file is not None or cert_file is not None or key_file is not None or insecure
    if not tls:
        if extras:
            raise ValueError("tls_ca_file, tls_cert_file, tls_key_file and tls_insecure_skip_verify need tls=True")
        return None
    if unix_socket_path:
        raise ValueError("tls cannot be combined with unix_socket_path")
    if isinstance(tls, ssl.SSLContext):
        if extras:
            raise ValueError("tls_* file options cannot be combined with a ready ssl.SSLContext")
        return tls
    context = ssl.create_default_context(cafile=ca_file)
    if cert_file is not None:
        context.load_cert_chain(cert_file, key_file)
    elif key_file is not None:
        raise ValueError("tls_key_file needs tls_cert_file")
    if insecure:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context
