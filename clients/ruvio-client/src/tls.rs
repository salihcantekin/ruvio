//! TLS for a connection: the options, and the stream type a connection reads and writes.
//!
//! Plain TCP stays the default and costs nothing extra. TLS needs the `tls` cargo feature;
//! without it, asking for TLS fails the connect with [`Error::Protocol`].

use std::fmt;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::error::Error;

/// How a TLS connection verifies the server and, optionally, proves who it is.
///
/// Set [`ClientOptions::tls`](crate::ClientOptions::tls) to `Some(TlsOptions::default())` to
/// turn TLS on. The default checks the server's certificate chain against the built-in public
/// roots and checks that the certificate names the host you connected to.
#[derive(Clone, Default)]
pub struct TlsOptions {
    /// PEM text of the certificate authorities to trust. When set, only these are trusted (use
    /// it for a private CA); when `None`, the built-in public roots are used.
    pub ca_pem: Option<Vec<u8>>,
    /// PEM text of the client certificate chain, for a server that asks for mutual TLS. Needs
    /// [`Self::client_key_pem`].
    pub client_cert_pem: Option<Vec<u8>>,
    /// PEM text of the private key that belongs to [`Self::client_cert_pem`].
    pub client_key_pem: Option<Vec<u8>>,
    /// The name the server's certificate must carry, when it differs from the host you connect
    /// to (for example connecting by IP address to a certificate issued for a DNS name).
    /// Defaults to the host.
    pub server_name: Option<String>,
    /// Accept any certificate without checking it. This removes the protection TLS gives
    /// against an attacker in the middle: use it for a throwaway test, never in production.
    /// Defaults to `false`.
    pub insecure_skip_verify: bool,
}

impl fmt::Debug for TlsOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsOptions")
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<pem>"))
            .field(
                "client_cert_pem",
                &self.client_cert_pem.as_ref().map(|_| "<pem>"),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("server_name", &self.server_name)
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .finish()
    }
}

/// The socket under a connection: plain TCP, or TCP wrapped in a TLS session.
pub(crate) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl Stream {
    fn socket(&self) -> &TcpStream {
        match self {
            Stream::Plain(socket) => socket,
            #[cfg(feature = "tls")]
            Stream::Tls(session) => &session.sock,
        }
    }

    pub(crate) fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.socket().set_read_timeout(timeout)
    }

    /// Wraps `socket` in a TLS session for `host` and finishes the handshake, so a rejected
    /// certificate fails the connect and not the first command.
    pub(crate) fn tls(
        socket: TcpStream,
        host: &str,
        options: &TlsOptions,
        handshake_timeout: Duration,
    ) -> Result<Self, Error> {
        #[cfg(feature = "tls")]
        {
            imp::connect(socket, host, options, handshake_timeout)
        }

        #[cfg(not(feature = "tls"))]
        {
            let _ = (socket, host, options, handshake_timeout);

            Err(Error::Protocol(
                "TLS is requested, but this build of ruvio-client has no `tls` feature".into(),
            ))
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(socket) => socket.read(buffer),
            #[cfg(feature = "tls")]
            Stream::Tls(session) => session.read(buffer),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(socket) => socket.write(buffer),
            #[cfg(feature = "tls")]
            Stream::Tls(session) => session.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(socket) => socket.flush(),
            #[cfg(feature = "tls")]
            Stream::Tls(session) => session.flush(),
        }
    }
}

#[cfg(feature = "tls")]
mod imp {
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{CryptoProvider, ring};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
    use rustls::{
        ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, SignatureScheme,
    };

    use super::{Stream, TlsOptions};
    use crate::error::Error;

    pub(super) fn connect(
        mut socket: TcpStream,
        host: &str,
        options: &TlsOptions,
        handshake_timeout: Duration,
    ) -> Result<Stream, Error> {
        let config = Arc::new(config(options)?);
        let name = options.server_name.as_deref().unwrap_or(host).to_owned();
        let name = ServerName::try_from(name)
            .map_err(|_| Error::Protocol(format!("`{host}` is not a valid TLS server name")))?;
        let mut session = ClientConnection::new(config, name)
            .map_err(|error| Error::Protocol(format!("TLS setup failed: {error}")))?;

        socket.set_read_timeout(Some(handshake_timeout))?;
        socket.set_write_timeout(Some(handshake_timeout))?;

        while session.is_handshaking() {
            session.complete_io(&mut socket)?;
        }

        socket.set_write_timeout(None)?;

        Ok(Stream::Tls(Box::new(rustls::StreamOwned::new(
            session, socket,
        ))))
    }

    fn config(options: &TlsOptions) -> Result<ClientConfig, Error> {
        let provider = Arc::new(ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|error| Error::Protocol(format!("TLS setup failed: {error}")))?;
        let builder = if options.insecure_skip_verify {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        } else {
            builder.with_root_certificates(roots(options)?)
        };

        match (&options.client_cert_pem, &options.client_key_pem) {
            (None, None) => Ok(builder.with_no_client_auth()),
            (Some(chain), Some(key)) => {
                let chain = CertificateDer::pem_slice_iter(chain)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| Error::Protocol(format!("client certificate: {error}")))?;
                let key = PrivateKeyDer::from_pem_slice(key)
                    .map_err(|error| Error::Protocol(format!("client key: {error}")))?;

                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|error| Error::Protocol(format!("client certificate: {error}")))
            }
            _ => Err(Error::Protocol(
                "client_cert_pem and client_key_pem must be set together".into(),
            )),
        }
    }

    fn roots(options: &TlsOptions) -> Result<RootCertStore, Error> {
        let mut roots = RootCertStore::empty();

        match &options.ca_pem {
            Some(pem) => {
                let mut added = 0;

                for certificate in CertificateDer::pem_slice_iter(pem) {
                    let certificate =
                        certificate.map_err(|error| Error::Protocol(format!("ca_pem: {error}")))?;

                    roots
                        .add(certificate)
                        .map_err(|error| Error::Protocol(format!("ca_pem: {error}")))?;
                    added += 1;
                }

                if added == 0 {
                    return Err(Error::Protocol("ca_pem holds no certificate".into()));
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }

        Ok(roots)
    }

    /// Takes any certificate; only for `insecure_skip_verify`.
    #[derive(Debug)]
    struct AcceptAny(Arc<CryptoProvider>);

    impl ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            certificate: &CertificateDer<'_>,
            signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                certificate,
                signature,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            certificate: &CertificateDer<'_>,
            signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                certificate,
                signature,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

#[cfg(all(test, not(feature = "tls")))]
mod tests {
    use super::*;

    #[test]
    fn asking_for_tls_without_the_feature_fails_the_connect() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let result = Stream::tls(
            socket,
            "127.0.0.1",
            &TlsOptions::default(),
            Duration::from_secs(1),
        );

        assert!(matches!(result, Err(Error::Protocol(message)) if message.contains("tls")));
    }
}
