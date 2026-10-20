//! TLS on the coordinator port and on the connections the coordinator opens: to the other
//! coordinators when the port has TLS, and to data nodes with `node_tls`. Loaded once when the
//! coordinator starts (one coordinator per process); everything is plain TCP when off.

use std::{
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::timeout,
};

use super::config::{Endpoint, TlsSettings};
use crate::tls::{TlsAcceptor, TlsDialer};

/// The longest a server-side handshake may take, as on data nodes.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection to or from a coordinator.
pub enum Stream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(context, bytes),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(context, bytes),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(context),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(context),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
        }
    }
}

/// The coordinator's TLS material.
#[derive(Default)]
pub struct Tls {
    acceptor: Option<TlsAcceptor>,
    dialer: Option<Arc<TlsDialer>>,
    node_tls: bool,
}

static INSTALLED: OnceLock<Tls> = OnceLock::new();

/// Loads the files named in `settings` for the rest of the process.
pub fn install(settings: &TlsSettings) -> io::Result<()> {
    INSTALLED
        .set(Tls::load(settings)?)
        .map_err(|_| io::Error::other("coordinator TLS was already set up"))
}

/// What [`install`] loaded; plain TCP everywhere before it ran.
pub fn get() -> &'static Tls {
    INSTALLED.get_or_init(Tls::default)
}

impl Tls {
    pub fn load(settings: &TlsSettings) -> io::Result<Self> {
        let acceptor = match &settings.pair {
            Some((cert, key)) => Some(TlsAcceptor::load(cert, key)?),
            None => None,
        };
        let dialer = match &settings.ca {
            Some(ca) => Some(Arc::new(TlsDialer::load(
                ca.clone(),
                settings.pair.clone(),
            )?)),
            None => None,
        };

        Ok(Self {
            acceptor,
            dialer,
            node_tls: settings.node_tls,
        })
    }

    /// An accepted connection, after the TLS handshake when the port has TLS.
    pub async fn accept(&self, stream: TcpStream) -> io::Result<Stream> {
        let Some(acceptor) = &self.acceptor else {
            return Ok(Stream::Plain(stream));
        };
        let acceptor = tokio_rustls::TlsAcceptor::from(acceptor.current_config()?);

        match timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
            Ok(Ok(stream)) => Ok(Stream::Tls(Box::new(stream.into()))),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "tls handshake timed out",
            )),
        }
    }

    /// A connection to the coordinator at `address` (`host:port`); TLS when this one's port
    /// has it.
    pub async fn dial_peer(&self, address: &str) -> io::Result<Stream> {
        let stream = TcpStream::connect(address).await?;

        stream.set_nodelay(true)?;
        if self.acceptor.is_none() {
            return Ok(Stream::Plain(stream));
        }
        let host = Endpoint::parse(address)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
            .host;

        self.wrap(&host, stream).await
    }

    /// A connection to a data node; TLS with `node_tls`.
    pub async fn dial_node(&self, address: &Endpoint) -> io::Result<Stream> {
        let stream = TcpStream::connect((address.host.as_str(), address.port)).await?;

        stream.set_nodelay(true)?;
        if !self.node_tls {
            return Ok(Stream::Plain(stream));
        }

        self.wrap(&address.host, stream).await
    }

    /// The client handshake with the server named `host`, which its certificate must cover.
    async fn wrap(&self, host: &str, stream: TcpStream) -> io::Result<Stream> {
        let dialer = self
            .dialer
            .as_ref()
            .ok_or_else(|| io::Error::other("tls is on without tls_ca"))?;
        let name = rustls::pki_types::ServerName::try_from(host.to_owned()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bad server name {host:?}: {error}"),
            )
        })?;
        let connector = tokio_rustls::TlsConnector::from(dialer.current_config()?);
        let stream = connector.connect(name, stream).await?;

        Ok(Stream::Tls(Box::new(stream.into())))
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use std::{fs, path::Path};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A CA and a server certificate it signed for `localhost` and 127.0.0.1.
    fn certificates(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(vec!["Ruvio test CA".into()]).unwrap();

        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let key = rcgen::KeyPair::generate().unwrap();
        let params =
            rcgen::CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let cert = params.signed_by(&key, &issuer).unwrap();
        let paths = (
            dir.join("ca.crt"),
            dir.join("node.crt"),
            dir.join("node.key"),
        );

        fs::write(&paths.0, ca_cert.pem()).unwrap();
        fs::write(&paths.1, cert.pem()).unwrap();
        fs::write(&paths.2, key.serialize_pem()).unwrap();
        paths
    }

    async fn echo_once(tls: Tls) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            if let Ok(mut stream) = tls.accept(stream).await {
                let mut byte = [0; 1];

                if stream.read_exact(&mut byte).await.is_ok() {
                    let _ = stream.write_all(&byte).await;
                    let _ = stream.flush().await;
                }
            }
        });
        address
    }

    fn settings(dir: &Path, node_tls: bool) -> TlsSettings {
        let (ca, cert, key) = certificates(dir);

        TlsSettings {
            pair: Some((cert, key)),
            ca: Some(ca),
            node_tls,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn coordinators_talk_over_tls_with_a_shared_ca() {
        let dir = tempfile::tempdir().unwrap();
        let settings = settings(dir.path(), false);
        let address = echo_once(Tls::load(&settings).unwrap()).await;
        let mut stream = timeout(
            Duration::from_secs(10),
            Tls::load(&settings)
                .unwrap()
                .dial_peer(&address.to_string()),
        )
        .await
        .unwrap()
        .unwrap();
        let mut echoed = [0; 1];

        assert!(matches!(stream, Stream::Tls(_)));
        stream.write_all(&[42]).await.unwrap();
        stream.flush().await.unwrap();
        timeout(Duration::from_secs(10), stream.read_exact(&mut echoed))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(echoed, [42]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_server_from_another_ca_and_a_plain_client_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let ours = settings(dir.path(), true);
        let theirs = settings(other.path(), true);

        // Our CA does not vouch for their certificate.
        let address = echo_once(Tls::load(&theirs).unwrap()).await;
        let refused = timeout(
            Duration::from_secs(10),
            Tls::load(&ours).unwrap().dial_peer(&address.to_string()),
        )
        .await
        .unwrap();

        assert!(refused.is_err());
        let address = echo_once(Tls::load(&theirs).unwrap()).await;
        let node = Endpoint {
            host: "127.0.0.1".into(),
            port: address.port(),
        };

        assert!(
            timeout(
                Duration::from_secs(10),
                Tls::load(&ours).unwrap().dial_node(&node)
            )
            .await
            .unwrap()
            .is_err()
        );

        // A plain client gets no answer from a TLS port.
        let address = echo_once(Tls::load(&ours).unwrap()).await;
        let mut plain = Tls::default()
            .dial_peer(&address.to_string())
            .await
            .unwrap();
        let mut reply = Vec::new();

        plain.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let _ = timeout(Duration::from_secs(10), plain.read_to_end(&mut reply))
            .await
            .unwrap();
        assert!(!reply.starts_with(b"+PONG"), "{reply:?}");
    }
}
