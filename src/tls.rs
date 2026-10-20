use std::{
    fs, io,
    net::TcpStream,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection, StreamOwned};

use crate::security::SecurityConfig;

#[derive(Clone, Debug)]
struct LoadedTls {
    config: Arc<ServerConfig>,
    cert_mtime: Option<SystemTime>,
    key_mtime: Option<SystemTime>,
    client_ca_mtime: Option<SystemTime>,
}

/// Accept-path TLS material. Reload checks file mtimes under a mutex used only
/// at handshake time; shard command execution does not take this lock.
pub struct TlsAcceptor {
    cert_path: PathBuf,
    key_path: PathBuf,
    client_ca: Option<PathBuf>,
    client_auth: bool,
    loaded: Mutex<LoadedTls>,
}

impl TlsAcceptor {
    pub fn from_security(security: &SecurityConfig) -> io::Result<Option<Self>> {
        if !security.tls_enabled() {
            return Ok(None);
        }

        let client_ca = (!security.tls_client_ca.as_os_str().is_empty())
            .then(|| security.tls_client_ca.clone());

        Self::load_with(
            &security.tls_cert,
            &security.tls_key,
            client_ca,
            security.tls_client_auth,
        )
        .map(Some)
    }

    pub fn load(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> io::Result<Self> {
        Self::load_with(cert_path, key_path, None, false)
    }

    pub fn load_with(
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        client_ca: Option<PathBuf>,
        client_auth: bool,
    ) -> io::Result<Self> {
        install_crypto_provider();
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        let loaded = load_material(&cert_path, &key_path, client_ca.as_deref(), client_auth)?;

        Ok(Self {
            cert_path,
            key_path,
            client_ca,
            client_auth,
            loaded: Mutex::new(loaded),
        })
    }

    pub fn handshake(
        &self,
        mut stream: TcpStream,
    ) -> io::Result<StreamOwned<ServerConnection, TcpStream>> {
        let config = self.current_config()?;
        let mut connection = ServerConnection::new(config)
            .map_err(|error| io::Error::other(format!("tls handshake setup failed: {error}")))?;

        connection.complete_io(&mut stream).map_err(|error| {
            io::Error::new(error.kind(), format!("tls handshake failed: {error}"))
        })?;

        Ok(StreamOwned::new(connection, stream))
    }

    pub(crate) fn current_config(&self) -> io::Result<Arc<ServerConfig>> {
        let mut loaded = self
            .loaded
            .lock()
            .map_err(|_| io::Error::other("tls reload lock poisoned"))?;
        let cert_mtime = mtime(&self.cert_path);
        let key_mtime = mtime(&self.key_path);
        let client_ca_mtime = self.client_ca.as_deref().and_then(mtime);

        if cert_mtime != loaded.cert_mtime
            || key_mtime != loaded.key_mtime
            || client_ca_mtime != loaded.client_ca_mtime
        {
            // A half-rotated pair (new certificate, old key) or a half-written file must not
            // take the listener down: keep serving the last good material and try again when
            // a file changes next. The attempted times are remembered so the same bad files
            // are not parsed again on every connection.
            match load_material(
                &self.cert_path,
                &self.key_path,
                self.client_ca.as_deref(),
                self.client_auth,
            ) {
                Ok(fresh) => *loaded = fresh,
                Err(error) => {
                    tracing::warn!(%error, "tls reload failed, keeping the previous certificate");
                    loaded.cert_mtime = cert_mtime;
                    loaded.key_mtime = key_mtime;
                    loaded.client_ca_mtime = client_ca_mtime;
                }
            }
        }

        Ok(Arc::clone(&loaded.config))
    }
}

/// The replica's side of a TLS replication link: trusts the primary's certificate chain from
/// `ca_path`, and presents a client certificate when the primary asks for one.
pub struct ReplicaTlsClient {
    config: Arc<ClientConfig>,
}

impl ReplicaTlsClient {
    pub fn load(ca_path: &Path, client_pair: Option<(&Path, &Path)>) -> io::Result<Self> {
        Ok(Self {
            config: Arc::new(client_config(ca_path, client_pair, "replication tls_ca")?),
        })
    }

    /// Completes the handshake with the primary named `server_name` (a host name or an IP
    /// address that its certificate covers) before returning the stream.
    pub fn connect(
        &self,
        server_name: &str,
        mut stream: TcpStream,
    ) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
        let name =
            rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("bad server name: {error}"),
                )
            })?;
        let mut connection = ClientConnection::new(Arc::clone(&self.config), name)
            .map_err(|error| io::Error::other(format!("tls setup failed: {error}")))?;

        connection.complete_io(&mut stream).map_err(|error| {
            io::Error::new(error.kind(), format!("tls handshake failed: {error}"))
        })?;

        Ok(StreamOwned::new(connection, stream))
    }
}

/// Client TLS material that is reloaded when its files change, as [`TlsAcceptor`] does: the
/// coordinator's connections to the other coordinators and to data nodes. Trusts `ca`, and
/// presents `pair` to a server that asks for a client certificate.
#[cfg(feature = "coordinator")]
pub struct TlsDialer {
    ca: PathBuf,
    pair: Option<(PathBuf, PathBuf)>,
    loaded: Mutex<LoadedClient>,
}

#[cfg(feature = "coordinator")]
struct LoadedClient {
    config: Arc<ClientConfig>,
    mtimes: [Option<SystemTime>; 3],
}

#[cfg(feature = "coordinator")]
impl TlsDialer {
    pub fn load(ca: PathBuf, pair: Option<(PathBuf, PathBuf)>) -> io::Result<Self> {
        install_crypto_provider();
        let mtimes = Self::mtimes(&ca, pair.as_ref());
        let config = Self::build(&ca, pair.as_ref())?;

        Ok(Self {
            ca,
            pair,
            loaded: Mutex::new(LoadedClient {
                config: Arc::new(config),
                mtimes,
            }),
        })
    }

    fn build(ca: &Path, pair: Option<&(PathBuf, PathBuf)>) -> io::Result<ClientConfig> {
        client_config(
            ca,
            pair.map(|(cert, key)| (cert.as_path(), key.as_path())),
            "tls_ca",
        )
    }

    fn mtimes(ca: &Path, pair: Option<&(PathBuf, PathBuf)>) -> [Option<SystemTime>; 3] {
        [
            mtime(ca),
            pair.and_then(|(cert, _)| mtime(cert)),
            pair.and_then(|(_, key)| mtime(key)),
        ]
    }

    pub fn current_config(&self) -> io::Result<Arc<ClientConfig>> {
        let mut loaded = self
            .loaded
            .lock()
            .map_err(|_| io::Error::other("tls reload lock poisoned"))?;
        let mtimes = Self::mtimes(&self.ca, self.pair.as_ref());

        if mtimes != loaded.mtimes {
            // As on the accept path: half-rotated files keep the last good material.
            match Self::build(&self.ca, self.pair.as_ref()) {
                Ok(fresh) => loaded.config = Arc::new(fresh),
                Err(error) => {
                    tracing::warn!(%error, "tls reload failed, keeping the previous certificate");
                }
            }
            loaded.mtimes = mtimes;
        }

        Ok(Arc::clone(&loaded.config))
    }
}

/// A client configuration that trusts the certificates in `ca_path` (`label` names the
/// setting in errors) and presents `client_pair` when the server asks for a certificate.
fn client_config(
    ca_path: &Path,
    client_pair: Option<(&Path, &Path)>,
    label: &str,
) -> io::Result<ClientConfig> {
    install_crypto_provider();
    let bytes = fs::read(ca_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read {label} {}: {error}", ca_path.display()),
        )
    })?;
    let mut roots = rustls::RootCertStore::empty();

    for certificate in CertificateDer::pem_slice_iter(&bytes) {
        let certificate = certificate
            .map_err(|error| io::Error::other(format!("invalid {label} PEM: {error}")))?;

        roots
            .add(certificate)
            .map_err(|error| io::Error::other(format!("{label} was not added: {error}")))?;
    }

    if roots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} contains no certificates"),
        ));
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);

    match client_pair {
        None => Ok(builder.with_no_client_auth()),
        Some((cert_path, key_path)) => {
            let (certs, key) = read_pair(cert_path, key_path)?;

            builder
                .with_client_auth_cert(certs, key)
                .map_err(|error| io::Error::other(format!("invalid client certificate: {error}")))
        }
    }
}

fn read_pair(
    cert_path: &Path,
    key_path: &Path,
) -> io::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_bytes = fs::read(cert_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read {}: {error}", cert_path.display()),
        )
    })?;
    let key_bytes = fs::read(key_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read {}: {error}", key_path.display()),
        )
    })?;
    let certs = CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<CertificateDer<'static>>, _>>()
        .map_err(|error| io::Error::other(format!("invalid certificate PEM: {error}")))?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} contains no certificates", cert_path.display()),
        ));
    }

    let key = PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(|error| io::Error::other(format!("invalid private key PEM: {error}")))?;

    Ok((certs, key))
}

fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).and_then(|meta| meta.modified()).ok()
}

fn load_material(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
    client_auth: bool,
) -> io::Result<LoadedTls> {
    install_crypto_provider();
    let cert_bytes = fs::read(cert_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read tls-cert {}: {error}", cert_path.display()),
        )
    })?;

    let key_bytes = fs::read(key_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to read tls-key {}: {error}", key_path.display()),
        )
    })?;

    let certs = CertificateDer::pem_slice_iter(&cert_bytes)
        .collect::<Result<Vec<CertificateDer<'static>>, _>>()
        .map_err(|error| io::Error::other(format!("invalid tls-cert PEM: {error}")))?;

    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tls-cert contains no certificates",
        ));
    }

    let key = PrivateKeyDer::from_pem_slice(&key_bytes)
        .map_err(|error| io::Error::other(format!("invalid tls-key PEM: {error}")))?;
    let builder = ServerConfig::builder();
    let config = match client_ca {
        None => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|error| {
                io::Error::other(format!("invalid tls certificate/key pair: {error}"))
            })?,
        Some(ca_path) => {
            let verifier = client_verifier(ca_path, client_auth)?;

            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|error| {
                    io::Error::other(format!("invalid tls certificate/key pair: {error}"))
                })?
        }
    };

    Ok(LoadedTls {
        config: Arc::new(config),
        cert_mtime: mtime(cert_path),
        key_mtime: mtime(key_path),
        client_ca_mtime: client_ca.and_then(mtime),
    })
}

fn client_verifier(
    ca_path: &Path,
    required: bool,
) -> io::Result<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
    let bytes = fs::read(ca_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "failed to read tls-client-ca {}: {error}",
                ca_path.display()
            ),
        )
    })?;

    let mut roots = rustls::RootCertStore::empty();

    for certificate in CertificateDer::pem_slice_iter(&bytes) {
        let certificate = certificate
            .map_err(|error| io::Error::other(format!("invalid tls-client-ca PEM: {error}")))?;

        roots
            .add(certificate)
            .map_err(|error| io::Error::other(format!("tls-client-ca was not added: {error}")))?;
    }

    if roots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "tls-client-ca contains no certificates",
        ));
    }

    let builder = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots));
    let verifier = if required {
        builder.build()
    } else {
        builder.allow_unauthenticated().build()
    };

    verifier.map_err(|error| io::Error::other(format!("tls client verifier: {error}")))
}

#[cfg(all(test, not(miri)))]
pub(crate) fn write_self_signed_pair(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    let certified = rcgen::generate_simple_self_signed(["localhost".into()]).unwrap();
    let cert = dir.join(format!("{name}.crt"));
    let key = dir.join(format!("{name}.key"));

    fs::write(&cert, certified.cert.pem()).unwrap();
    fs::write(&key, certified.signing_key.serialize_pem()).unwrap();
    (cert, key)
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn loads_pem_material_and_reloads_when_files_change() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed_pair(dir.path(), "one");
        let acceptor = TlsAcceptor::load(&cert, &key).unwrap();
        let first = acceptor.current_config().unwrap();
        let loaded_at = mtime(&cert).unwrap();
        let (cert2, key2) = write_self_signed_pair(dir.path(), "two");

        fs::copy(&cert2, &cert).unwrap();
        fs::copy(&key2, &key).unwrap();
        let mut touch = fs::OpenOptions::new().append(true).open(&cert).unwrap();

        writeln!(touch, "# reloaded").unwrap();
        // Writes within one tick of a file system's clock can keep the old modification
        // time: give the file one that differs whatever the clock's granularity.
        touch
            .set_modified(loaded_at + std::time::Duration::from_secs(60))
            .unwrap();
        drop(touch);
        let second = acceptor.current_config().unwrap();

        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn a_failed_reload_keeps_the_previous_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed_pair(dir.path(), "one");
        let acceptor = TlsAcceptor::load(&cert, &key).unwrap();
        let first = acceptor.current_config().unwrap();
        let loaded_at = mtime(&cert).unwrap();

        fs::write(&cert, b"not a certificate").unwrap();
        let broken = fs::OpenOptions::new().append(true).open(&cert).unwrap();

        broken
            .set_modified(loaded_at + std::time::Duration::from_secs(60))
            .unwrap();
        drop(broken);

        let second = acceptor.current_config().unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        // The same bad file is not parsed again, and a repaired pair is picked up.
        assert!(Arc::ptr_eq(&first, &acceptor.current_config().unwrap()));
        let (cert2, key2) = write_self_signed_pair(dir.path(), "two");

        fs::copy(&cert2, &cert).unwrap();
        fs::copy(&key2, &key).unwrap();
        let fixed = fs::OpenOptions::new().append(true).open(&cert).unwrap();

        fixed
            .set_modified(loaded_at + std::time::Duration::from_secs(120))
            .unwrap();
        drop(fixed);

        assert!(!Arc::ptr_eq(&first, &acceptor.current_config().unwrap()));
    }

    #[test]
    fn required_client_certificate_rejects_a_client_without_one() {
        let dir = tempfile::tempdir().unwrap();
        let (server_cert, server_key) = write_self_signed_pair(dir.path(), "server");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca_params = rcgen::CertificateParams::new(vec!["Ruvio CA".into()]).unwrap();

        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_path = dir.path().join("ca.crt");

        fs::write(&ca_path, ca_cert.pem()).unwrap();
        let client_key = rcgen::KeyPair::generate().unwrap();
        let client_params = rcgen::CertificateParams::new(vec!["client".into()]).unwrap();
        let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
        let client_cert = client_params.signed_by(&client_key, &issuer).unwrap();
        let acceptor =
            TlsAcceptor::load_with(&server_cert, &server_key, Some(ca_path), true).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        let (report, receive) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let stream = accept_bounded(&listener);
            let missing = acceptor
                .handshake(stream)
                .err()
                .map(|error| error.to_string());

            report.send(missing).unwrap();
            let stream = accept_bounded(&listener);

            acceptor.handshake(stream).unwrap();
        });

        let roots = trusted_server(&server_cert);
        connect_tls(address, roots.clone(), None).unwrap();
        let server_missing = receive
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("server thread reported no handshake result within 10 s");

        assert!(
            server_missing
                .as_deref()
                .is_some_and(|error| error.contains("no certificates")),
            "{server_missing:?}"
        );
        assert!(
            connect_tls(
                address,
                roots,
                Some((client_cert.pem(), client_key.serialize_pem()))
            )
            .is_ok()
        );
        server.join().unwrap();
    }

    fn accept_bounded(listener: &std::net::TcpListener) -> std::net::TcpStream {
        let timeout = std::time::Duration::from_secs(10);
        let deadline = std::time::Instant::now() + timeout;

        listener.set_nonblocking(true).unwrap();
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    stream.set_read_timeout(Some(timeout)).unwrap();
                    stream.set_write_timeout(Some(timeout)).unwrap();
                    return stream;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "no TLS client connected within 10 s"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(error) => panic!("test listener accept failed: {error}"),
            }
        }
    }

    fn trusted_server(cert_path: &Path) -> Arc<rustls::RootCertStore> {
        let bytes = fs::read(cert_path).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        let certificate = CertificateDer::from_pem_slice(&bytes).unwrap();

        roots.add(certificate).unwrap();
        Arc::new(roots)
    }

    fn connect_tls(
        address: std::net::SocketAddr,
        roots: Arc<rustls::RootCertStore>,
        client: Option<(String, String)>,
    ) -> io::Result<()> {
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let config = match client {
            None => builder.with_no_client_auth(),
            Some((cert_pem, key_pem)) => {
                let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();

                builder.with_client_auth_cert(certs, key).unwrap()
            }
        };

        let mut stream = std::net::TcpStream::connect(address)?;

        stream.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(10)))?;
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut connection = rustls::ClientConnection::new(Arc::new(config), name)
            .map_err(|error| io::Error::other(error.to_string()))?;

        connection
            .complete_io(&mut stream)
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;

        Ok(())
    }

    fn serve_once(acceptor: TlsAcceptor) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();

            if let Ok(mut secure) = acceptor.handshake(stream) {
                let mut byte = [0u8; 1];

                io::Read::read_exact(&mut secure, &mut byte).unwrap();
                secure.write_all(&byte).unwrap();
                secure.flush().unwrap();
            }
        });

        (address, handle)
    }

    #[test]
    fn replica_client_talks_to_a_primary_that_its_ca_vouches_for() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed_pair(dir.path(), "primary");
        let (address, server) = serve_once(TlsAcceptor::load(&cert, &key).unwrap());
        let client = ReplicaTlsClient::load(&cert, None).unwrap();
        let stream = std::net::TcpStream::connect(address).unwrap();

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        let mut secure = client.connect("localhost", stream).unwrap();
        let mut echoed = [0u8; 1];

        secure.write_all(&[42]).unwrap();
        secure.flush().unwrap();
        io::Read::read_exact(&mut secure, &mut echoed).unwrap();
        assert_eq!(echoed, [42]);
        server.join().unwrap();
    }

    #[test]
    fn replica_client_refuses_a_primary_from_another_ca() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed_pair(dir.path(), "primary");
        let (other, _) = write_self_signed_pair(dir.path(), "other");
        let (address, server) = serve_once(TlsAcceptor::load(&cert, &key).unwrap());
        let client = ReplicaTlsClient::load(&other, None).unwrap();
        let stream = std::net::TcpStream::connect(address).unwrap();

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        assert!(client.connect("localhost", stream).is_err());
        server.join().unwrap();
    }

    #[test]
    fn rejects_empty_or_mismatched_pem() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("empty.crt");
        let key = dir.path().join("empty.key");

        fs::write(&cert, b"").unwrap();
        fs::write(&key, b"").unwrap();
        assert!(TlsAcceptor::load(&cert, &key).is_err());
    }
}
