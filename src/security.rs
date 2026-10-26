use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::acl::{AclDirectory, AclUser};

/// Startup security settings. Clones share one live ACL directory.
///
/// The directory is filled from these fields on first use. `ACL SETUSER`
/// then changes only that directory. A restart reads the fields again.
#[derive(Clone, Debug)]
pub struct SecurityConfig {
    pub requirepass: String,
    pub protected_mode: bool,
    /// Public bind with protected mode off and no password. Protocol tests set
    /// this. A production listener leaves it false.
    pub allow_unauthenticated_public: bool,
    /// Bearer secret for `GET /metrics`. Empty is allowed only on loopback.
    pub metrics_token: String,
    /// Handshake secret for the replication port. Empty uses `requirepass`.
    pub replication_password: String,
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    /// PEM issuer used to check a client certificate. Empty means clients are not asked for one.
    pub tls_client_ca: PathBuf,
    /// When true, the handshake rejects a client that does not present a certificate from `tls_client_ca`.
    pub tls_client_auth: bool,
    /// Live ACL users are saved and loaded here. Empty means `ACL SAVE` and
    /// `ACL LOAD` are refused. `GET` and `SET` never open this path.
    pub acl_file: PathBuf,
    pub users: Vec<AclUser>,
    /// Shared live user list. Visible inside the crate so struct updates can
    /// copy `..SecurityConfig::default()`. Callers use `directory()`.
    pub(crate) directory: Arc<Mutex<Option<Arc<AclDirectory>>>>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            requirepass: String::new(),
            protected_mode: true,
            allow_unauthenticated_public: false,
            metrics_token: String::new(),
            replication_password: String::new(),
            tls_cert: PathBuf::new(),
            tls_key: PathBuf::new(),
            tls_client_ca: PathBuf::new(),
            tls_client_auth: false,
            acl_file: PathBuf::new(),
            users: Vec::new(),
            directory: Arc::new(Mutex::new(None)),
        }
    }
}

impl PartialEq for SecurityConfig {
    fn eq(&self, other: &Self) -> bool {
        self.requirepass == other.requirepass
            && self.protected_mode == other.protected_mode
            && self.allow_unauthenticated_public == other.allow_unauthenticated_public
            && self.metrics_token == other.metrics_token
            && self.replication_password == other.replication_password
            && self.tls_cert == other.tls_cert
            && self.tls_key == other.tls_key
            && self.tls_client_ca == other.tls_client_ca
            && self.tls_client_auth == other.tls_client_auth
            && self.acl_file == other.acl_file
            && self.users == other.users
    }
}

impl Eq for SecurityConfig {}

impl SecurityConfig {
    /// Returns the process-wide user list. The first call copies `effective_users`.
    pub fn directory(&self) -> Arc<AclDirectory> {
        let mut slot = self
            .directory
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        if let Some(directory) = slot.as_ref() {
            return Arc::clone(directory);
        }

        let directory = Arc::new(AclDirectory::from_users(self.startup_users()));

        *slot = Some(Arc::clone(&directory));
        directory
    }
}

impl SecurityConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.requirepass.len() > 512 {
            return Err("requirepass must be at most 512 bytes");
        }

        if self.metrics_token.len() > 512 {
            return Err("metrics token must be at most 512 bytes");
        }

        if self.replication_password.len() > usize::from(u16::MAX) {
            return Err("replication password must be at most 65535 bytes");
        }

        if self.tls_enabled() != !self.tls_key.as_os_str().is_empty() {
            return Err("tls-cert and tls-key must be set together");
        }

        if self.tls_client_auth && self.tls_client_ca.as_os_str().is_empty() {
            return Err("tls client authentication requires tls-client-ca");
        }

        if !self.tls_client_ca.as_os_str().is_empty() && !self.tls_enabled() {
            return Err("tls-client-ca requires tls-cert and tls-key");
        }

        let mut names = std::collections::BTreeSet::new();

        for user in &self.users {
            if user.name.is_empty() || user.name.len() > 64 {
                return Err("ACL user name must be 1 to 64 bytes");
            }

            if !names.insert(user.name.to_ascii_lowercase()) {
                return Err("ACL user names must be unique");
            }
        }

        if self.acl_file.is_file() && crate::acl::parse_acl_file(&self.acl_file).is_err() {
            return Err("ACL file is invalid");
        }

        Ok(())
    }

    /// Users for the first directory fill. An existing ACL file wins over the
    /// config list. A missing file keeps the config list.
    pub(crate) fn startup_users(&self) -> Vec<AclUser> {
        if self.acl_file.is_file()
            && let Ok(users) = crate::acl::parse_acl_file(&self.acl_file)
        {
            return self.with_default_user(users);
        }

        self.effective_users()
    }

    pub fn tls_enabled(&self) -> bool {
        !self.tls_cert.as_os_str().is_empty()
    }

    pub fn redacted_requirepass(&self) -> &'static str {
        if self.requirepass.is_empty() {
            ""
        } else {
            "***"
        }
    }

    /// True when a new connection has to log in: the default user (from the ACL file when
    /// there is one, else the config) is disabled or has a password. For startup checks;
    /// connections look at the live directory through [`ConnectionAuth::from_security`].
    ///
    /// [`ConnectionAuth::from_security`]: crate::acl::ConnectionAuth::from_security
    pub fn authentication_enabled(&self) -> bool {
        self.startup_users()
            .iter()
            .find(|user| user.name.eq_ignore_ascii_case(b"default"))
            .is_none_or(|user| !user.enabled || user.has_password())
    }

    /// Secret the replication handshake checks. A dedicated password wins.
    pub fn replication_secret(&self) -> &[u8] {
        if self.replication_password.is_empty() {
            self.requirepass.as_bytes()
        } else {
            self.replication_password.as_bytes()
        }
    }
}

/// Compares two secrets without leaking where they differ or how long either one is: both are
/// hashed to 32 bytes first, then the digests are compared byte by byte with no early exit.
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let left = blake3::hash(left);
    let right = blake3::hash(right);
    let mut diff = 0u8;

    for (left_byte, right_byte) in left.as_bytes().iter().zip(right.as_bytes()) {
        diff |= left_byte ^ right_byte;
    }

    std::hint::black_box(diff) == 0
}

pub fn bind_is_loopback(address: &str) -> bool {
    address
        .parse::<SocketAddr>()
        .map(|address| address.ip().is_loopback())
        .unwrap_or(false)
}

pub fn ip_is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
}

pub const PROTECTED_MODE_ERROR: &[u8] = b"-DENIED Ruvio is running in protected mode because protected mode is enabled, no password is set, and the server is bound to a public interface\r\n";

/// Protected mode refuses a non-loopback client that would be logged in without a password.
/// `auth` is the connection's starting identity from the live ACL directory, so `ACL SETUSER
/// default nopass` or an ACL file count, not only the startup config.
pub fn protected_mode_denies(
    security: &SecurityConfig,
    auth: &crate::acl::ConnectionAuth,
    peer_ip: IpAddr,
) -> bool {
    security.protected_mode && auth.authenticated && !ip_is_loopback(peer_ip)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_compare_rejects_length_and_content_mismatches() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
        assert!(!constant_time_eq(b"secret", b"secrat"));
        assert!(!constant_time_eq(b"", b"secret"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn loopback_detection_matches_ipv4_and_ipv6() {
        assert!(bind_is_loopback("127.0.0.1:6379"));
        assert!(bind_is_loopback("[::1]:6379"));
        assert!(!bind_is_loopback("0.0.0.0:6379"));
        assert!(ip_is_loopback("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn protected_mode_denies_non_loopback_without_auth() {
        let denies = |security: &SecurityConfig, ip: &str| {
            let auth = crate::acl::ConnectionAuth::from_security(security);

            protected_mode_denies(security, &auth, ip.parse().unwrap())
        };
        let remote = "203.0.113.10";

        assert!(denies(&SecurityConfig::default(), remote));
        assert!(!denies(&SecurityConfig::default(), "127.0.0.1"));
        let locked = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };
        assert!(!denies(&locked, remote));
        let open = SecurityConfig {
            protected_mode: false,
            ..SecurityConfig::default()
        };
        assert!(!denies(&open, remote));
    }

    #[test]
    fn protected_mode_follows_the_live_acl_directory() {
        let remote = "203.0.113.10".parse().unwrap();
        let locked = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };

        // ACL SETUSER default nopass opens the server: protected mode must notice.
        locked
            .directory()
            .set_user(b"default", &[b"nopass".as_slice()])
            .unwrap();
        let auth = crate::acl::ConnectionAuth::from_security(&locked);

        assert!(auth.authenticated);
        assert!(protected_mode_denies(&locked, &auth, remote));

        // A password that lives only in the ACL file locks the server.
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("users.acl");

        std::fs::write(&file, "user default on >secret ~* +@all\n").unwrap();
        let from_file = SecurityConfig {
            acl_file: file,
            ..SecurityConfig::default()
        };
        let auth = crate::acl::ConnectionAuth::from_security(&from_file);

        assert!(from_file.authentication_enabled());
        assert!(!auth.authenticated);
        assert!(!protected_mode_denies(&from_file, &auth, remote));
    }
}
