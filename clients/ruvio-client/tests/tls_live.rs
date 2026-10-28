//! TLS against live servers: needs the `tls` feature and the servers `scripts/run_with_servers.sh`
//! starts (`RUVIO_TLS_ADDR`, `RUVIO_TLS_CLIENT_AUTH_ADDR`, `RUVIO_TLS_CA`, `RUVIO_TLS_CLIENT_CERT`,
//! `RUVIO_TLS_CLIENT_KEY`). Without them the tests return early, like the other live tests.
#![cfg(feature = "tls")]

use std::time::Duration;

use ruvio_client::{Client, ClientOptions, Error, TlsOptions};

fn pem(variable: &str) -> Option<Vec<u8>> {
    std::fs::read(std::env::var(variable).ok()?).ok()
}

fn options(variable: &str, tls: TlsOptions) -> Option<ClientOptions> {
    let address = std::env::var(variable).ok()?;
    let (host, port) = address.rsplit_once(':')?;

    Some(ClientOptions {
        host: host.to_owned(),
        port: port.parse().ok()?,
        connect_timeout: Duration::from_secs(5),
        command_timeout: Some(Duration::from_secs(10)),
        tls: Some(tls),
        ..ClientOptions::default()
    })
}

fn trusting_ca() -> Option<TlsOptions> {
    Some(TlsOptions {
        ca_pem: Some(pem("RUVIO_TLS_CA")?),
        ..TlsOptions::default()
    })
}

#[test]
fn a_verified_connection_runs_commands() {
    let Some(tls) = trusting_ca() else { return };
    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };
    let mut client = Client::connect_with(options).unwrap();

    client.set("tls:key", "value").unwrap();
    assert_eq!(
        client.get_string("tls:key").unwrap().as_deref(),
        Some("value")
    );

    // A blocking command widens the read timeout on the TLS socket and puts it back.
    assert_eq!(
        client
            .blpop(&["tls:missing"], Duration::from_secs(1))
            .unwrap(),
        None
    );
    assert_eq!(
        client.get_string("tls:key").unwrap().as_deref(),
        Some("value")
    );
}

#[test]
fn a_large_value_survives_the_tls_records() {
    let Some(tls) = trusting_ca() else { return };
    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };
    let mut client = Client::connect_with(options).unwrap();
    let value = "x".repeat(300_000);

    client.set("tls:big", value.as_str()).unwrap();
    assert_eq!(
        client.get_string("tls:big").unwrap().as_deref(),
        Some(value.as_str())
    );
}

#[test]
fn an_untrusted_certificate_is_refused() {
    // The built-in public roots do not know the test CA.
    let Some(options) = options("RUVIO_TLS_ADDR", TlsOptions::default()) else {
        return;
    };

    match Client::connect_with(options) {
        Err(Error::Io(error)) => assert!(
            error.to_string().to_lowercase().contains("certificate"),
            "unexpected error: {error}"
        ),
        other => panic!("expected a certificate error, got {:?}", other.map(|_| ())),
    }
}

#[test]
fn a_name_the_certificate_lacks_is_refused() {
    let Some(mut tls) = trusting_ca() else { return };

    tls.server_name = Some("elsewhere.example".to_owned());

    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };

    assert!(matches!(Client::connect_with(options), Err(Error::Io(_))));
}

#[test]
fn a_dns_name_can_be_checked_while_connecting_by_address() {
    let Some(mut tls) = trusting_ca() else { return };

    tls.server_name = Some("localhost".to_owned());

    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };

    assert!(Client::connect_with(options).is_ok());
}

#[test]
fn skipping_verification_is_an_explicit_choice() {
    let tls = TlsOptions {
        insecure_skip_verify: true,
        ..TlsOptions::default()
    };
    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };
    let mut client = Client::connect_with(options).unwrap();

    assert_eq!(client.ping_message("hi").unwrap(), "hi");
}

#[test]
fn a_server_that_wants_a_client_certificate_gets_one() {
    let (Some(mut tls), Some(cert), Some(key)) = (
        trusting_ca(),
        pem("RUVIO_TLS_CLIENT_CERT"),
        pem("RUVIO_TLS_CLIENT_KEY"),
    ) else {
        return;
    };

    tls.client_cert_pem = Some(cert);
    tls.client_key_pem = Some(key);

    let Some(options) = options("RUVIO_TLS_CLIENT_AUTH_ADDR", tls) else {
        return;
    };
    let mut client = Client::connect_with(options).unwrap();

    assert_eq!(client.ping_message("mtls").unwrap(), "mtls");
}

#[test]
fn a_missing_client_certificate_is_refused() {
    let Some(tls) = trusting_ca() else { return };
    let Some(options) = options("RUVIO_TLS_CLIENT_AUTH_ADDR", tls) else {
        return;
    };

    assert!(Client::connect_with(options).is_err());
}

#[test]
fn a_client_certificate_without_its_key_is_a_setup_error() {
    let Some(mut tls) = trusting_ca() else { return };

    tls.client_cert_pem = pem("RUVIO_TLS_CLIENT_CERT");

    let Some(options) = options("RUVIO_TLS_ADDR", tls) else {
        return;
    };

    assert!(matches!(
        Client::connect_with(options),
        Err(Error::Protocol(_))
    ));
}

#[test]
fn plain_tcp_to_a_tls_port_does_not_connect() {
    let Some(mut options) = options("RUVIO_TLS_ADDR", TlsOptions::default()) else {
        return;
    };

    options.tls = None;
    options.command_timeout = Some(Duration::from_secs(2));

    assert!(Client::connect_with(options).is_err());
}
