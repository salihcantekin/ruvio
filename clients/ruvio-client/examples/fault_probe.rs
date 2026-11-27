//! Probe for `scripts/client_faults.py`: writes `fault:<name>:<i>` = `i` with durability replicas 1,
//! reads each key back, and keeps going through a failover until a line arrives on stdin.
//! Environment: `RUVIO_SEEDS`, `RUVIO_PROBE_NAME` and, for TLS, `RUVIO_PROBE_CA` (PEM path). Output,
//! one line each: `ready`, `ack <i>`, `err <i> <text>`, `bad <i> <value>`, `done`.
//!
//! ```text
//! cargo build --example fault_probe --features tls
//! python3 scripts/client_faults.py target/debug/ruvio
//! ```

use ruvio_client::{Client, ClientOptions, Durability};
use std::{
    io::BufRead,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

fn main() {
    let seeds = std::env::var("RUVIO_SEEDS").expect("RUVIO_SEEDS is set by the script");
    let name = std::env::var("RUVIO_PROBE_NAME").unwrap_or_default();
    let seeds = seeds
        .split(',')
        .map(|seed| {
            let (host, port) = seed.rsplit_once(':').expect("host:port");

            (host.to_owned(), port.parse().expect("port"))
        })
        .collect();
    #[allow(unused_mut)]
    let mut options = ClientOptions {
        seeds,
        durability: Some(Durability {
            replicas: 1,
            timeout_ms: None,
        }),
        reconnect_base_delay: Duration::from_millis(50),
        ..ClientOptions::default()
    };

    if let Ok(ca) = std::env::var("RUVIO_PROBE_CA") {
        #[cfg(feature = "tls")]
        {
            options.tls = Some(ruvio_client::TlsOptions {
                ca_pem: Some(std::fs::read(ca).expect("CA file")),
                ..ruvio_client::TlsOptions::default()
            });
        }
        #[cfg(not(feature = "tls"))]
        {
            let _ = ca;
            panic!("build with --features tls");
        }
    }

    let mut client = Client::connect_with(options).expect("connect");
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);

    thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().lock().read_line(&mut line);
        flag.store(true, Ordering::SeqCst);
    });

    println!("ready");

    let mut i: u64 = 1;

    while !stop.load(Ordering::SeqCst) {
        let key = format!("fault:{name}:{i}");
        let result = client
            .set(&key, &i.to_string())
            .and_then(|_| client.get_string(&key));

        match result {
            Ok(got) => {
                if got.as_deref() != Some(i.to_string().as_str()) {
                    println!("bad {i} {got:?}");
                }

                println!("ack {i}");
                i += 1;
            }
            Err(error) => {
                let text = format!("{error:?}");

                println!(
                    "err {i} {}",
                    text.split(['(', ' ', '{']).next().unwrap_or("")
                );
                thread::sleep(Duration::from_millis(20));
            }
        }
    }

    println!("done");
}
