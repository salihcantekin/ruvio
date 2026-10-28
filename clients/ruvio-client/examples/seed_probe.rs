//! Probe for `scripts/seed_failover.py`: connects with the seeds in `RUVIO_SEEDS`, reads
//! `seed:name`, waits for a line on stdin (the first server is stopped by then) and reads it again.
//!
//! ```text
//! scripts/seed_failover.py target/debug/ruvio -- target/debug/examples/seed_probe
//! ```

use ruvio_client::{Client, ClientOptions};
use std::{io::BufRead, thread, time::Duration};

fn main() {
    let seeds = std::env::var("RUVIO_SEEDS").expect("RUVIO_SEEDS is set by the script");
    let seeds = seeds
        .split(',')
        .map(|seed| {
            let (host, port) = seed.rsplit_once(':').expect("host:port");

            (host.to_owned(), port.parse().expect("port"))
        })
        .collect();
    let mut client = Client::connect_with(ClientOptions {
        seeds,
        reconnect_base_delay: Duration::from_millis(50),
        ..ClientOptions::default()
    })
    .expect("connect");

    let first = client.get_string("seed:name").expect("first GET");

    println!("first {}", first.unwrap_or_default());

    let mut line = String::new();

    std::io::stdin().lock().read_line(&mut line).expect("stdin");

    for _ in 0..50 {
        if let Ok(value) = client.get_string("seed:name") {
            println!("after {}", value.unwrap_or_default());

            return;
        }

        thread::sleep(Duration::from_millis(100));
    }

    std::process::exit(1);
}
