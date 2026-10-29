//! Read-only view of the `ruvio coordinator` processes. The addresses and the secret come from
//! the environment and never reach the browser. Only the fixed read commands below are sent:
//! there is no passthrough, so the secret cannot be used from the UI to change anything.

use std::{
    collections::HashMap,
    env,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};

use ruvio_client::{Client, ClientOptions, RespValue};
use serde_json::{Value, json};

use crate::{Shared, encode_value, parallel, topology::text_of};

type Clients = Mutex<HashMap<(String, u16), Shared<Client>>>;

struct Coordinators {
    addresses: Vec<(String, u16)>,
    secret: Option<String>,
    clients: Clients,
}

static COORDINATORS: LazyLock<Option<Coordinators>> = LazyLock::new(|| {
    let addresses = parse_addresses(&env::var("RUVIO_UI_COORDINATORS").unwrap_or_default());

    (!addresses.is_empty()).then(|| Coordinators {
        addresses,
        secret: env::var("RUVIO_UI_COORDINATOR_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty()),
        clients: Mutex::new(HashMap::new()),
    })
});

/// `host:port,host:port`; `[::1]:26400` for IPv6. Entries without a valid port are skipped.
pub fn parse_addresses(text: &str) -> Vec<(String, u16)> {
    text.split(',')
        .filter_map(|entry| {
            let (host, port) = entry.trim().rsplit_once(':')?;
            let host = host.trim_matches(['[', ']']);

            (!host.is_empty()).then_some(())?;

            Some((host.to_owned(), port.parse().ok()?))
        })
        .collect()
}

pub fn configured() -> bool {
    COORDINATORS.is_some()
}

impl Coordinators {
    fn client(&self, address: &(String, u16)) -> Result<Shared<Client>, String> {
        if let Some(client) = self.clients.lock().expect("coordinators").get(address) {
            return Ok(Arc::clone(client));
        }

        let mut options = ClientOptions::default();

        options.host = address.0.clone();
        options.port = address.1;
        options.discover_cluster = false;
        options.password = self.secret.clone();
        options.connect_timeout = Duration::from_secs(1);
        options.max_reconnect_attempts = 1;
        options.reconnect_base_delay = Duration::from_millis(50);

        let mut client = Client::connect_with(options).map_err(|error| error.to_string())?;
        let _ = client.set_read_timeout(Some(Duration::from_secs(2)));
        let client = Arc::new(Mutex::new(client));

        Ok(Arc::clone(
            self.clients
                .lock()
                .expect("coordinators")
                .entry(address.clone())
                .or_insert(client),
        ))
    }

    fn call(&self, address: &(String, u16), args: &[&str]) -> Result<RespValue, String> {
        let client = self.client(address)?;
        let mut client = client.lock().expect("coordinator client");

        match client.execute(args) {
            Ok(RespValue::Error(message)) => Err(message),
            Ok(value) => Ok(value),
            Err(error) => Err(error.to_string()),
        }
    }
}

/// `INFO` from every coordinator; then, from the leader (or the first that answered while
/// there is none), the agreed primaries, each group's other members and the slot move status.
pub fn report() -> Value {
    let Some(coordinators) = COORDINATORS.as_ref() else {
        return json!({"ok": true, "configured": false});
    };

    let infos = parallel(&coordinators.addresses, |address| {
        let started = Instant::now();
        let result = coordinators.call(address, &["INFO"]);

        (result, started.elapsed())
    });
    let mut source = None;
    let entries: Vec<Value> = coordinators
        .addresses
        .iter()
        .zip(&infos)
        .map(|(address, (result, elapsed))| {
            let name = format!("{}:{}", address.0, address.1);

            match result {
                Ok(value) => {
                    let text = text_of(value).unwrap_or_default();

                    if text.contains("is_leader:1") || source.is_none() {
                        source = Some(address.clone());
                    }

                    json!({"address": name, "ok": true, "info": text, "ms": elapsed.as_millis() as u64})
                }
                Err(error) => json!({"address": name, "ok": false, "error": error}),
            }
        })
        .collect();

    let Some(source) = source else {
        return json!({"ok": true, "configured": true, "coordinators": entries});
    };
    let masters = coordinators.call(&source, &["SENTINEL", "MASTERS"]);
    let names: Vec<String> = match &masters {
        Ok(RespValue::Array(items)) => items.iter().filter_map(field_name).collect(),
        _ => Vec::new(),
    };
    let replicas: serde_json::Map<String, Value> = names
        .iter()
        .map(|name| {
            let value = coordinators
                .call(&source, &["SENTINEL", "REPLICAS", name])
                .map_or_else(
                    |error| json!({"kind": "error", "text": error}),
                    |value| encode_value(&value),
                );

            (name.clone(), value)
        })
        .collect();
    let encode = |result: Result<RespValue, String>| {
        result.map_or_else(
            |error| json!({"kind": "error", "text": error}),
            |value| encode_value(&value),
        )
    };

    json!({
        "ok": true,
        "configured": true,
        "coordinators": entries,
        "source": format!("{}:{}", source.0, source.1),
        "masters": encode(masters),
        "replicas": replicas,
        "status": encode(coordinators.call(&source, &["COORDINATOR", "STATUS"])),
    })
}

/// The `name` field of a `SENTINEL masters` entry (a flat field list).
fn field_name(entry: &RespValue) -> Option<String> {
    let RespValue::Array(fields) = entry else {
        return None;
    };

    fields
        .chunks(2)
        .find(|pair| pair.first().and_then(text_of).as_deref() == Some("name"))
        .and_then(|pair| pair.get(1))
        .and_then(text_of)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinator_addresses_are_parsed_and_bad_entries_skipped() {
        assert_eq!(
            parse_addresses("c1:26400, c2:26401,[::1]:26402,nohost,:1,c3:x"),
            vec![
                ("c1".to_owned(), 26400),
                ("c2".to_owned(), 26401),
                ("::1".to_owned(), 26402)
            ]
        );
        assert!(parse_addresses("").is_empty());
    }

    #[test]
    fn the_group_name_is_read_from_a_field_list() {
        let bulk = |text: &str| RespValue::Bulk(text.as_bytes().to_vec());
        let entry = RespValue::Array(vec![bulk("ip"), bulk("a"), bulk("name"), bulk("main")]);

        assert_eq!(field_name(&entry).as_deref(), Some("main"));
        assert_eq!(field_name(&RespValue::Array(vec![bulk("ip")])), None);
    }
}
