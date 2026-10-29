use std::{
    collections::HashMap,
    env, fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use include_dir::{Dir, include_dir};

mod coordinator;
mod topology;

use ruvio_client::{
    Client, ClientOptions, Error as ClientError, PubSubMessage, RespValue, TlsOptions,
};

use serde::Deserialize;
use serde_json::{Value, json};

static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

type Shared<T> = Arc<Mutex<T>>;

/// One signed-in browser tab. Its lock is held only for bookkeeping: every RESP call runs under
/// the lock of the connection it uses, so a slow or unreachable node delays only the requests
/// that go to it.
struct Session {
    last_activity: Instant,
    client: Shared<Client>,
    direct: Shared<Client>,
    host: String,
    port: u16,
    username: String,
    password: String,
    mailbox: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ListenRequest>>>,
    stop: Arc<AtomicBool>,
    listener: Arc<AtomicBool>,
    bound: Option<(String, u16)>,
    nodes: HashMap<(String, u16), Shared<Client>>,
    /// Nodes whose last connect failed, with when and why; not dialled again for a while.
    unreachable: HashMap<(String, u16), (Instant, String)>,
}

struct ListenRequest {
    subscribe: bool,
    channels: Vec<String>,
    patterns: Vec<String>,
    sharded_channels: Vec<String>,
    reply: mpsc::Sender<Result<(), String>>,
}

static SESSIONS: LazyLock<Mutex<HashMap<String, Shared<Session>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);
/// How long a node whose connect failed is answered from memory instead of dialled again.
const UNREACHABLE_FOR: Duration = Duration::from_secs(3);
const NODE_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const NODE_READ_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_READ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_PROBE_NODES: usize = 512;

fn session_of(session_id: &str) -> Option<Shared<Session>> {
    SESSIONS.lock().expect("sessions").get(session_id).cloned()
}

fn expire_sessions() {
    SESSIONS.lock().expect("sessions").retain(|_, session| {
        let session = session.lock().expect("session");

        if session.last_activity.elapsed() < SESSION_IDLE {
            true
        } else {
            session.stop.store(true, Ordering::Relaxed);
            false
        }
    });
}

#[derive(Deserialize)]
struct ConnectBody {
    host: String,
    port: u16,
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
}

#[derive(Deserialize)]
struct CommandBody {
    args: Vec<String>,
    #[serde(default)]
    shard: Option<NodeTarget>,
}

#[derive(Deserialize, Clone)]
struct NodeTarget {
    host: String,
    port: u16,
}

#[derive(Deserialize)]
struct PubSubBody {
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    sharded_channels: Vec<String>,
    #[serde(default)]
    shard: Option<NodeTarget>,
}

#[derive(Deserialize)]
struct PublishBody {
    channel: String,
    message: String,
    #[serde(default)]
    shard: Option<NodeTarget>,
}

fn main() {
    let disk = env::args().nth(1).map(PathBuf::from);
    let bind = env::var("RUVIO_UI_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = TcpListener::bind(&bind).unwrap_or_else(|error| panic!("bind {bind}: {error}"));

    match &disk {
        Some(path) => eprintln!("ruvio-ui http://{bind}  files {}", path.display()),
        None => eprintln!("ruvio-ui http://{bind}  embedded screens"),
    }

    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(60));
            expire_sessions();
        }
    });

    for connection in listener.incoming() {
        let Ok(stream) = connection else {
            continue;
        };

        let disk = disk.clone();

        std::thread::spawn(move || {
            let _ = handle(stream, disk.as_deref());
        });
    }
}

fn handle(mut stream: TcpStream, disk: Option<&Path>) -> std::io::Result<()> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            return write_json(
                &mut stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap_or(request.len());
    let head = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = head.lines();
    let Some(start) = lines.next() else {
        return write_json(
            &mut stream,
            400,
            json!({"ok": false, "error": "empty request"}),
        );
    };

    let mut parts = start.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("/");
    let body = request.get(header_end + 4..).unwrap_or(&[]);
    let session_id = match request_session_id(&head) {
        Ok(Some(id)) => id,
        // The page, its assets and the defaults need no session; every other API call does.
        Ok(None) if !path.starts_with("/api/") || path == "/api/defaults" => String::new(),
        Ok(None) => {
            return write_json(
                &mut stream,
                400,
                json!({"ok": false, "error": "missing UI session id"}),
            );
        }
        Err(error) => {
            return write_json(&mut stream, 400, json!({"ok": false, "error": error}));
        }
    };
    if let Some(session) = session_of(&session_id) {
        session.lock().expect("session").last_activity = Instant::now();
    }

    if method == "POST" && path == "/api/connect" {
        return connect(&mut stream, body, &session_id);
    }

    if method == "POST" && path == "/api/command" {
        return command(&mut stream, body, &session_id);
    }

    if method == "POST" && path == "/api/disconnect" {
        release_session(&session_id);

        return write_json(&mut stream, 200, json!({"ok": true}));
    }

    if method == "POST" && path == "/api/pubsub/subscribe" {
        return pubsub_listen(&mut stream, body, true, &session_id);
    }

    if method == "POST" && path == "/api/pubsub/unsubscribe" {
        return pubsub_listen(&mut stream, body, false, &session_id);
    }

    if method == "POST" && path == "/api/pubsub/reset" {
        let Some(session) = session_of(&session_id) else {
            return write_json(
                &mut stream,
                200,
                json!({"ok": false, "error": "not connected"}),
            );
        };

        reset_listener(&mut session.lock().expect("session"));

        return write_json(&mut stream, 200, json!({"ok": true}));
    }

    if method == "POST" && path == "/api/pubsub/publish" {
        return publish(&mut stream, body, &session_id);
    }

    if method == "GET" && path.split('?').next() == Some("/api/pubsub/poll") {
        return poll_pubsub(&mut stream, &session_id);
    }

    if method == "GET" && path == "/api/session" {
        return session(&mut stream, &session_id);
    }

    if method == "GET" && path == "/api/shards" {
        return shards(&mut stream, &session_id);
    }

    if method == "POST" && path == "/api/probe" {
        return probe(&mut stream, body, &session_id);
    }

    if method == "GET" && path == "/api/coordinators" {
        if session_of(&session_id).is_none() {
            return write_json(
                &mut stream,
                200,
                json!({"ok": false, "error": "not connected"}),
            );
        }

        return write_json(&mut stream, 200, coordinator::report());
    }

    if method == "GET" && path == "/api/defaults" {
        return defaults(&mut stream);
    }

    if method == "GET" {
        return serve_file(&mut stream, disk, path);
    }

    write_json(&mut stream, 404, json!({"ok": false, "error": "not found"}))
}

fn read_request(reader: &mut impl Read) -> std::io::Result<Vec<u8>> {
    const MAX_REQUEST: usize = 1024 * 1024;
    let mut request = Vec::new();
    let mut expected = None;
    let mut buffer = [0; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete HTTP request",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > MAX_REQUEST {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request exceeds 1 MiB",
            ));
        }
        if expected.is_none()
            && let Some(end) = request.windows(4).position(|window| window == b"\r\n\r\n")
        {
            let headers = std::str::from_utf8(&request[..end])
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            let mut length = None;
            for line in headers.lines().skip(1) {
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("transfer-encoding") {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Transfer-Encoding is not supported",
                        ));
                    }
                    if name.eq_ignore_ascii_case("content-length") {
                        if length.is_some() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "duplicate Content-Length",
                            ));
                        }
                        length = Some(value.trim().parse::<usize>().map_err(|error| {
                            std::io::Error::new(std::io::ErrorKind::InvalidData, error)
                        })?);
                    }
                }
            }
            let size = (end + 4)
                .checked_add(length.unwrap_or(0))
                .filter(|size| *size <= MAX_REQUEST)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "HTTP request exceeds 1 MiB",
                    )
                })?;
            expected = Some(size);
        }
        if let Some(size) = expected
            && request.len() >= size
        {
            request.truncate(size);
            return Ok(request);
        }
    }
}

/// The session named by `x-ruvio-session`, or `None` when the request carries none. API calls that
/// need a session refuse `None`; there is no shared fallback session.
fn request_session_id(headers: &str) -> Result<Option<String>, &'static str> {
    let id = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("x-ruvio-session")
                .then(|| value.trim())
        })
        .unwrap_or("");

    if id.is_empty() {
        return Ok(None);
    }

    if id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("invalid UI session id");
    }

    Ok(Some(id.to_owned()))
}

fn connect(stream: &mut TcpStream, body: &[u8], session_id: &str) -> std::io::Result<()> {
    let parsed: ConnectBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return write_json(
                stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };

    let mut parsed = parsed;

    if parsed.password.is_empty()
        && let Some(password) = target_password(&parsed.host, parsed.port)
    {
        parsed.password = password;
    }

    let mut options = ClientOptions::default();

    options.host = parsed.host.clone();
    options.port = parsed.port;
    options.username = if parsed.username.is_empty() {
        None
    } else {
        Some(parsed.username.clone())
    };

    options.password = if parsed.password.is_empty() {
        None
    } else {
        Some(parsed.password.clone())
    };
    options.max_reconnect_attempts = 2;
    options.reconnect_base_delay = Duration::from_millis(50);

    match target_tls() {
        Ok(tls) => options.tls = tls,
        Err(error) => {
            return write_json(stream, 200, json!({"ok": false, "error": error}));
        }
    }

    let mut direct_options = options.clone();

    direct_options.discover_cluster = false;

    match Client::connect_with(options) {
        Ok(mut client) => {
            let mut direct = match Client::connect_with(direct_options) {
                Ok(direct) => direct,
                Err(error) => {
                    return write_json(
                        stream,
                        200,
                        json!({"ok": false, "error": error.to_string()}),
                    );
                }
            };

            let _ = client.set_read_timeout(Some(Duration::from_secs(5)));
            let _ = direct.set_read_timeout(Some(Duration::from_secs(5)));

            release_session(session_id);

            SESSIONS.lock().expect("sessions").insert(
                session_id.to_owned(),
                Arc::new(Mutex::new(Session {
                    last_activity: Instant::now(),
                    client: Arc::new(Mutex::new(client)),
                    direct: Arc::new(Mutex::new(direct)),
                    host: parsed.host,
                    port: parsed.port,
                    username: parsed.username,
                    password: parsed.password,
                    mailbox: Arc::new(Mutex::new(Vec::new())),
                    requests: Arc::new(Mutex::new(Vec::new())),
                    stop: Arc::new(AtomicBool::new(false)),
                    listener: Arc::new(AtomicBool::new(false)),
                    bound: None,
                    nodes: HashMap::new(),
                    unreachable: HashMap::new(),
                })),
            );

            write_json(stream, 200, json!({"ok": true}))
        }

        Err(error) => write_json(
            stream,
            200,
            json!({"ok": false, "error": error.to_string()}),
        ),
    }
}

fn command(stream: &mut TcpStream, body: &[u8], session_id: &str) -> std::io::Result<()> {
    let parsed: CommandBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return write_json(
                stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };

    if parsed.args.is_empty() {
        return write_json(stream, 400, json!({"ok": false, "error": "args is empty"}));
    }

    let Some(session) = session_of(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let args: Vec<&str> = parsed.args.iter().map(String::as_str).collect();
    let result = execute_targeted(&session, &args, parsed.shard.as_ref());

    match result {
        Ok(value) => write_json(
            stream,
            200,
            json!({"ok": true, "value": encode_value(&value)}),
        ),
        Err(message) => write_json(stream, 200, json!({"ok": false, "error": message})),
    }
}

fn release_session(session_id: &str) {
    if let Some(session) = SESSIONS.lock().expect("sessions").remove(session_id) {
        session
            .lock()
            .expect("session")
            .stop
            .store(true, Ordering::Relaxed);
    }
}

fn reset_listener(session: &mut Session) {
    session.stop.store(true, Ordering::Relaxed);
    fail_pending(&session.requests, "subscription stopped");
    session.stop = Arc::new(AtomicBool::new(false));
    session.requests = Arc::new(Mutex::new(Vec::new()));
    session.mailbox = Arc::new(Mutex::new(Vec::new()));
    session.listener = Arc::new(AtomicBool::new(false));
    session.bound = None;
}

fn pubsub_listen(
    stream: &mut TcpStream,
    body: &[u8],
    subscribe: bool,
    session_id: &str,
) -> std::io::Result<()> {
    let parsed: PubSubBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return write_json(
                stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };

    let (reply_tx, reply_rx) = mpsc::channel();

    {
        let Some(handle) = session_of(session_id) else {
            return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
        };
        let mut guard = handle.lock().expect("session");
        let session = &mut *guard;

        let (host, port) = match parsed.shard.as_ref().filter(|node| !node.host.is_empty()) {
            Some(node) => (node.host.clone(), node.port),
            None => (session.host.clone(), session.port),
        };

        if session.bound.as_ref() != Some(&(host.clone(), port)) {
            reset_listener(session);
        }

        session.bound = Some((host.clone(), port));

        session
            .requests
            .lock()
            .expect("listener")
            .push(ListenRequest {
                subscribe,
                channels: parsed.channels,
                patterns: parsed.patterns,
                sharded_channels: parsed.sharded_channels,
                reply: reply_tx,
            });

        if !session.listener.load(Ordering::Relaxed) {
            session.listener.store(true, Ordering::Relaxed);

            let mut options = ClientOptions::default();

            options.host = host;
            options.port = port;
            options.discover_cluster = false;
            options.max_reconnect_attempts = 2;
            options.reconnect_base_delay = Duration::from_millis(50);

            match target_tls() {
                Ok(tls) => options.tls = tls,
                Err(error) => {
                    drop(guard);

                    return write_json(stream, 200, json!({"ok": false, "error": error}));
                }
            }

            options.username = if session.username.is_empty() {
                None
            } else {
                Some(session.username.clone())
            };

            options.password = if session.password.is_empty() {
                None
            } else {
                Some(session.password.clone())
            };

            let requests = Arc::clone(&session.requests);
            let mailbox = Arc::clone(&session.mailbox);
            let stop = Arc::clone(&session.stop);
            let running = Arc::clone(&session.listener);

            thread::spawn(move || run_listener(options, requests, mailbox, stop, running));
        }
    }

    let result = reply_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| Err("subscribe timed out".to_owned()));

    match result {
        Ok(()) => write_json(stream, 200, json!({"ok": true})),
        Err(error) => write_json(stream, 200, json!({"ok": false, "error": error})),
    }
}

fn run_listener(
    options: ClientOptions,
    requests: Arc<Mutex<Vec<ListenRequest>>>,
    mailbox: Arc<Mutex<Vec<Value>>>,
    stop: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
) {
    let mut client = match Client::connect_with(options) {
        Ok(client) => client,
        Err(error) => {
            running.store(false, Ordering::Relaxed);
            fail_pending(&requests, &error.to_string());

            return;
        }
    };

    let _ = client.set_read_timeout(Some(Duration::from_millis(200)));

    loop {
        if stop.load(Ordering::Relaxed) {
            fail_pending(&requests, "signed out");

            break;
        }

        for request in drain_requests(&requests) {
            let result = apply_listen(&mut client, &request);

            let _ = request.reply.send(result);
        }

        match client.next_message() {
            Ok(message) => push_mail(&mailbox, encode_pubsub(message)),
            Err(ClientError::Io(error)) if is_timeout(&error) => {}
            Err(error) => {
                push_mail(
                    &mailbox,
                    json!({"kind": "error", "text": error.to_string()}),
                );
                break;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    fail_pending(&requests, "subscription connection closed; subscribe again");
}

fn drain_requests(requests: &Mutex<Vec<ListenRequest>>) -> Vec<ListenRequest> {
    std::mem::take(&mut *requests.lock().expect("listener"))
}

fn fail_pending(requests: &Mutex<Vec<ListenRequest>>, error: &str) {
    for request in drain_requests(requests) {
        let _ = request.reply.send(Err(error.to_owned()));
    }
}

fn apply_listen(client: &mut Client, request: &ListenRequest) -> Result<(), String> {
    let channels: Vec<&str> = request.channels.iter().map(String::as_str).collect();
    let patterns: Vec<&str> = request.patterns.iter().map(String::as_str).collect();
    let sharded_channels: Vec<&str> = request
        .sharded_channels
        .iter()
        .map(String::as_str)
        .collect();
    let result = if !sharded_channels.is_empty() {
        if request.subscribe {
            client.ssubscribe(&sharded_channels).map(|_| ())
        } else {
            client.sunsubscribe(&sharded_channels).map(|_| ())
        }
    } else if request.subscribe {
        if !channels.is_empty() {
            client.subscribe(&channels).map(|_| ())
        } else {
            client.psubscribe(&patterns).map(|_| ())
        }
    } else if !channels.is_empty() {
        client.unsubscribe(&channels).map(|_| ())
    } else {
        client.punsubscribe(&patterns).map(|_| ())
    };

    result.map_err(|error| error.to_string())
}

fn push_mail(mailbox: &Mutex<Vec<Value>>, message: Value) {
    let mut messages = mailbox.lock().expect("mailbox");

    messages.push(message);

    if messages.len() > 200 {
        let extra = messages.len() - 200;

        messages.drain(..extra);
    }
}

fn publish(stream: &mut TcpStream, body: &[u8], session_id: &str) -> std::io::Result<()> {
    let parsed: PublishBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return write_json(
                stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };

    let Some(session) = session_of(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let client = match parsed.shard.as_ref().filter(|node| !node.host.is_empty()) {
        Some(node) => match node_handle(&session, node) {
            Ok(client) => client,
            Err(error) => {
                return write_json(stream, 200, json!({"ok": false, "error": error}));
            }
        },
        None => Arc::clone(&session.lock().expect("session").direct),
    };
    let result = client
        .lock()
        .expect("client")
        .publish(&parsed.channel, &parsed.message);

    match result {
        Ok(receivers) => write_json(stream, 200, json!({"ok": true, "receivers": receivers})),
        Err(error) => write_json(
            stream,
            200,
            json!({"ok": false, "error": error.to_string()}),
        ),
    }
}

fn poll_pubsub(stream: &mut TcpStream, session_id: &str) -> std::io::Result<()> {
    let Some(session) = session_of(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let mailbox = Arc::clone(&session.lock().expect("session").mailbox);
    let messages = std::mem::take(&mut *mailbox.lock().expect("mailbox"));

    write_json(stream, 200, json!({"ok": true, "messages": messages}))
}

fn encode_pubsub(message: PubSubMessage) -> Value {
    let text = String::from_utf8(message.payload.clone()).ok();

    json!({
        "kind": message.kind,
        "channel": message.channel,
        "pattern": message.pattern,
        "text": text,
    })
}

fn is_timeout(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

fn execute_targeted(
    session: &Shared<Session>,
    args: &[&str],
    shard: Option<&NodeTarget>,
) -> Result<RespValue, String> {
    let client = match shard.filter(|node| !node.host.is_empty()) {
        Some(node) => node_handle(session, node)?,
        None => Arc::clone(&session.lock().expect("session").client),
    };
    let mut client = client.lock().expect("client");

    execute_read_retry(&mut client, args).map_err(error_text)
}

fn error_text(error: ClientError) -> String {
    match error {
        ClientError::Server(message) => message,
        other => other.to_string(),
    }
}

fn retryable_read(args: &[&str]) -> bool {
    args.first().is_some_and(|command| {
        matches!(
            command.to_ascii_uppercase().as_str(),
            "PING"
                | "INFO"
                | "TYPE"
                | "TTL"
                | "PTTL"
                | "GET"
                | "MGET"
                | "EXISTS"
                | "SCAN"
                | "LLEN"
                | "LRANGE"
                | "HLEN"
                | "HSCAN"
                | "HGET"
                | "HGETALL"
                | "SCARD"
                | "SMEMBERS"
                | "SISMEMBER"
                | "ZCARD"
                | "ZRANGE"
                | "ZSCORE"
                | "XLEN"
                | "XRANGE"
                | "XINFO"
                | "DBSIZE"
        )
    }) || (args
        .first()
        .is_some_and(|command| command.eq_ignore_ascii_case("ACL"))
        && args.get(1).is_some_and(|sub| {
            matches!(
                sub.to_ascii_uppercase().as_str(),
                "USERS" | "GETUSER" | "LIST"
            )
        }))
        || (args
            .first()
            .is_some_and(|command| command.eq_ignore_ascii_case("CLUSTER"))
            && args.get(1).is_some_and(|sub| {
                sub.eq_ignore_ascii_case("SLOTS") || sub.eq_ignore_ascii_case("SHARDS")
            }))
}

fn execute_read_retry(client: &mut Client, args: &[&str]) -> Result<RespValue, ClientError> {
    match client.execute(args) {
        Err(ClientError::Io(error))
            if retryable_read(args)
                && matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::WouldBlock
                ) =>
        {
            client.reconnect()?;
            client.execute(args)
        }
        Err(ClientError::Protocol(message))
            if retryable_read(args) && message == "connection closed" =>
        {
            client.reconnect()?;
            client.execute(args)
        }
        result => result,
    }
}

/// The connection to one node of the session, opened on first use. A node whose connect failed
/// is not dialled again for [`UNREACHABLE_FOR`]; the remembered error is answered instead.
fn node_handle(session: &Shared<Session>, node: &NodeTarget) -> Result<Shared<Client>, String> {
    let key = (node.host.clone(), node.port);
    let (username, password) = {
        let session = session.lock().expect("session");

        if let Some(client) = session.nodes.get(&key) {
            return Ok(Arc::clone(client));
        }
        if let Some((at, error)) = session.unreachable.get(&key)
            && at.elapsed() < UNREACHABLE_FOR
        {
            return Err(error.clone());
        }

        (session.username.clone(), session.password.clone())
    };
    let mut options = ClientOptions::default();

    options.host = node.host.clone();
    options.port = node.port;
    options.discover_cluster = false;
    options.tls = target_tls()?;
    options.connect_timeout = NODE_CONNECT_TIMEOUT;
    options.max_reconnect_attempts = 1;
    options.reconnect_base_delay = Duration::from_millis(50);
    options.username = (!username.is_empty()).then_some(username);
    options.password = (!password.is_empty()).then_some(password);

    // Dialled without the session lock, so other requests of the session go on meanwhile.
    let connected = Client::connect_with(options).map_err(|error| error.to_string());
    let mut session = session.lock().expect("session");

    match connected {
        Ok(mut client) => {
            let _ = client.set_read_timeout(Some(NODE_READ_TIMEOUT));
            session.unreachable.remove(&key);

            Ok(Arc::clone(
                session
                    .nodes
                    .entry(key)
                    .or_insert_with(|| Arc::new(Mutex::new(client))),
            ))
        }
        Err(error) => {
            session
                .unreachable
                .insert(key, (Instant::now(), error.clone()));

            Err(error)
        }
    }
}

/// Runs `work` for every item, at most 32 at a time, and answers the results in item order.
pub(crate) fn parallel<T: Sync, R: Send>(items: &[T], work: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let mut results = Vec::with_capacity(items.len());

    for chunk in items.chunks(32) {
        thread::scope(|scope| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|item| scope.spawn(|| work(item)))
                .collect();

            results.extend(
                handles
                    .into_iter()
                    .map(|handle| handle.join().expect("worker")),
            );
        });
    }

    results
}

/// `GET /api/shards`: the shards from the entry node's `CLUSTER SHARDS`, one entry per shard
/// with all its ranges and replicas. In a multi-node cluster the entry node lists other groups'
/// shards by their primary only, so those primaries are asked for their own replicas.
fn shards(stream: &mut TcpStream, session_id: &str) -> std::io::Result<()> {
    let Some(session) = session_of(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };
    let (direct, host, port) = {
        let session = session.lock().expect("session");

        (
            Arc::clone(&session.direct),
            session.host.clone(),
            session.port,
        )
    };
    let (listed, version) = {
        let mut direct = direct.lock().expect("client");
        let listed = execute_read_retry(&mut direct, &["CLUSTER", "SHARDS"]);
        let version = direct
            .execute(&["RUVIO", "TOPOLOGY", "VERSION"])
            .ok()
            .as_ref()
            .and_then(topology::integer_of)
            .unwrap_or(0);

        (listed, version)
    };
    let fallback = json!([{
        "id": 0,
        "host": host,
        "port": port,
        "ranges": [[0, 16383]],
        "slots": 16384,
        "replicas": [],
    }]);

    let mut warnings = Vec::new();
    let listed = match listed {
        Ok(value) => match topology::parse_shards(&value).filter(|items| !items.is_empty()) {
            Some(mut found) => {
                warnings.extend(topology::reroute_loopback(&mut found, &host));

                if version > 0 {
                    let lone = topology::lone_primaries(&found);
                    let answers = parallel(&lone, |(host, port)| {
                        let node = NodeTarget {
                            host: host.clone(),
                            port: *port,
                        };
                        let client = node_handle(&session, &node)?;
                        let reply = execute_read_retry(
                            &mut client.lock().expect("client"),
                            &["CLUSTER", "SHARDS"],
                        )
                        .map_err(error_text)?;

                        topology::parse_shards(&reply).ok_or_else(|| "unexpected reply".to_owned())
                    });

                    for ((host, port), answer) in lone.iter().zip(answers) {
                        match answer {
                            Ok(mut answer) => {
                                topology::reroute_loopback(&mut answer, host);
                                topology::merge_members(&mut found, &answer);
                            }
                            Err(error) => {
                                warnings.push(format!("Replicas of {host}:{port} unknown: {error}"))
                            }
                        }
                    }
                }

                topology::sort_by_slots(&mut found);

                Some(topology::shards_json(&found))
            }
            None => {
                warnings
                    .push("No shard topology reported. Showing the entry node only.".to_owned());

                None
            }
        },
        Err(error) => {
            warnings.push(format!(
                "Shard discovery unavailable: {}. Showing the entry node only.",
                error_text(error)
            ));

            None
        }
    };
    let warning = (!warnings.is_empty()).then(|| warnings.join(" "));

    write_json(
        stream,
        200,
        json!({
            "ok": true,
            "shards": listed.unwrap_or_else(|| vec![fallback[0].clone()]),
            "topology_version": version,
            "warning": warning,
        }),
    )
}

#[derive(Deserialize)]
struct ProbeBody {
    nodes: Vec<NodeTarget>,
    /// Also read the node's `cluster-group` setting (on topology changes, not every refresh).
    #[serde(default)]
    extended: bool,
}

/// `POST /api/probe`: `INFO` and the cluster map version from every node, in parallel, each
/// with a short read timeout. A node that cannot be reached is reported, not waited for.
fn probe(stream: &mut TcpStream, body: &[u8], session_id: &str) -> std::io::Result<()> {
    let parsed: ProbeBody = match serde_json::from_slice(body) {
        Ok(parsed) => parsed,
        Err(error) => {
            return write_json(
                stream,
                400,
                json!({"ok": false, "error": error.to_string()}),
            );
        }
    };
    let Some(session) = session_of(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };
    if parsed.nodes.len() > MAX_PROBE_NODES {
        return write_json(
            stream,
            400,
            json!({"ok": false, "error": format!("at most {MAX_PROBE_NODES} nodes per probe")}),
        );
    }

    let readings = parallel(&parsed.nodes, |node| {
        probe_node(&session, node, parsed.extended)
    });

    write_json(stream, 200, json!({"ok": true, "nodes": readings}))
}

fn probe_node(session: &Shared<Session>, node: &NodeTarget, extended: bool) -> Value {
    let started = Instant::now();
    let client = match node_handle(session, node) {
        Ok(client) => client,
        Err(error) => {
            return json!({"host": node.host, "port": node.port, "reachable": false, "error": error});
        }
    };
    let mut client = client.lock().expect("client");
    let _ = client.set_read_timeout(Some(PROBE_READ_TIMEOUT));
    let info = execute_read_retry(&mut client, &["INFO"]);
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    let reading = match info {
        // An error reply (a missing permission, a shard still loading) still means it answered.
        Err(ClientError::Server(error)) => json!({
            "host": node.host, "port": node.port, "reachable": true, "error": format!("INFO: {error}"), "ms": ms,
        }),
        Err(error) => json!({
            "host": node.host, "port": node.port, "reachable": false, "error": format!("INFO: {error}"),
        }),
        Ok(info) => {
            let version = client
                .execute(&["RUVIO", "TOPOLOGY", "VERSION"])
                .ok()
                .as_ref()
                .and_then(topology::integer_of);
            let group = extended
                .then(|| client.execute(&["CONFIG", "GET", "cluster-group"]).ok())
                .flatten()
                .and_then(|reply| match reply {
                    RespValue::Array(items) => items.get(1).and_then(topology::text_of),
                    _ => None,
                })
                .filter(|group| !group.is_empty());

            json!({
                "host": node.host,
                "port": node.port,
                "reachable": true,
                "info": topology::text_of(&info),
                "topology_version": version,
                "group": group,
                "ms": ms,
            })
        }
    };
    let _ = client.set_read_timeout(Some(NODE_READ_TIMEOUT));

    reading
}

fn parsed_port(port: &str) -> u16 {
    port.parse::<u16>().unwrap_or(6379)
}

/// TLS for every connection to the target, from the environment: `RUVIO_UI_TLS=true` turns it
/// on (certificate and host name are checked), `RUVIO_UI_TLS_CA` is a PEM file of the CA to
/// trust, `RUVIO_UI_TLS_CLIENT_CERT` / `RUVIO_UI_TLS_CLIENT_KEY` are PEM files for a server that
/// asks for a client certificate, and `RUVIO_UI_TLS_SERVER_NAME` is the name the certificate
/// carries when it differs from the host. `RUVIO_UI_TLS_INSECURE=true` skips verification.
fn target_tls() -> Result<Option<TlsOptions>, String> {
    tls_from(|name| env::var(name).ok())
}

fn tls_from(get: impl Fn(&str) -> Option<String>) -> Result<Option<TlsOptions>, String> {
    let flag = |name: &str| {
        get(name).is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
    };

    if !flag("RUVIO_UI_TLS") {
        return Ok(None);
    }

    let file = |name: &str| -> Result<Option<Vec<u8>>, String> {
        match get(name).filter(|path| !path.is_empty()) {
            Some(path) => std::fs::read(&path)
                .map(Some)
                .map_err(|error| format!("{name}: cannot read {path}: {error}")),
            None => Ok(None),
        }
    };

    Ok(Some(TlsOptions {
        ca_pem: file("RUVIO_UI_TLS_CA")?,
        client_cert_pem: file("RUVIO_UI_TLS_CLIENT_CERT")?,
        client_key_pem: file("RUVIO_UI_TLS_CLIENT_KEY")?,
        server_name: get("RUVIO_UI_TLS_SERVER_NAME").filter(|name| !name.is_empty()),
        insecure_skip_verify: flag("RUVIO_UI_TLS_INSECURE"),
    }))
}

/// `RUVIO_UI_TARGET_PASSWORD`, only for the configured target host and port.
fn target_password(host: &str, port: u16) -> Option<String> {
    let target_host = env::var("RUVIO_UI_TARGET_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let target_port = env::var("RUVIO_UI_TARGET_PORT").unwrap_or_else(|_| "6379".into());

    password_for_target(
        env::var("RUVIO_UI_TARGET_PASSWORD").ok(),
        (&target_host, parsed_port(&target_port)),
        (host, port),
    )
}

fn password_for_target(
    password: Option<String>,
    target: (&str, u16),
    requested: (&str, u16),
) -> Option<String> {
    password.filter(|password| !password.is_empty() && target == requested)
}

fn defaults(stream: &mut TcpStream) -> std::io::Result<()> {
    let host = env::var("RUVIO_UI_TARGET_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = env::var("RUVIO_UI_TARGET_PORT").unwrap_or_else(|_| "6379".into());
    let pinned = env::var_os("RUVIO_UI_TARGET_HOST").is_some()
        || env::var_os("RUVIO_UI_TARGET_PORT").is_some();

    write_json(
        stream,
        200,
        json!({
            "ok": true,
            "host": host,
            "port": parsed_port(&port),
            // The target password never leaves the server: `connect` applies it when the
            // sign-in form leaves the password empty for this host and port.
            "password": "",
            "password_set": target_password(&host, parsed_port(&port)).is_some(),
            "pinned": pinned,
        }),
    )
}

fn session(stream: &mut TcpStream, session_id: &str) -> std::io::Result<()> {
    match session_of(session_id) {
        Some(session) => {
            let session = session.lock().expect("session");

            write_json(
                stream,
                200,
                json!({
                    "ok": true,
                    "connected": true,
                    "host": session.host,
                    "port": session.port,
                    "username": session.username,
                    "coordinators": coordinator::configured(),
                }),
            )
        }
        None => write_json(stream, 200, json!({"ok": true, "connected": false})),
    }
}

fn encode_value(value: &RespValue) -> Value {
    match value {
        RespValue::Simple(text) => json!({"kind": "simple", "text": text}),
        RespValue::Error(text) => json!({"kind": "error", "text": text}),
        RespValue::Integer(value) => json!({"kind": "integer", "value": value}),
        RespValue::Null => json!({"kind": "null"}),
        RespValue::Bulk(bytes) => match String::from_utf8(bytes.clone()) {
            Ok(text) => json!({"kind": "bulk", "text": text}),
            Err(_) => json!({"kind": "bulk", "base64": base64(bytes)}),
        },
        RespValue::Array(items) => json!({
            "kind": "array",
            "items": items.iter().map(encode_value).collect::<Vec<_>>(),
        }),
    }
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let pack = (b0 << 16) | (b1 << 8) | b2;

        out.push(TABLE[((pack >> 18) & 63) as usize] as char);
        out.push(TABLE[((pack >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((pack >> 6) & 63) as usize] as char
        } else {
            '='
        });

        out.push(if chunk.len() > 2 {
            TABLE[(pack & 63) as usize] as char
        } else {
            '='
        });
    }

    out
}

fn serve_file(stream: &mut TcpStream, disk: Option<&Path>, path: &str) -> std::io::Result<()> {
    let path = path.split('?').next().unwrap_or(path);
    let mut relative = path.trim_start_matches('/');

    if relative.is_empty() {
        relative = "index.html";
    }

    if relative.split('/').any(|part| part == "..") {
        return write_json(stream, 404, json!({"ok": false, "error": "not found"}));
    }

    if let Some(root) = disk {
        return serve_disk(stream, root, relative);
    }

    if let Some(file) = ASSETS.get_file(relative) {
        return write_bytes(stream, 200, content_type(relative), file.contents());
    }

    if let Some(index) = ASSETS.get_file("index.html") {
        return write_bytes(stream, 200, "text/html; charset=utf-8", index.contents());
    }

    write_json(
        stream,
        404,
        json!({"ok": false, "error": "embedded UI is missing"}),
    )
}

fn serve_disk(stream: &mut TcpStream, root: &Path, relative: &str) -> std::io::Result<()> {
    let full = root.join(relative);

    if !full.starts_with(root) {
        return write_json(stream, 404, json!({"ok": false, "error": "not found"}));
    }

    let full = if full.is_dir() {
        full.join("index.html")
    } else {
        full
    };

    let Ok(bytes) = fs::read(&full) else {
        let index = root.join("index.html");
        let Ok(bytes) = fs::read(&index) else {
            return write_json(
                stream,
                404,
                json!({"ok": false, "error": "UI build is missing. Run npm run build in web/."}),
            );
        };

        return write_bytes(stream, 200, "text/html; charset=utf-8", &bytes);
    };

    write_bytes(stream, 200, content_type(relative), &bytes)
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("html") => "text/html; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn write_json(stream: &mut TcpStream, status: u16, body: Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
    let reason = if status == 200 { "OK" } else { "Error" };

    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{SECURITY_HEADERS}Connection: close\r\n\r\n",
        bytes.len()
    )?;
    stream.write_all(&bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        execute_read_retry, password_for_target, read_request, request_session_id, retryable_read,
        tls_from,
    };
    use ruvio_client::{Client, ClientOptions, RespValue};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    #[test]
    fn tls_is_off_unless_asked_for() {
        assert!(tls_from(|_| None).unwrap().is_none());
        assert!(
            tls_from(|name| (name == "RUVIO_UI_TLS").then(|| "false".to_owned()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn tls_verifies_by_default_and_skipping_is_explicit() {
        let on = |name: &str| (name == "RUVIO_UI_TLS").then(|| "true".to_owned());
        let tls = tls_from(on).unwrap().unwrap();

        assert!(!tls.insecure_skip_verify);
        assert!(tls.ca_pem.is_none() && tls.client_cert_pem.is_none());

        let skip = tls_from(|name| match name {
            "RUVIO_UI_TLS" | "RUVIO_UI_TLS_INSECURE" => Some("1".to_owned()),
            _ => None,
        })
        .unwrap()
        .unwrap();

        assert!(skip.insecure_skip_verify);
    }

    #[test]
    fn an_unreadable_ca_file_is_an_error_naming_the_variable() {
        let error = tls_from(|name| match name {
            "RUVIO_UI_TLS" => Some("true".to_owned()),
            "RUVIO_UI_TLS_CA" => Some("/nonexistent/ca.pem".to_owned()),
            _ => None,
        })
        .unwrap_err();

        assert!(error.starts_with("RUVIO_UI_TLS_CA: cannot read /nonexistent/ca.pem"));
    }

    #[test]
    fn fragmented_http_requests_include_the_whole_body() {
        struct Fragmented<'a>(&'a [u8]);
        impl Read for Fragmented<'_> {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                let size = out.len().min(3).min(self.0.len());
                out[..size].copy_from_slice(&self.0[..size]);
                self.0 = &self.0[size..];
                Ok(size)
            }
        }
        let payload = b"POST /api/command HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc";
        assert_eq!(read_request(&mut Fragmented(payload)).unwrap(), payload);
        assert!(read_request(&mut &payload[..payload.len() - 1]).is_err());
        assert!(
            read_request(&mut &b"POST / HTTP/1.1\r\nContent-Length: 99999999\r\n\r\n"[..]).is_err()
        );
        assert!(
            read_request(
                &mut &b"POST / HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n"[..]
            )
            .is_err()
        );
    }

    #[test]
    fn retries_are_limited_to_read_only_commands() {
        for args in [
            &["GET", "k"][..],
            &["INFO"],
            &["ACL", "USERS"],
            &["CLUSTER", "SLOTS"],
        ] {
            assert!(retryable_read(args), "{args:?}");
        }
        for args in [
            &["SET", "k", "v"][..],
            &["INCR", "k"],
            &["DEL", "k"],
            &["GETEX", "k"],
            &["ACL", "SETUSER", "app"],
            &["XREADGROUP", "GROUP", "g", "c"],
            &["FLUSHALL"],
        ] {
            assert!(!retryable_read(args), "{args:?}");
        }
    }

    #[test]
    fn a_dropped_read_is_reconnected_and_retried_once() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("accept: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = [0; 20];
                socket.read_exact(&mut request).unwrap();
                assert_eq!(&request, b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
                if attempt == 1 {
                    socket.write_all(b"$2\r\nok\r\n").unwrap();
                }
            }
        });
        let mut client = Client::connect_with(ClientOptions {
            port,
            discover_cluster: false,
            max_reconnect_attempts: 1,
            ..ClientOptions::default()
        })
        .unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert_eq!(
            execute_read_retry(&mut client, &["GET", "k"]).unwrap(),
            RespValue::Bulk(b"ok".to_vec())
        );
        server.join().unwrap();
    }

    #[test]
    fn the_target_password_is_only_for_the_target() {
        let target = ("db.internal", 6379);

        assert_eq!(
            password_for_target(Some("pw".into()), target, target),
            Some("pw".into())
        );
        assert_eq!(
            password_for_target(Some("pw".into()), target, ("evil.example", 6379)),
            None
        );
        assert_eq!(
            password_for_target(Some("pw".into()), target, ("db.internal", 6380)),
            None
        );
        assert_eq!(
            password_for_target(Some(String::new()), target, target),
            None
        );
        assert_eq!(password_for_target(None, target, target), None);
    }

    #[test]
    fn request_session_ids_are_read_and_validated() {
        assert_eq!(
            request_session_id("GET / HTTP/1.1\r\nX-Ruvio-Session: tab_123-abc\r\n"),
            Ok(Some("tab_123-abc".to_owned()))
        );
        assert_eq!(request_session_id("GET / HTTP/1.1\r\n"), Ok(None));
        assert_eq!(
            request_session_id("GET / HTTP/1.1\r\nX-Ruvio-Session: bad/id\r\n"),
            Err("invalid UI session id")
        );
    }
}

/// Sent with every response: no framing of the UI, no content-type sniffing.
const SECURITY_HEADERS: &str = "X-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\nContent-Security-Policy: frame-ancestors 'none'\r\nReferrer-Policy: no-referrer\r\n";

fn write_bytes(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{SECURITY_HEADERS}Connection: close\r\n\r\n",
        bytes.len()
    )?;
    stream.write_all(bytes)
}
