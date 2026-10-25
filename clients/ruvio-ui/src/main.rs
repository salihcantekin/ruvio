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

use ruvio_client::{Client, ClientOptions, Error as ClientError, PubSubMessage, RespValue};

use serde::Deserialize;
use serde_json::{Value, json};

static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/web/dist");

struct Session {
    last_activity: Instant,
    client: Client,
    direct: Client,
    host: String,
    port: u16,
    username: String,
    password: String,
    mailbox: Arc<Mutex<Vec<Value>>>,
    requests: Arc<Mutex<Vec<ListenRequest>>>,
    stop: Arc<AtomicBool>,
    listener: Arc<AtomicBool>,
    bound: Option<(String, u16)>,
    nodes: HashMap<(String, u16), Client>,
}

struct ListenRequest {
    subscribe: bool,
    channels: Vec<String>,
    patterns: Vec<String>,
    sharded_channels: Vec<String>,
    reply: mpsc::Sender<Result<(), String>>,
}

static SESSIONS: LazyLock<Mutex<HashMap<String, Session>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const SESSION_IDLE: Duration = Duration::from_secs(30 * 60);

fn expire_sessions() {
    SESSIONS.lock().expect("sessions").retain(|_, session| {
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
    if let Some(session) = SESSIONS.lock().expect("sessions").get_mut(&session_id) {
        session.last_activity = Instant::now();
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
        let mut guard = SESSIONS.lock().expect("sessions");
        let Some(session) = guard.get_mut(&session_id) else {
            return write_json(
                &mut stream,
                200,
                json!({"ok": false, "error": "not connected"}),
            );
        };

        reset_listener(session);

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
                Session {
                    last_activity: Instant::now(),
                    client,
                    direct,
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
                },
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

    let mut sessions = SESSIONS.lock().expect("sessions");
    let Some(session) = sessions.get_mut(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let args: Vec<&str> = parsed.args.iter().map(String::as_str).collect();
    let result = execute_targeted(session, &args, parsed.shard.as_ref());

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
        session.stop.store(true, Ordering::Relaxed);
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
        let mut guard = SESSIONS.lock().expect("sessions");
        let Some(session) = guard.get_mut(session_id) else {
            return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
        };

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

    let mut guard = SESSIONS.lock().expect("sessions");
    let Some(session) = guard.get_mut(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let result = if let Some(node) = parsed.shard.as_ref().filter(|node| !node.host.is_empty()) {
        match node_client(session, node) {
            Ok(client) => client.publish(&parsed.channel, &parsed.message),
            Err(error) => {
                return write_json(stream, 200, json!({"ok": false, "error": error}));
            }
        }
    } else {
        session.direct.publish(&parsed.channel, &parsed.message)
    };

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
    let guard = SESSIONS.lock().expect("sessions");
    let Some(session) = guard.get(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let messages = std::mem::take(&mut *session.mailbox.lock().expect("mailbox"));

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
    session: &mut Session,
    args: &[&str],
    shard: Option<&NodeTarget>,
) -> Result<RespValue, String> {
    let client = if let Some(node) = shard.filter(|node| !node.host.is_empty()) {
        node_client(session, node)?
    } else {
        &mut session.client
    };

    execute_read_retry(client, args).map_err(|error| match error {
        ClientError::Server(message) => message,
        other => other.to_string(),
    })
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
            && args
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case("SLOTS")))
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

fn node_client<'a>(session: &'a mut Session, node: &NodeTarget) -> Result<&'a mut Client, String> {
    let key = (node.host.clone(), node.port);

    if !session.nodes.contains_key(&key) {
        let mut options = ClientOptions::default();

        options.host = node.host.clone();
        options.port = node.port;
        options.discover_cluster = false;
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

        let mut client = Client::connect_with(options).map_err(|error| error.to_string())?;

        let _ = client.set_read_timeout(Some(Duration::from_secs(5)));

        session.nodes.insert(key.clone(), client);
    }

    Ok(session.nodes.get_mut(&key).expect("node client"))
}

fn shards(stream: &mut TcpStream, session_id: &str) -> std::io::Result<()> {
    let mut guard = SESSIONS.lock().expect("sessions");
    let Some(session) = guard.get_mut(session_id) else {
        return write_json(stream, 200, json!({"ok": false, "error": "not connected"}));
    };

    let fallback = json!([{
        "id": 0,
        "host": session.host,
        "port": session.port,
        "start": 0,
        "end": 16383,
    }]);

    let (listed, warning) = match execute_read_retry(&mut session.client, &["CLUSTER", "SLOTS"]) {
        Ok(value) => match slots_json(&value).filter(|items| !items.is_empty()) {
            Some(mut nodes) => {
                let note = reroute_loopback(&mut nodes, &session.host);
                (Some(nodes), note)
            }
            None => (
                None,
                Some("No shard topology reported. Showing the entry node only.".to_owned()),
            ),
        },
        Err(error) => (
            None,
            Some(format!(
                "Shard discovery unavailable: {error}. Showing the entry node only."
            )),
        ),
    };

    write_json(
        stream,
        200,
        json!({"ok": true, "shards": listed.unwrap_or(vec![fallback[0].clone()]), "warning": warning}),
    )
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost") || host == "::1" || host.starts_with("127.")
}

/// A server that does not set `advertised_host` names its shards `127.0.0.1`, which points back
/// at this process when the server is elsewhere (another machine, a host the UI container
/// reaches by name). Such shards are reached through the address the user signed in with.
fn reroute_loopback(nodes: &mut [Value], entry_host: &str) -> Option<String> {
    if is_loopback_host(entry_host) {
        return None;
    }

    let mut changed = false;

    for node in nodes.iter_mut() {
        if node["host"].as_str().is_some_and(is_loopback_host) {
            node["host"] = json!(entry_host);
            changed = true;
        }
    }

    changed.then(|| {
        format!(
            "The server names its shards 127.0.0.1, so they are reached through {entry_host}. Set RUVIO_ADVERTISE_HOST on the server to a name clients can reach to make this permanent."
        )
    })
}

fn slots_json(value: &RespValue) -> Option<Vec<Value>> {
    let RespValue::Array(rows) = value else {
        return None;
    };

    let mut shards = Vec::new();

    for (index, row) in rows.iter().enumerate() {
        let RespValue::Array(parts) = row else {
            continue;
        };

        let start = integer_of(parts.first()?)?;
        let end = integer_of(parts.get(1)?)?;
        let RespValue::Array(node) = parts.get(2)? else {
            continue;
        };

        let host = text_of(node.first()?)?;
        let port = integer_of(node.get(1)?)?;

        shards.push(json!({
            "id": index,
            "host": host,
            "port": port,
            "start": start,
            "end": end,
        }));
    }

    Some(shards)
}

fn integer_of(value: &RespValue) -> Option<u64> {
    match value {
        RespValue::Integer(value) => u64::try_from(*value).ok(),
        RespValue::Bulk(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        RespValue::Simple(text) => text.parse().ok(),
        _ => None,
    }
}

fn text_of(value: &RespValue) -> Option<String> {
    match value {
        RespValue::Bulk(bytes) => String::from_utf8(bytes.clone()).ok(),
        RespValue::Simple(text) => Some(text.clone()),
        _ => None,
    }
}

fn parsed_port(port: &str) -> u16 {
    port.parse::<u16>().unwrap_or(6379)
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
    let sessions = SESSIONS.lock().expect("sessions");

    match sessions.get(session_id) {
        Some(session) => write_json(
            stream,
            200,
            json!({
                "ok": true,
                "connected": true,
                "host": session.host,
                "port": session.port,
                "username": session.username,
            }),
        ),
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
        execute_read_retry, password_for_target, read_request, request_session_id,
        reroute_loopback, retryable_read,
    };
    use ruvio_client::{Client, ClientOptions, RespValue};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

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
    fn loopback_shards_are_reached_through_the_entry_host() {
        let mut nodes = vec![
            json!({"id": 0, "host": "127.0.0.1", "port": 6379}),
            json!({"id": 1, "host": "localhost", "port": 6380}),
            json!({"id": 2, "host": "10.0.0.9", "port": 6381}),
        ];
        let note = reroute_loopback(&mut nodes, "ruvio");
        assert!(note.is_some_and(|text| text.contains("through ruvio")));
        assert_eq!(nodes[0]["host"], "ruvio");
        assert_eq!(nodes[1]["host"], "ruvio");
        assert_eq!(nodes[2]["host"], "10.0.0.9");

        let mut local = vec![json!({"id": 0, "host": "127.0.0.1", "port": 6379})];
        assert!(reroute_loopback(&mut local, "127.0.0.1").is_none());
        assert!(reroute_loopback(&mut local, "localhost").is_none());
        assert_eq!(local[0]["host"], "127.0.0.1");
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
