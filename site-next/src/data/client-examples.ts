// Client-library versions of the redis-cli examples on /use-cases/, keyed by use case id and client slug.
// Each snippet uses the client's real typed methods; keep the steps and key names the same as the redis-cli example.
export type ClientSlug = "rust" | "csharp" | "go" | "java" | "python" | "node";
export const clientExamples: Record<string, Record<ClientSlug, string>> = {
  "rate-limits": {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
let left = db.limit("api:key-7", 100, 60_000)?;
// Some(99) -> 99 calls left in this 60 s window
// None     -> window is full, nothing was counted

// Smoother: sliding window, or a token bucket that tells you how long to wait
let sliding = db.limit_sliding("api:key-7", 100, 60_000)?;
let bucket = db.limit_tokens_with_retry("api:key-7:burst", 10, 1_000, 2)?;
// bucket.allowed, bucket.remaining, bucket.retry_after_ms`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

long? left = await db.LimitAsync("api:key-7", 100, 60_000);
// 99   -> 99 calls left in this 60 s window
// null -> window is full, nothing was counted

// Smoother: sliding window, or a token bucket that tells you how long to wait
long? sliding = await db.LimitSlidingAsync("api:key-7", 100, 60_000);
LimitOutcome bucket = await db.LimitTokensWithRetryAsync("api:key-7:burst", 10, 1_000, 2);`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

left, ok, err := c.Limit(ctx, "api:key-7", 100, time.Minute)
if err != nil { return err }
fmt.Println(left, ok)
// 99 true -> 99 calls left in this 60 s window
// 0 false -> window is full, nothing was counted

// Smoother: sliding window, or a token bucket that tells you how long to wait
left, ok, err = c.LimitSliding(ctx, "api:key-7", 100, time.Minute)
bucket, err := c.LimitTokensWithRetry(ctx, "api:key-7:burst", 10, time.Second, 2)
// bucket.Allowed, bucket.Remaining, bucket.RetryAfter`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

Long left = c.limit("api:key-7", 100, 60_000);
// 99   -> 99 calls left in this 60 s window
// null -> window is full, nothing was counted

// Smoother: sliding window, or a token bucket that tells you how long to wait
Long sliding = c.limitSliding("api:key-7", 100, 60_000);
LimitOutcome bucket = c.limitTokensWithRetry("api:key-7:burst", 10, 1_000, 2);`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
left = r.limit("api:key-7", 100, 60_000)
# 99   -> 99 calls left in this 60 s window
# None -> window is full, nothing was counted

# Smoother: sliding window, or a token bucket that tells you how long to wait
sliding = r.limit_sliding("api:key-7", 100, 60_000)
allowed, remaining, retry_after_ms = r.limit_tokens_with_retry("api:key-7:burst", 10, 1_000, 2)`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
const left = await c.limit('api:key-7', 100, 60_000);
// 99   -> 99 calls left in this 60 s window
// null -> window is full, nothing was counted

// Smoother: sliding window, or a token bucket that tells you how long to wait
const sliding = await c.limitSliding('api:key-7', 100, 60_000);
const bucket = await c.limitTokensWithRetry('api:key-7:burst', 10, 1_000, 2);
// bucket.allowed, bucket.remaining, bucket.retryAfterMs`,
  },
  locks: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
let token = db.lease("job:nightly-report", 30_000)?;
// Some(token) -> you hold it
// None     -> someone else holds it
if let Some(token) = token {
    let released = db.release("job:nightly-report", token as u64)?;
    // 1 -> released (0 if the token no longer matches)
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

long? token = await db.LeaseAsync("job:nightly-report", 30_000);
// token -> you hold it
// null -> someone else holds it
if (token is long held)
{
    long released = await db.ReleaseAsync("job:nightly-report", held);
    // 1 -> released (0 if the token no longer matches)
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

token, ok, err := c.Lease(ctx, "job:nightly-report", 30*time.Second)
if err != nil { return err }
if !ok { return nil } // someone else holds it
// you hold it
released, err := c.Release(ctx, "job:nightly-report", token)
if err != nil { return err }
fmt.Println(released) // true -> released (false if the token no longer matches)`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

Long token = c.lease("job:nightly-report", 30_000);
// token -> you hold it
// null -> someone else holds it
if (token != null) {
    Boolean released = c.release("job:nightly-report", token);
    // true -> released (false if the token no longer matches)
}`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
token = r.lease("job:nightly-report", 30_000)
# token -> you hold it
# None -> someone else holds it
if token is not None:
    released = r.release("job:nightly-report", token)
    # True -> released (False if the token no longer matches)`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
const token = await c.lease('job:nightly-report', 30_000);
// token -> you hold it
// null -> someone else holds it
if (token !== null) {
  const released = await c.release('job:nightly-report', token);
  // true -> released (false if the token no longer matches)
}`,
  },
  workers: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
let token = db.semaphore("upstream:billing", 8, 15_000)?;
// Some(1700000000123) -> a slot, this is your token
// None                -> all 8 slots are taken
if let Some(token) = token {
    db.release("upstream:billing", token as u64)?;
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

long? token = await db.AcquireSemaphoreAsync("upstream:billing", 8, 15_000);
// 1700000000123 -> a slot, this is your token
// null          -> all 8 slots are taken
if (token is long held)
{
    await db.ReleaseAsync("upstream:billing", held);
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

token, ok, err := c.Semaphore(ctx, "upstream:billing", 8, 15*time.Second)
if err != nil { return err }
if !ok { return nil } // all 8 slots are taken
// a slot, token is 1700000000123
if _, err := c.Release(ctx, "upstream:billing", token); err != nil { return err }`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

Long token = c.semaphore("upstream:billing", 8, 15_000);
// 1700000000123 -> a slot, this is your token
// null          -> all 8 slots are taken
if (token != null) {
    c.release("upstream:billing", token);
}`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
token = r.semaphore("upstream:billing", 8, 15_000)
# 1700000000123 -> a slot, this is your token
# None          -> all 8 slots are taken
if token is not None:
    r.release("upstream:billing", token)`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
const token = await c.semaphore('upstream:billing', 8, 15_000);
// '1700000000123' -> a slot, this is your token (a decimal string)
// null            -> all 8 slots are taken
if (token !== null) {
  await c.release('upstream:billing', token);
}`,
  },
  inventory: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
db.set("stock:ticket-42", "500")?;
let left = db.take("stock:ticket-42", 2)?;
// Some(498) -> took 2, 498 left
// None      -> not enough, the key is unchanged

let seats = db.incr_by_max("seats:room-9", 1, 120)?;
// Some(57) -> joined
// None     -> room is full, nothing was written`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.SetAsync("stock:ticket-42", "500");
long? left = await db.TakeAsync("stock:ticket-42", 2);
// 498  -> took 2, 498 left
// null -> not enough, the key is unchanged

long? seats = await db.IncrementWithLimitAsync("seats:room-9", 1, 120);
// 57   -> joined
// null -> room is full, nothing was written`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

if err := c.Set(ctx, "stock:ticket-42", "500"); err != nil { return err }
left, took, err := c.Take(ctx, "stock:ticket-42", 2)
if err != nil { return err }
fmt.Println(left, took) // 498 true -> took 2; 0 false -> not enough, the key is unchanged

seats, joined, err := c.IncrByMax(ctx, "seats:room-9", 1, 120)
if err != nil { return err }
fmt.Println(seats, joined) // 57 true -> joined; 0 false -> room is full, nothing was written`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.set("stock:ticket-42", "500");
Long left = c.take("stock:ticket-42", 2);
// 498  -> took 2, 498 left
// null -> not enough, the key is unchanged

Long seats = c.incrbyMax("seats:room-9", 1, 120);
// 57   -> joined
// null -> room is full, nothing was written`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.set("stock:ticket-42", 500)
left = r.take("stock:ticket-42", 2)
# 498  -> took 2, 498 left
# None -> not enough, the key is unchanged

seats = r.incrby("seats:room-9", 1, max=120)
# 57   -> joined
# None -> room is full, nothing was written`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.set('stock:ticket-42', 500);
const left = await c.take('stock:ticket-42', 2);
// 498  -> took 2, 498 left
// null -> not enough, the key is unchanged

const seats = await c.incrbyMax('seats:room-9', 1, 120);
// 57   -> joined
// null -> room is full, nothing was written`,
  },
  idempotency: {
    rust: `use std::time::Duration;
use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
let first = db.idem_begin("order:42", "request-digest", "attempt-1", Duration::from_secs(60))?;
// IdemState::Acquired -> run the handler once
db.idem_complete("order:42", "request-digest", "attempt-1", b"saved-result")?; // true
let retry = db.idem_begin("order:42", "request-digest", "attempt-2", Duration::from_secs(60))?;
// IdemState::Completed(b"saved-result".to_vec())`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

var first = await db.TryBeginIdempotencyAsync("order:42", "request-digest", "attempt-1", TimeSpan.FromSeconds(60));
// first.State == IdempotencyState.Acquired -> run the handler once
await db.CompleteIdempotencyAsync("order:42", "request-digest", "attempt-1", "saved-result"); // true
var retry = await db.TryBeginIdempotencyAsync("order:42", "request-digest", "attempt-2", TimeSpan.FromSeconds(60));
// retry.State == IdempotencyState.Completed, retry.Result holds "saved-result" as bytes`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

first, err := c.IdemBegin(ctx, "order:42", "request-digest", "attempt-1", time.Minute)
if err != nil { return err }
fmt.Println(first.State) // acquired -> run the handler once
n, err := c.IdemComplete(ctx, "order:42", "request-digest", "attempt-1", []byte("saved-result"))
if err != nil { return err }
fmt.Println(n) // 1
retry, err := c.IdemBegin(ctx, "order:42", "request-digest", "attempt-2", time.Minute)
if err != nil { return err }
fmt.Println(retry.State, string(retry.Result)) // completed saved-result`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

var first = c.idemBegin("order:42", "request-digest", "attempt-1", 60_000);
// first.state() == IdemResult.State.ACQUIRED -> run the handler once
c.idemComplete("order:42", "request-digest", "attempt-1", "saved-result"); // 1
var retry = c.idemBegin("order:42", "request-digest", "attempt-2", 60_000);
// retry.state() == COMPLETED, retry.resultString() == "saved-result"`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
first = r.idem_begin("order:42", "request-digest", "attempt-1", 60_000)
# IdemResult(state='acquired', result=None) -> run the handler once
r.idem_complete("order:42", "request-digest", "attempt-1", "saved-result")  # 1
retry = r.idem_begin("order:42", "request-digest", "attempt-2", 60_000)
# IdemResult(state='completed', result='saved-result')`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
const first = await c.idemBegin('order:42', 'request-digest', 'attempt-1', 60_000);
// { state: 'acquired', result: null } -> run the handler once
await c.idemComplete('order:42', 'request-digest', 'attempt-1', 'saved-result'); // 1
const retry = await c.idemBegin('order:42', 'request-digest', 'attempt-2', 60_000);
// { state: 'completed', result: 'saved-result' }`,
  },
  versions: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
db.setv("profile:ada", "{...}", None)?;    // 1 -> version 1 created
db.setv("profile:ada", "{...}", Some(1))?; // 2 -> accepted, now version 2
let stale = db.setv("profile:ada", "{...}", Some(1));
// Err(Error::Server("ERR version mismatch")) -> stale version, value unchanged`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.SetVersionAsync("profile:ada", "{...}");    // 1 -> version 1 created
await db.SetVersionAsync("profile:ada", "{...}", 1); // 2 -> accepted, now version 2
try
{
    await db.SetVersionAsync("profile:ada", "{...}", 1);
}
catch (RuvioException)
{
    // stale version, value unchanged
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

v, err := c.SetV(ctx, "profile:ada", "{...}", 0) // 1 -> version 1 created (0 sends no version)
if err != nil { return err }
v, err = c.SetV(ctx, "profile:ada", "{...}", 1) // 2 -> accepted, now version 2
if err != nil { return err }
_, err = c.SetV(ctx, "profile:ada", "{...}", 1)
fmt.Println(v, err) // 2 ERR version mismatch -> stale version, value unchanged`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.setv("profile:ada", "{...}");    // 1 -> version 1 created
c.setv("profile:ada", "{...}", 1); // 2 -> accepted, now version 2
try {
    c.setv("profile:ada", "{...}", 1);
} catch (RuvioServerException e) {
    // stale version, value unchanged
}`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.setv("profile:ada", "{...}")     # 1 -> version 1 created
r.setv("profile:ada", "{...}", 1)  # 2 -> accepted, now version 2
try:
    r.setv("profile:ada", "{...}", 1)
except ruvio.ResponseError:
    pass  # stale version, value unchanged`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.setv('profile:ada', '{...}');    // 1 -> version 1 created
await c.setv('profile:ada', '{...}', 1); // 2 -> accepted, now version 2
try {
  await c.setv('profile:ada', '{...}', 1);
} catch (err) {
  // RuvioError: stale version, value unchanged
}`,
  },
  sessions: {
    rust: `use std::time::Duration;
use ruvio_client::{Client, SetMode};

let mut db = Client::connect("127.0.0.1", 6379)?;
db.set_with("session:9f3", "{...}", Some(Duration::from_millis(1_800_000)), SetMode::Always)?;
let session = db.getex("session:9f3", 1_800_000)?;
// Some("{...}") and the deadline is now 30 min from now`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.SetAsync("session:9f3", "{...}", TimeSpan.FromMilliseconds(1_800_000));
string? session = await db.GetExpireAsync("session:9f3", 1_800_000);
// "{...}" and the deadline is now 30 min from now`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

if _, err := c.SetArgs(ctx, "session:9f3", "{...}", ruvio.SetOptions{TTL: 30 * time.Minute}); err != nil { return err }
session, ok, err := c.GetEx(ctx, "session:9f3", 30*time.Minute)
if err != nil { return err }
fmt.Println(session, ok) // {...} true, and the deadline is now 30 min from now`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.set("session:9f3", "{...}", SetOptions.px(1_800_000));
String session = c.getex("session:9f3", 1_800_000);
// "{...}" and the deadline is now 30 min from now`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.set("session:9f3", "{...}", px=1_800_000)
session = r.getex("session:9f3", 1_800_000)
# '{...}' and the deadline is now 30 min from now`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.set('session:9f3', '{...}', { px: 1_800_000 });
const session = await c.getex('session:9f3', 1_800_000);
// '{...}' and the deadline is now 30 min from now`,
  },
  cache: {
    rust: `use std::time::Duration;
use ruvio_client::{Client, FillOutcome};

let mut db = Client::connect("127.0.0.1", 6379)?;
let outcome = db.fill_get_wait("product:42", Duration::from_millis(5000), Duration::from_millis(200))?;
// Hit(value)  -> served from memory      Fill(token)            -> you refill it
// Busy        -> someone else is filling Stale { value, token } -> inside a stale-while-revalidate window
if let FillOutcome::Fill(token) | FillOutcome::Stale { token: Some(token), .. } = outcome {
    let stored = db.fill_set("product:42", "{...}", Duration::from_secs(60), &token, Some(Duration::from_secs(30)))?;
    // true -> stored (false if the token is no longer live)
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

FillResult fill = await db.FillGetAsync("product:42", TimeSpan.FromMilliseconds(5000), TimeSpan.FromMilliseconds(200));
// fill.State: Hit (fill.Value), Fill (fill.Token), Busy, or Stale (fill.Value, and fill.Token when you refresh)
if (fill.Token is long token)
{
    bool stored = await db.FillSetAsync("product:42", "{...}", TimeSpan.FromSeconds(60), token, TimeSpan.FromSeconds(30));
    // true -> stored (false if the token is no longer live)
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

f, err := c.FillGetWait(ctx, "product:42", 5*time.Second, 200*time.Millisecond)
if err != nil { return err }
// f.State: ruvio.FillHit (f.Value), FillFill (f.Token), FillBusy, or FillStale (f.Value, f.Token)
if f.Token != "" { // you refill it
    stored, err := c.FillSet(ctx, "product:42", "{...}", time.Minute, f.Token, 30*time.Second)
    if err != nil { return err }
    fmt.Println(stored) // true -> stored (false if the token is no longer live)
}`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

var fill = c.fillgetWait("product:42", 5000, 200);
// fill.state(): HIT (fill.value()), FILL (fill.token()), BUSY, or STALE (fill.value(), fill.token())
if (fill.token() != null) { // you refill it
    Boolean stored = c.fillset("product:42", "{...}", 60_000, fill.token(), 30_000);
    // true -> stored (false if the token is no longer live)
}`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
fill = r.fillget("product:42", 5000, wait_ms=200)
# FillResult(state, value, token): state is HIT, FILL, BUSY or STALE
if fill.token is not None:  # you refill it
    stored = r.fillset("product:42", "{...}", 60_000, fill.token, swr_ms=30_000)
    # True -> stored (False if the token is no longer live)`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
const fill = await c.fillget('product:42', 5000, 200);
// { state: 'hit' | 'fill' | 'busy' | 'stale', value, token }
if (fill.token !== null) { // you refill it
  const stored = await c.fillset('product:42', '{...}', 60_000, fill.token, 30_000);
  // true -> stored (false if the token is no longer live)
}`,
  },
  delayed: {
    rust: `use std::time::Duration;
use ruvio_client::{Client, StreamReadOptions};

let mut db = Client::connect("127.0.0.1", 6379)?;
db.xgroup_create("jobs", "workers", "$", true)?; // true adds MKSTREAM
db.xadd_delay("jobs", 30_000, "*", &[("kind", "send-reminder"), ("user", "42")])?;
let options = StreamReadOptions { count: Some(10), block: Some(Duration::from_millis(5000)), ..Default::default() };
for stream in db.xreadgroup("workers", "w1", &options, &[("jobs", ">")])? {
    for entry in stream.entries { // empty until the 30 s delay has passed; read in a loop
        db.xack("jobs", "workers", &[&entry.id])?;
    }
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.CreateStreamGroupAsync("jobs", "workers", "$", true); // true adds MKSTREAM
await db.AddStreamEntryDelayedAsync("jobs", 30_000, "*", ("kind", "send-reminder"), ("user", "42"));
var read = await db.GetStreamGroupUpdatesAsync("workers", "w1", "jobs", ">",
    new StreamReadOptions { Count = 10, Block = TimeSpan.FromMilliseconds(5000) });
foreach (var entry in read.SelectMany(stream => stream.Entries)) // empty until the 30 s delay has passed; read in a loop
{
    await db.AcknowledgeStreamAsync("jobs", "workers", entry.Id);
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

if err := c.XGroupCreate(ctx, "jobs", "workers", "$", true); err != nil { return err } // true adds MKSTREAM
_, _, err = c.XAdd(ctx, ruvio.XAddArgs{Stream: "jobs", Delay: 30 * time.Second, ID: "*",
    Values: []string{"kind", "send-reminder", "user", "42"}})
if err != nil { return err }
msgs, err := c.XReadGroup(ctx, ruvio.XReadGroupArgs{Group: "workers", Consumer: "w1", Stream: "jobs",
    Count: 10, Block: 5 * time.Second})
if err != nil { return err }
for _, m := range msgs { // empty until the 30 s delay has passed; read in a loop
    if _, err := c.XAck(ctx, "jobs", "workers", m.ID); err != nil { return err }
}`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.xgroupCreate("jobs", "workers", "$", true); // true adds MKSTREAM
c.xadd("jobs", XAddArgs.builder().delay(30_000), Map.of("kind", "send-reminder", "user", "42"));
var read = c.xreadgroup("workers", "w1", XReadArgs.builder().count(10).block(5000), "jobs", ">");
for (var entries : read.values()) {
    for (var entry : entries) { // empty until the 30 s delay has passed; read in a loop
        c.xack("jobs", "workers", entry.id());
    }
}`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.xgroup_create("jobs", "workers", "$", mkstream=True)
r.xadd("jobs", {"kind": "send-reminder", "user": 42}, delay_ms=30_000)
read = r.xreadgroup("workers", "w1", {"jobs": ">"}, count=10, block=5000)
for _stream, entries in read or []:
    for entry in entries:  # the entry appears here after 30 s
        r.xack("jobs", "workers", entry.id)`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.xgroupCreate('jobs', 'workers', '$', { mkStream: true });
await c.xadd('jobs', '*', { kind: 'send-reminder', user: 42 }, { delayMs: 30_000 });
const read = await c.xreadgroup('workers', 'w1', 'jobs', { count: 10, block: 5000 });
for (const entry of read?.[0]?.entries ?? []) { // empty until the 30 s delay has passed; read in a loop
  await c.xack('jobs', 'workers', entry.id);
}`,
  },
  changes: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
// No typed method for CHANGES SUBSCRIBE: execute sends it, read_message reads each event.
let start = db.execute(&["CHANGES", "SUBSCRIBE", "FROM", "NOW", "MATCH", "user:*", "TYPES", "put,del"])?;
// start: the cursor the stream begins after, "<shard>:<seq>"
loop {
    let event = db.read_message()?;
    // ["change", cursor, db, type, key, value]; save the cursor to resume with FROM
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

// Ruvio.Client cannot read the CHANGES SUBSCRIBE push stream, so this polls the lighter
// in-memory feed from CHANGES START instead (this shard only, no MATCH or TYPES filter).
long cursor = await db.StartChangesAsync();
while (true)
{
    foreach (ChangeNote note in await db.ReadChangesAsync(cursor))
    {
        cursor = note.Sequence; // note.Text names the command and key, such as "SET user:7"
    }
    await Task.Delay(TimeSpan.FromSeconds(1));
}`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379", Protocol: 3,
    OnPush: func(event ruvio.Value) {
        // ["change", cursor, db, type, key, value]; save the cursor to resume with FROM
    }})
if err != nil { return err }

// No typed method for CHANGES SUBSCRIBE: Do sends it, and the events arrive in OnPush.
start, err := c.Do(ctx, "CHANGES", "SUBSCRIBE", "FROM", "NOW", "MATCH", "user:*", "TYPES", "put,del")
if err != nil { return err }
fmt.Println(start.Str()) // the cursor the stream begins after, "<shard>:<seq>"`,
    java: `var c = RuvioClient.connect(RuvioOptions.builder().host("127.0.0.1").port(6379)
    .pushListener(event -> {
        // ["change", cursor, db, type, key, value]; save the cursor to resume with FROM
    })
    .build());
c.hello(3); // events arrive as RESP3 pushes

// No typed method for CHANGES SUBSCRIBE: executeText sends it.
Object start = c.executeText("CHANGES", "SUBSCRIBE", "FROM", "NOW", "MATCH", "user:*", "TYPES", "put,del");
// the cursor the stream begins after, "<shard>:<seq>"`,
    python: `from ruvio.asyncio import Ruvio

def on_change(event):
    ...  # ["change", cursor, db, type, key, value]; save the cursor to resume with FROM

r = Ruvio("127.0.0.1", 6379, on_push=on_change)
await r.hello(3)  # events arrive as RESP3 pushes
# No typed method for CHANGES SUBSCRIBE: execute_command sends it.
start = await r.execute_command("CHANGES", "SUBSCRIBE", "FROM", "NOW", "MATCH", "user:*", "TYPES", "put,del")
# the cursor the stream begins after, "<shard>:<seq>"`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379, protocol: 3 });
c.on('push', (event) => {
  // ['change', cursor, db, type, key, value]; save the cursor to resume with FROM
});
// No typed method for CHANGES SUBSCRIBE: call sends it.
const start = await c.call('CHANGES', 'SUBSCRIBE', 'FROM', 'NOW', 'MATCH', 'user:*', 'TYPES', 'put,del');
// the cursor the stream begins after, '<shard>:<seq>'`,
  },
  "replica-reads": {
    rust: `use ruvio_client::{Client, ClientOptions, ReadPolicy};

let mut db = Client::connect_with(ClientOptions {
    host: "10.0.0.5".into(),
    port: 7000,
    read_policy: ReadPolicy::PreferReplica,
    ..ClientOptions::default()
})?;
db.set("user:1", "Ada")?;            // primary
let name = db.get_string("user:1")?; // a replica; may not have the write yet`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync(new RuvioClientOptions
{
    Host = "10.0.0.5",
    Port = 7000,
    ReadPolicy = ReadPolicy.PreferReplica,
});
var db = await connection.GetDefaultDatabaseAsync();

await db.SetAsync("user:1", "Ada");              // primary
string? name = await db.GetStringAsync("user:1"); // a replica; may not have the write yet`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{
	Addr:       "10.0.0.5:7000",
	ReadPolicy: ruvio.ReadPreferReplica,
})
if err != nil { return err }

err = c.Set(ctx, "user:1", "Ada")        // primary
name, ok, err := c.Get(ctx, "user:1")    // a replica; may not have the write yet`,
    java: `var c = RuvioClient.connect(RuvioOptions.builder()
        .host("10.0.0.5").port(7000).cluster(true)
        .readPolicy(ReadPolicy.PREFER_REPLICA)
        .build());

c.set("user:1", "Ada");        // primary
String name = c.get("user:1"); // a replica; may not have the write yet`,
    python: `from ruvio import ClusterRuvio, ReadPolicy

r = ClusterRuvio("10.0.0.5", 7000, read_policy=ReadPolicy.PREFER_REPLICA)
r.set("user:1", "Ada")   # primary
name = r.get("user:1")   # a replica; may not have the write yet`,
    node: `import { createClusterClient } from 'ruvio-client';

const c = createClusterClient({ host: '10.0.0.5', port: 7000, readPolicy: 'prefer-replica' });
await c.set('user:1', 'Ada');          // primary
const name = await c.get('user:1');    // a replica; may not have the write yet`,
  },
  rebalance: {
    rust: `use ruvio_client::Client;

let mut db = Client::connect("127.0.0.1", 6379)?;
db.move_slots(0, 2047, 3)?; // starts the move to shard 3
loop {
    let status = db.move_slots_status()?;
    let state = status.iter().find(|(name, _)| name == "state").map(|(_, v)| v.clone());
    if !matches!(state.as_deref(), Some("copying" | "cleaning")) { break; } // "done"
    std::thread::sleep(std::time::Duration::from_millis(100));
}`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.MoveSlotsAsync(0, 2047, 3); // starts the move to shard 3
while ((await db.GetMoveSlotsStatusAsync())["state"] is "copying" or "cleaning")
{
    await Task.Delay(100);
}
// state "done"`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

if err := c.MoveSlots(ctx, 0, 2047, 3); err != nil { return err } // starts the move to shard 3
for {
	status, err := c.MoveSlotsStatus(ctx)
	if err != nil { return err }
	if s := status["state"]; s != "copying" && s != "cleaning" { break } // "done"
	time.Sleep(100 * time.Millisecond)
}`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.moveSlots(0, 2047, 3); // starts the move to shard 3
while (List.of("copying", "cleaning").contains(c.moveSlotsStatus().get("state"))) {
    Thread.sleep(100);
}
// state "done"`,
    python: `import time
from ruvio import Ruvio

r = Ruvio("127.0.0.1", 6379)
r.move_slots(0, 2047, 3)  # starts the move to shard 3
while r.move_slots_status()["state"] in ("copying", "cleaning"):
    time.sleep(0.1)
# state "done"`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.moveSlots(0, 2047, 3); // starts the move to shard 3
while (['copying', 'cleaning'].includes((await c.moveSlotsStatus()).state)) {
  await new Promise((resolve) => setTimeout(resolve, 100));
}
// state "done"`,
  },
  data: {
    rust: `use ruvio_client::{Client, GeoFrom, GeoSearch, JsonIndexType};

let mut db = Client::connect("127.0.0.1", 6379)?;
db.zadd("leaderboard", &[("ada", 1200.0), ("grace", 980.0)])?;
let top = db.zrevrange_with_scores("leaderboard", 0, 9)?; // highest score first
let search = GeoSearch { count: Some(10), ..GeoSearch::radius(GeoFrom::LonLat(13.36, 38.11), 50.0, "km") };
let near = db.geo_search_with("cities", &search)?;
db.json_set("user:7", "$", r#"{"plan":"pro","country":"TR"}"#)?;
db.json_index_create("by_plan", "user:", "$.plan", Some(JsonIndexType::String))?;
let pro = db.json_find("by_plan", "pro", Some(20), None)?; // ["user:7"]`,
    csharp: `await using var connection = await RuvioConnection.ConnectAsync("127.0.0.1", 6379);
var db = await connection.GetDefaultDatabaseAsync();

await db.AddSortedSetAsync("leaderboard", ("ada", 1200), ("grace", 980));
var top = await db.GetSortedSetRangeByRankWithScoresAsync("leaderboard", 0, 9, descending: true);
var near = (await db.GeoSearchAsync("cities", 13.36, 38.11, 50, "km")).Take(10); // no COUNT argument here
await db.JsonSetAsync("user:7", "$", """{"plan":"pro","country":"TR"}""");
await db.JsonCreateIndexAsync("by_plan", "user:", "$.plan", JsonIndexType.String);
var pro = await db.JsonFindAsync("by_plan", "pro", count: 20); // ["user:7"]`,
    go: `c, err := ruvio.Connect(ctx, ruvio.Options{Addr: "127.0.0.1:6379"})
if err != nil { return err }

if _, err := c.ZAdd(ctx, "leaderboard", ruvio.Z{Member: "ada", Score: 1200}, ruvio.Z{Member: "grace", Score: 980}); err != nil { return err }
top, err := c.ZRangeArgsWithScores(ctx, ruvio.ZRangeArgs{Key: "leaderboard", Start: "0", Stop: "9", Rev: true})
if err != nil { return err }
near, err := c.GeoSearch(ctx, "cities", ruvio.GeoSearchQuery{Longitude: 13.36, Latitude: 38.11, Radius: 50, Unit: "km", Count: 10})
if err != nil { return err }
if err := c.JSONSet(ctx, "user:7", "$", \`{"plan":"pro","country":"TR"}\`); err != nil { return err }
if err := c.JSONIndexCreate(ctx, "by_plan", "user:", "$.plan", "string"); err != nil { return err }
pro, err := c.JSONFind(ctx, "by_plan", "pro", 20, "")
if err != nil { return err }
fmt.Println(top, near, pro) // pro: [user:7]`,
    java: `var c = RuvioClient.connect("127.0.0.1", 6379);

c.zadd("leaderboard", Map.of("ada", 1200.0, "grace", 980.0));
var top = c.zrangeWithScores("leaderboard", "0", "9", ZRangeArgs.builder().rev());
var near = c.geosearch("cities", GeoSearchArgs.fromLonLat(13.36, 38.11).byRadius(50, GeoUnit.KM).count(10));
c.jsonSet("user:7", "$", "{\\"plan\\":\\"pro\\",\\"country\\":\\"TR\\"}");
c.jsonIndexCreate("by_plan", "user:", "$.plan", "string");
var pro = c.jsonFind("by_plan", "pro", 20, null); // [user:7]`,
    python: `import ruvio

r = ruvio.Ruvio("127.0.0.1", 6379)
r.zadd("leaderboard", {"ada": 1200, "grace": 980})
top = r.zrange("leaderboard", 0, 9, rev=True, withscores=True)  # [('ada', 1200.0), ('grace', 980.0)]
near = r.geosearch("cities", longitude=13.36, latitude=38.11, radius=50, unit="km", count=10)
r.json_set("user:7", "$", '{"plan":"pro","country":"TR"}')
r.json_index_create("by_plan", "user:", "$.plan", type="string")
pro = r.json_find("by_plan", "pro", count=20)  # ['user:7']`,
    node: `import { createClient } from 'ruvio-client';

const c = createClient({ host: '127.0.0.1', port: 6379 });
await c.zadd('leaderboard', [[1200, 'ada'], [980, 'grace']]);
const top = await c.zrangeWithScores('leaderboard', 0, 9, { rev: true }); // [{ member: 'ada', score: 1200 }, ...]
const near = await c.geosearch('cities', { fromLonLat: [13.36, 38.11], byRadius: { radius: 50, unit: 'km' }, count: 10 });
await c.jsonSet('user:7', '$', '{"plan":"pro","country":"TR"}');
await c.jsonIndexCreate('by_plan', 'user:', '$.plan', 'string');
const pro = await c.jsonFind('by_plan', 'pro', { count: 20 }); // ['user:7']`,
  },
};
