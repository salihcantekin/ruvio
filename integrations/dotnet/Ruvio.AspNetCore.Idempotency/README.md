<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.AspNetCore.Idempotency <sub>NuGet</sub></h1>

<p align="center">Endpoint-opt-in bounded HTTP request deduplication and response replay.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.Idempotency"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.AspNetCore.Idempotency?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.Idempotency"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.AspNetCore.Idempotency?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

Opt-in HTTP result deduplication for bounded mutation endpoints on .NET 8. Uses
the native `IDEM` command through
`IRuvioClient.BeginIdempotencyAsync` and `CompleteIdempotencyAsync`. No Lua
transitions or fallback, `ONCE` changes, background workers, renewal timers, or
automatic retries. Deploy a server supporting this native command first.

```csharp
using Ruvio.AspNetCore.Idempotency;
using Ruvio.Client;
using Ruvio.Client.AspNetCore;

// Add the Ruvio.Client.AspNetCore package and register its owning connection once.
builder.Services.AddRuvioClient(builder.Configuration.GetSection("Ruvio"));
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders-production-v1"; // stable across replicas
    options.ScopeSelector = context => context.User.FindFirst("tenant_id")?.Value;
    options.Retention = TimeSpan.FromHours(24);
    options.MaxRequestBodyBytes = 64 * 1024;
    options.MaxResponseBodyBytes = 64 * 1024;
});

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization(); // MUST execute before idempotency, including on replays
app.UseRuvioIdempotency();
app.MapPost("/orders", HandleOrder)
    .RequireAuthorization()
    .WithRuvioIdempotency();
// Controllers can use [RuvioIdempotency] together with [Authorize]; endpoints can also use .WithMetadata(new RuvioIdempotencyAttribute()).
```

By default, each request must have an authenticated `NameIdentifier` or `sub`
claim. For an API that authenticates callers differently (for example, validated
API keys), provide a trusted stable caller identity instead:

```csharp
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "partner-api-production-v1";
    options.ScopeSelector = context => context.Items["tenant_id"] as string;
    options.IdentitySelector = context => context.Items["validated_client_id"] as string;
});
```

Populate those `HttpContext.Items` values in authentication/credential-validation
middleware that runs before idempotency. Do not use an unvalidated request header
as the identity or tenant scope. If callers truly have no identity, set
`AllowAnonymous = true` explicitly instead. All such requests then share one
anonymous identity per namespace and scope, so idempotency keys must be random
and unguessable; use this only when that shared boundary is appropriate.

## Isolation and HTTP contract

- Unmarked endpoints immediately invoke the next middleware: no async state
  machine, body read, client resolution, or instrumentation.
- Configure a trusted, stable tenant/user `ScopeSelector` and deployment
  `Namespace`. Missing configuration fails startup validation; an empty request
  scope throws before acquisition/handler execution. Never trust an unvalidated
  caller-supplied tenant header. Changes to scope/namespace create a new domain.
- By default an authenticated identity with a nonempty `NameIdentifier` (or
  `sub`) is mandatory, otherwise 401. Its authentication type, subject issuer
  and subject are included **in addition** to your scope. `IdentitySelector`
  replaces that subject identity with a caller-defined stable identity; a null
  or blank result returns 401. `AllowAnonymous` explicitly permits missing
  authenticated subjects and groups them into one shared anonymous identity per
  scope. Tenant-only scopes do not share results between authenticated
  principals. Use stable identities across replicas.
- Authentication and endpoint authorization **must run before this middleware**.
  It is not an authorization component and cannot verify your middleware order.
  Authorization must be reevaluated for every replay; do not put it inside the
  handler or after idempotency. Outer middleware may still add fresh security
  headers/cookies, but these are never loaded from the stored result.
- Only POST, PUT, PATCH, DELETE are supported (other marked methods: 405).
  Each marked request requires one `Idempotency-Key` (or the header named by
  `KeyHeader`), 1–128 printable non-space ASCII characters (invalid/missing: 400).
  Keep keys random and unguessable.
- SHA-256 length-delimited hashes cover namespace, authenticated subject, scope,
  and key for storage addressing, and method, path base, path, raw query,
  content type, raw body, namespace/subject/scope for the request fingerprint.
  Storage keys contain only a versioned prefix and one hex hash tag; no raw
  credentials, tenant, key, URL, or payload. Hashing is not encryption.
  A key reused in the same scope for different request content returns 409, or
  `MismatchStatusCode` (set 422 to follow the IETF `Idempotency-Key` header draft).
- Responses must depend only on this fingerprinted input. Headers such as Accept
  and API-version headers are **not** fingerprinted: normalize them upstream or
  place a validated variant in the scope. Streaming, upgrades, trailers, changing
  response body features and indefinite handlers are unsupported.
- Matching completed requests replay status, body, and only the headers in
  `ReplayHeaders`: by default `Content-Type`, `Content-Language`, `ETag`,
  `Last-Modified`. `Content-Length` is recomputed, and a replay carries
  `Idempotent-Replayed: true`. Add `Location` for endpoints that answer
  `201 Created`. Cookies, authentication, CORS and transport-security headers
  are refused at startup and never stored. A stored header outside the list is
  treated as a corrupt record (an error, never replayed), so removing a header
  from the list fails the records stored with it until they expire.
  Normal returned error responses (including 4xx/5xx) are stored too.
  Thrown handler/storage errors and cancellation propagate, never become a
  success response, and are never automatically retried.

## Waiting for a running request and per-endpoint settings

A request whose key is still running answers `409` with `Retry-After: 1`. With
`PendingWait` (default zero, at most 30 s) it waits instead: it asks Ruvio again
with a growing delay of at most 25 ms and, when the first request completes,
replays its result, so a double click or a client retry gets the real answer. If
the first request does not finish in time the answer is the same `409`. The
wait ends when the caller disconnects. A waiting request keeps its buffered body
and costs one `IDEM BEGIN` per poll, which only reads.

```csharp
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders-production-v1";
    options.ScopeSelector = context => context.User.FindFirst("tenant_id")?.Value;
    options.MismatchStatusCode = StatusCodes.Status422UnprocessableEntity;
    options.PendingWait = TimeSpan.FromSeconds(2);
    options.ReplayHeaders.Add("Location");
});

// Settings of its own for one endpoint; the rest come from the options above.
app.MapPost("/payments", Pay).WithRuvioIdempotency(e =>
{
    e.RetentionSeconds = 7 * 24 * 3600;
    e.KeyHeader = "X-Request-Id";
});
app.MapPost("/emails", Send).WithRuvioIdempotency(e => e.KeyRequired = false);
// Controllers: [RuvioIdempotency(RetentionSeconds = 300, PendingWaitMilliseconds = 2000)]
```

`KeyRequired = false` lets a request without the key header run unchecked, before
any body is read; a request that sends the key is checked as usual. Endpoint
values are validated on each request (a wrong one throws), since endpoint
metadata is not seen at startup.

## State and failure model (not downstream exactly-once)

Native `IDEM key BEGIN ...` creates a **pending** record with a random ownership
token and one retention TTL. An existing pending request returns 409 (after
`PendingWait`, when set), including
after crashes, cancellation, response overflow or uncertain downstream outcomes.
It is not renewed, stolen or retried by this package, and it is deleted only by
the opt-in `AbortOnException` below. Completion checks
the exact owner, fingerprint, pending state and positive remaining TTL, atomically
stores the result and changes state to **completed**, preserving the original
expiration. A lost owner/expired record fails completion. Nothing is sent from
the captured handler body until completion is acknowledged.
An identical native completion is acknowledged without rewriting; a changed
result is refused. The middleware itself does not retry uncertain writes.

**TTL does not guarantee downstream exactly-once.** Expiration can occur while a
handler still runs; a later request can execute again. Store loss, eviction,
operator deletion, persistence/failover loss and namespace changes can also
permit duplicates. A lost completion reply is uncertain (the server may have
completed it). Downstream transactions/outboxes and downstream deduplication
using a durable operation identifier remain necessary for irreversible effects.
Choose retention beyond expected handler duration and retry horizon; configure
appropriate Ruvio durability, ACLs and no-eviction/storage capacity. There is no
distributed transaction between HTTP, Ruvio and your business database.

The server stores each transition and its original deadline in one WAL record
(opcode 53); replication and recovery use the same mutation. Upgrade replicas
and recovery tools as well as the server before using it. The native command
keeps a packed string rather than the former Lua hash. The HTTP key prefix is
unchanged to avoid silently admitting the same operation in a new namespace.
Old hash records fail closed until they expire; they are not overwritten,
converted or replayed via Lua. Drain old admissions, handlers and retry retention
before switching all application instances. Do not delete old keys or change
namespace to bypass this safety barrier. See [the native contract](../../../docs/IDEM.md).

### Freeing the key after a handler exception

By default a handler that throws leaves its record pending, so every retry with
that key gets 409 until `Retention` ends (24 hours by default). For handlers that
throw before their work takes effect, or whose work is safe to repeat, turn on
`AbortOnException`:

```csharp
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders";
    options.AbortOnException = true;
});
```

When the handler throws, the middleware sends `IDEM key ABORT fingerprint owner`
before the exception continues up the pipeline. The record is removed only if it
is still pending with this request's owner, so a result stored by another
request is never removed, and the next retry runs the handler again. A response
the handler returns, 5xx included, is stored and replayed exactly as before. If
the abort itself fails (Ruvio unreachable), the handler's exception still
reaches the caller and the record stays pending until its retention ends. Each
removed record counts as state `aborted` on `ruvio.idempotency.states`, and each
attempt as operation `abort` on `ruvio.idempotency.storage_calls`. Needs a
server with `IDEM ... ABORT`.

## When Ruvio is unreachable

By default a storage error reaches your exception handling (`Outage.Mode = Throw`). Choose what to do instead:

```csharp
builder.Services.AddRuvioIdempotency(options =>
{
    options.Namespace = "orders";
    options.ScopeSelector = context => context.User.FindFirst("tenant_id")?.Value;
    options.Outage.Mode = RuvioOutageMode.FailOpen;   // or FailClosed: 503, the request does not run
    options.Outage.CircuitBreaker = new RuvioCircuitBreakerOptions();   // optional
});
```

- `FailClosed`: `503 Service Unavailable` and the handler does not run, so a retry later is safe.
- `FailOpen`: the handler runs **without idempotency**. A client that retries with the same key during the outage runs the
  operation again; accept this only where a duplicate is cheaper than refusing the request. If Ruvio is lost after the
  handler ran, so its result cannot be recorded, the response is still delivered, and a retry with the same key finds the
  request pending (409) rather than running it twice.
- With `FailClosed`, the same loss after the handler ran answers `503` although the operation happened: the key stays
  pending, so a retry gets `409` until the retention ends.

See [When Ruvio is unreachable](../README.md#when-ruvio-is-unreachable) for the modes and the circuit breaker.

## Bounds and pipeline caveats

- Defaults: 64 KiB request and response; configurable 1 byte–1 MiB each.
  Unknown-length request bodies are read only up to the limit plus one probe
  byte; oversized bodies return 413 before acquisition. No disk spooling.
- Response stream, `BodyWriter`, and send-file writes are bounded before copying.
  Overflow is sticky even if a handler catches the exception. Request and response
  features/bodies are restored in `finally`, including failure paths. Handler status,
  reason phrase, and headers are isolated from the original response and copied
  only after completion is acknowledged; handler/store failures preserve the
  original response metadata for outer error handling.
- Fingerprint fields/scope are limited to 8,192 characters each; saved response
  headers to 8,192 total characters. Retention is whole milliseconds from 1 ms to
  7 days, matching the native command's retention bound. `TimeSpan.MaxValue`,
  negative/zero/sub-millisecond values are rejected.
- Known-length request buffers allocate exactly the validated content length;
  mismatched lengths fail before acquisition. Unknown-length request buffers
  start at up to 4 KiB and grow only as needed, capped at the request limit.
  Response buffers reserve the configured response maximum after acquisition.
  This HTTP adapter's response envelope uses base64 (roughly 4/3 expansion), JSON and UTF-16
  strings plus UTF-8 command/RESP copies. **Full bounded payload duplication is
  unavoidable here**, not zero-copy. A stored result is at most about 1.45 MiB
  with maximum options, below the native command's 4 MiB result limit. The native
  command itself is binary-safe and does not require JSON/Base64. If server
  protocol limits are reduced, allow for the serialized response plus the native
  packed header and tokens. The client may allocate a received RESP bulk before package validation;
  restrict access to this namespace and configure transport/server limits too.
- Bounds are per request, not global concurrency/rate limits. Configure upstream
  admission/rate/request-header limits. Application allocations outside these
  streams are outside this package's control.
- Handler `OnStarting` callbacks run before snapshot/completion (deferred until
  the handler finishes); flushing/starting a captured response does not send it.
  Middleware outside this boundary runs on every request. Put exception handling
  outside idempotency; if error handling is inside it, returned errors are stored.
  Do not let outer callbacks change the stored status/representation.
  Test compression/transform middleware ordering for your application.
- Instrumentation is off by default. `EnableInstrumentation` enables the
  `Ruvio.AspNetCore.Idempotency` ActivitySource with an `idempotency` activity and
  only the bounded `idempotency.state` tag. No request/identity/key/body data.
- Metrics are always published on the `Ruvio.AspNetCore.Idempotency` Meter (an exporter
  only sees them when the host adds that Meter): `ruvio.idempotency.states`, tagged
  `state` = `acquired` (the handler runs), `completed` (a stored result is replayed),
  `pending` (the first request still runs: 409) or `conflict` (the key was used for a
  different request: 409); `ruvio.idempotency.storage_calls`, `storage_commands`,
  `storage_duration` (tagged `operation` and `outcome`); and `ruvio.idempotency.outage`.

## Tests

```sh
dotnet test integrations/dotnet/Ruvio.AspNetCore.Idempotency.Tests
# Also execute native transitions against an isolated live Ruvio:
RUVIO_TEST_ADDR=127.0.0.1:6379 \
  dotnet test integrations/dotnet/Ruvio.AspNetCore.Idempotency.Tests
```

Live tests use unique keys and delete only their own test keys.
