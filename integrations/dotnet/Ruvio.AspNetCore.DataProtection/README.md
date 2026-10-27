<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio.AspNetCore.DataProtection <sub>NuGet</sub></h1>

<p align="center">Encrypted Data Protection key-ring persistence for shared cookies and protected payloads.</p>

<p align="center">
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.DataProtection"><img alt="NuGet" src="https://img.shields.io/nuget/v/Ruvio.AspNetCore.DataProtection?label=nuget"></a>
  <a href="https://www.nuget.org/packages/Ruvio.AspNetCore.DataProtection"><img alt="Downloads" src="https://img.shields.io/nuget/dt/Ruvio.AspNetCore.DataProtection?label=downloads"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

<p align="center">
  <a href="https://github.com/salihcantekin/ruvio/blob/main/integrations/dotnet/README.md">All .NET integrations</a> ·
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/csharp/Ruvio.Client/README.md">Ruvio.Client</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

An append-only `IXmlRepository` for ASP.NET Core Data Protection key rings.
Sharing the key ring and application name lets instances decrypt each other's
authentication/session cookies, antiforgery tokens and other protected payloads.
This is **durable key material**, not an evictable cache or distributed session store.

## Setup

Register `Ruvio.Client.AspNetCore` to provide the owning connection and its
default database-scoped `IRuvioClient`, then:

```csharp
using Microsoft.AspNetCore.DataProtection;
using Ruvio.AspNetCore.DataProtection;

builder.Services.AddDataProtection()
    .SetApplicationName("orders-production")
    .PersistKeysToRuvio(o => o.Key = "orders:production:data-protection:v1")
    .ProtectKeysWithCertificate(certificate);
```

`certificate` must come from the application's secure certificate configuration,
have an accessible private key, and be usable by every reader of this key ring.
Other framework `IXmlEncryptor` implementations also work. Encryptor configuration
is checked after options configuration, so method ordering does not disable the
requirement. Keep old decryption certificates available when rotating certificates.
The package does not provision certificates, export secrets or install an exporter.

For a separate durable Ruvio connection, use a connection factory, for example
after registering a keyed `IRuvioConnection` named `"key-ring"`:

```csharp
builder.Services.AddDataProtection()
    .SetApplicationName("orders-production")
    .PersistKeysToRuvio(
        services => services.GetRequiredKeyedService<Ruvio.Client.IRuvioConnection>("key-ring"),
        o => o.Key = "orders:production:data-protection:v1")
    .ProtectKeysWithCertificate(certificate);
```

The application/DI container owns the selected connection and all its clients;
the repository borrows a client in that connection's default database and never
disposes it or changes its database. Register persistence once per Data Protection
configuration. `SetApplicationName` provides cryptographic application isolation;
use separate keys/deployments for separate trust boundaries as well.

**Encryption is required by default.** Setting `RequireEncryption = false` is an
explicit opt-out for isolated development, not a production recommendation.
Unencrypted elements marked `requiresEncryption` are refused by the repository
when the requirement is enabled. This does not encrypt arbitrary XML supplied
directly to `IXmlRepository`; use the framework key manager to create keys.

## Durability and failure contract

Use persistent, backed-up, **non-evicting** storage and restricted access. Do not
point production key rings at a memory-only deployment or an eviction-prone cache.
The provider cannot inspect or guarantee the deployment's durability, replication
or failover policy. A separate durable connection is recommended when ordinary
caches use different retention/eviction settings.

The server's default durability profile is `memory`: a ring stored there is gone when the server restarts. On its
first key-ring access the repository reads `INFO server` once. When the profile is `memory`, or the server does not say
(an ACL without `INFO`), it logs a warning through the host's logging. Set `RequireDurableStorage = true` to refuse
reads and writes instead, before the ring is touched:

```csharp
builder.Services.AddDataProtection()
    .PersistKeysToRuvio(o => o.RequireDurableStorage = true)
    .ProtectKeysWithCertificate(certificate);
```

The check costs one command per process; a transport error leaves it for the next access. It does not see replication
or failover settings.

There is no TTL, key pruning, automatic retry or fallback to an ephemeral local
ring. An existing key with a TTL is rejected on reads and writes. Keep old keys
and revocation records: removing them can make existing cookies/tokens permanently
unreadable or remove revocation information. Monitor storage capacity and back up
both the ring and the decryption certificates. Persistence acknowledgment alone
does not turn asynchronous replication into zero-loss failover.

Each XML element is stored in a single hash, under the SHA-256 digest of its
serialized bytes. Distinct key/revocation elements are append-only even if their
`friendlyName` values match. Repeating identical serialized content is a no-op;
`friendlyName` is advisory and is not a storage identity. Concurrent writers do
not replace the whole ring or lose each other's appends.

A read is `PTTL` plus `HGETALL`. A write is `PTTL`, `HEXISTS`, `HLEN`, and `HSET`.
The ring must not have a TTL. These checks are separate commands.
The length check happens before the write. These operations occur when
Data Protection loads/updates its ring, not for every `Protect`/`Unprotect` call;
the framework owns its own in-process key-ring caching and refresh behavior.
No package-owned polling loop, per-request middleware or background task is added.

`IXmlRepository` is synchronous. Calls use a thread-pool dispatch to avoid blocking
on client continuations captured by a caller's synchronization context.
`OperationTimeout` defaults to 10 seconds and cancels the command. A timed-out
write is still an **unknown outcome**, not proof that it did not persist.

Defaults: 128 elements and 64 KiB per XML element (8 MiB payload budget). Options
allow up to 4096 elements and 1 MiB per element while retaining the 8 MiB total
budget. Limits deliberately fail loudly instead of pruning keys or returning an
empty ring. Account for both key rotation and revocation records when choosing
limits. The private namespace must not contain unrelated values.

XML parsing prohibits DTDs/external entity resolution and limits document size.
Corrupt XML, wrong server types, capacity errors and transport failures propagate
to Data Protection; none are treated as a successful empty read.

No outage mode: a key ring that cannot be read or written cannot be replaced by "carry on", because requests would
fail to decrypt, or new keys would be issued that other instances never see. Keep the framework's in-process key-ring
cache and `OperationTimeout`, and run the ring on storage that stays up.

## Session integration

This package protects the **session cookie**, while
`Ruvio.Extensions.Caching` stores the **session contents**:

```csharp
builder.Services.AddRuvioDistributedCache(o => o.InstanceName = "orders-session-v1");
builder.Services.AddSession(o => o.IdleTimeout = TimeSpan.FromMinutes(20));
// Configure AddDataProtection().PersistKeysToRuvio(...).ProtectKeysWithCertificate(...)
// as above, with a consistent application name on all instances.

var app = builder.Build();
app.UseRouting();
app.UseSession();
app.MapGet("/visits", async (HttpContext context) =>
{
    await context.Session.LoadAsync(context.RequestAborted);
    var visits = (context.Session.GetInt32("visits") ?? 0) + 1;
    context.Session.SetInt32("visits", visits);
    return visits;
});
```

Import `Ruvio.Extensions.Caching` for `AddRuvioDistributedCache`. Preserve normal
cookie consent, HTTPS, authentication and authorization configuration in your
application. Session remains ASP.NET Core's non-locking, last-writer-wins model;
the counter above is illustrative, not a concurrent atomic counter. Use native
Ruvio coordination commands for that requirement.

## Development

```bash
RUVIO_TEST_ADDR=127.0.0.1:16382 dotnet test integrations/dotnet/Ruvio.AspNetCore.DataProtection.Tests -c Release
dotnet pack integrations/dotnet/Ruvio.AspNetCore.DataProtection -c Release -o dist
```

Live cases cover concurrent appends, certificate-encrypted cross-instance
protection, revocations, app isolation, and real ASP.NET Core Session with a shared
cookie across two HTTP hosts. Tests require an isolated server and delete their
own key-ring keys; unset `RUVIO_TEST_ADDR` explicitly skips live cases.
