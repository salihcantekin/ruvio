# Ruvio.Extensions.FusionCache

A [FusionCache](https://github.com/ZiggyCreatures/FusionCache) backplane for Ruvio. FusionCache keeps an in-memory (L1) copy of each entry on every app node. When one node sets, removes or expires an entry, the backplane tells the other nodes to drop their copy, so they read the new value from the shared level instead of serving the old one until it expires.

Ruvio can be both parts of a FusionCache setup: the shared level (L2) through [`Ruvio.Extensions.Caching`](../Ruvio.Extensions.Caching/README.md), and the backplane through this package. No other server is needed.

```csharp
builder.Services.AddRuvioClient(o => o.Host = "ruvio.internal");   // Ruvio.Client.AspNetCore
builder.Services.AddRuvioDistributedCache();                      // Ruvio.Extensions.Caching

builder.Services.AddFusionCache()
    .WithSystemTextJsonSerializer()
    .WithRegisteredDistributedCache()
    .WithRuvioBackplane();
```

The package does not create a connection. It uses the registered `IRuvioClient`, or the keyed one named by `RuvioBackplaneOptions.ClientKey`, and host start fails when that client is missing.

## How it works

- The backplane publishes on a **sharded** channel (`SPUBLISH` / `SSUBSCRIBE`). A channel belongs to one hash slot, so every app node reaches the same owner, also in a multi-node cluster. Classic `PUBLISH` stays inside one Ruvio process, which is why it is not used.
- A message is FusionCache's own `BackplaneMessage` bytes, base64 encoded (about a third larger; the messages are small).
- The channel is `ChannelPrefix` (default `ruvio:fusioncache:`) plus FusionCache's channel name, which defaults to the cache name. Caches that should hear each other need the same cache name and prefix.

## Limits

- Messages are not stored. A node whose connection dropped misses what was published meanwhile; its in-memory copies then live until they expire. Give entries a modest `Duration` (and use FusionCache's own auto-recovery where it applies).
- This package does not detect a reconnect of the Pub/Sub socket. The client subscribes again by itself when `RuvioClientOptions.Reconnect` is on, but FusionCache is not told (`IsReconnection` is always false).
- Needs a Ruvio server with sharded Pub/Sub (`SPUBLISH`) and `Ruvio.Client` 0.4.0.
- Needs FusionCache 2.1.0 or newer.

## Tests

```bash
RUVIO_TEST_ADDR=127.0.0.1:6379 dotnet test integrations/dotnet/Ruvio.Extensions.FusionCache.Tests
```

The live tests run two FusionCache nodes (own connections, shared Ruvio level) and check that a set or remove on one node reaches the other node's in-memory copy, and that a message that is not from FusionCache is ignored.
