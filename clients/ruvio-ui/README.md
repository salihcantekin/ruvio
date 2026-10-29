<p align="center">
  <a href="https://ruviodb.com/"><img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ruvio-mark.png" width="88" alt="Ruvio"></a>
</p>

<h1 align="center">Ruvio UI</h1>

<p align="center">Browser operator UI for a running <a href="https://ruviodb.com/">Ruvio</a> server or cluster: a live dashboard, a read-only cluster view, data, command forms, Pub/Sub and ACL users.</p>

<p align="center">
  <a href="https://hub.docker.com/r/salihcantekin/ruvio-ui"><img alt="Docker" src="https://img.shields.io/docker/v/salihcantekin/ruvio-ui?sort=semver&label=docker"></a>
  <a href="https://hub.docker.com/r/salihcantekin/ruvio-ui"><img alt="Docker pulls" src="https://img.shields.io/docker/pulls/salihcantekin/ruvio-ui?label=pulls"></a>
  <a href="https://github.com/salihcantekin/ruvio/blob/main/clients/ruvio-ui/LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue"></a>
</p>

<p align="center">
  <a href="https://ruviodb.com/get-started/#ui">Operator UI guide</a> ·
  <a href="https://hub.docker.com/r/salihcantekin/ruvio-ui">Docker Hub</a> ·
  <a href="https://github.com/salihcantekin/ruvio">Ruvio server</a>
</p>

---

## Run it

```bash
docker run --rm -p 8080:8080 \
  -e RUVIO_UI_TARGET_HOST=127.0.0.1 \
  -e RUVIO_UI_TARGET_PORT=6379 \
  salihcantekin/ruvio-ui:linux
```

Open http://127.0.0.1:8080. `RUVIO_UI_TARGET_HOST` and `RUVIO_UI_TARGET_PORT` are the Ruvio server, the same address `redis-cli` uses. Port `8080` is only the UI.

## Sign-in and users

The browser talks HTTP to this process. This process talks RESP through
`ruvio-client`. Sign-in is the server’s ACL user. An empty user and password
send no `AUTH` and the session is `default`. A password alone is
`AUTH password`. A user and a password are `AUTH user password`.

`default` always exists. It is `on nopass ~* +@all` until `requirepass` is
set. Extra users that must survive a restart belong in `ruvio.toml`:

```toml
[[security.users]]
name = "app"
rules = "on >app-secret ~app:* +@read +ping +auth +info"
```

The ACL screen can run `ACL SETUSER`. That change lasts until the process
restarts. The next start reads `[[security.users]]` again.

The header contains a compact shard dropdown, one entry per shard with its slot ranges
(numbered by their first slot). **All shards** routes keyed commands and
walks `SCAN` until the page is full or every shard has been read. One shard
pins Data, command forms, and Pub/Sub to that node. Multiple selections filter
Overview readings and Data scans to those nodes; keyed commands still route to
their owner and are never broadcast as writes. Non-keyed commands use the entry
node when several shards are selected.

Refreshing the browser restores the existing operator session, without saving
ACL credentials in browser storage. The tab keeps only its session identifier;
host and port are remembered locally for the next sign-in. Sign out explicitly
to release the session. Sessions expire after 30 minutes without API activity,
or when the operator process restarts.

Read-only commands reconnect and retry once after a dropped RESP connection.
The browser also retries failed HTTP transports and HTTP 502/503/504 responses
up to twice for read-only requests. Mutations and subscription message polls
are not replayed; permission, wrong-type, and persistent failures remain visible.

## Operator screens

The panel uses the public site's Dusk Rust theme, with responsive layouts and
keyboard-accessible controls. Every color and font lives in
`web/src/tokens.css`; change the look there.

<p align="center">
  <img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ui-overview.png" width="90%" alt="Overview: shards and replicas responding, keys, memory, trend lines for commands per second, memory and clients, and the slot map">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ui-cluster.png" width="90%" alt="Cluster: groups, shards, the hash slot map and the coordinators with their leader">
</p>
<p align="center">
  <img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ui-data.png" width="45%" alt="Data: a JSON document as a tree">
  <img src="https://raw.githubusercontent.com/salihcantekin/ruvio/main/docs/images/ui-console.png" width="45%" alt="Console: commands and a formatted JSON reply">
</p>

- **Overview** is the first screen after sign-in. It reads every primary and
  replica in parallel (`INFO`, one short-timeout request for all of them) every
  five seconds by default. Choose 2, 5, 10, 30, or 60 seconds, pause, or refresh
  manually. The interval and pause setting survive navigation and reloads. Keys
  and charged store memory are summed over primaries, since replicas hold copies.
  Commands per second, charged memory and connected clients are drawn as trend
  lines for as long as the tab stays open; point at a line to read a value. A
  slot map and a per-shard table show how slots, keys and memory are spread and
  how far replicas are behind. **Needs attention** lists a node that does not
  answer, a persistence error, a primary a newer one replaced, a replica whose
  link is down or that takes a full copy, nodes on different cluster map
  versions, and slots no shard covers. Missing
  metrics show **Not reported**, not zero; incomplete shard readings do not
  produce misleading totals. Command counters come from the server's `INFO`
  response. Memory breakdowns distinguish live data, index capacity, and reusable
  buffers from process RSS. Deleting the last key releases empty index capacity;
  retained reusable buffers can still account for a nonzero memory charge.
- **Cluster** shows the topology and changes nothing. Shards are grouped by each
  node's `cluster-group` (or by server process), with their slot ranges, the
  primary, each replica with its link state and how many records it is behind,
  and the cluster map version every node serves. After a failover or a slot
  move raises that version, the topology is read again. With coordinators
  configured (below) it also shows which coordinator leads, each group's agreed
  primary, epoch, members answering and state, the slot move or rebalance in
  progress, and failovers seen while the screen is open. Rebalancing, slot
  moves and failovers are not started from the UI.
- **Data** scans across owners in All shards, or only the pinned node. Inspect
  strings, lists, sets, hashes, sorted sets, streams, and JSON documents; refresh values, change
  expiry, or delete with confirmation. Large sets use membership checks rather
  than an unbounded member listing. Collection details report total fields,
  members, or entries independently of pagination; task replies label the number
  of items in that reply, rather than claiming it is the complete collection.
  Reading a key updates only its detail pane, not the key scan or the page.
- **JSON documents** open in Data as a collapsible tree (colored by type) or raw,
  pretty-printed text, with the document size and TTL. Click a node to see its
  path (`$.addr.city`, `$.tags[0]`), edit its value as JSON, delete it, add to a
  number, toggle a boolean, clear or append to a container, or replace the whole
  document. Every edit is validated in the browser first and sent as the matching
  `JSON.SET`, `JSON.DEL`, `JSON.NUMINCRBY`, `JSON.TOGGLE`, `JSON.CLEAR`, or
  `JSON.ARRAPPEND`, so the key's lifetime is kept. The path box runs `JSON.GET key
  path`. Keys that are not plain identifiers (spaces, dots) have no path in the
  server's path language and are shown but cannot be edited from the tree.
  Documents over 4 MiB are not loaded whole; read them by path. The **JSON
  documents** task family has a form for each `JSON.*` command, including
  `JSON.INDEX` and `JSON.FIND`; `JSON.GET` and `JSON.MERGE` replies render as the same tree.
- **Tasks** is searchable by intent or command. Find locking (`LEASE` /
  `RELEASE`), conditional writes (`SETV`), rate limiting (`LIMIT`), idempotency
  (`ONCE`), inventory (`TAKE`), sliding sessions (`GETEX`), delayed jobs
  (`XADD DELAY` and worker-group actions), and change feeds (`CHANGES`), alongside
  the standard data-type and server forms. Plain `SET` has no token/version check.
- **Pub/Sub** manages channel and pattern subscriptions and publishing, with a
  bounded message feed. Changing shard scope stops subscriptions and clears the
  old feed. In All shards, Pub/Sub uses the entry node; it does not fan out.
- **Console** (bottom of the menu) runs server commands. It stays closed until
  you press **Connect console**, and commands run with the permissions of the
  signed-in user. Replies print redis-cli style; a single string reply shows as
  plain lines, and JSON is pretty-printed (switch between Auto, Text, JSON and
  Quoted, and Trim whitespace). Only the first 200 lines and 1,000 characters
  per value are drawn, with **Show more**; **Copy all** and **Download** give
  the full result. Errors keep the server's text and add a short hint.
  `FLUSHALL`, `FLUSHDB`, `SHUTDOWN`, `DEBUG`, `KEYS`, `CONFIG SET` and
  `ACL SETUSER`/`DELUSER` ask first; subscriptions, `MONITOR`, blocking
  commands and `MULTI`/`EXEC` are refused (Pub/Sub has its own screen). Up and
  Down browse history, Esc clears the line, Ctrl+L clears the screen, and the
  console can go full screen.
- **ACL** loads the live user directory and lets you inspect or update a user.
  The persistence boundary is shown beside the form: surviving restart still
  means configuring `ruvio.toml`, not saving from this panel.

Server commands, ACL operations, and change feeds are node-local: in All shards
they use the entry node. Pin a shard to start and read its change-feed cursor.
Replies and failures remain visible while editing or choosing the next action;
The header and Session activity footer remain docked while the workspace scrolls.
Session activity retains this panel's recent command outcomes, not a server-wide
command trace.

## Container

Published image: `salihcantekin/ruvio-ui`. Tags `linux` and `latest`
are the same image. `RUVIO_UI_TARGET_HOST` and `RUVIO_UI_TARGET_PORT` are the
Ruvio server. A default process listens on `127.0.0.1:6379`. Port `8080` is
only the UI.

```bash
docker pull salihcantekin/ruvio-ui:linux
docker run --rm -p 8080:8080 \
  -e RUVIO_UI_TARGET_HOST=127.0.0.1 \
  -e RUVIO_UI_TARGET_PORT=6379 \
  salihcantekin/ruvio-ui:linux
```

From a checkout, this compose file builds the UI and starts it next to the
published Ruvio server image.

```bash
cd clients/ruvio-ui
docker compose up --build
```

Open http://127.0.0.1:8080. Host and port default to `127.0.0.1` and `6380`
when the compose file’s server is the two-shard container. Leave User and
Password empty when that server has no `requirepass`.

## Binary

The same executable, without Docker. Node builds the screens. Rust embeds
them.

```bash
cd clients/ruvio-ui/web
npm ci
npm run build
cd ..
cargo run --release
```

Open http://127.0.0.1:8080. If that port is taken, bind another:
`RUVIO_UI_ADDR=127.0.0.1:8081 cargo run --release`.

Host binaries are built on the machine that will run them. GitHub Actions is
not used to publish them while this repository is private.

| Variable | Default | Role |
| --- | --- | --- |
| `RUVIO_UI_ADDR` | `127.0.0.1:8080` | HTTP bind |
| `RUVIO_UI_TARGET_HOST` | `127.0.0.1` | Ruvio server host, the host in `RUVIO_ADDR` |
| `RUVIO_UI_TARGET_PORT` | `6379` | Ruvio server port, the port in `RUVIO_ADDR` |
| `RUVIO_UI_TARGET_PASSWORD` | empty | Password used when the sign-in form leaves it empty for `RUVIO_UI_TARGET_HOST` and `RUVIO_UI_TARGET_PORT`; the browser never receives it |
| `RUVIO_UI_TLS` | off | `true` connects to Ruvio over TLS (every connection the UI opens, including to other shards); the certificate chain and host name are checked |
| `RUVIO_UI_TLS_CA` | empty | PEM file of the CA to trust; when empty the public web roots are used |
| `RUVIO_UI_TLS_CLIENT_CERT`, `RUVIO_UI_TLS_CLIENT_KEY` | empty | PEM files for a server that demands a client certificate (`tls_client_auth`) |
| `RUVIO_UI_TLS_SERVER_NAME` | empty | The name the certificate carries, when it differs from the host you sign in to |
| `RUVIO_UI_TLS_INSECURE` | off | `true` skips the certificate check; for a throwaway test only |
| `RUVIO_UI_COORDINATORS` | empty | `ruvio coordinator` addresses, `host:port,host:port`, for the Cluster screen; empty hides the coordinator part |
| `RUVIO_UI_COORDINATOR_SECRET` | empty | The coordinators' `secret`. It stays in this process; the UI sends the coordinators only `INFO`, `SENTINEL masters`, `SENTINEL replicas` and `COORDINATOR STATUS`. Coordinator connections are plain TCP |

The first argument can be a directory of built screens, used instead of the
files embedded in the binary. `npm run dev` in `web/` proxies `/api` to the
process.

## Development checks

```bash
cd clients/ruvio-ui/web
npm ci
npm test
npx playwright install chromium
npm run test:browser
npm run build
cd ..
cargo check -p ruvio-ui
```

Browser checks use mocked HTTP responses on a test-only Vite server and run at
desktop and phone widths. They cover navigation, authentication payloads,
owner-scanning, pinned requests, error recovery, Pub/Sub scope changes, and ACL
forms. The Node checks validate every form against the compatibility matrix and
check the job-specific wire arguments.
