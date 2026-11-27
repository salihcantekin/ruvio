[Ruvio](../../README.md) / [Tests](../README.md) / Soak

# Long runs of a multi-node cluster

Short tests run on one machine over loopback for seconds. This tool runs a small multi-node
cluster for hours, with load whose every operation is recorded, faults on a schedule, metrics,
and a check of the recorded history at the end. It runs locally (every process on this machine,
to test the tool itself) or on four Azure VMs built from [`azure/`](azure) and deleted after the
run. Python standard library only.

## The cluster

Three hosts, each running one coordinator and one member of each of two groups:

| Process | Port | Data |
| --- | --- | --- |
| coordinator | 26400 | `/var/lib/ruvio/coordinator` |
| group `g1` (slots 0-8191, durability `always`) | 7000, replication 27000 | `/var/lib/ruvio/g1` |
| group `g2` (slots 8192-16383, durability `everysec`) | 7100, replication 27100 | `/var/lib/ruvio/g2` |

`g1` starts with its primary on the first host, `g2` on the second. Locally every host is
127.0.0.1 and host `i`'s ports are shifted by `10 * i`. A fresh `requirepass` and coordinator
secret are generated for each run and kept only in the run directory (`cluster.json`, mode 0600).
`python3 tests/soak/run.py render` prints the files one host gets.

On Azure a fourth VM, the controller, runs this tool, the load and the checker. Only the
controller has a public IP, and SSH reaches it only from one address.

## What a run does

1. Copies the configs (and on Azure the binary) to the hosts, wiping old data, and starts every
   process; waits until the cluster is healthy.
2. Starts the workers (`load` in the plan). For each profile, `default` and `durable`
   (`CLIENT DURABILITY REPLICAS n`), counters get `INCR`, lists get `RPUSH` of 1, 2, 3, ...,
   registers get `SET` of increasing versions and are read by other workers. Each key has one
   writer. Every operation goes to `history.jsonl` as `invoke`, then `ok` (acknowledged), `fail`
   (certainly not applied: refused connection, `MOVED`, `TRYAGAIN`, `READONLY`, ... until the
   15 s retry budget ran out) or `info` (outcome unknown: the connection broke after the send, a
   timeout, `NOREPLICAS`).
3. Injects faults: first the plan's `sequence`, then a random kind from `kinds` every
   `interval_s`. Each fault lasts `down_s`, is healed, and the cluster must be healthy again
   within `recovery_timeout_s`: every process runs, one coordinator leads, the coordinators agree
   on each group's primary, every member reports the right role and names that primary in
   `CLUSTER SLOTS`, and the primary takes a write. A process that exits by itself is a failure.
4. Samples every process every `metrics_interval_s` into `metrics.jsonl`: the `INFO` fields
   (every byte count the server reports: `charged_memory_bytes`, the key, value, collection and
   index parts, buffers, persistence) and the process memory from `/proc/<pid>/status` (RSS,
   peak, anonymous and file-backed parts, threads). Once a series spans `rss_min_window_s`,
   `status.json` gets each process's RSS slope since its last start and the slope of
   `charged_memory_bytes`, the server's own count of its data. RSS climbing while the data stays
   flat points at the allocator or a leak; both climbing together is the data growing.
5. Uploads `events.jsonl` and `metrics.jsonl` to Blob every `artifact_interval_s` (default 600).
6. Stops the load, heals everything, reads every key back, uploads the history, events and
   metrics, then checks the history and uploads `check.json`. The phases in `status.json` are
   `final`, `uploading`, `checking`, `done`.

| Fault | What happens | Backends |
| --- | --- | --- |
| `kill-primary` | SIGKILL to a group's primary, started again after `down_s` | local, ssh |
| `kill-replica` | SIGKILL to a replica | local, ssh |
| `stall-replica` | SIGSTOP to a replica, SIGCONT after `down_s` | local, ssh |
| `kill-coordinator-leader` | SIGKILL to the coordinator that leads | local, ssh |
| `kill-host` | SIGKILL to all three processes of one host | local, ssh |
| `isolate-host` | nftables drops traffic between one host and the other hosts; the controller still reaches it | ssh |
| `netem` | `tc netem` on one host (`faults.netem`, for example `delay 50ms 20ms loss 2%`) | ssh |
| `partition-coordinator-leader` | nftables cuts the leading coordinator's Raft port from the other coordinators; it still reaches the data nodes | ssh |
| `migrate-slots` | `COORDINATOR MOVE` of `faults.migrate_range` (default `4096-4607`) to the other group under load, back the next time; healed when every member names the new owner | local, ssh |
| `slot-churn` | `faults.churn_moves` (default 4) pieces of `faults.migrate_range`, each `COORDINATOR MOVE`d to the other group one after another, then each back | local, ssh |
| `rebalance-slots` | `faults.rebalance_skew` (default 512) slots moved to the other group, then `COORDINATOR REBALANCE`; healed when the rebalance is `done` and every group holds its configured number of slots again | local, ssh |
| `restart-all-data` | SIGKILL to every data node at once; the primary the coordinators name starts first | local, ssh |

Faults run between `faults.start_s` and `faults.stop_s` (seconds after the load started), so a
run can keep fault-free windows at both ends; `faults.sequence` runs first, in order.
`"restart_as": "mixed"` picks `replica` or `configured` for each restart.

A fault a backend cannot inject is recorded as skipped, never as passed. On the VMs every
network fault is also scheduled for removal on the host itself (`systemd-run --on-active`), so it
never outlives a controller that died.

A data node that comes back starts as a replica of its group's current primary
(`"restart_as": "replica"`, the default). `"restart_as": "configured"` starts it in its first
role instead; see [#142](https://github.com/salihcantekin/ruvio/issues/142) for what that does
today.

## What else is recorded

| File | Every | What |
| --- | --- | --- |
| `history-NNNN.jsonl` | `history_segment_s` (3600) | The checked operations, one segment per hour, each uploaded when it closes |
| `latency.jsonl` | minute | Per profile and command: counts by outcome, p50/p90/p99/p99.9/max and the sparse histogram (16 buckets per power of two), so hours merge exactly |
| `background-latency-N.jsonl` | minute | The same for the background load (`load.background`, see `ruvio_soak/background.py`): about a million keys of seven types, bounded in size |
| `metrics.jsonl` | `metrics_interval_s` | Per process: memory, CPU ticks, open descriptors, threads, context switches, data directory, WAL and checkpoint bytes, and the INFO fields of `metrics.py`; `ping_ms`, the fastest of five PINGs from the controller |
| `info.jsonl` | `full_info_every` samples | The whole `INFO` of every process |
| `hosts.jsonl` | `metrics_interval_s` | Per VM: load, free memory and disk, network bytes, chrony offset; the controller's own CPU |
| `logs/hostN-role.log` | `log_interval_s` (3600) | Each process's journal, with panic, ERROR and WARN counts in `status.json` |
| `findings.jsonl`, `check.json` | end | Every anomaly with its time and key; `check.json` ties each to the fault window it fell in (`attribution`) |
| `report.json`, `report.md` | end | `run.py report`: the readiness criteria below, each `pass`, `fail` or `not_measured` |

`load.hot_slots` (for example `["4096-4607"]`) gives every other key of a worker (`load.hot_fraction`, default 0.5) a hash tag that puts it inside those slots, so the checked operations are the ones a slot move touches. Each `slot-move` event holds how long the move took and how many `MOVED` and `ASK` replies the workers' clients followed meanwhile; `status.json` has the totals under `redirects`. `plans/local-slot-moves.json` (10 minutes, local, no other fault) runs the three move kinds under this load.

`load.official` adds a `client` profile that runs counters and registers through the official
Python client (`clients/python`, cluster mode); its operations are checked like the others.

## The readiness report

`python3 tests/soak/run.py report <run directory>` (also run at the end of every run). The
quiet windows are the fault-free stretches before the first and after the last fault, minus
`report_settle_s` at their start; a run without faults compares the first and the last third of
its load instead.

| # | Criterion |
| --- | --- |
| 1 | `durable`: no lost, excess, phantom, duplicate, out-of-order, unread, stale or backwards read |
| 2 | `default`, `client`: none of excess, phantom, duplicate, order, unread; losses and stale reads only inside a fault window, or from 10 s before a failover no fault caused to 30 s after it (criterion 8 fails that failover) |
| 3 | After `restart-all-data`: no acknowledged write lost on the `always` group |
| 4 | No anomaly inside a slot-move window (`migrate-slots`, `slot-churn`, `rebalance-slots`), and no failed or unknown operation 5 to 7 s after any single move landed (`fails_after_5s` of the `slot-move` event) |
| 5 | Every fault healed; counted from the end of the fault (the plan draws its length at random), recovery max <= 15 s, median <= 5 s; each single slot move <= 60 s |
| 6 | No unexpected process exit, no panic in the logs |
| 7 | No failed or unknown operation outside fault windows and host disk stall minutes: at most max(5, one per million ok operations); the ones inside a stall are counted apart |
| 8 | Failovers (`failovers_total`) at most two per fault that hits a primary, and none (`last_failover_unix_ms`) outside a fault window |
| 9 | p99 and p99.9 in the last quiet window within 1.5 x (+0.2 ms) of the first |
| 10 | p99 back within 1.6 x the quiet baseline (+0.2 ms) 60 s after each fault |
| 11 | Background throughput within +-8 % in the quiet windows, among minutes with the same primaries (the background load is closed loop, so its rate follows the latency of the nodes that hold them), minutes that overlap a host disk stall left out. At most one minute in four (at least one) may dip below -5 % of the median, as a checkpoint does, but not below -25 %; the dips are listed in the report |
| 12 | Replica lag p99 below 1 s in the quiet windows |
| 13 | Anonymous memory: last quiet window <= 1.1 x the first (or the same anonymous memory per charged byte); there, anonymous memory per charged byte rising < 2 %/h on data nodes, anonymous memory < 0.5 MiB/h on coordinators. A coordinator that won or lost the Raft leadership after the first window is judged only by its slope since that change (a leader holds more state) |
| 14 | Open descriptors and threads do not grow |
| 15 | WAL bounded (last <= 1.5 x first + 64 MiB), checkpoints in step with the data (bytes per charged byte <= 1.5 x first + 0.1), free disk >= 20 % |
| 16 | Coordinator data grows less than 64 MiB |
| 17 | Metric gaps < 1 %, clock offset < 10 ms, controller CPU p95 < 70 %, check finished |
| 18 | Only when the plan has `load.primitives`: `LEASE`, `SEMAPHORE`, `TRANSFER` and `IDEM` rules hold and every lock key is linearizable (Porcupine; needs Go on the machine that checks). A check that could not run or did not decide is `not_measured`, never a pass |
| 19 | Only for runs with host samples: after each host disk stall (the everysec writes wait while it lasts, the accepted cost of not losing an acknowledged write), from 30 s after it ends, p99 within 1.6 x the quiet baseline (+0.2 ms) and background throughput >= 75 % of the quiet median. A stall that ends in a fault window or near another stall is not judged; no stall is a pass |

## Disk and crash scenarios on this machine

`python3 tests/soak/local_disk.py <scenario> --binary target/debug/ruvio` runs on loop-mounted ext4 disks and needs root, `losetup`, `mkfs.ext4`, `mount` and `fsfreeze` (it prints `skipped` and exits 2 without them). No cloud, nothing to pay, each under ten minutes. A loop disk freezes cleanly and a cloud disk does not, so this shows what the server does when a disk stops or fails, not how an Azure disk behaves.

| Scenario | What it does | Passes when |
| --- | --- | --- |
| `freeze` | Three hosts, each on its own disk, a soak run without plan faults (`--duration`, default 420 s); each host's disk is frozen twice for 8 s (`run.py run --work` puts the nodes on the mounted disks) | Soak criteria 1, 2, 6 and 8: no anomaly of the durable profile, no loss outside faults, no process exit, no failover without a fault |
| `kill9` | One node with `always` and a checkpoint every 300 ms; `kill -9` at random moments while a writer sets keys (`--rounds`, default 40) | Every acknowledged key is there after every restart |
| `diskfull` | The server on a 24 MiB disk is written until the disk is full, then restarted full, then restarted with space freed | The refused write gets an error that says its outcome is unknown; reads keep working and the server stays up; the log names the cause; a restart that cannot write says so; acknowledged keys survive; writes work again with space |
| `walcorrupt` | A log with the last record cut, one damaged byte, and an emptied segment | No key ever has a wrong value; a server that refuses to start says why; a dropped end of the log is in the log |

`results.json` in `--base` lists every check. Scenarios record some outcomes without judging them: whether a restart on a full disk starts, and how many keys an emptied segment leaves.

### Is the environment fit to judge?

Before a run on Azure, `python3 tests/soak/run.py preflight --hosts <node addresses> --seconds 120 --target-iops <n> --out <run directory>` writes 8 KiB blocks with `fdatasync` on every node's data disk at the same time (the write-ahead log's pattern). A node fails the check when it reaches under 80 % of `--target-iops` syncs per second or one sync takes over a second; replace the VMs and run it again instead of starting the run. Without `--target-iops` only a stall fails. `preflight.json` in the run directory is read by the report. Run on this machine without `--hosts` (`--dir` picks the disk). Start the run with `run.py run ... --preflight <that preflight.json>`: a failed node stops the run before it starts, and the file is copied into the run directory (the run directory's name is only known once the run starts, so `--out` of the preflight cannot point there). Over ssh the check takes `--seconds` plus a minute of margin; it needs Python 3 and passwordless `sudo` on the node.

The report's `environment` block counts what the machines did during the load: host stalls (a sample in which a host waited on its disk or lost its CPU for more than half of the time; consecutive ones are one stall, and a stall counts the share of the sample it covered), steal, and the preflight result. The verdict is `invalid_environment` instead of `pass`, `fail` or `incomplete` when:

- the stalls add up to more than 10 % of the load, or
- one stall lasts over 60 s, or
- any sample shows CPU steal, or
- a node failed its preflight.

A failure of criteria 1-4 (acknowledged writes lost, stale reads, anomalies in a restart or a slot move) is never put down to the environment: the verdict stays `fail`. The exit code of `report` is 1 for `invalid_environment`. Not covered yet: a criterion for how fast a node is back to normal after a stall.

## The history check

`python3 tests/soak/run.py check history-*.jsonl [--buckets 16] [--events events.jsonl]` (also run at the end of every run, written to
`check.json`):

| Count | Meaning | Verdict |
| --- | --- | --- |
| `lost` | An acknowledged write missing at the end | `fail` in `durable`, `warn` in `default` |
| `excess` | A counter above acknowledged + unknown increments | `fail` |
| `phantom` | A value never written, or written by an operation reported as not applied | `fail` |
| `duplicate`, `order` | A list value twice, or out of its writer's order | `fail` |
| `stale_read` | A read that began after version v was acknowledged returned an older one | `warn` |
| `backwards_read` | One reader saw a register go back | `warn` |
| `unread` | A key whose final read failed | `fail` |

Lost writes in `default` and stale reads are expected around a primary failure without the
primary lease (see [COORDINATOR.md](../../docs/COORDINATOR.md)); they are counted, not failed.

Each finding keeps the time it happened, so the report can tie it to a fault or a failover. A
lost list value or register version has the time its write was acknowledged. A lost counter
increment is timed from the `INCR` replies the history records: a reply at or below one that
came back before it was sent shows where the counter went back. A history without those replies
falls back to its newest acknowledged increments, which put the loss at the end of the run.

## Correctness of LEASE, SEMAPHORE, TRANSFER and IDEM

With `load.primitives` in the plan (see `plans/local-primitives.json`) the same run also drives
the primitives that make a correctness promise, in both profiles, under the same faults:

| Workload | What the check says |
| --- | --- |
| `LEASE`, `SEMAPHORE` + `RELEASE` | tokens never repeat and grow across grants that do not overlap; no grant lands while a holder certainly holds a full lock; the history of every lock key is linearizable (Porcupine) |
| `TRANSFER` between accounts of one slot | the final balances add up to what was put in; no balance below 0; each account ends within what its acknowledged and unknown transfers allow |
| `IDEM BEGIN` raced by several workers | one `acquired` per request, one effect per request, a `completed` answer replays the completed result |

`python3 tests/soak/run.py check-primitives history.jsonl` repeats the check on a recorded history.
The rule checks are Python (`ruvio_soak/primitives_check.py`); the linearizability check is the Go
program in [`porcupine/`](porcupine) (`go run . history.jsonl`, needs Go and the Porcupine module).
Without Go the check is reported as not run, which is a `warn`, never a pass. The model assumes a
holder that never released expires at its time to live at the earliest; it may miss a violation but
never reports one that did not happen. Violations that no failover can explain (a changed sum, a
repeated token, two `acquired`) fail in every profile; lost or doubled holds fail in `durable` and warn in `default`.

Locally `isolate-host` works when the controller runs as root with `iptables`: every host's processes
run as their own user id, and packets between two hosts are dropped in both directions while the
controller still reaches every node (`--out` must be below a directory other users can search, for
example `/var/tmp`).

## Run it locally

```bash
cargo build --bin ruvio
python3 tests/soak/run.py run tests/soak/plans/local-smoke.json --binary target/debug/ruvio --out /tmp/soak-runs
python3 tests/soak/test_soak.py     # the tool's own unit tests, about a second
```

`local-smoke.json` takes about 80 s: 60 s of load with each local fault kind once, then random
ones. The exit code is 1 for a `fail` verdict (`--strict`: also for `warn`). The run directory
holds `status.json`, `check.json`, `events.jsonl`, `history.jsonl`, `metrics.jsonl` and the
process logs.

## Run it on Azure

Everything below runs from the operator's machine with `az` signed in. Nothing in this
repository creates Azure resources by itself.

Region: `uksouth` (both VM sizes in zones 1-3). The lean cluster (3 × `Standard_D4ads_v6`, 1 ×
`Standard_D4as_v6`, 16 vCPU) costs about 1.1 $/h there with disks (list prices, October 2026);
a 3-hour run about 4 $, 24 hours about 28 $. Spot VMs are not used: an eviction would look like
the failures under test.

Once, for results and the budget alert:

```bash
az group create -n rg-ruvio-soak-results -l uksouth
az deployment group create -g rg-ruvio-soak-results -f tests/soak/azure/storage.bicep -p accountName=<unique name>
az deployment sub create -l uksouth -f tests/soak/azure/budget.bicep -p amount=50 contactEmail=<address> startDate=<first of next month>
```

For each run (the resource group is deleted at the end):

```bash
mkdir -p ~/.ruvio-soak && ssh-keygen -q -t ed25519 -N '' -f ~/.ruvio-soak/run-key -C ruvio-soak-run
az group create -n rg-ruvio-soak -l uksouth
az deployment group create -g rg-ruvio-soak -n main -f tests/soak/azure/main.bicep -p \
  sshPublicKey="$(cat ~/.ssh/ruvio_azure.pub)" runKey="$(cat ~/.ruvio-soak/run-key.pub)" \
  allowedSshCidr="$(curl -s https://ifconfig.me)/32" autoShutdownTimeUtc=<HHmm, end of run + 1 h>
CTL=$(az deployment group show -g rg-ruvio-soak -n main --query properties.outputs.controllerPublicIp.value -o tsv)

# A container SAS that can only create and write blobs, for the controller.
ACCOUNT=<account>; KEY=$(az storage account keys list -g rg-ruvio-soak-results -n $ACCOUNT --query [0].value -o tsv)
SAS=$(az storage container generate-sas --account-name $ACCOUNT --account-key "$KEY" -n soak --permissions cw --expiry <now + 2 days> -o tsv)

ssh -i ~/.ssh/ruvio_azure soak@$CTL 'cloud-init status --wait && mkdir -p ~/.ruvio-soak && chmod 700 ~/.ruvio-soak'
echo "https://$ACCOUNT.blob.core.windows.net/soak?$SAS" | ssh -i ~/.ssh/ruvio_azure soak@$CTL 'umask 077; cat > ~/.ruvio-soak/blob-sas-url'
scp -i ~/.ssh/ruvio_azure ~/.ruvio-soak/run-key soak@$CTL:.ssh/id_ed25519
git archive --format=tar.gz HEAD | ssh -i ~/.ssh/ruvio_azure soak@$CTL 'mkdir -p ruvio && tar xz -C ruvio'
ssh -i ~/.ssh/ruvio_azure soak@$CTL 'cd ruvio && ~/.cargo/bin/rustup toolchain install && ~/.cargo/bin/cargo build --profile release-fast --bin ruvio'
ssh -i ~/.ssh/ruvio_azure soak@$CTL "tmux new -d -s soak 'cd ruvio && python3 tests/soak/run.py run tests/soak/plans/azure-mini.json --backend ssh --binary target/release-fast/ruvio --out ~/soak-runs 2>&1 | tee ~/soak.log'"
```

The run uploads `<run-id>/status.json` and `current/status.json` every `status_interval_s`, the
gzipped events and metrics every `artifact_interval_s`, the gzipped history before the check and
`check.json` after it. The check of a 3-hour history (about 9 million operations) needs a few
minutes and a few GB of memory on the controller. To follow it without SSH, make a
read-only SAS (`--permissions r`, short expiry) and read
`https://<account>.blob.core.windows.net/soak/current/status.json?<sas>`. Keep both SAS strings
out of the repository, issues and chat logs that outlive the run.

After the run: wait for `"phase": "done"` in `status.json` and `check.json` in Blob (or copy
`~/soak-runs` from the controller), then `az group delete -n rg-ruvio-soak --yes`. A scheduled
backup delete should check for `check.json` first and give the check time to finish; the run
files live only on the controller and in Blob.
The auto-shutdown deallocates every VM at `autoShutdownTimeUtc` if that is forgotten; the disks
still cost a little until the group is deleted.

`plans/azure-24h.json` is the production-readiness run: 23 hours of load with the primary lease
and snapshots on, no faults in the first 2 and the last 3 hours, every fault kind at least five
times in between (three full restarts), the background load and the official client. It needs
a bigger controller and disk (`controllerSize=Standard_D8as_v6 controllerDiskSizeGb=128`) and,
to delete its own VMs at the end, `selfDelete=true` and
`run.py run ... --delete-resource-group rg-ruvio-soak` (the group is deleted only once every
file is in Blob; the auto-shutdown stays as the backstop). Before deleting, the controller saves Azure Monitor's view of each node (how much of the VM's and the disk's IOPS and bandwidth limit was used, queue depth, CPU; one-minute points for the whole run) as `azure-monitor.json` in the run directory and in Blob, through its managed identity (`ruvio_soak/azure_monitor.py`). The file informs, it decides nothing, and an export that fails is printed and does not keep the group from being deleted. A metric name Azure does not offer is listed under `missing`. `plans/local-rehearsal.json` runs
the same machinery locally in about 12 minutes.

`plans/azure-12h.json` is the same in 12 hours: 2 quiet hours, a fault every 10-15 minutes from
hour 2 to hour 9.5 (each of the ten kinds three times, the rest at random, about 35 faults), then
2.5 quiet hours. It uses the same controller, disk and self-delete settings as the 24-hour plan,
with `ttlHours=16` and the backstop at 16 hours.

`plans/azure-1h-tools.json` is a one-hour trial of what the 24-hour run adds: a 9-fault sequence with `slot-churn`, `rebalance-slots`, `partition-coordinator-leader`, `kill-host` and `restart-all-data`, the `LEASE`/`SEMAPHORE`/`TRANSFER`/`IDEM` load with the Porcupine check, and the preflight and Azure Monitor export steps. It proves the tools on real nodes before the long run; its quiet windows are short, so criteria 9 and 13 are judged loosely.

`plans/azure-1h.json` is the same in one hour: 18 quiet minutes, a fault every 150-170 s for
24 minutes (each kind about once), 18 quiet minutes. Its quiet windows are under the 10 minutes
a memory slope needs, so criterion 13 judges only the first and last window there.

`plans/azure-30m-disk.json` and `plans/azure-30m-faults.json` split a readiness check into two
30-minute runs. The disk run has no faults and a background data set that grows to about 2
million keys, so checkpoints grow through the run: it judges what grows with the data
(criteria 7, 9, 11, 13, 15) between the first and the last third of its load. The fault run
injects each of the ten fault kinds once, every 90-100 s with faults of 5-15 s, between minutes 5
and 25: it judges criteria 2-5, 8 and 10; its quiet windows are too short for 11 and 13.

`plans/azure-3h.json` joins both in three hours: 40 quiet minutes after warmup, the ten fault
kinds in order every 240-300 s with faults of 5-30 s between minutes 45 and 135 (each kind about
twice), then 45 quiet minutes. It judges all 17 criteria, the memory and disk slopes over windows
long enough to show them.

`plans/azure-40m-disk-stall.json` looks for a whole host stalling under checkpoint and WAL
traffic: no faults, 40 minutes of load over a data set growing to about 3 million keys, a
checkpoint every 20 s. Run it on two clusters at once that differ only in the data disk
(`dataDiskSizeGb`, `dataDiskSku`) to tell the disk from the server.

Every host sample also records the cumulative counters a short stall leaves behind:
`/proc/pressure/io` (`io_some_us`, `io_full_us`), CPU iowait and steal ticks, dirty and
writeback memory, and the data disk's `/proc/diskstats` (reads, writes, sectors written,
in-flight, busy and queue time, flushes). The report lists under "Host stalls" each sample
interval in which a node host spent more than half the time with every task waiting for the
disk, or with its CPU taken by the hypervisor.

`plans/azure-short.json` is the same in 30 minutes (faults every 2-4 minutes), to try the tool.
`plans/azure-mini.json` is a 3-hour run: every fault kind every 5-10 minutes, faults lasting
5-30 s, `durable` writes waiting for both replicas. The cloud-init data-disk detection and the
`nft`/`tc` faults have since run on Azure in every plan above.

## Files

| Path | What |
| --- | --- |
| `run.py` | Entry point: `run`, `check`, `report`, `preflight`, `render` |
| `ruvio_soak/cluster.py` | Topology and the configs it renders |
| `ruvio_soak/backends.py` | Local processes, or systemd units over SSH with nftables and netem faults |
| `ruvio_soak/resp.py` | RESP connection and the slot-routing client that classifies outcomes |
| `ruvio_soak/workload.py` | Workers and the history file |
| `ruvio_soak/history.py` | The history checker |
| `ruvio_soak/runner.py` | Health check, fault schedule, final reads |
| `ruvio_soak/metrics.py`, `ruvio_soak/status.py` | Metrics, `status.json` and the Blob upload |
| `ruvio_soak/latency.py`, `ruvio_soak/background.py` | Latency histograms; the background load |
| `ruvio_soak/report.py` | The readiness criteria and the environment block |
| `ruvio_soak/preflight.py` | The disk check before a run (stdlib only; sent to the nodes over ssh) |
| `ruvio_soak/azure.py` | The controller deleting its own resource group |
| `ruvio_soak/azure_monitor.py` | The controller saving Azure Monitor's disk and VM metrics before that |
| `plans/` | Run plans |
| `azure/main.bicep` | The run's network, NSG, four VMs, data disks, auto-shutdown |
| `azure/storage.bicep`, `azure/budget.bicep` | Results storage (90-day retention) and the monthly budget alert |
| `azure/cloud-init/` | OS preparation and systemd units of the data nodes and the controller |
| `test_soak.py` | Unit tests of the tool |
