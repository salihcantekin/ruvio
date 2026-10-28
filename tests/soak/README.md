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

A fault a backend cannot inject is recorded as skipped, never as passed. On the VMs every
network fault is also scheduled for removal on the host itself (`systemd-run --on-active`), so it
never outlives a controller that died.

A data node that comes back starts as a replica of its group's current primary
(`"restart_as": "replica"`, the default). `"restart_as": "configured"` starts it in its first
role instead; see [#142](https://github.com/salihcantekin/ruvio/issues/142) for what that does
today.

## The history check

`python3 tests/soak/run.py check <history.jsonl>` (also run at the end of every run, written to
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

`plans/azure-short.json` is the same in 30 minutes (faults every 2-4 minutes), to try the tool.
`plans/azure-mini.json` is a 3-hour run: every fault kind every 5-10 minutes, faults lasting
5-30 s, `durable` writes waiting for both replicas. The cloud-init data-disk detection and the
`nft`/`tc` faults have not been run on Azure yet.

## Files

| Path | What |
| --- | --- |
| `run.py` | Entry point: `run`, `check`, `render` |
| `ruvio_soak/cluster.py` | Topology and the configs it renders |
| `ruvio_soak/backends.py` | Local processes, or systemd units over SSH with nftables and netem faults |
| `ruvio_soak/resp.py` | RESP connection and the slot-routing client that classifies outcomes |
| `ruvio_soak/workload.py` | Workers and the history file |
| `ruvio_soak/history.py` | The history checker |
| `ruvio_soak/runner.py` | Health check, fault schedule, final reads |
| `ruvio_soak/metrics.py`, `ruvio_soak/status.py` | Metrics, `status.json` and the Blob upload |
| `plans/` | Run plans |
| `azure/main.bicep` | The run's network, NSG, four VMs, data disks, auto-shutdown |
| `azure/storage.bicep`, `azure/budget.bicep` | Results storage (90-day retention) and the monthly budget alert |
| `azure/cloud-init/` | OS preparation and systemd units of the data nodes and the controller |
| `test_soak.py` | Unit tests of the tool |
