"""What Azure itself says about the data nodes' disks, saved into the run directory before the
controller deletes the resource group (the metrics go with the VMs).

The soak tool measures the disk from inside the VM (`/proc/pressure`, `diskstats`). Azure Monitor
adds the platform's view: how much of the VM's and the disk's IOPS and bandwidth limit was used,
and the queue depth. That tells a disk that is slow from a disk that hit its cap. The numbers
arrive a few minutes late, so the export runs after the load, the check and the upload, not at
the start (an export taken while the run was still starting held nothing).

Uses the controller's managed identity (Contributor on its own resource group, `selfDelete` in
main.bicep) and the REST API, standard library only. The export never decides a verdict and
never blocks the deletion of the group: a metric it could not read is listed as missing."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

from .azure import _imds

ARM = "https://management.azure.com"
# Names as Azure lists them in a VM's metric definitions; a name the VM does not offer is
# reported as missing, so a renamed metric shows up instead of silently leaving a hole.
WANTED = (
    "Percentage CPU",
    "Data Disk IOPS Consumed Percentage",
    "Data Disk Bandwidth Consumed Percentage",
    "Data Disk Queue Depth",
    "Data Disk Read Operations/Sec",
    "Data Disk Write Operations/Sec",
    "Data Disk Read Bytes/sec",
    "Data Disk Write Bytes/sec",
    "VM Cached IOPS Consumed Percentage",
    "VM Uncached IOPS Consumed Percentage",
    "VM Cached Bandwidth Consumed Percentage",
    "VM Uncached Bandwidth Consumed Percentage",
)


def _get(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def _summary(points: list[list]) -> dict:
    averages = [p[1] for p in points if p[1] is not None]
    maxima = [p[2] for p in points if p[2] is not None]
    return {"points": len(points), "avg": round(sum(averages) / len(averages), 3) if averages else None,
            "max": max(maxima) if maxima else None, "last_point": points[-1][0] if points else None}


def export(expected_group: str, start: str, end: str, get=_get, imds=_imds, log=print) -> dict:
    """`start` and `end` are UTC ISO times (`2026-11-28T10:00:00Z`). Returns the document that
    goes to `azure-monitor.json`; raises only when this VM is not in `expected_group`."""
    compute = imds("instance/compute", **{"api-version": "2021-02-01"})
    group, subscription = compute["resourceGroupName"], compute["subscriptionId"]
    if group.lower() != expected_group.lower():
        raise RuntimeError(f"this VM is in {group}, not {expected_group}")
    token = imds("identity/oauth2/token", **{"api-version": "2018-02-01", "resource": f"{ARM}/"})["access_token"]
    vms_url = (f"{ARM}/subscriptions/{subscription}/resourceGroups/{urllib.parse.quote(group)}"
               "/providers/Microsoft.Compute/virtualMachines?api-version=2024-07-01")
    document = {"exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "timespan": f"{start}/{end}",
                "interval": "PT1M", "vms": {}}
    for vm in get(vms_url, token).get("value", []):
        entry = document["vms"][vm["name"]] = {"size": vm.get("properties", {}).get("hardwareProfile", {}).get("vmSize"),
                                                "metrics": {}, "summary": {}, "missing": [], "error": None}
        try:
            definitions = get(f"{ARM}{vm['id']}/providers/Microsoft.Insights/metricDefinitions?api-version=2018-01-01", token)
            offered = {d["name"]["value"] for d in definitions.get("value", [])}
            entry["missing"] = [name for name in WANTED if name not in offered]
            names = [name for name in WANTED if name in offered]
            if not names:
                continue
            query = urllib.parse.urlencode({"api-version": "2018-01-01", "metricnames": ",".join(names),
                                            "timespan": f"{start}/{end}", "interval": "PT1M",
                                            "aggregation": "Average,Maximum"}, quote_via=urllib.parse.quote)
            reply = get(f"{ARM}{vm['id']}/providers/Microsoft.Insights/metrics?{query}", token)
            for metric in reply.get("value", []):
                series = metric.get("timeseries") or []
                points = [[d["timeStamp"], d.get("average"), d.get("maximum")] for d in (series[0].get("data", []) if series else [])]
                name = metric["name"]["value"]
                entry["metrics"][name] = points
                entry["summary"][name] = _summary(points)
        except (urllib.error.URLError, OSError, ValueError, KeyError) as error:
            entry["error"] = f"{type(error).__name__}: {str(error)[:200]}"
            log(f"azure monitor: {vm['name']}: {entry['error']}")
    return document


def export_to(directory, expected_group: str, start: str, end: str, **kwargs) -> dict | None:
    """Writes `azure-monitor.json` into the run directory. Returns the document, or None when
    nothing could be exported (the reason is printed)."""
    from pathlib import Path

    try:
        document = export(expected_group, start, end, **kwargs)
    except (urllib.error.URLError, OSError, ValueError, KeyError, RuntimeError) as error:
        print(f"azure monitor: not exported: {error}", flush=True)
        return None
    (Path(directory) / "azure-monitor.json").write_text(json.dumps(document))
    return document
