"""`status.json`: a small summary of a run, written to the run directory and, when a container
SAS URL is configured, uploaded to Azure Blob with one HTTPS PUT per file.

The SAS URL is read from a file outside the repository (`--blob-sas-file`) and never printed: any
error message shows the URL without its query string."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def _redact(url: str) -> str:
    return url.split("?", 1)[0]


class BlobUploader:
    def __init__(self, container_sas_url: str, prefix: str, timeout: float = 20.0):
        parts = urllib.parse.urlsplit(container_sas_url.strip())
        if parts.scheme not in ("https", "http") or not parts.query:
            raise ValueError("the blob SAS file must hold a container URL with its SAS query")
        self.parts, self.prefix, self.timeout = parts, prefix.strip("/"), timeout
        self.errors = 0
        self.last_error = ""

    @classmethod
    def from_file(cls, path: Path | None, prefix: str):
        if path is None or not path.exists():
            return None
        return cls(path.read_text(), prefix)

    def url(self, name: str, prefix: str | None = None) -> str:
        prefix = self.prefix if prefix is None else prefix
        path = f"{self.parts.path.rstrip('/')}/{urllib.parse.quote(f'{prefix}/{name}' if prefix else name)}"
        return urllib.parse.urlunsplit((self.parts.scheme, self.parts.netloc, path, self.parts.query, ""))

    def put(self, name: str, data: bytes, content_type: str = "application/json", prefix: str | None = None) -> bool:
        request = urllib.request.Request(
            self.url(name, prefix),
            data=data,
            method="PUT",
            headers={"x-ms-blob-type": "BlockBlob", "Content-Type": content_type, "x-ms-version": "2021-08-06"},
        )
        # A large file gets a second per 2 MB on top of the base timeout.
        timeout = self.timeout + len(data) / (2 << 20)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                ok = 200 <= response.status < 300
        except (urllib.error.URLError, OSError) as error:
            code = getattr(error, "code", None)
            self.errors += 1
            self.last_error = f"PUT {_redact(self.url(name, prefix))}: {code or type(error).__name__}"
            return False
        return ok


class Status:
    """The run's state. `save` writes `status.json` locally and uploads it as
    `<run>/status.json` and `current/status.json` (the reader's fixed name)."""

    def __init__(self, directory: Path, run_id: str, plan: dict, backend: str, uploader: BlobUploader | None):
        self.directory, self.uploader = directory, uploader
        self.started = time.time()
        self.data = {
            "run_id": run_id,
            "plan": plan.get("name"),
            "backend": backend,
            "phase": "starting",
            "started_at": _iso(self.started),
            "updated_at": None,
            "elapsed_s": 0,
            "duration_s": plan.get("duration_s"),
            "verdict": "ok",
            "problems": [],
            "faults": {"injected": 0, "skipped": 0, "recovery_s_max": 0.0, "by_kind": {}},
            "operations": {"ok": 0, "fail": 0, "info": 0},
            "processes": {},
            "rss_slope_percent_per_hour": {},
            "check": None,
            "upload_errors": 0,
        }

    def problem(self, text: str, severity: str = "fail") -> None:
        if text not in self.data["problems"]:
            self.data["problems"].append(text)
        self.raise_verdict(severity)

    def raise_verdict(self, verdict: str) -> None:
        if _rank(verdict) > _rank(self.data["verdict"]):
            self.data["verdict"] = verdict

    def fault(self, kind: str, injected: bool, recovery_s: float | None = None) -> None:
        faults = self.data["faults"]
        faults["injected" if injected else "skipped"] += 1
        entry = faults["by_kind"].setdefault(kind, {"injected": 0, "skipped": 0})
        entry["injected" if injected else "skipped"] += 1
        if recovery_s is not None:
            faults["recovery_s_max"] = round(max(faults["recovery_s_max"], recovery_s), 2)

    def save(self) -> None:
        now = time.time()
        self.data["updated_at"] = _iso(now)
        self.data["elapsed_s"] = int(now - self.started)
        if self.uploader:
            self.data["upload_errors"] = self.uploader.errors
        body = json.dumps(self.data, indent=2).encode()
        (self.directory / "status.json").write_bytes(body)
        if self.uploader:
            self.uploader.put("status.json", body)
            self.uploader.put("status.json", body, prefix="current")


def _rank(verdict: str) -> int:
    return {"ok": 0, "warn": 1, "fail": 2}[verdict]


def _iso(seconds: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(seconds))
