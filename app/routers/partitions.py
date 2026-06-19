from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
import subprocess
import os
import re
import json
import yaml
import time
import threading
import urllib.request
import urllib.parse
from collections import deque

router = APIRouter(prefix="/api/partitions", tags=["partitions"])

SLURM_BIN       = "/opt/slurm/bin"
CONFIG_PATH     = os.path.join(os.path.dirname(__file__), "..", "..", "slurm_advisor_config.yaml")
GPU_POLICY_PATH = "/etc/slurm/gpu_policy.json"

OOD_USER_HDR = "X-Remote-User"
_USERNAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")

_DEFAULT_PROM = "http://monitor-node:9090"
_FB_QUERY = ("DCGM_FI_DEV_FB_FREE + ignoring(__name__) DCGM_FI_DEV_FB_USED "
             "+ ignoring(__name__) DCGM_FI_DEV_FB_RESERVED")

_CACHE_TTL      = 60
_HISTORY_POINTS = 30
_SAMPLE_EVERY   = 60

_global: dict = {"partitions": {}, "ts": 0.0}
_global_lock = threading.Lock()
_history: dict[str, deque] = {}

_sampler_started = False
_sampler_lock = threading.Lock()


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def get_remote_user(request: Request) -> str:
    return (
        request.headers.get(OOD_USER_HDR)
        or request.headers.get("REMOTE_USER")
        or os.environ.get("REMOTE_USER", "unknown")
    )


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def _parse_gres_count(token: str) -> int:
    if not token or token == "(null)":
        return 0
    total = 0
    for part in token.split(","):
        head = part.split("(")[0]
        segs = head.split(":")
        try:
            total += int(segs[-1])
        except (ValueError, IndexError):
            continue
    return total


def _gpu_pressure(total: int, used: int, pending: int, free_low_min: int) -> tuple[str, str]:
    free = total - used
    if free <= 0:
        if pending > 0:
            return "Very High", "1-3 hours+"
        return "High", "Until a GPU frees"
    if free >= min(free_low_min, total):
        return "Low", "< 15 min"
    return "Medium", "< 1 hour"


def _fetch_host_vram(cfg: dict) -> dict[str, set]:
    base = (cfg.get("prometheus_url") or _DEFAULT_PROM).rstrip("/")
    url = base + "/api/v1/query?" + urllib.parse.urlencode({"query": _FB_QUERY})
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception:
        return {}
    out: dict[str, set] = {}
    for item in data.get("data", {}).get("result", []):
        host = item.get("metric", {}).get("Hostname")
        try:
            gb = round(float(item["value"][1]) / 1024)
        except (KeyError, ValueError, IndexError, TypeError):
            continue
        if host:
            out.setdefault(host, set()).add(gb)
    return out


def _vram_label(hosts: set, host_vram: dict[str, set]) -> tuple[str, int]:
    vset: set = set()
    for h in hosts:
        vset |= host_vram.get(h, set())
    vs = sorted(vset)
    if not vs:
        return "—", 0
    if len(vs) == 1:
        return f"{vs[0]} GB", vs[0]
    return f"{vs[0]}-{vs[-1]} GB", vs[-1]


def _compute_global() -> None:
    cfg = _load_config()
    free_low_min = cfg.get("gpu_pressure", {}).get("free_low_min", 2)

    host_vram = _fetch_host_vram(cfg)

    pending_out = run([
        f"{SLURM_BIN}/squeue", "-a", "--state=PENDING",
        "--Format", "Partition", "--noheader",
    ])
    sinfo_out = run([
        f"{SLURM_BIN}/sinfo", "-a", "-h", "-N",
        "-O", "Partition:30,NodeList:25,Gres:50,GresUsed:50",
    ])

    pending_counts: dict[str, int] = {}
    for line in pending_out.splitlines():
        p = line.strip()
        if p:
            pending_counts[p] = pending_counts.get(p, 0) + 1

    part: dict[str, dict] = {}
    for line in sinfo_out.splitlines():
        toks = line.split()
        if len(toks) < 3:
            continue
        name  = toks[0].rstrip("*")
        host  = toks[1]
        gres  = toks[2]
        gused = toks[3] if len(toks) >= 4 else "(null)"
        d = part.setdefault(name, {"total": 0, "used": 0, "hosts": set()})
        d["total"] += _parse_gres_count(gres)
        d["used"]  += _parse_gres_count(gused)
        d["hosts"].add(host)

    snapshot: dict[str, dict] = {}
    for name, d in part.items():
        total, used = d["total"], d["used"]
        if total <= 0:
            continue
        pending = pending_counts.get(name, 0)
        free    = max(total - used, 0)
        pressure, wait_hint = _gpu_pressure(total, used, pending, free_low_min)
        alloc_pct = round(used / total * 100)

        buf = _history.setdefault(name, deque(maxlen=_HISTORY_POINTS))
        buf.append(alloc_pct)

        vram_label, vram_gb = _vram_label(d["hosts"], host_vram)

        snapshot[name] = {
            "partition":     name,
            "pressure":      pressure,
            "wait_hint":     wait_hint,
            "pending_jobs":  pending,
            "total_gpus":    total,
            "used_gpus":     used,
            "free_gpus":     free,
            "gpu_alloc_pct": alloc_pct,
            "history":       list(_history.get(name, [])),
            "vram_gb":       vram_gb,
            "vram_label":    vram_label,
        }

    with _global_lock:
        _global["partitions"] = snapshot
        _global["ts"]         = time.monotonic()


def _ensure_global_fresh() -> None:
    if _global["partitions"] and (time.monotonic() - _global["ts"]) < _CACHE_TTL:
        return
    _compute_global()


def _user_partitions(user: str) -> set:
    if not user or user == "unknown" or not _USERNAME_RE.match(user):
        return set()
    try:
        out = run(["sudo", "-n", "-u", user, f"{SLURM_BIN}/sinfo", "-h", "-O", "Partition"])
    except RuntimeError:
        return set()
    return {line.strip().rstrip("*") for line in out.splitlines() if line.strip()}


def _sampler_loop():
    while True:
        try:
            _compute_global()
        except Exception:
            pass
        time.sleep(_SAMPLE_EVERY)


def _ensure_sampler():
    global _sampler_started
    with _sampler_lock:
        if _sampler_started:
            return
        t = threading.Thread(target=_sampler_loop, daemon=True, name="partition-sampler")
        t.start()
        _sampler_started = True


_ensure_sampler()


# ── Resource-policy markdown (for AI coding assistants) ───────────────────────

_POLICY_PROSE = """# HPC Cluster Resource Allocation Policy

_Generated for user `__USER__`. The Resource Matrix below lists only the partitions you are permitted to submit to, and reflects current cluster state._

This document is intended for AI coding assistants (Claude Code, Codex, Cursor, OpenHands, Gemini CLI, and similar tools) generating Slurm job submissions for this HPC cluster.

The objective is to select the correct partition and request reasonable resources while minimizing impact on shared infrastructure.

## Partition Selection Rules

1. CPU-only workloads MUST use the `CPUonly` partition.
2. Do not submit CPU-only jobs to GPU partitions.
3. If the user does not require a specific GPU model, use the `shared` partition.
4. If the workload requires more than 24 GB of GPU VRAM, use `shared_a6000`.
5. If a specific GPU model is required, use the matching partition.
6. If the user belongs to a research group with a dedicated partition, prefer that partition when available.
7. Request the smallest GPU count that satisfies the workload.
8. Scale CPU and memory requests proportionally to GPU count.
9. Avoid requesting maximum resources unless justified by the workload.
10. Do not request exclusive nodes unless explicitly required.

## GPU Selection Guidance

- Use the smallest GPU that satisfies the workload.
- Workloads requiring more than 24 GB of GPU VRAM should use `shared_a6000`.
- Workloads requiring 24 GB of GPU VRAM or less should prefer non-A6000 partitions.
- Do not request A6000 GPUs unless the workload genuinely benefits from their 48 GB VRAM capacity.
- When no specific GPU model is required, use the `shared` partition.

## Resource Matrix

Memory and CPU values are the recommended per-GPU starting points; memory is in MB (use directly with `--mem`).

| Partition | GPU Model | VRAM | Max GPUs / Job | CPUs / GPU | Memory / GPU | Max Runtime | Access | Owner |
|------------|------------|------------|------------|------------|------------|------------|------------|------------|
__MATRIX__

## Restricted Research Partitions

Some partitions are owned by specific research groups and are not intended for general use.

Use a restricted partition only when:
- The user is a member of the owning group.
- The user's PI instructed them to use it.
- HPC staff instructed them to use it.

## Reserved Partitions

The `course` partition is reserved for teaching activities and course students. Researchers and research workloads should not use this partition unless explicitly instructed by HPC staff.
"""


def _fmt_runtime(mt: str) -> str:
    if mt in ("UNLIMITED", "INFINITE"):
        return "unlimited"
    days = 0
    hms = mt
    if "-" in mt:
        d, hms = mt.split("-", 1)
        try:
            days = int(d)
        except ValueError:
            days = 0
    parts = hms.split(":")
    try:
        hours = int(parts[0])
    except (ValueError, IndexError):
        hours = 0
    mins = parts[1] if len(parts) > 1 else "00"
    if days and hours == 0 and mins == "00":
        return f"{days} day" + ("s" if days != 1 else "")
    if not days and hours and mins == "00":
        return f"{hours} hour" + ("s" if hours != 1 else "")
    out = []
    if days:
        out.append(f"{days}d")
    if hours:
        out.append(f"{hours}h")
    return " ".join(out) or mt


def _partition_runtimes() -> dict:
    """partition -> formatted time limit, from one `sinfo -a` call (apache-readable)."""
    try:
        out = run([f"{SLURM_BIN}/sinfo", "-a", "-h", "-O", "Partition:30,Time:25"])
    except RuntimeError:
        return {}
    rt = {}
    for line in out.splitlines():
        toks = line.split()
        if len(toks) >= 2:
            rt[toks[0].rstrip("*")] = _fmt_runtime(toks[1])
    return rt


def _build_policy_md(user: str, allowed: set) -> str:
    try:
        with open(GPU_POLICY_PATH) as f:
            policy = json.load(f)
    except Exception:
        policy = {}

    ordered = [p for p in policy if p in allowed] + sorted(p for p in allowed if p not in policy)
    runtimes = _partition_runtimes()

    rows = []
    for name in ordered:
        runtime = runtimes.get(name, "unknown")
        if name in policy:
            d = policy[name]
            model  = d.get("gpu_model", "?")
            vram   = d.get("vram", "?")
            maxg   = d.get("max_gpu", "?")
            cpg    = d.get("rec_cpu", "?")
            mem    = d.get("rec_mem", None)
            mem_s  = f"{mem} MB" if mem is not None else "N/A"
            access = "Restricted" if d.get("restricted") else "Researchers"
            owner  = d.get("owner_group") or "N/A"
        else:
            model = vram = maxg = cpg = mem_s = "N/A"
            access = "Researchers"
            owner = "N/A"
        rows.append(f"| {name} | {model} | {vram} | {maxg} | {cpg} | {mem_s} | {runtime} | {access} | {owner} |")

    matrix = "\n".join(rows) if rows else "| _(no partitions available to you)_ | | | | | | | | |"
    return _POLICY_PROSE.replace("__USER__", user).replace("__MATRIX__", matrix)


@router.get("/config")
async def get_partition_config():
    cfg = _load_config()
    return {"partitions": cfg["partitions"]}


@router.get("/")
async def list_partitions(request: Request):
    user = get_remote_user(request)

    try:
        _ensure_global_fresh()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    allowed = _user_partitions(user)

    with _global_lock:
        snapshot = _global["partitions"]
        results = [snapshot[p] for p in snapshot if p in allowed]

    return {
        "partitions": results,
        "user":       user,
        "cached_at":  int(_global["ts"]),
        "cache_ttl":  _CACHE_TTL,
    }


@router.get("/policy.md")
async def resource_policy_md(request: Request):
    user = get_remote_user(request)
    allowed = _user_partitions(user)
    md = _build_policy_md(user, allowed)
    return Response(
        content=md,
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="cluster_resources.md"'},
    )


@router.get("/info/{partition_name}")
async def partition_info(partition_name: str):
    cfg = _load_config()
    if partition_name not in cfg["partitions"]:
        raise HTTPException(status_code=404, detail="Unknown partition")

    try:
        out = run([f"{SLURM_BIN}/scontrol", "show", "partition", partition_name])
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    fields = {}
    for token in out.split():
        if "=" in token:
            k, _, v = token.partition("=")
            fields[k] = v

    return {
        "partition":    partition_name,
        "max_time":     fields.get("MaxTime", "unknown"),
        "max_nodes":    fields.get("MaxNodes", "unknown"),
        "state":        fields.get("State", "unknown"),
        "default_time": fields.get("DefaultTime", "unknown"),
    }
