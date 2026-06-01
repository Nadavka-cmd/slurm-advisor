from fastapi import APIRouter, HTTPException
import subprocess
import os
import yaml
import time

router = APIRouter(prefix="/api/partitions", tags=["partitions"])

SLURM_BIN   = "/opt/slurm/bin"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "slurm_advisor_config.yaml")

# ── Simple in-memory cache ────────────────────────────────────────────────────
_CACHE_TTL = 60  # seconds

_pressure_cache: dict = {"data": None, "ts": 0.0}


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


@router.get("/config")
async def get_partition_config():
    """Expose partition config to the frontend (VRAM, CPU/mem limits, tier)."""
    cfg = _load_config()
    return {"partitions": cfg["partitions"]}


@router.get("/")
async def list_partitions():
    """
    Return abstracted queue pressure per partition.
    Results are cached for 60 seconds to avoid hammering squeue on every page load.
    """
    global _pressure_cache

    now = time.monotonic()
    if _pressure_cache["data"] is not None and (now - _pressure_cache["ts"]) < _CACHE_TTL:
        return _pressure_cache["data"]

    cfg            = _load_config()
    partitions_cfg = cfg["partitions"]
    pressure_cfg   = cfg.get("pressure", {})
    low_max    = pressure_cfg.get("low_max", 0)
    medium_max = pressure_cfg.get("medium_max", 3)
    high_max   = pressure_cfg.get("high_max", 8)

    try:
        pending_out = run([
            f"{SLURM_BIN}/squeue", "--state=PENDING",
            "--Format", "Partition", "--noheader",
        ])
        running_out = run([
            f"{SLURM_BIN}/squeue", "--state=RUNNING",
            "--Format", "Partition", "--noheader",
        ])
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    pending_counts: dict[str, int] = {}
    for line in pending_out.splitlines():
        p = line.strip()
        pending_counts[p] = pending_counts.get(p, 0) + 1

    running_counts: dict[str, int] = {}
    for line in running_out.splitlines():
        p = line.strip()
        running_counts[p] = running_counts.get(p, 0) + 1

    results = []
    for partition, pcfg in partitions_cfg.items():
        pending = pending_counts.get(partition, 0)
        running = running_counts.get(partition, 0)
        pressure, wait_hint = _pressure_label(pending, low_max, medium_max, high_max)
        results.append({
            "partition":    partition,
            "pressure":     pressure,
            "wait_hint":    wait_hint,
            "pending_jobs": pending,
            "running_jobs": running,
            "vram_gb":      pcfg.get("vram_gb", 0),
            "tier":         pcfg.get("tier", ""),
        })

    response = {"partitions": results, "cached_at": int(now), "cache_ttl": _CACHE_TTL}
    _pressure_cache = {"data": response, "ts": now}
    return response


@router.get("/info/{partition_name}")
async def partition_info(partition_name: str):
    """Return partition limits via scontrol."""
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


def _pressure_label(pending: int, low_max: int, medium_max: int, high_max: int) -> tuple[str, str]:
    if pending <= low_max:
        return "Low", "< 15 min"
    elif pending <= medium_max:
        return "Medium", "~30–60 min"
    elif pending <= high_max:
        return "High", "1–3 hours"
    else:
        return "Very High", "> 3 hours"
