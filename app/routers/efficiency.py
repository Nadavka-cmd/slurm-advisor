from fastapi import APIRouter, Request, HTTPException
import httpx
import os

router = APIRouter(prefix="/api/efficiency", tags=["efficiency"])

GSEFF_URL       = "http://127.0.0.1:8766"  # g-seff internal
OOD_USER_HEADER = "X-Remote-User"

_DEGRADED_MSG = (
    "GPU efficiency data is temporarily unavailable. "
    "Your job history is still shown below. "
    "Efficiency recommendations will appear once the service recovers."
)


def get_remote_user(request: Request) -> str:
    return (
        request.headers.get("X-Remote-User")
        or request.headers.get("REMOTE_USER")
        or os.environ.get("REMOTE_USER", "unknown")
    )


async def _fetch_gseff(path: str, user: str) -> tuple[bool, dict]:
    """
    Try to fetch from g-seff. Returns (ok, data).
    On any failure returns (False, {}) — never raises.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{GSEFF_URL}{path}",
                headers={OOD_USER_HEADER: user},
            )
        if resp.status_code == 200:
            return True, resp.json()
        return False, {}
    except (httpx.ConnectError, httpx.TimeoutException, Exception):
        return False, {}


@router.get("/me")
async def my_efficiency(request: Request):
    """
    Fetch efficiency data from g-seff for the current user.
    Degrades gracefully if g-seff is unavailable — returns partial
    response with degraded=True instead of an error.
    """
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    ok, data = await _fetch_gseff("/api/me", user)

    if not ok:
        return {
            "user":            user,
            "degraded":        True,
            "degraded_msg":    _DEGRADED_MSG,
            "days":            15,
            "summary":         {},
            "jobs":            [],
            "recommendations": [],
        }

    jobs    = data.get("jobs", [])
    summary = data.get("summary", {})

    return {
        "user":            user,
        "degraded":        False,
        "degraded_msg":    None,
        "days":            data.get("days", 15),
        "summary":         summary,
        "jobs":            jobs,
        "recommendations": _build_recommendations(summary, jobs),
    }


# ── Recommendation engine ─────────────────────────────────────────────────────

def _build_recommendations(summary: dict, jobs: list[dict]) -> list[dict]:
    recs = []

    avg_gpu   = summary.get("avg_gpu_eff")
    avg_cpu   = summary.get("avg_cpu_eff")
    avg_mem   = summary.get("avg_mem_eff")
    flagged   = summary.get("flagged", 0)
    job_count = summary.get("job_count", 0)

    if job_count == 0:
        return []

    # GPU utilization
    if avg_gpu is not None:
        if avg_gpu < 15:
            recs.append({
                "severity": "high",
                "metric":   "GPU Utilization",
                "value":    f"{avg_gpu}%",
                "message": (
                    f"Your average GPU utilization is very low ({avg_gpu}%). "
                    "This suggests your workload may not be GPU-bound, or your data pipeline "
                    "is bottlenecking the GPU. Consider profiling with nvidia-smi or torch.profiler, "
                    "and verify your dataloader is not the bottleneck."
                ),
            })
        elif avg_gpu < 30:
            recs.append({
                "severity": "medium",
                "metric":   "GPU Utilization",
                "value":    f"{avg_gpu}%",
                "message": (
                    f"GPU utilization averaged {avg_gpu}% across your recent jobs. "
                    "If you are requesting 2 GPUs, consider whether 1 GPU would suffice — "
                    "this would free resources for other users and may improve your queue wait time."
                ),
            })
        elif avg_gpu >= 70:
            recs.append({
                "severity": "ok",
                "metric":   "GPU Utilization",
                "value":    f"{avg_gpu}%",
                "message":  f"Good GPU utilization at {avg_gpu}%. Your jobs are using the GPU effectively.",
            })

    # Flagged jobs ratio
    if flagged > 0 and job_count > 0:
        pct = round(flagged / job_count * 100)
        if pct >= 50:
            recs.append({
                "severity": "high",
                "metric":   "Low-Efficiency Jobs",
                "value":    f"{flagged}/{job_count} jobs",
                "message": (
                    f"{flagged} of your {job_count} recent jobs had GPU utilization below 30%. "
                    "Check whether these jobs were test runs, data preprocessing, or genuinely "
                    "underutilizing the GPU. Consider running preprocessing on CPU-only resources."
                ),
            })

    # CPU efficiency
    if avg_cpu is not None and avg_cpu < 25:
        recs.append({
            "severity": "medium",
            "metric":   "CPU Efficiency",
            "value":    f"{avg_cpu}%",
            "message": (
                f"CPU efficiency is low at {avg_cpu}%. "
                "You may be over-allocating CPUs. Reducing your --cpus-per-task request "
                "can improve scheduling priority and helps other users."
            ),
        })

    # Memory efficiency
    if avg_mem is not None and avg_mem < 20:
        recs.append({
            "severity": "low",
            "metric":   "Memory Efficiency",
            "value":    f"{avg_mem}%",
            "message": (
                f"Only {avg_mem}% of your requested memory was used on average. "
                "Consider reducing your --mem request to match actual usage."
            ),
        })

    # Multi-GPU overallocation check
    multi_gpu_jobs = [j for j in jobs if j.get("gpus", 0) >= 2 and (j.get("gpu_eff") or 0) < 30]
    if multi_gpu_jobs:
        recs.append({
            "severity": "high",
            "metric":   "Multi-GPU Waste",
            "value":    f"{len(multi_gpu_jobs)} job(s)",
            "message": (
                f"{len(multi_gpu_jobs)} job(s) requested 2+ GPUs but had low GPU utilization. "
                "Unless your code explicitly uses torch.nn.DataParallel or multi-GPU training, "
                "a single GPU is likely sufficient and will schedule faster."
            ),
        })

    if not recs:
        recs.append({
            "severity": "ok",
            "metric":   "Overall",
            "value":    "—",
            "message":  "No efficiency concerns detected in your recent jobs. Keep it up.",
        })

    return recs


@router.get("/all")
async def all_users_efficiency(request: Request):
    """Admin-only: proxy g-seff /api/all for cluster-wide efficiency view."""
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    ok, data = await _fetch_gseff("/api/all", user)

    if not ok:
        return {
            "degraded":     True,
            "degraded_msg": _DEGRADED_MSG,
            "users":        [],
        }

    if data.get("error") == "forbidden":
        raise HTTPException(status_code=403, detail="Admins only")

    return {**data, "degraded": False, "degraded_msg": None}
