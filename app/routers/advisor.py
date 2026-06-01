from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import subprocess
import httpx
import os
import yaml

router = APIRouter(prefix="/api/advisor", tags=["advisor"])

SLURM_BIN    = "/opt/slurm/bin"
GSEFF_URL    = "http://127.0.0.1:8766"
OOD_USER_HDR = "X-Remote-User"
CONFIG_PATH  = os.path.join(os.path.dirname(__file__), "..", "..", "slurm_advisor_config.yaml")

QOS_MAX_GPUS   = 2
QOS_MAX_WALL_H = 24


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _partition_cfg(cfg: dict) -> dict:
    return cfg.get("partitions", {})


def get_remote_user(request: Request) -> str:
    return (
        request.headers.get("X-Remote-User")
        or request.headers.get("REMOTE_USER")
        or os.environ.get("REMOTE_USER", "unknown")
    )


# ── History-based endpoint ────────────────────────────────────────────────────

@router.get("/from-job/{job_id}")
async def recommend_from_job(job_id: str, request: Request):
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    try:
        result = subprocess.run([
            f"{SLURM_BIN}/sacct",
            "--job", job_id, "--user", user,
            "--format", "JobID,JobName,State,Partition,AllocCPUS,AllocTRES,Elapsed,ExitCode",
            "--noheader", "--parsable2",
        ], capture_output=True, text=True, timeout=15)
        sacct_out = result.stdout.strip()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"sacct error: {e}")

    job_line = None
    for line in sacct_out.splitlines():
        parts = line.split("|")
        if len(parts) < 8:
            continue
        if "." in parts[0]:
            continue
        job_line = parts
        break

    if not job_line:
        raise HTTPException(status_code=404, detail="Job not found or does not belong to you")

    job_id_found, name, state, partition, cpus, alloc_tres, elapsed, exitcode = job_line[:8]
    gpus    = _parse_gpus_from_tres(alloc_tres)
    mem_str = _parse_mem_from_tres(alloc_tres)

    gpu_eff = cpu_eff = mem_eff = None
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(f"{GSEFF_URL}/api/me", headers={OOD_USER_HDR: user})
        if resp.status_code == 200:
            for j in resp.json().get("jobs", []):
                if str(j.get("job_id")) == str(job_id_found):
                    gpu_eff = j.get("gpu_eff")
                    cpu_eff = j.get("cpu_eff")
                    mem_eff = j.get("mem_eff")
                    break
    except Exception:
        pass

    cfg = _load_config()
    rec = _recommend_from_usage(
        cfg=cfg, gpus=gpus, gpu_eff=gpu_eff, cpu_eff=cpu_eff, mem_eff=mem_eff,
        prev_partition=partition, exitcode=exitcode, elapsed=elapsed,
    )

    return {
        "job_id": job_id_found, "name": name, "state": state,
        "prev_partition": partition, "gpus": gpus, "cpus": cpus,
        "mem": mem_str, "elapsed": elapsed, "exitcode": exitcode,
        "gpu_eff": gpu_eff, "cpu_eff": cpu_eff, "mem_eff": mem_eff,
        "recommendation": rec,
    }


# ── Guided questions endpoint ─────────────────────────────────────────────────

class GuidedRequest(BaseModel):
    task:        str  = "training"
    model_size:  str  = "medium"
    high_vram:   bool = False
    interactive: bool = False
    course:      bool = False
    urgent:      bool = False

@router.post("/guided")
async def recommend_guided(req: GuidedRequest):
    cfg         = _load_config()
    parts_cfg   = _partition_cfg(cfg)

    if req.course and not req.high_vram:
        return {
            "recommendations": [_make_rec("course", parts_cfg,
                "Use the course partition for all coursework unless your job requires more than 24 GB VRAM "
                "or your TA/instructor explicitly directs otherwise.", "primary")],
            "warning": None, "notes": [],
        }

    if req.task == "preprocessing":
        return {
            "recommendations": [], "warning": None,
            "notes": ["Data preprocessing typically does not require a GPU. "
                      "Consider running on the shared CPU partition."],
        }

    notes      = []
    candidates = []

    high_vram_parts    = [p for p, c in parts_cfg.items() if c.get("tier") == "high_vram"]
    moderate_parts     = [p for p, c in parts_cfg.items() if c.get("tier") == "moderate" and p not in ("course",)]
    low_parts          = [p for p, c in parts_cfg.items() if c.get("tier") == "low" and p not in ("course", "shared")]

    if req.high_vram:
        candidates = high_vram_parts
        notes.append(f"You indicated this job needs more than 24 GB VRAM. Only {', '.join(high_vram_parts)} qualifies.")
    else:
        if req.model_size == "small":
            candidates = low_parts + moderate_parts[:2]
            notes.append("Small models typically fit within 11–24 GB VRAM.")
        elif req.model_size == "large":
            candidates = moderate_parts
            notes.append("Large models may be tight on 24 GB VRAM. If you hit CUDA OOM, try a high-VRAM partition.")
        elif req.model_size == "unknown":
            candidates = moderate_parts
            notes.append(
                "We're not sure of your VRAM requirements, so we're suggesting moderate-tier partitions (24 GB). "
                "If your job crashes with a CUDA out-of-memory error, switch to a high-VRAM partition. "
                "If it runs fine with low GPU utilization, a lower-tier partition may be more available."
            )
        else:
            candidates = moderate_parts

    if req.task == "inference":
        notes.append("Inference jobs are usually shorter — consider requesting less walltime.")
    if req.task == "finetuning":
        notes.append("Fine-tuning usually needs 1 GPU unless you are using DataParallel explicitly.")
    if req.interactive:
        notes.append("For interactive sessions, prefer partitions with lower queue pressure (check the Partitions tab).")

    recs = [_make_rec(p, parts_cfg, _guided_rationale(p, parts_cfg, req), "primary" if i == 0 else "secondary")
            for i, p in enumerate(candidates[:3])]

    return {"recommendations": recs, "warning": None, "notes": notes}


# ── helpers ───────────────────────────────────────────────────────────────────

def _parse_gpus_from_tres(alloc_tres: str) -> int:
    for part in alloc_tres.split(","):
        if "gres/gpu=" in part:
            try:
                return int(part.split("=")[1])
            except (IndexError, ValueError):
                pass
    return 0


def _parse_mem_from_tres(alloc_tres: str) -> str:
    for part in alloc_tres.split(","):
        if part.startswith("mem="):
            return part.split("=")[1]
    return "—"


def _make_rec(partition: str, parts_cfg: dict, rationale: str, rank: str) -> dict:
    pcfg = parts_cfg.get(partition, {})
    return {
        "partition": partition,
        "vram_gb":   pcfg.get("vram_gb", 0),
        "rationale": rationale,
        "rank":      rank,
    }


def _recommend_from_usage(cfg, gpus, gpu_eff, cpu_eff, mem_eff,
                           prev_partition, exitcode, elapsed) -> dict:
    parts_cfg   = _partition_cfg(cfg)
    adjustments = []
    warnings    = []
    partition   = prev_partition

    recommended_gpus = gpus
    if gpus >= 2 and gpu_eff is not None and gpu_eff < 30:
        recommended_gpus = 1
        adjustments.append(
            f"Your previous job requested {gpus} GPUs but GPU utilization was only {gpu_eff:.0f}%. "
            "1 GPU should be sufficient — this will also improve your queue wait time."
        )
    elif gpus >= 2 and gpu_eff is None:
        adjustments.append(
            f"Your previous job requested {gpus} GPUs. "
            "Verify your training loop actually uses multiple GPUs (DataParallel / DistributedDataParallel)."
        )

    prev_tier = parts_cfg.get(prev_partition, {}).get("tier", "")
    if prev_tier == "high_vram":
        if gpu_eff is None or gpu_eff < 30:
            moderate = [p for p, c in parts_cfg.items() if c.get("tier") == "moderate" and p != "course"]
            if moderate:
                partition = moderate[0]
                adjustments.append(
                    f"Your previous job ran on {prev_partition} ({parts_cfg[prev_partition].get('vram_gb',0)} GB VRAM). "
                    f"Unless your job requires more than 24 GB VRAM, {partition} will schedule faster."
                )

    if exitcode and exitcode != "0:0":
        warnings.append(
            f"Your previous job exited with code {exitcode}. "
            "If the failure was due to CUDA out-of-memory, you may need a higher-VRAM partition. "
            "Otherwise check your job logs before resubmitting."
        )

    if cpu_eff is not None and cpu_eff < 20:
        adjustments.append(f"CPU efficiency was {cpu_eff:.0f}%. Consider reducing --cpus-per-task.")

    if mem_eff is not None and mem_eff < 20:
        adjustments.append(f"Only {mem_eff:.0f}% of requested memory was used. Reducing --mem will help scheduling.")

    eff_str   = f"{gpu_eff:.0f}%" if gpu_eff is not None else "not measured"
    rationale = (
        f"Based on your previous job: {recommended_gpus} GPU(s), "
        f"GPU utilization {eff_str}, partition {partition}."
    )
    if not adjustments and not warnings:
        rationale += " No changes recommended — your previous resource request looks appropriate."

    return {
        "partition": partition, "recommended_gpus": recommended_gpus,
        "rationale": rationale, "adjustments": adjustments, "warnings": warnings,
    }


def _guided_rationale(partition: str, parts_cfg: dict, req: GuidedRequest) -> str:
    pcfg  = parts_cfg.get(partition, {})
    vram  = pcfg.get("vram_gb", 0)
    notes_str = pcfg.get("notes", f"{partition} — {vram} GB VRAM.")
    lines = [notes_str]

    if req.task == "training" and req.model_size == "large":
        lines.append("Large model training can exceed 24 GB VRAM at larger batch sizes. "
                     "If you hit CUDA OOM, switch to a high-VRAM partition.")
    elif req.task == "training" and req.model_size == "unknown":
        lines.append("Start with 1 GPU and monitor VRAM usage with nvidia-smi. "
                     "Scale up only if you hit memory limits.")
    elif req.task == "finetuning":
        lines.append("Fine-tuning typically needs less VRAM. 1 GPU is usually sufficient.")
    elif req.task == "inference":
        lines.append("Inference jobs are usually short — request only the walltime you need.")

    return " ".join(lines)
