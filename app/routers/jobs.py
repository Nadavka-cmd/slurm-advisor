from fastapi import APIRouter, Request, HTTPException
import subprocess
from datetime import datetime, timedelta
import json
import os
import yaml

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

SLURM_BIN   = "/opt/slurm/bin"
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "slurm_advisor_config.yaml")


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _get_admin_group() -> str:
    return _load_config().get("efficiency", {}).get("admin_group", "hpc_admins")


def get_remote_user(request: Request) -> str:
    return (
        request.headers.get("X-Remote-User")
        or request.headers.get("REMOTE_USER")
        or os.environ.get("REMOTE_USER", "unknown")
    )


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def _is_admin(username: str) -> bool:
    try:
        admin_group = _get_admin_group()
        r = subprocess.run(["id", username], capture_output=True, text=True, timeout=5)
        return admin_group in r.stdout
    except Exception:
        return False


@router.get("/me/role")
async def my_role(request: Request):
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")
    admin = _is_admin(user)
    return {
        "user":     user,
        "is_admin": admin,
        "role":     "Admin" if admin else "Researcher",
    }


@router.get("/active")
async def active_jobs(request: Request):
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    try:
        out = run([
            f"{SLURM_BIN}/squeue",
            "--user", user,
            "--format", "%i|%j|%T|%P|%D|%C|%10b|%M|%l|%R",
            "--noheader",
        ])
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    jobs = []
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) < 10:
            continue
        jobs.append({
            "job_id":    parts[0].strip(),
            "name":      parts[1].strip(),
            "state":     parts[2].strip(),
            "partition": parts[3].strip(),
            "nodes":     parts[4].strip(),
            "cpus":      parts[5].strip(),
            "gpus":      _parse_gpus(parts[6].strip()),
            "runtime":   parts[7].strip(),
            "timelimit": parts[8].strip(),
            "reason":    parts[9].strip(),
        })
    return {"user": user, "jobs": jobs}


@router.get("/recent")
async def recent_jobs(request: Request, limit: int = 10):
    """Recently completed/failed jobs via sacct."""
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    try:
        out = run([
            f"{SLURM_BIN}/sacct",
            "--user", user,
            "--format", "JobID,JobName,State,Partition,AllocCPUS,AllocTRES,Elapsed,Timelimit,ExitCode,Submit",
            "--noheader",
            "--parsable2",
            "--starttime", (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        ])
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    jobs = []
    seen = set()
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) < 9:
            continue
        job_id = parts[0]
        # skip sub-steps (e.g. 1234.batch)
        if "." in job_id:
            continue
        if job_id in seen:
            continue
        seen.add(job_id)
        jobs.append({
            "job_id":     job_id,
            "name":       parts[1],
            "state":      parts[2],
            "partition":  parts[3],
            "cpus":       parts[4],
            "alloc_tres": parts[5],
            "elapsed":    parts[6],
            "timelimit":  parts[7],
            "exitcode":   parts[8],
            "submit":     parts[9] if len(parts) > 9 else "",
            "gpus":       _parse_gpus_from_tres(parts[5]),
        })
        if len(jobs) >= limit:
            break

    return {"user": user, "jobs": jobs}


@router.get("/pending-reason/{job_id}")
async def pending_reason(job_id: str, request: Request):
    """Translate a pending job's Slurm reason into plain language."""
    user = get_remote_user(request)
    if user == "unknown":
        raise HTTPException(status_code=401, detail="No authenticated user")

    try:
        out = run([
            f"{SLURM_BIN}/squeue",
            "--job", job_id,
            "--Format", "JobID,State,Reason,Partition,tres-per-node,TimeLimit",
            "--noheader",
        ])
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not out:
        raise HTTPException(status_code=404, detail="Job not found")

    parts = out.split()
    if len(parts) < 4:
        raise HTTPException(status_code=500, detail="Unexpected squeue output")

    # Verify job belongs to requesting user
    try:
        owner_out = run([
            f"{SLURM_BIN}/squeue",
            "--job", job_id,
            "--Format", "User",
            "--noheader",
        ])
    except RuntimeError:
        raise HTTPException(status_code=404, detail="Job not found")

    owner_parts = owner_out.strip().split()
    if owner_parts and owner_parts[0] != user:
        raise HTTPException(status_code=403, detail="Not your job")

    raw_reason = parts[2] if len(parts) > 2 else "Unknown"
    explained  = _explain_reason(raw_reason)
    return {
        "job_id":      parts[0],
        "state":       parts[1],
        "raw_reason":  raw_reason,
        "explanation": explained["explanation"],
        "suggestion":  explained["suggestion"],
        "fix_command": explained["fix_command"].replace("{job_id}", parts[0]) if explained["fix_command"] else None,
        "partition":   parts[3] if len(parts) > 3 else "",
    }


# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_gpus(tres_per_node: str) -> str:
    """Parse GPU count from squeue tres-per-node field like 'gres:gpu:2'."""
    if not tres_per_node or tres_per_node == "N/A":
        return "0"
    for part in tres_per_node.split(","):
        if "gpu" in part.lower():
            segments = part.split(":")
            return segments[-1] if segments else "?"
    return "0"


def _parse_gpus_from_tres(alloc_tres: str) -> str:
    """Parse GPU count from sacct AllocTRES field like 'cpu=4,gres/gpu=1,mem=8G'."""
    if not alloc_tres:
        return "0"
    for part in alloc_tres.split(","):
        if "gpu" in part.lower():
            kv = part.split("=")
            return kv[-1] if len(kv) == 2 else "?"
    return "0"


# Each entry: (explanation, suggestion, fix_command or None)
_REASON_MAP = {
    "Resources": (
        "Waiting for enough GPUs, CPUs, or memory to become available on this partition.",
        "This is normal — your job will start automatically once resources free up. "
        "Check the Partitions tab to see which partition has lower queue pressure.",
        None,
    ),
    "Priority": (
        "Higher-priority jobs are ahead of yours in the queue.",
        "Your job will run once they clear. Jobs gain priority the longer they wait — no action needed.",
        None,
    ),
    "QOSMaxWallDurationPerJobLimit": (
        "Your requested runtime exceeds the maximum allowed walltime for your QoS policy (24 hours).",
        "Reduce your --time request to 24 hours or less and resubmit.",
        "#SBATCH --time=24:00:00",
    ),
    "QOSMaxGRESPerUser": (
        "You have reached the maximum number of GPUs allowed simultaneously under your QoS (2 GPUs).",
        "Wait for one of your running GPU jobs to finish before submitting more.",
        None,
    ),
    "QOSMaxJobsPerUserLimit": (
        "You have hit the maximum number of simultaneous jobs allowed for your account (2 jobs).",
        "Wait for one of your running jobs to finish before submitting more.",
        None,
    ),
    "PartitionTimeLimit": (
        "Your requested walltime exceeds this partition's maximum allowed time.",
        "Reduce --time to fit within the partition limit, or switch to a partition with a longer limit.",
        "#SBATCH --time=24:00:00",
    ),
    "PartitionNodeLimit": (
        "You requested more nodes than this partition allows per job.",
        "Reduce --nodes to 1 — most GPU workloads on this cluster run on a single node.",
        "#SBATCH --nodes=1",
    ),
    "AssocMaxJobsLimit": (
        "Your account has reached its maximum concurrent job limit.",
        "Wait for one of your running jobs to finish before submitting more.",
        None,
    ),
    "ReqNodeNotAvail": (
        "The specific node(s) you requested are currently unavailable — drained, reserved, or down.",
        "Remove --nodelist from your job script and let Slurm choose the node automatically.",
        "# Remove from your script: #SBATCH --nodelist=<nodename>",
    ),
    "Reservation": (
        "The partition or nodes you are targeting are currently under a maintenance reservation.",
        "Try a different partition or wait for the reservation to end. Contact the HPC team for details.",
        None,
    ),
    "JobHeldUser": (
        "This job was placed on hold by you.",
        "Release it when you are ready to run:",
        "scontrol release {job_id}",
    ),
    "JobHeldAdmin": (
        "This job was placed on hold by an administrator.",
        "Contact the HPC team to find out why and request a release.",
        None,
    ),
    "BeginTime": (
        "This job is scheduled to start at a future time you specified with --begin.",
        "If you want it to run sooner, cancel and resubmit without --begin.",
        "scancel {job_id}",
    ),
    "NodeDown": (
        "A node required for your job is currently down.",
        "The job will start automatically once the node recovers. If it persists, contact the HPC team.",
        None,
    ),
    "None": (
        "No pending reason reported — the job may be about to start.",
        "No action needed.",
        None,
    ),
}


def _explain_reason(reason: str) -> dict:
    entry = _REASON_MAP.get(reason)
    if entry:
        explanation, suggestion, fix_command = entry
        return {
            "explanation": explanation,
            "suggestion":  suggestion,
            "fix_command": fix_command,
        }
    return {
        "explanation": f"Slurm reported reason: '{reason}'. This may be a temporary condition.",
        "suggestion":  "If the job remains pending for a long time, contact the HPC team.",
        "fix_command": None,
    }
