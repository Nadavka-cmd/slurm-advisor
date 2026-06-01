from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os

from app.routers import jobs, partitions, advisor, efficiency

app = FastAPI(root_path=os.environ.get("SCRIPT_NAME", ""))

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

app.include_router(jobs.router)
app.include_router(partitions.router)
app.include_router(advisor.router)
app.include_router(efficiency.router)


def get_remote_user(request: Request) -> str:
    """Extract authenticated username from OOD-injected header."""
    return (
        request.headers.get("X-Remote-User")
        or request.headers.get("REMOTE_USER")
        or os.environ.get("REMOTE_USER", "unknown")
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_remote_user(request)
    root_path = request.scope.get("root_path", "").rstrip("/")
    return templates.TemplateResponse("index.html", {
        "request": request,
        "user": user,
        "root_path": root_path,
    })
