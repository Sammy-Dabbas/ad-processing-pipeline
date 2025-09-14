from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
def health_check():
    """
    TODO: Return service status and lightweight diagnostics.
    For now: { "status": "ok" }
    Later: include versions, counters, uptime.
    """
    return {"status": "ok"}


