from fastapi import APIRouter, HTTPException, Query
from pathlib import Path
from typing import List
import os
import json
from collections import deque 

# TODO: If you move shared constants, import DATA_DIR/PROCESSED_FILE from a shared module.
# In container, data is always at /app/data
DATA_DIR = Path("/app/data")
PROCESSED_FILE = DATA_DIR / "processed.jsonl"

router = APIRouter(prefix="/events", tags=["events"])


@router.get("/latest")
def latest_events(limit: int = Query(50, ge=1, le=500)) -> List[dict]:
    encoding = "utf-8"
    path = PROCESSED_FILE
    if not PROCESSED_FILE.exists(): 
        return []
    with open(path, "r", encoding=encoding, newline="") as f:
        lines = list(deque(f, maxlen=limit))
    out: List[dict] = []
    for ln in lines: 
        ln = ln.rstrip("\r\n")
        if not ln: 
            continue
        try: 
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out 



@router.get("/by_page/{page_id}")
def events_by_page(
    page_id: int,
    limit: int = Query(50, ge=1, le=500),
    wiki: str | None = None,
    namespace: int | None = None,
    title: str | None = None,
) -> List[dict]:
    if not PROCESSED_FILE.exists():
        return []

    scan_window = max(limit * 20, 1000)
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        lines = list(deque(f, maxlen=scan_window))

    expected_key = (
        f"{wiki}:{namespace}:{title}"
        if wiki is not None and namespace is not None and title is not None
        else None
    )

    results: List[dict] = []
    for ln in reversed(lines):
        ln = ln.rstrip("\r\n")
        if not ln:
            continue
        try:
            item = json.loads(ln)
        except json.JSONDecodeError:
            continue

        if item.get("page_id") == page_id:
            results.append(item)
        elif expected_key is not None and item.get("page_key") == expected_key:
            results.append(item)

        if len(results) >= limit:
            break

    return results