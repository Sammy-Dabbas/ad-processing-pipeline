import asyncio
import json
import os
from typing import AsyncIterator, Optional
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
# Consumer (local mode)  reads JSONL events from data/raw.jsonl (or latest rotated file),
# de-duplicates by revision id, optionally normalizes/enriches, and writes JSONL to data/processed.jsonl.

# In container, data is always at /app/data
DATA_DIR = Path("/app/data")

# Default local file paths (rotation-aware helpers below may override RAW_FILE dynamically)
PROCESSED_FILE = DATA_DIR / "processed.jsonl"
SEEN_REV_IDS = DATA_DIR / "consumer_seen_rev_ids.jsonl"


def should_rotate(file_path: Path, max_megabytes: int, max_lines: int) -> bool:
    try: 
        file_size = os.path.getsize(file_path)
        if file_size >= max_megabytes * 1024 * 1024: 
            return True
        else: 
            return False 
    except Exception as e: 
        print(e)
        return False

def next_rotation_path(base_dir: Path, prefix: str) -> Path:
    """
    Build a new timestamped file path for the next JSONL file.

    Example: data/raw-YYYYMMDD-HHMMSS.jsonl
    """
    # TODO: format datetime.utcnow() and return base_dir / f"{prefix}-{stamp}.jsonl"
    now = datetime.utcnow()
    return base_dir / f"{prefix}-{now.strftime('%Y%m%d-%H%M%S')}.jsonl"
def parse_json_line(line: str) -> Optional[dict]:
    text = line.rstrip("\r\n")
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None





def extract_rev_id(event: dict) -> Optional[str]:
    rev_new = event.get("revision", {}).get("new")
    if isinstance(rev_new, int):
        return str(rev_new)
    if isinstance(rev_new, str) and rev_new.isdigit():
        return rev_new

    rev_id = event.get("rev_id")
    if isinstance(rev_id, int):
        return str(rev_id)
    if isinstance(rev_id, str) and rev_id.isdigit():
        return rev_id

    rcid = event.get("id")
    if isinstance(rcid, int):
        return str(rcid)
    if isinstance(rcid, str) and rcid.isdigit():
        return rcid

    return None
    




def build_processed_record(event: dict) -> Optional[dict]:
    rev_id = extract_rev_id(event)
    if rev_id is None:
        return None

    ts_iso = event.get("meta", {}).get("dt")
    if ts_iso is None:
        ts_epoch = event.get("timestamp")
        if isinstance(ts_epoch, (int, float)):
            ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat()

    length = event.get("length") or {}
    len_old = length.get("old") if isinstance(length.get("old"), int) else None
    len_new = length.get("new") if isinstance(length.get("new"), int) else None
    delta = (len_new - len_old) if (isinstance(len_new, int) and isinstance(len_old, int)) else None

    return {
        "rev_id": str(rev_id),
        "page_id": event.get("page_id"),
        "title": event.get("title"),
        "wiki": event.get("wiki"),
        "namespace": event.get("namespace"),
        "comment": event.get("comment"), 
        "page_key": f"{event.get('wiki', '')}:{event.get('namespace', '')}:{event.get('title', '')}",
        "user": event.get("user"),
        "timestamp": ts_iso,
        "bot": event.get("bot"),
        "minor": event.get("minor"),
        "len_old": len_old,
        "len_new": len_new,
        "delta": delta,
    }
   

def serialize_jsonl(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

def append_jsonl(file_path: Path, json_text: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(json_text + "\n")


async def tail_jsonl(file_path: Path) -> AsyncIterator[str]:
    base_dir = file_path.parent
    current_path = find_latest_raw_path(base_dir)
    delay = 0.2
    no_progress_count = 0

    while True:
        try:
            with open(current_path, "r", encoding="utf-8") as f:
                f.seek(0, os.SEEK_END)  # or 0 to replay from start
                while True:
                    offset = f.tell()
                    size = os.stat(current_path).st_size
                    if offset == size:
                        await asyncio.sleep(delay)
                        continue
                    if offset > size:  # truncated
                        break

                    line = await asyncio.to_thread(f.readline)
                    if line:
                        no_progress_count = 0
                        yield line
                    else:
                        no_progress_count += 1
                        await asyncio.sleep(delay)
                        if no_progress_count > 10:
                            next_path = find_latest_raw_path(base_dir)
                            if next_path != current_path:
                                current_path = next_path
                                no_progress_count = 0
                                break
        except FileNotFoundError:
            await asyncio.sleep(delay)
            current_path = find_latest_raw_path(base_dir)
            continue
        except asyncio.CancelledError:
            return
        


def find_latest_raw_path(base_dir: Path) -> Path:
    raw_path = base_dir / "raw.jsonl"
    base_dir.mkdir(parents=True, exist_ok=True)

    latest = max(
        base_dir.glob("raw-*.jsonl"),
        key=lambda p: p.name,  # YYYYMMDD-HHMMSS sorts lexically
        default=None,
    )
    return latest or raw_path
    


async def run_consumer() -> None:
    print("running consumer")
    deduped = 0
    errors = 0
    written = 0

    PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    SEEN_REV_IDS.parent.mkdir(parents=True, exist_ok=True)

    input_path = find_latest_raw_path(DATA_DIR)
    seen_ids: set[str] = set()
    
    # Track current processed file for rotation
    current_processed_file = PROCESSED_FILE

    try:
        async for line in tail_jsonl(input_path):
            # Check rotation every 1000 events (but not on first event)
            if written % 1000 == 0 and written > 0:
                if should_rotate(current_processed_file, 128, 1_000_000):
                    # Rotate: rename current file with timestamp
                    rotated_file = next_rotation_path(DATA_DIR, "processed")
                    if current_processed_file.exists():
                        current_processed_file.rename(rotated_file)
                        print(f"Rotated processed file to: {rotated_file}")
                    # Reset to default name for new file
                    current_processed_file = PROCESSED_FILE
            event = parse_json_line(line)
            if event is None:
                errors += 1
                continue

            rev_id = extract_rev_id(event)
            if rev_id is None:
                errors += 1
                continue

            if rev_id in seen_ids:
                deduped += 1
                continue

            record = build_processed_record(event)
            if record is None:
                errors += 1
                continue

            text = serialize_jsonl(record)
            append_jsonl(current_processed_file, text)

            seen_ids.add(rev_id)
            written += 1
            
            # Log progress every 5000 events
            if written % 5000 == 0:
                print(f"Consumer processed {written} events (deduped: {deduped}, errors: {errors})")
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"Consumer error: {e}")
        return


async def run_consumer_forever() -> None:
    """
    Run the consumer with automatic restart on errors.
    This ensures the consumer keeps running even if there are transient failures.
    """
    while True:
        try:
            print("Starting consumer...")
            await run_consumer()
        except KeyboardInterrupt:
            print("Consumer stopped by user")
            break
        except Exception as e:
            print(f"Consumer crashed with error: {e}")
            print("Restarting consumer in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_consumer_forever())


