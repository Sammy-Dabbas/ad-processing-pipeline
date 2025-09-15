import asyncio
import aiohttp
import os
from typing import AsyncIterator, Optional
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from random import random 
load_dotenv()

# Endpoint for Wikimedia RecentChanges SSE stream
WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# In container, data is always at /app/data
base_dir = Path("/app/data")
base_dir.mkdir(parents=True, exist_ok=True)
file_path = base_dir / "raw.jsonl"
    
def build_headers(app_name: str, contact: str) -> dict:
    """
    Build HTTP headers for Wikimedia EventStreams.

    Required:
    - User-Agent: Descriptive string with a way to contact you.
      Example: "FriendlyStreamerBot/0.1 (you@example.com)".
    - Accept: Must be "text/event-stream" for SSE.

    Return a dict you can pass to aiohttp (session or per-request).
    
    """
    headers = {"User-Agent": f"{app_name} ({contact})", "Accept": "text/event-stream"}
    return headers 
   
app_name = os.getenv("HEADER_USER")
contact = os.getenv("HEADER_EMAIL")
base_headers = build_headers(app_name, contact); 


async def open_stream(session: aiohttp.ClientSession, url: str, headers: dict) -> aiohttp.ClientResponse:
    """
    Open a long-lived GET request to the SSE endpoint.

    Expectations:
    - Use session.get(url, headers=headers).
    - Verify response.status == 200.
    - Verify response.headers["Content-Type"].startswith("text/event-stream").

    Return the aiohttp response object so caller can iterate over response.content.
    """
     
    resp = await session.get(url, headers=headers)
    if resp.status != 200:
        await resp.release()
        raise Exception(f"Bad status: {resp.status}")
    elif resp.content_type != "text/event-stream":
        await resp.release()
        raise Exception(f"Bad Content-Type: {resp.content_type}")
    else:
        return resp  # keep open; caller uses `async with`


def is_sse_comment(line: str) -> bool:
    """
    Return True if the line is an SSE comment/heartbeat line.
    - SSE comments start with ':' and should be ignored for data collection.
    """
    # TODO: return line.startswith(":")
    if line.startswith(":"):
        return True
    
    return False 


def is_sse_event_terminator(line: str) -> bool:
    """
    Return True if the line marks the end of one SSE event.
    - In SSE, a blank line separates events.
    """
    return line == ""
    


def extract_data_field(line: str) -> Optional[str]:
    """
    If the line starts with "data:", return the value after the prefix, else None.
    - An SSE event can have multiple data: lines; concatenate them with "\n".
    """
    # TODO: if line.startswith("data:"): return line[len("data:"):].lstrip()
    if line.startswith("data:"): return line[len("data:"):].lstrip()
    
def extract_id_field(line: str) -> Optional[str]: 
    if line.startswith("id:"): return line[len("id:"):].lstrip()
    return None
def is_sse_id(line:str) -> bool: 
    return line.startswith("id:")
async def read_sse_events(content) -> AsyncIterator[str]:
    """
    Yield complete SSE event payloads as raw JSON strings.

    Algorithm (no network code here beyond iteration):
    - Call open_stream(...) to get a response.
    - Iterate over response.content (async for raw_line in response.content).
    - Decode bytes to UTF-8 and strip the trailing '\n' (keep other whitespace).
    - Ignore comment/heartbeat lines (is_sse_comment).
    - Collect data lines using extract_data_field(line) into a buffer list.
    - When you see a blank line (is_sse_event_terminator), join the buffer with '\n' and yield it.
    - Clear the buffer and continue.
    - returns (Optional[str], str)
    Notes:
    - This function should not parse JSON; it only yields the raw JSON string.
    - Wrap the loop in try/except and reconnect with backoff in the caller.
    """
    
    buffer = []
    event_id = None
    async for raw_line in content: 
        raw_line = raw_line.decode("utf-8").rstrip("\n")
        if is_sse_comment(raw_line): continue
        if is_sse_id(raw_line): 
            event_id = extract_id_field(raw_line)
            continue
        if is_sse_event_terminator(raw_line): 
            if buffer: yield (event_id, "\n".join(buffer))
            buffer.clear()
            event_id = None
            continue
        maybe_data = extract_data_field(raw_line)
        if maybe_data is not None: buffer.append(maybe_data)
  


def append_jsonl(file_path: Path, json_text: str) -> None:
    """
    Append one JSON object (already a compact JSON string) to a JSONL file.

    Requirements:
    - Ensure the parent directory exists.
    - Open in append mode with UTF-8.
    - Write json_text + "\n" (exactly one newline).
    - Optionally flush after each write while developing.
    """
    
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "a", encoding="utf-8") as f: 
            f.write(json_text + "\n")
           
    return 


def should_rotate(file_path: Path, max_megabytes: int, max_lines: int) -> bool:
    """
    Decide if the current JSONL file should be rotated.

    Heuristics:
    - If file size exceeds max_megabytes, return True.
    - Or if approximate line count exceeds max_lines, return True.

    Keep it simple early; exact counting is optional.
    """

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
   


async def run_reader() -> None:
    """
    Orchestrate the reader:
    - Build headers with build_headers(...).
    - Create aiohttp.ClientSession.
    - Loop reading SSE events via read_sse_events(...).
    - For each event, append to JSONL via append_jsonl(...).
    - event_id: Optional[str], payload: str
    - Periodically consider rotation via should_rotate(...); if needed, pick next path.
    - On errors, log and back off (1s, 2s, 4s, ... up to ~30s) before reconnecting.

    Keep simple counters (received, written, errors) and log progress every N events.
    """
    # TODO: implement orchestration and reconnection/backoff

    current_file_path = file_path
    retry_count = 0 
    timeout = aiohttp.ClientTimeout(total=None, sock_read=None, connect=10, sock_connect=10)
    last_event_id = None
    request_headers = base_headers
    async with aiohttp.ClientSession(timeout=timeout) as session: 
            while True: 
                if last_event_id is not None: 
                    request_headers = {**base_headers, "Last-Event-ID": last_event_id}
                else: 
                    request_headers = base_headers
                try: 
                    async with await open_stream(session, WIKIMEDIA_URL, request_headers) as resp:
                        append_count = 0 
                        retry_count = 0 
                        async for event_json in read_sse_events(resp.content):
                                event_id, payload = event_json
                               
                                append_jsonl(current_file_path, payload)
                                append_count += 1
                                if event_id is not None: 
                                    last_event_id  = event_id
                                if append_count % 100 == 0: 
                                    print(f"appended {append_count} events")
                                    if should_rotate(current_file_path, 128, 1_000_000): 
                                        current_file_path = next_rotation_path(base_dir, "raw")
                except asyncio.CancelledError:
                        print("reader cancelled")
                        break
                except Exception as e: 
                    #retry exponential backoff
                    delay = min(30, 1 * (2 ** retry_count))  # cap at 30s
                    jitter = delay * 0.2
                    jitter_delay = delay - jitter + random() * 2 * jitter
                    print(f"retrying in {jitter_delay} seconds") 
                    await asyncio.sleep(jitter_delay)  
                    retry_count += 1
                    print(e)
                 
                
     

# Entry point (leave commented until you implement run_reader)
if __name__ == "__main__":
     asyncio.run(run_reader())



