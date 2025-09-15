from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pathlib import Path
import json
import asyncio
from typing import List
import time
from collections import deque

router = APIRouter()

# Active WebSocket connections
active_connections: List[WebSocket] = []

# In container, data is always at /app/data
DATA_DIR = Path("/app/data")
PROCESSED_FILE = DATA_DIR / "processed.jsonl"

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except:
                # Remove stale connections
                self.active_connections.remove(connection)

manager = ConnectionManager()

@router.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial stats
        stats = get_current_stats()
        await websocket.send_text(json.dumps({
            "type": "stats",
            "data": stats
        }))
        
        # Keep connection alive and send periodic updates
        while True:
            # Send recent events every 2 seconds
            recent_events = get_recent_events(5)
            if recent_events:
                await websocket.send_text(json.dumps({
                    "type": "events",
                    "data": recent_events
                }))
            
            # Send updated stats every 5 seconds
            stats = get_current_stats()
            await websocket.send_text(json.dumps({
                "type": "stats", 
                "data": stats
            }))
            
            await asyncio.sleep(2)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)

def get_recent_events(limit: int = 10) -> List[dict]:
    """Get the most recent events"""
    if not PROCESSED_FILE.exists():
        return []
    
    try:
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            lines = list(deque(f, maxlen=limit))
        
        events = []
        for line in lines:
            line = line.rstrip("\r\n")
            if not line:
                continue
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError:
                continue
        
        return events[-limit:]  # Return most recent first
    except:
        return []

def get_current_stats() -> dict:
    """Get current telemetry stats"""
    if not PROCESSED_FILE.exists():
        return {
            "total_events": 0,
            "events_per_minute": 0,
            "top_wikis": [],
            "bot_percentage": 0,
            "avg_edit_size": 0
        }
    
    try:
        # Get last 100 events for stats
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            lines = list(deque(f, maxlen=100))
        
        events = []
        for line in lines:
            line = line.rstrip("\r\n") 
            if not line:
                continue
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError:
                continue
        
        if not events:
            return {"total_events": 0, "events_per_minute": 0, "top_wikis": [], "bot_percentage": 0, "avg_edit_size": 0}
        
        # Calculate stats
        total_events = len(events)
        bot_count = sum(1 for e in events if e.get("bot"))
        bot_percentage = (bot_count / total_events * 100) if total_events > 0 else 0
        
        # Wiki distribution
        wiki_counts = {}
        edit_sizes = []
        
        for event in events:
            wiki = event.get("wiki", "unknown")
            wiki_counts[wiki] = wiki_counts.get(wiki, 0) + 1
            
            delta = event.get("delta")
            if isinstance(delta, (int, float)):
                edit_sizes.append(abs(delta))
        
        top_wikis = sorted(wiki_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        avg_edit_size = sum(edit_sizes) / len(edit_sizes) if edit_sizes else 0
        
        return {
            "total_events": total_events,
            "events_per_minute": total_events * 0.6,  # Rough estimate
            "top_wikis": [{"name": w[0], "count": w[1]} for w in top_wikis],
            "bot_percentage": round(bot_percentage, 1),
            "avg_edit_size": round(avg_edit_size, 1)
        }
    except:
        return {"total_events": 0, "events_per_minute": 0, "top_wikis": [], "bot_percentage": 0, "avg_edit_size": 0}
