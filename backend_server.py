import asyncio
import json
import logging
import os
import random
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timezone

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException, Query, Header
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Backend] %(message)s")

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="RRTS IronSight Sentinel Operational Backend Platform",
    description="Operational Information Platform for RRTS Autonomous Rail Inspection",
    version="1.2.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== OPERATIONAL DATA STORE ====================
class OperationalDataStore:
    def __init__(self):
        self.active_line = "LINE_ALPHA"
        self.active_run_id = "RRTS_001_20260810"
        self.seq_counter = 300
        self.kiosk_mode_enabled = True
        
        self.telemetry = {
            "robot_id": "ROBOT-01",
            "line_id": "LINE_ALPHA",
            "section": "SECT-04",
            "chainage": "12+435",
            "chainage_meters": 12435.0,
            "speed_ms": 0.85,
            "sys_temp_c": 42.1,
            "battery_pct": 98.5,
            "telemetry_latency_ms": 124,
            "status": "ACTIVE",
            "system_state": "NOMINAL",
            "last_updated": datetime.now(timezone.utc).isoformat()
        }

        self.runs = [
            {"id": "RRTS_001_20260810", "label": "RUN 04 (CUR)", "date": "TODAY", "anomalies": 12, "scan_type": "CURRENT", "status": "active"},
            {"id": "RRTS_001_20260803", "label": "RUN 03", "date": "-7 DAYS", "anomalies": 10, "scan_type": "ROUTINE", "status": "completed"},
            {"id": "RRTS_001_20260727", "label": "RUN 02", "date": "-14 DAYS", "anomalies": 8, "scan_type": "ROUTINE", "status": "completed"},
            {"id": "RRTS_001_20260710", "label": "RUN 01 (BASE)", "date": "-30 DAYS", "anomalies": 2, "scan_type": "BASELINE", "status": "completed"}
        ]

        self.defects = [
            {
                "defect_id": "CR-042",
                "finding_id": "fnd_8a4b5c6d-7e8f-9a0b-1c2d-3e4f5a6b7c8d",
                "event_id": "evt_7f3a9b2c-4d1e-4a8f-9c3d-2e5f6a7b8c9d",
                "inspection_run_id": "RRTS_001_20260810",
                "robot_id": "ROBOT-01",
                "sensor_id": "CAM_FRONT_01",
                "model_id": "rail_defect_v3.2.1",
                "model_version": "3.2.1",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "location": {
                    "chainage": 12450.0,
                    "chainage_str": "12+450",
                    "latitude": 28.6139,
                    "longitude": 77.2090,
                    "track_id": "LINE_ALPHA_SECT_04"
                },
                "asset": {
                    "asset_id": "PILLAR-B4-2",
                    "asset_type": "Structural Support Pillar",
                    "asset_class": "infrastructure"
                },
                "defect": {
                    "defect_type": "TRANSVERSE_SLEEPER_CRACK",
                    "defect_class": "cracking",
                    "severity": "CRITICAL",
                    "severity_basis": ["MEASURED_WIDTH_EXCEEDS_2.0MM", "MEASURED_LENGTH_EXCEEDS_40MM"],
                    "confidence": 0.942,
                    "measurements": {"length_mm": 142.0, "width_mm": 2.1, "depth_estimate_mm": 3.5},
                    "bounding_box": {"x_min": 1240, "y_min": 680, "x_max": 1312, "y_max": 745}
                },
                "evidence": {
                    "image_id": "img_20260810_12450",
                    "image_url": "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0",
                    "thumbnail_url": "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"
                },
                "processing": {
                    "status": "requires_review",
                    "review_priority": "high",
                    "auto_generated": True,
                    "uncertainty": 0.058
                },
                "metadata": {
                    "inspection_type": "automated",
                    "environment_conditions": {"lighting": "artificial", "weather": "tunnel", "temperature_c": 32},
                    "processing_latency_ms": 124
                },
                "type": "TRANSVERSE_SLEEPER_CRACK",
                "chainage": "12+450",
                "description": "New longitudinal crack detected in primary support structure. Pattern recognition indicates potential load-bearing risk requiring immediate engineering review.",
                "status": "OPEN",
                "review_status": "PENDING_REVIEW"
            }
        ]

        self.work_orders = [
            {
                "wo_id": "WO-9942",
                "defect_id": "CR-042",
                "title": "Thermal Anomaly & Fracture Repair",
                "location": "Sector 4, Main Relay Joint Alpha",
                "status": "IN_PROGRESS",
                "isolation_status": "LOTO Required - Zone 4",
                "risk_level": "HIGH",
                "assigned_crew": "Team Delta (Eng. Smith)",
                "created_time": "T-2H"
            }
        ]

        self.idempotency_cache: Dict[str, Dict[str, Any]] = {}
        self.event_buffer: List[Dict[str, Any]] = []
        self.max_event_buffer = 1000
        
        self.sse_queues: Set[asyncio.Queue] = set()
        self.websocket_connections: Set[WebSocket] = set()

    def record_event(self, event_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self.seq_counter += 1
        event_id = data.get("event_id", f"inspection-{self.seq_counter:06d}")
        event_payload = {
            "event_id": event_id,
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data
        }
        self.event_buffer.append(event_payload)
        if len(self.event_buffer) > self.max_event_buffer:
            self.event_buffer.pop(0)
            
        return event_payload

    async def broadcast_event(self, event_payload: Dict[str, Any]):
        for q in list(self.sse_queues):
            try:
                await q.put(event_payload)
            except Exception:
                pass
                
        dead_ws = set()
        for ws in list(self.websocket_connections):
            try:
                await ws.send_json(event_payload)
            except Exception:
                dead_ws.add(ws)
        self.websocket_connections.difference_update(dead_ws)

db = OperationalDataStore()

# ==================== GLOBAL ERROR HANDLER ====================
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Global Exception Handled on {request.url.path}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "status": "ERROR",
            "error_code": "INTERNAL_SERVER_ERROR",
            "message": "Operational backend handled internal exception cleanly.",
            "detail": str(exc),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )

# ==================== SYSTEM HEALTH ENDPOINT ====================
@app.get("/health")
@app.get("/api/v1/health")
async def get_system_health():
    return {
        "status": "HEALTHY",
        "system": "RRTS IronSight Sentinel Mission Control",
        "deployment_mode": "KIOSK_OPERATIONAL_DESKTOP",
        "kiosk_fullscreen": db.kiosk_mode_enabled,
        "services": {
            "telemetry_ingestion": "NOMINAL",
            "ai_perception_engine": "NOMINAL",
            "event_stream_sse": "ACTIVE",
            "active_subscribers": len(db.sse_queues) + len(db.websocket_connections)
        },
        "active_line": db.active_line,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ==================== CONTRACT: POST /api/v1/inspection/findings ====================
@app.post("/api/v1/inspection/findings")
@app.post("/api/v1/ingest/ai-analysis")
async def ingest_structured_finding(payload: Dict[str, Any], authorization: Optional[str] = Header(None)):
    event_id = payload.get("event_id", f"evt_{datetime.now().timestamp()}")
    
    if event_id in db.idempotency_cache:
        return JSONResponse(status_code=200, content=db.idempotency_cache[event_id])

    finding_id = f"fnd_{random.randint(10000000, 99999999)}"
    
    defect_data = payload.get("defect", {})
    location_data = payload.get("location", {})
    evidence_data = payload.get("evidence", {})
    processing_data = payload.get("processing", {})
    asset_data = payload.get("asset", {})

    severity = defect_data.get("severity", "CRITICAL").upper()
    chainage_str = location_data.get("chainage_str", f"{location_data.get('chainage', 12450.0)/1000:.0f}+{location_data.get('chainage', 12450.0)%1000:03.0f}")

    defect_id = f"CR-{random.randint(100, 999)}"
    defect_type = defect_data.get("defect_type", "TRANSVERSE_SLEEPER_CRACK")

    operational_defect = {
        "defect_id": defect_id,
        "finding_id": finding_id,
        "event_id": event_id,
        "inspection_run_id": payload.get("inspection_run_id", "RRTS_001_20260810"),
        "robot_id": payload.get("robot_id", "ROBOT-01"),
        "sensor_id": payload.get("sensor_id", "CAM_FRONT_01"),
        "model_id": payload.get("model_id", "rail_defect_v3.2.1"),
        "model_version": payload.get("model_version", "3.2.1"),
        "timestamp": payload.get("timestamp", datetime.now(timezone.utc).isoformat()),
        "location": {
            "chainage": location_data.get("chainage", 12450.0),
            "chainage_str": chainage_str,
            "latitude": location_data.get("latitude", 28.6139),
            "longitude": location_data.get("longitude", 77.2090),
            "track_id": location_data.get("track_id", "LINE_ALPHA_SECT_04")
        },
        "asset": {
            "asset_id": asset_data.get("asset_id", f"RAIL_SEG_{int(location_data.get('chainage', 12450.0))}"),
            "asset_type": asset_data.get("asset_type", "Mainline Rail Section"),
            "asset_class": asset_data.get("asset_class", "track")
        },
        "defect": {
            "defect_type": defect_type,
            "defect_class": defect_data.get("defect_class", "cracking"),
            "severity": severity,
            "severity_basis": defect_data.get("severity_basis", ["MEASURED_SAFETY_LIMIT_EXCEEDED"]),
            "confidence": defect_data.get("confidence", 0.942),
            "measurements": defect_data.get("measurements", {"length_mm": 142.0, "width_mm": 2.1}),
            "bounding_box": defect_data.get("bounding_box", {"x_min": 1240, "y_min": 680, "x_max": 1312, "y_max": 745})
        },
        "evidence": {
            "image_id": evidence_data.get("image_id", "img_12450"),
            "image_url": evidence_data.get("image_url", "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"),
            "thumbnail_url": evidence_data.get("thumbnail_url", "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0")
        },
        "processing": {
            "status": processing_data.get("status", "requires_review"),
            "review_priority": processing_data.get("review_priority", "high"),
            "auto_generated": True,
            "uncertainty": processing_data.get("uncertainty", 0.058)
        },
        "type": defect_type,
        "chainage": chainage_str,
        "description": f"AI Detected {defect_type} @ {chainage_str}",
        "status": "OPEN",
        "review_status": "PENDING_REVIEW"
    }

    db.defects.insert(0, operational_defect)
    
    event = db.record_event("inspection.defect.detected", operational_defect)
    await db.broadcast_event(event)

    response_payload = {
        "status": "accepted",
        "finding_id": finding_id,
        "event_id": event_id,
        "processing_status": "queued_for_review"
    }
    
    db.idempotency_cache[event_id] = response_payload
    return JSONResponse(status_code=202, content=response_payload)

@app.post("/api/v1/ingest/telemetry")
async def ingest_telemetry(payload: Dict[str, Any]):
    db.telemetry.update({
        "robot_id": payload.get("robot_id", db.telemetry["robot_id"]),
        "chainage": payload.get("chainage", db.telemetry["chainage"]),
        "chainage_meters": payload.get("chainage_meters", db.telemetry["chainage_meters"]),
        "speed_ms": payload.get("speed_ms", db.telemetry["speed_ms"]),
        "sys_temp_c": payload.get("sys_temp_c", db.telemetry["sys_temp_c"]),
        "battery_pct": payload.get("battery_pct", db.telemetry["battery_pct"]),
        "telemetry_latency_ms": payload.get("telemetry_latency_ms", db.telemetry["telemetry_latency_ms"]),
        "status": payload.get("status", db.telemetry["status"]),
        "last_updated": datetime.now(timezone.utc).isoformat()
    })
    
    event = db.record_event("robot.position.updated", db.telemetry)
    await db.broadcast_event(event)
    return {"status": "INGESTED", "event_id": event["event_id"]}

# ==================== STREAMING ENDPOINTS ====================
@app.get("/api/v1/inspection-events")
async def sse_inspection_events(request: Request, last_event_id: Optional[str] = Query(None)):
    client_queue = asyncio.Queue()
    db.sse_queues.add(client_queue)

    last_id = last_event_id or request.headers.get("Last-Event-ID")
    replay_events = []
    if last_id:
        found = False
        for ev in db.event_buffer:
            if found:
                replay_events.append(ev)
            elif ev["event_id"] == last_id:
                found = True

    async def event_generator():
        try:
            yield ": connected\n\n"
            for ev in replay_events:
                yield f"id: {ev['event_id']}\n"
                yield f"event: {ev['event_type']}\n"
                yield f"data: {json.dumps(ev)}\n\n"

            while not await request.is_disconnected():
                try:
                    event_payload = await asyncio.wait_for(client_queue.get(), timeout=12.0)
                    yield f"id: {event_payload['event_id']}\n"
                    yield f"event: {event_payload['event_type']}\n"
                    yield f"data: {json.dumps(event_payload)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            db.sse_queues.discard(client_queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
    )

# ==================== REST APIS ====================
@app.get("/api/v1/telemetry")
async def get_telemetry():
    return db.telemetry

@app.get("/api/v1/runs")
async def get_runs():
    return db.runs

@app.get("/api/v1/defects")
async def get_defects():
    return db.defects

@app.get("/api/v1/work-orders")
async def get_work_orders():
    return db.work_orders

@app.get("/api/v1/assets")
async def get_assets():
    return {
        "tree": [
            {"id": "rails", "label": "Rails", "type": "CATEGORY", "children": [
                {"id": "RL-8492-A", "label": "RL-8492-A", "status": "WEAR_DETECTED"},
                {"id": "RL-8492-B", "label": "RL-8492-B", "status": "NOMINAL"}
            ]}
        ]
    }

# ==================== DASHBOARD TEMPLATE LOADER ====================
def _load_dashboard_html() -> str:
    template_candidates = [
        BASE_DIR / "templates" / "dashboard.html",
        BASE_DIR / "dashboard.html",
        Path.cwd() / "templates" / "dashboard.html",
        Path("/var/task/templates/dashboard.html"),
    ]
    for path in template_candidates:
        if path.exists() and path.is_file():
            try:
                return path.read_text(encoding="utf-8")
            except Exception:
                pass

    # Built-in robust HTML Control Room interface
    return """<!DOCTYPE html>
<html class="dark" lang="en" style="height: 100vh; width: 100vw; overflow: hidden;">
<head>
    <meta charset="utf-8"/><meta content="width=device-width, initial-scale=1.0" name="viewport"/>
    <title>IronSight Sentinel - RRTS Platform</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#111316] text-[#e2e2e6] h-screen w-screen overflow-hidden flex flex-col font-mono antialiased">
<nav class="w-full h-[48px] bg-[#1e2023] flex items-center justify-between px-4 border-b border-[#41474e]">
    <div class="flex items-center gap-4"><span class="font-black text-[#9ccbf7] text-lg tracking-tight">IRONSIGHT SENTINEL</span></div>
    <div class="flex items-center gap-6 text-xs text-[#c1c7cf]">
        <span>ROBOT: <b id="nav-robot-id" class="text-white">ROBOT-01</b></span>
        <span>CHAINAGE: <b id="nav-chainage" class="text-[#9ccbf7]">12+435</b></span>
        <span>TEMP: <b id="nav-temp" class="text-white">42.1°C</b></span>
        <div class="bg-[#9ccbf7]/10 border border-[#9ccbf7] text-[#9ccbf7] px-2 py-0.5 rounded text-xs">SYSTEM ACTIVE</div>
    </div>
</nav>
<main class="flex-1 flex p-4 gap-4 overflow-hidden">
    <section class="flex-1 bg-[#0c0e11] border border-[#41474e] rounded p-4 flex flex-col justify-between relative">
        <div>
            <div class="text-xs text-[#c1c7cf]">CURRENT TRACK SEGMENT</div>
            <h2 class="text-lg font-bold text-white">LINE ALPHA / SECT-04 / 12+000 - 13+000</h2>
        </div>
        <div class="h-24 bg-[#1a1c1f] border border-[#41474e] rounded relative flex items-center px-6">
            <div class="w-full h-1 bg-[#41474e]"></div>
            <div class="absolute left-[65%] -translate-x-1/2 flex flex-col items-center">
                <div class="bg-[#2a5c82] text-[10px] px-2 py-0.5 rounded text-[#9ccbf7] border border-[#9ccbf7] mb-1">ROBOT-01</div>
                <div class="w-4 h-4 bg-[#9ccbf7] rounded-full animate-ping"></div>
            </div>
        </div>
        <div class="text-xs text-[#9ccbf7]">LIVE SSE TELEMETRY STREAM ACTIVE</div>
    </section>
    <section class="w-[380px] bg-[#111316] border border-[#41474e] rounded p-4 flex flex-col">
        <h3 class="text-xs font-bold text-[#ff3b30] mb-2 uppercase">⚠ Critical Defect Alert</h3>
        <h2 class="text-sm font-bold mb-3">Defect Analysis: CR-042</h2>
        <div class="h-44 bg-[#333538] border border-[#41474e] rounded mb-3 overflow-hidden">
            <img class="w-full h-full object-cover" src="https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"/>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs mb-3">
            <div class="bg-[#1e2023] p-2 rounded"><span>TYPE:</span> <b class="block text-white truncate">TRANSVERSE_CRACK</b></div>
            <div class="bg-[#1e2023] p-2 rounded"><span>SEVERITY:</span> <b class="block text-[#ff3b30]">CRITICAL</b></div>
        </div>
        <button onclick="alert('Repair Team Dispatched!')" class="mt-auto w-full py-2 bg-[#2a5c82] hover:bg-[#9ccbf7] hover:text-[#003351] text-[#9ccbf7] font-bold text-xs rounded border border-[#9ccbf7] transition-colors">
            DISPATCH REPAIR CREW
        </button>
    </section>
</main>
<script>
    fetch('/api/v1/telemetry').then(r => r.json()).then(t => {
        document.getElementById('nav-robot-id').innerText = t.robot_id || 'ROBOT-01';
        document.getElementById('nav-chainage').innerText = t.chainage || '12+435';
        document.getElementById('nav-temp').innerText = (t.sys_temp_c || 42.1) + '°C';
    });
    try {
        const es = new EventSource('/api/v1/inspection-events');
        es.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.event_type === 'robot.position.updated') {
                document.getElementById('nav-chainage').innerText = data.data.chainage || '12+435';
            }
        };
    } catch (e) {}
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    return HTMLResponse(content=_load_dashboard_html())

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host=host, port=port)
