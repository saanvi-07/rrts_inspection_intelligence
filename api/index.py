import asyncio
import json
import logging
import os
import random
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timezone

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException, Query, Header
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Backend] %(message)s")

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
            {"id": "RRTS_001_20260803", "label": "RUN 03", "date": "-7 DAYS", "anomalies": 10, "scan_type": "ROUTINE", "status": "completed"}
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
                    "image_url": "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"
                },
                "processing": {
                    "status": "requires_review",
                    "review_priority": "high",
                    "auto_generated": True,
                    "uncertainty": 0.058
                },
                "type": "TRANSVERSE_SLEEPER_CRACK",
                "chainage": "12+450",
                "description": "New longitudinal crack detected in primary support structure.",
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
                "assigned_crew": "Team Delta (Eng. Smith)"
            }
        ]

        self.idempotency_cache: Dict[str, Dict[str, Any]] = {}
        self.event_buffer: List[Dict[str, Any]] = []
        self.sse_queues: Set[asyncio.Queue] = set()

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
        if len(self.event_buffer) > 500:
            self.event_buffer.pop(0)
        return event_payload

    async def broadcast_event(self, event_payload: Dict[str, Any]):
        for q in list(self.sse_queues):
            try:
                await q.put(event_payload)
            except Exception:
                pass

db = OperationalDataStore()

@app.get("/health")
@app.get("/api/v1/health")
async def get_system_health():
    return {
        "status": "HEALTHY",
        "system": "RRTS IronSight Sentinel Mission Control",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/api/v1/inspection/findings")
async def ingest_structured_finding(payload: Dict[str, Any]):
    event_id = payload.get("event_id", f"evt_{datetime.now().timestamp()}")
    if event_id in db.idempotency_cache:
        return JSONResponse(status_code=200, content=db.idempotency_cache[event_id])

    finding_id = f"fnd_{random.randint(10000000, 99999999)}"
    operational_defect = {**payload, "finding_id": finding_id}
    db.defects.insert(0, operational_defect)
    
    event = db.record_event("inspection.defect.detected", operational_defect)
    await db.broadcast_event(event)

    response_payload = {"status": "accepted", "finding_id": finding_id, "event_id": event_id}
    db.idempotency_cache[event_id] = response_payload
    return JSONResponse(status_code=202, content=response_payload)

@app.post("/api/v1/ingest/telemetry")
async def ingest_telemetry(payload: Dict[str, Any]):
    db.telemetry.update({**payload, "last_updated": datetime.now(timezone.utc).isoformat()})
    event = db.record_event("robot.position.updated", db.telemetry)
    await db.broadcast_event(event)
    return {"status": "INGESTED", "event_id": event["event_id"]}

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

@app.get("/api/v1/inspection-events")
async def sse_inspection_events(request: Request):
    client_queue = asyncio.Queue()
    db.sse_queues.add(client_queue)

    async def event_generator():
        try:
            yield ": connected\n\n"
            for ev in db.event_buffer[-10:]:
                yield f"id: {ev['event_id']}\nevent: {ev['event_type']}\ndata: {json.dumps(ev)}\n\n"
            while not await request.is_disconnected():
                try:
                    event_payload = await asyncio.wait_for(client_queue.get(), timeout=5.0)
                    yield f"id: {event_payload['event_id']}\nevent: {event_payload['event_type']}\ndata: {json.dumps(event_payload)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            db.sse_queues.discard(client_queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

DASHBOARD_HTML = """<!DOCTYPE html>
<html class="dark" lang="en" style="height: 100vh; width: 100vw; overflow: hidden;">
<head>
    <meta charset="utf-8"/><meta content="width=device-width, initial-scale=1.0" name="viewport"/>
    <title>IronSight Sentinel - RRTS Control Room</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#111316] text-[#e2e2e6] h-screen w-screen overflow-hidden flex flex-col font-sans antialiased">
<nav class="w-full h-[48px] bg-[#1e2023] flex items-center justify-between px-4 border-b border-[#41474e]">
    <div class="flex items-center gap-6"><span class="font-black text-[#9ccbf7] tracking-tight">IRONSIGHT SENTINEL</span></div>
    <div class="flex items-center gap-4 text-xs font-mono text-[#c1c7cf]">
        <span id="nav-robot-id">ROBOT-01</span>
        <span id="nav-chainage" class="text-[#9ccbf7] font-bold">12+435</span>
        <span id="nav-temp">42.1°C</span>
        <div class="bg-[#9ccbf7]/10 border border-[#9ccbf7] text-[#9ccbf7] px-2 py-0.5 rounded font-mono text-xs">SYSTEM ACTIVE</div>
    </div>
</nav>
<main class="flex-1 flex p-4 gap-4 overflow-hidden">
    <section class="flex-1 bg-[#0c0e11] border border-[#41474e] rounded p-4 flex flex-col justify-between relative">
        <div>
            <div class="text-xs text-[#c1c7cf]">CURRENT TRACK SEGMENT</div>
            <h2 class="text-lg font-bold">LINE ALPHA / SECT-04 / 12+000 - 13+000</h2>
        </div>
        <div class="h-20 bg-[#1a1c1f] border border-[#41474e] rounded relative flex items-center px-4">
            <div class="w-full h-1 bg-[#41474e]"></div>
            <div id="robot-marker" class="absolute left-[65%] -translate-x-1/2 flex flex-col items-center">
                <div class="bg-[#2a5c82] text-xs font-mono px-2 py-0.5 rounded text-[#9ccbf7] border border-[#9ccbf7] mb-1">ROBOT-01</div>
                <div class="w-4 h-4 bg-[#9ccbf7] rounded-full animate-ping"></div>
            </div>
        </div>
        <div class="text-xs font-mono text-[#9ccbf7]">LIVE TELEMETRY STREAM ACTIVE</div>
    </section>
    <section class="w-[380px] bg-[#111316] border border-[#41474e] rounded p-4 flex flex-col">
        <h3 class="text-xs font-bold text-[#ff3b30] mb-2 uppercase tracking-wide">⚠ Critical Defect Alert</h3>
        <h2 id="defect-title" class="text-md font-bold mb-3">Defect Analysis: CR-042</h2>
        <div class="h-44 bg-[#333538] border border-[#41474e] rounded mb-3 overflow-hidden">
            <img class="w-full h-full object-cover" src="https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"/>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs font-mono mb-3">
            <div class="bg-[#1e2023] p-2 rounded"><span>TYPE:</span> <b class="block text-[#e2e2e6] truncate">TRANSVERSE_CRACK</b></div>
            <div class="bg-[#1e2023] p-2 rounded"><span>SEVERITY:</span> <b class="block text-[#ff3b30]">CRITICAL</b></div>
        </div>
        <button onclick="alert('Repair Crew Dispatched!')" class="mt-auto w-full py-2 bg-[#2a5c82] hover:bg-[#9ccbf7] hover:text-[#003351] text-[#9ccbf7] font-bold text-xs rounded border border-[#9ccbf7] transition-colors">
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
    return HTMLResponse(content=DASHBOARD_HTML)
