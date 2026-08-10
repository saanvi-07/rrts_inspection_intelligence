# RRTS IronSight Sentinel Autonomous Inspection Platform

Hardware-decoupled sensor-to-AI data pipeline, operational backend platform, and control-room mission control dashboard for RRTS (Rapid Rail Transit System) autonomous rail inspection.

## Project Structure

```
rrts_sentinel/
├── backend_server.py           # FastAPI Operational Backend (REST, SSE, WebSockets, Health)
├── ai_service.py              # AI Perception & Measurement Severity Engine
├── robot_pipeline.py          # Robot Sensor & Camera Data Pipeline Simulator
├── pipeline_integration_bridge.py # Bridge connecting Sensor Pipeline -> AI -> Backend
├── test_system.py             # System Integration & End-to-End Test Suite
├── start_ai.py                # Launcher for AI Perception Engine
├── requirements.txt           # Production Python dependencies
├── templates/
│   └── dashboard.html         # Claude Dark Brutalist 100vh/100vw Control Room Dashboard
└── rrts_sensor_pipeline/      # Hardware-decoupled Sensor Ingestion Package
    ├── schemas.py             # Canonical wire contracts (RawSensorFrame, SyncedSensorBundle)
    ├── pipeline_ingestion.py  # QualityGate & SyncEngine
    ├── ai_adapter.py          # AIInputAdapter (NCHW tensor generation)
    └── test_sensor_publisher.py # Multi-sensor topic bus & synthetic publisher
```

## Quick Start (Local Execution)

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start Operational Backend Server:
   ```bash
   python backend_server.py
   ```
   The control room dashboard is accessible at `http://localhost:8000/`.

3. Start AI Perception Engine (in a separate terminal):
   ```bash
   python start_ai.py
   ```

4. Run Data Pipeline & Integration Bridge:
   ```bash
   python pipeline_integration_bridge.py
   ```

5. Run End-to-End System Tests:
   ```bash
   python test_system.py
   ```

## Deployment Instructions

### Option A: GitHub & Render / Railway Deployment (Recommended)
1. Initialize Git repository and push to GitHub:
   ```bash
   git init
   git add .
   git commit -m "Initial commit - RRTS IronSight Sentinel Platform"
   git branch -M main
   git remote add origin https://github.com/<YOUR_USERNAME>/rrts-ironsight-sentinel.git
   git push -u origin main
   ```

2. Deploy on Render:
   - Create a new **Web Service** connected to your GitHub repository.
   - Set **Environment**: Python 3.
   - Set **Build Command**: `pip install -r requirements.txt`
   - Set **Start Command**: `uvicorn backend_server:app --host 0.0.0.0 --port $PORT`

### Option B: Vercel Deployment (Serverless ASGI)
1. Install Vercel CLI:
   ```bash
   npm install -g vercel
   ```
2. Create `vercel.json` in project root:
   ```json
   {
     "builds": [
       { "src": "backend_server.py", "use": "@vercel/python" }
     ],
     "routes": [
       { "src": "/(.*)", "dest": "backend_server.py" }
     ]
   }
   ```
3. Deploy:
   ```bash
   vercel
   ```
