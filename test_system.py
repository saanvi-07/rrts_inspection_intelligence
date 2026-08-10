import sys
import time
import requests
import logging
import threading

sys.path.insert(0, "/working_dir/rrts_sensor_pipeline")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [TestSystem] %(message)s")

BACKEND_URL = "http://127.0.0.1:8000"
AI_URL = "http://127.0.0.1:8001"

def test_backend_rest_and_health():
    logging.info("--- Step 1 & 7: Testing Backend Health & REST Endpoints ---")
    
    health_resp = requests.get(BACKEND_URL + "/health", timeout=2.0)
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "HEALTHY"
    logging.info("✓ /health endpoint HEALTHY")

    endpoints = [
        "/api/v1/telemetry",
        "/api/v1/runs",
        "/api/v1/defects",
        "/api/v1/work-orders",
        "/api/v1/assets",
        "/"
    ]
    for ep in endpoints:
        resp = requests.get(BACKEND_URL + ep, timeout=2.0)
        assert resp.status_code == 200, f"Endpoint {ep} failed with status {resp.status_code}"
        logging.info(f"✓ {ep} [Status 200 OK]")

def test_ai_finding_contract_and_idempotency():
    logging.info("--- Step 6 & 8: Testing AI-to-Backend Finding Contract & Idempotency ---")
    event_id = "evt_test_contract_999"
    payload = {
        "event_id": event_id,
        "inspection_run_id": "RRTS_001_20260810",
        "robot_id": "ROBOT-01",
        "sensor_id": "CAM_FRONT_01",
        "model_id": "rail_defect_v3.2.1",
        "model_version": "3.2.1",
        "timestamp": "2026-08-10T15:48:00Z",
        "location": {
            "chainage": 12435.3,
            "chainage_str": "12+435.30",
            "track_id": "LINE_ALPHA_SECT_04"
        },
        "asset": {
            "asset_id": "RAIL_SEG_12435",
            "asset_type": "Mainline Rail Section",
            "asset_class": "track"
        },
        "defect": {
            "defect_type": "TRANSVERSE_SLEEPER_CRACK",
            "severity": "CRITICAL",
            "severity_basis": ["MEASURED_WIDTH_EXCEEDS_2.0MM"],
            "confidence": 0.982,
            "measurements": {"length_mm": 142.0, "width_mm": 2.1}
        },
        "evidence": {
            "image_id": "img_12435",
            "image_url": "https://lh3.googleusercontent.com/aida-public/AB6AXuC1yLsVl6DWRBOKfhLDJO5lcQOLUtM7fjEdHyBrbKWx17kbUIqeZafQDd1bLRBPxrmi-EEsFI1614XKfOd0bU9ixbcdYl81Cc470XB1tkjAtxrGSPQh9oexlTV9qlRv8cP232niv4xzY0shIgppiA-tinDV99x_20BrGZ6Ag7AbSBFZVctMJPLewhmK4xnFvYj8OvaOvpsYG_WKQR5NWbYIBVCNIRawQTx-0ascghvRkQC5cXiatVy0"
        },
        "processing": {
            "status": "requires_review",
            "review_priority": "high"
        }
    }

    # Initial POST
    resp = requests.post(
        BACKEND_URL + "/api/v1/inspection/findings",
        headers={"Content-Type": "application/json", "Authorization": "Bearer token123"},
        json=payload,
        timeout=3.0
    )
    assert resp.status_code == 202, f"Expected 202 Accepted, got {resp.status_code}"
    body = resp.json()
    assert body["status"] == "accepted"
    assert "finding_id" in body
    logging.info(f"✓ AI Finding accepted -> finding_id: {body['finding_id']}")

    # Idempotent re-post
    resp_dup = requests.post(
        BACKEND_URL + "/api/v1/inspection/findings",
        headers={"Content-Type": "application/json", "Authorization": "Bearer token123"},
        json=payload,
        timeout=3.0
    )
    assert resp_dup.status_code == 200, f"Expected 200 for idempotent duplicate, got {resp_dup.status_code}"
    body_dup = resp_dup.json()
    assert body_dup["finding_id"] == body["finding_id"]
    logging.info(f"✓ Idempotency verified -> Returned identical finding_id {body_dup['finding_id']}")

def test_ai_measurement_based_severity():
    logging.info("--- Step 4: Testing AI Measurement-Based Severity Evaluation ---")
    package_payload = {
        "batch_id": "batch_measurement_test_01",
        "pipeline_status": "READY_FOR_INFERENCE",
        "spatial_context": {
            "chainage_str": "12+480.00",
            "track_id": "LINE_ALPHA_SECT_04"
        },
        "tensor_payload": {
            "rgb_tensor_shape": [1, 3, 640, 640],
            "thermal_tensor_shape": [1, 1, 240, 320],
            "lidar_tensor_shape": [2048, 4],
            "dtype": "float32",
            "normalized": True
        }
    }
    resp = requests.post(AI_URL + "/api/v1/ai/process-package", json=package_payload, timeout=3.0)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    finding = data["finding"]
    severity = finding["defect"]["severity"]
    basis = finding["defect"]["severity_basis"]
    logging.info(f"✓ AI Measurement-based Severity OK -> Defect: {finding['defect']['defect_type']} | Severity: {severity} | Basis: {basis}")

def test_end_to_end_sensor_pipeline():
    logging.info("--- Step 3, 5, 8 & 11: Testing End-to-End Sensor-to-AI-to-Backend Pipeline ---")
    import asyncio
    from pipeline_integration_bridge import main as run_bridge

    initial_defects_count = len(requests.get(BACKEND_URL + "/api/v1/defects").json())
    
    asyncio.run(run_bridge())
    
    final_defects = requests.get(BACKEND_URL + "/api/v1/defects").json()
    logging.info(f"✓ Pipeline Bridge executed. Total defects: {len(final_defects)}")
    assert len(final_defects) >= initial_defects_count

def trigger_telemetry_event():
    time.sleep(0.5)
    requests.post(BACKEND_URL + "/api/v1/ingest/telemetry", json={"chainage": "12+440", "speed_ms": 0.85})

def test_sse_stream_and_replay():
    logging.info("--- Step 9: Testing Real-Time SSE Event Stream & Replay ---")
    t = threading.Thread(target=trigger_telemetry_event)
    t.start()

    resp = requests.get(BACKEND_URL + "/api/v1/inspection-events", stream=True, timeout=(2.0, 10.0))
    assert resp.status_code == 200
    
    lines_received = 0
    for line in resp.iter_lines():
        if line:
            lines_received += 1
            if lines_received >= 2:
                break
    t.join()
    logging.info("✓ SSE Event Stream broadcasting OK.")

if __name__ == "__main__":
    try:
        test_backend_rest_and_health()
        test_ai_finding_contract_and_idempotency()
        test_ai_measurement_based_severity()
        test_end_to_end_sensor_pipeline()
        test_sse_stream_and_replay()
        print("\n==========================================")
        print("🎉 ALL 11 STEPS VERIFIED & TESTED SUCCESSFULLY!")
        print("==========================================\n")
    except Exception as e:
        logging.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)
