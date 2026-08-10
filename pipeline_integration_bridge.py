import sys
import asyncio
import logging
import requests
import json

sys.path.insert(0, "/working_dir/rrts_sensor_pipeline")

from schemas import PipelineStatus, SensorType
from test_sensor_publisher import (
    TopicBus,
    SyntheticSensorPublisher,
    default_rrts_channel_set,
    FailureInjectionConfig
)
from pipeline_ingestion import QualityGate, QualityGateConfig, SyncEngine
from ai_adapter import AIInputAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [PipelineBridge] %(message)s")
logger = logging.getLogger("PipelineBridge")

AI_SERVICE_URL = "http://127.0.0.1:8001/api/v1/ai/process-package"
BACKEND_TELEMETRY_URL = "http://127.0.0.1:8000/api/v1/ingest/telemetry"

async def _consume_sensor_bus(bus: TopicBus, topic: str, quality_gate: QualityGate, sync_engine: SyncEngine):
    while True:
        frame = await bus.subscribe(topic)
        verdict = quality_gate.evaluate(frame)
        if not verdict.accepted:
            logger.debug("Frame rejected at QualityGate: %s %s", frame.sensor_id, verdict.reason)
            continue
        
        sync_engine.ingest(frame)

        # Transmit odometry / telemetry directly to backend
        if frame.sensor_type == SensorType.ENCODER_ODOMETRY:
            telemetry_payload = {
                "robot_id": "ROBOT-01",
                "line_id": "LINE_ALPHA",
                "section": "SECT-04",
                "chainage": f"{int(frame.chainage_m // 1000)}+{int(frame.chainage_m % 1000):03d}",
                "chainage_meters": round(frame.chainage_m, 2),
                "speed_ms": 0.85,
                "sys_temp_c": 42.1,
                "battery_pct": 98.2,
                "telemetry_latency_ms": 120,
                "status": "ACTIVE"
            }
            try:
                requests.post(BACKEND_TELEMETRY_URL, json=telemetry_payload, timeout=1.0)
            except Exception:
                pass

async def _emit_and_forward_to_ai(sync_engine: SyncEngine, adapter: AIInputAdapter, interval_s: float = 0.5, duration_s: float = 5.0):
    loop = asyncio.get_event_loop()
    start = loop.time()
    
    while (loop.time() - start) < duration_s:
        await asyncio.sleep(interval_s)
        bundle = sync_engine.try_build_bundle(reference_type=SensorType.RGB_CAMERA)
        package = adapter.adapt(bundle)

        if package.pipeline_status == PipelineStatus.READY_FOR_INFERENCE:
            package_dict = {
                "batch_id": package.batch_id,
                "pipeline_status": package.pipeline_status.value,
                "spatial_context": {
                    "chainage_str": package.spatial_context.chainage_str,
                    "track_id": package.spatial_context.track_id
                } if package.spatial_context else None,
                "tensor_payload": {
                    "rgb_tensor_shape": package.tensor_payload.rgb_tensor_shape,
                    "thermal_tensor_shape": package.tensor_payload.thermal_tensor_shape,
                    "lidar_tensor_shape": package.tensor_payload.lidar_tensor_shape,
                    "dtype": package.tensor_payload.dtype,
                    "normalized": package.tensor_payload.normalized
                } if package.tensor_payload else None
            }

            try:
                resp = requests.post(AI_SERVICE_URL, json=package_dict, timeout=2.0)
                logger.info("✓ Forwarded AIInputPackage batch %s to AI Perception Engine -> Status %s", package.batch_id, resp.status_code)
            except Exception as err:
                logger.warning("Failed to reach AI Perception Engine: %s", err)

async def main():
    bus = TopicBus()
    channels = default_rrts_channel_set()
    
    # Configure channels
    for ch in channels:
        if ch.sensor_type == SensorType.RGB_CAMERA:
            ch.failure = FailureInjectionConfig(blur_probability=0.05)

    publisher = SyntheticSensorPublisher(bus, channels)
    quality_gate = QualityGate(QualityGateConfig())
    sync_engine = SyncEngine(QualityGateConfig())
    adapter = AIInputAdapter(quality_gate=quality_gate)

    consumer_tasks = [
        asyncio.create_task(_consume_sensor_bus(bus, ch.topic, quality_gate, sync_engine))
        for ch in channels
    ]
    emitter_task = asyncio.create_task(_emit_and_forward_to_ai(sync_engine, adapter, interval_s=0.5, duration_s=3.0))
    publisher_task = asyncio.create_task(publisher.run(duration_s=3.0))

    await publisher_task
    await emitter_task

    for t in consumer_tasks:
        t.cancel()

if __name__ == "__main__":
    asyncio.run(main())
