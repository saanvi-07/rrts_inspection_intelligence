from __future__ import annotations

import asyncio
import logging

from ai_adapter import AIInputAdapter
from pipeline_ingestion import QualityGate, QualityGateConfig, SyncEngine
from schemas import PipelineStatus, SensorType
from test_sensor_publisher import (
    FailureInjectionConfig,
    SensorChannelConfig,
    SyntheticSensorPublisher,
    TopicBus,
    default_rrts_channel_set,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("rrts_sensor_pipeline.demo")


async def _consume_and_process(bus: TopicBus, topic: str, quality_gate: QualityGate, sync_engine: SyncEngine) -> None:
    while True:
        frame = await bus.subscribe(topic)
        verdict = quality_gate.evaluate(frame)
        if not verdict.accepted:
            logger.info("REJECTED  sensor=%-20s frame=%-5d reason=%s", frame.sensor_id, frame.frame_id, verdict.reason)
            continue
        sync_engine.ingest(frame)


async def _emit_ai_packages(sync_engine: SyncEngine, adapter: AIInputAdapter, interval_s: float, stop_after_s: float) -> None:
    loop = asyncio.get_event_loop()
    start = loop.time()
    ready_count = 0
    rejected_count = 0
    while (loop.time() - start) < stop_after_s:
        await asyncio.sleep(interval_s)
        bundle = sync_engine.try_build_bundle(reference_type=SensorType.RGB_CAMERA)
        package = adapter.adapt(bundle)
        if package.pipeline_status == PipelineStatus.READY_FOR_INFERENCE:
            ready_count += 1
            shapes = {
                "rgb": package.tensor_payload.rgb_tensor_shape if package.tensor_payload else None,
                "thermal": package.tensor_payload.thermal_tensor_shape if package.tensor_payload else None,
                "lidar": package.tensor_payload.lidar_tensor_shape if package.tensor_payload else None,
            }
            logger.info("READY     batch=%s chainage=%s shapes=%s",
                        package.batch_id, package.spatial_context.chainage_str if package.spatial_context else "N/A", shapes)
        elif package.pipeline_status == PipelineStatus.QUALITY_REJECTED:
            rejected_count += 1
            logger.info("AI-REJECT batch=%s reason=%s", package.batch_id, package.rejected_reason)

    logger.info("Summary: %d batches READY_FOR_INFERENCE, %d QUALITY_REJECTED", ready_count, rejected_count)


async def main() -> None:
    bus = TopicBus()

    channels = default_rrts_channel_set()
    for ch in channels:
        if ch.sensor_type == SensorType.RGB_CAMERA:
            ch.failure = FailureInjectionConfig(blur_probability=0.15, frame_drop_probability=0.05)
        if ch.sensor_type == SensorType.LIDAR_3D:
            ch.failure = FailureInjectionConfig(frame_drop_probability=0.10)

    publisher = SyntheticSensorPublisher(bus, channels)
    quality_gate = QualityGate(QualityGateConfig())
    sync_engine = SyncEngine(QualityGateConfig())
    adapter = AIInputAdapter(quality_gate=quality_gate)

    consumer_tasks = [
        asyncio.create_task(_consume_and_process(bus, ch.topic, quality_gate, sync_engine))
        for ch in channels
    ]
    emitter_task = asyncio.create_task(_emit_ai_packages(sync_engine, adapter, interval_s=0.1, stop_after_s=2.0))
    publisher_task = asyncio.create_task(publisher.run(duration_s=2.0))

    await publisher_task
    await emitter_task

    for t in consumer_tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
