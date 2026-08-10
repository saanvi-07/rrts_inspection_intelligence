from __future__ import annotations

import logging

import numpy as np

from pipeline_ingestion import Preprocessor, QualityGate, QualityGateConfig
from schemas import (
    AIInputPackage,
    PipelineStatus,
    QualityFlag,
    RawSensorFrame,
    SensorType,
    SpatialContext,
    SyncedSensorBundle,
    TensorPayload,
)

logger = logging.getLogger("rrts_sensor_pipeline.ai_adapter")


def _chainage_to_str(chainage_m: float) -> str:
    km = int(chainage_m // 1000)
    remainder = chainage_m - km * 1000
    return f"{km}+{remainder:06.2f}"


class AIInputAdapter:
    def __init__(
        self,
        quality_gate: QualityGate | None = None,
        preprocessor: Preprocessor | None = None,
        track_id: str = "LINE_ALPHA_SECT_04",
    ) -> None:
        self.quality_gate = quality_gate or QualityGate()
        self.preprocessor = preprocessor or Preprocessor()
        self.track_id = track_id

    def adapt(self, bundle: SyncedSensorBundle | None) -> AIInputPackage:
        if bundle is None:
            return AIInputPackage(
                batch_id="UNKNOWN",
                pipeline_status=PipelineStatus.SYNC_TIMEOUT,
                rejected_reason="NO_SYNCHRONIZED_BUNDLE_AVAILABLE",
                action_taken="INFERENCE_SKIPPED_AWAITING_NEXT_BUNDLE",
            )

        for frame in bundle.frames.values():
            verdict = self.quality_gate.evaluate(frame)
            if not verdict.accepted:
                return AIInputPackage(
                    batch_id=bundle.batch_id,
                    pipeline_status=PipelineStatus.QUALITY_REJECTED,
                    rejected_reason=verdict.reason,
                    action_taken="INFERENCE_SKIPPED_LOGGED_FOR_REINSPECTION",
                )

        try:
            tensor_payload = self._build_tensor_payload(bundle)
        except Exception as exc:
            logger.exception("Tensor adaptation failed for batch %s", bundle.batch_id)
            return AIInputPackage(
                batch_id=bundle.batch_id,
                pipeline_status=PipelineStatus.QUALITY_REJECTED,
                rejected_reason=f"ADAPTER_EXCEPTION_{type(exc).__name__}",
                action_taken="INFERENCE_SKIPPED_LOGGED_FOR_REINSPECTION",
            )

        return AIInputPackage(
            batch_id=bundle.batch_id,
            spatial_context=SpatialContext(
                chainage_str=_chainage_to_str(bundle.chainage_m),
                track_id=self.track_id,
            ),
            tensor_payload=tensor_payload,
            pipeline_status=PipelineStatus.READY_FOR_INFERENCE,
            rejected_reason=None,
        )

    def _build_tensor_payload(self, bundle: SyncedSensorBundle) -> TensorPayload:
        arrays: dict[str, np.ndarray] = {}
        rgb_shape = thermal_shape = lidar_shape = None

        rgb_frame = bundle.frames.get(SensorType.RGB_CAMERA)
        if rgb_frame is not None:
            processed = self.preprocessor.process(rgb_frame)
            tensor = np.transpose(processed, (2, 0, 1))[np.newaxis, ...]
            arrays["rgb"] = tensor.astype(np.float32)
            rgb_shape = tuple(tensor.shape)

        thermal_frame = bundle.frames.get(SensorType.THERMAL_CAMERA)
        if thermal_frame is not None:
            processed = self.preprocessor.process(thermal_frame)
            tensor = processed[np.newaxis, np.newaxis, ...]
            arrays["thermal"] = tensor.astype(np.float32)
            thermal_shape = tuple(tensor.shape)

        lidar_frame = bundle.frames.get(SensorType.LIDAR_3D)
        if lidar_frame is not None:
            processed = self.preprocessor.process(lidar_frame)
            arrays["lidar"] = processed.astype(np.float32)
            lidar_shape = tuple(processed.shape)

        for aux_type in (SensorType.IMU, SensorType.ENCODER_ODOMETRY):
            aux_frame = bundle.frames.get(aux_type)
            if aux_frame is not None:
                arrays[aux_type.value.lower()] = self.preprocessor.process(aux_frame)

        return TensorPayload(
            rgb_tensor_shape=rgb_shape,
            thermal_tensor_shape=thermal_shape,
            lidar_tensor_shape=lidar_shape,
            dtype="float32",
            normalized=True,
            arrays=arrays,
        )
