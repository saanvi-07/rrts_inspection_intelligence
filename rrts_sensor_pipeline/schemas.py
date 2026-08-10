from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

import numpy as np
from pydantic import BaseModel, Field, validator


class QualityFlag(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"
    UNAVAILABLE = "UNAVAILABLE"


class SensorType(str, Enum):
    RGB_CAMERA = "RGB_CAMERA"
    THERMAL_CAMERA = "THERMAL_CAMERA"
    LIDAR_3D = "LIDAR_3D"
    IMU = "IMU"
    ENCODER_ODOMETRY = "ENCODER_ODOMETRY"


class PipelineStatus(str, Enum):
    READY_FOR_INFERENCE = "READY_FOR_INFERENCE"
    QUALITY_REJECTED = "QUALITY_REJECTED"
    SYNC_TIMEOUT = "SYNC_TIMEOUT"


class RawSensorFrame(BaseModel):
    class Config:
        frozen = True
        arbitrary_types_allowed = True

    timestamp_ns: int = Field(..., description="Hardware/monotonic capture timestamp, nanoseconds.")
    sensor_id: str = Field(..., description="Unique physical/logical sensor identifier.")
    sensor_type: SensorType
    frame_id: int = Field(..., ge=0, description="Monotonically increasing sequence number.")
    chainage_m: float = Field(..., description="Railway track position in metres.")
    quality: QualityFlag = QualityFlag.OK
    raw_bytes_or_array: Optional[np.ndarray] = None
    calibration_matrix: Optional[list[list[float]]] = None
    laplacian_variance: Optional[float] = None

    @validator("timestamp_ns")
    def _positive_timestamp(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("timestamp_ns must be positive")
        return v

    def capture_time_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ns / 1e9, tz=timezone.utc)


class SyncedSensorBundle(BaseModel):
    class Config:
        frozen = True
        arbitrary_types_allowed = True

    batch_id: str
    reference_timestamp_ns: int
    chainage_m: float
    frames: dict[SensorType, RawSensorFrame] = Field(default_factory=dict)
    max_intra_bundle_skew_ns: int = 0


class SpatialContext(BaseModel):
    chainage_str: str
    track_id: str


class TensorPayload(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    rgb_tensor_shape: Optional[tuple[int, int, int, int]] = None
    thermal_tensor_shape: Optional[tuple[int, int, int, int]] = None
    lidar_tensor_shape: Optional[tuple[int, int]] = None
    dtype: str = "float32"
    normalized: bool = True
    arrays: dict[str, Any] = Field(default_factory=dict, exclude=True)


class AIInputPackage(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    batch_id: str
    spatial_context: Optional[SpatialContext] = None
    tensor_payload: Optional[TensorPayload] = None
    pipeline_status: PipelineStatus
    rejected_reason: Optional[str] = None
    action_taken: Optional[str] = None
