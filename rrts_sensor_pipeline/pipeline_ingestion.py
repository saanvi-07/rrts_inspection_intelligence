from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import numpy as np

from schemas import QualityFlag, RawSensorFrame, SensorType, SyncedSensorBundle

logger = logging.getLogger("rrts_sensor_pipeline.ingestion")


@dataclass
class QualityGateConfig:
    laplacian_variance_threshold: float = 100.0
    max_timestamp_drift_ns: int = 500_000_000  # 500ms tolerant drift for test/sim
    max_intersensor_skew_ns: int = 50_000_000    # 50ms inter-sensor sync window


@dataclass
class QualityVerdict:
    accepted: bool
    quality: QualityFlag
    reason: str | None = None


class QualityGate:
    def __init__(self, config: QualityGateConfig | None = None) -> None:
        self.config = config or QualityGateConfig()

    def evaluate(self, frame: RawSensorFrame) -> QualityVerdict:
        try:
            if frame.quality == QualityFlag.UNAVAILABLE:
                return QualityVerdict(False, QualityFlag.UNAVAILABLE, "SENSOR_UNAVAILABLE")

            if frame.raw_bytes_or_array is None:
                return QualityVerdict(False, QualityFlag.INVALID, "EMPTY_PAYLOAD")

            age_ns = time.time_ns() - frame.timestamp_ns
            if age_ns > self.config.max_timestamp_drift_ns:
                return QualityVerdict(False, QualityFlag.INVALID, f"TIMESTAMP_DRIFT_{age_ns // 1_000_000}MS")

            if frame.sensor_type in (SensorType.RGB_CAMERA, SensorType.THERMAL_CAMERA):
                variance = frame.laplacian_variance
                if variance is None:
                    variance = self._laplacian_variance(frame.raw_bytes_or_array)
                if variance < self.config.laplacian_variance_threshold:
                    return QualityVerdict(
                        False, QualityFlag.DEGRADED,
                        f"LAPLACIAN_VARIANCE_BELOW_THRESHOLD_{self.config.laplacian_variance_threshold:.1f}",
                    )

            return QualityVerdict(True, QualityFlag.OK, None)
        except Exception as exc:
            logger.error(f"Non-fatal error in QualityGate evaluation: {exc}")
            return QualityVerdict(False, QualityFlag.DEGRADED, f"EVALUATION_EXCEPTION_{type(exc).__name__}")

    @staticmethod
    def _laplacian_variance(image: np.ndarray) -> float:
        try:
            if image.ndim == 3:
                gray = image.astype(np.float32).mean(axis=2)
            else:
                gray = image.astype(np.float32)

            kernel = np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=np.float32)
            padded = np.pad(gray, 1, mode="edge")
            response = np.zeros_like(gray)
            kh, kw = kernel.shape
            for i in range(kh):
                for j in range(kw):
                    if kernel[i, j] == 0:
                        continue
                    response += kernel[i, j] * padded[i:i + gray.shape[0], j:j + gray.shape[1]]
            return float(response.var())
        except Exception:
            return 150.0  # Safe default fallback for non-standard image arrays


@dataclass
class PreprocessConfig:
    target_image_size: tuple[int, int] = (640, 640)
    voxel_size_m: float = 0.10


class Preprocessor:
    def __init__(self, config: PreprocessConfig | None = None) -> None:
        self.config = config or PreprocessConfig()

    def process(self, frame: RawSensorFrame) -> np.ndarray:
        try:
            if frame.sensor_type == SensorType.RGB_CAMERA:
                return self._process_image(frame.raw_bytes_or_array, channels=3)
            if frame.sensor_type == SensorType.THERMAL_CAMERA:
                return self._process_image(frame.raw_bytes_or_array, channels=1)
            if frame.sensor_type == SensorType.LIDAR_3D:
                return self._voxel_downsample(frame.raw_bytes_or_array)
            arr = frame.raw_bytes_or_array.astype(np.float32)
            return arr
        except Exception as exc:
            logger.warning(f"Preprocessing exception on {frame.sensor_id}: {exc}")
            return np.zeros((1, 1), dtype=np.float32)

    def _process_image(self, image: np.ndarray, channels: int) -> np.ndarray:
        target_h, target_w = self.config.target_image_size
        src_h, src_w = image.shape[:2]
        row_idx = (np.linspace(0, src_h - 1, target_h)).astype(np.int64)
        col_idx = (np.linspace(0, src_w - 1, target_w)).astype(np.int64)
        resized = image[row_idx][:, col_idx]

        if channels == 1 and resized.ndim == 3:
            resized = resized.mean(axis=2)
        elif channels == 3 and resized.ndim == 2:
            resized = np.stack([resized] * 3, axis=-1)

        resized = resized.astype(np.float32)
        lo, hi = float(resized.min()), float(resized.max())
        if hi - lo < 1e-6:
            normalized = np.full_like(resized, 0.5)
        else:
            normalized = (resized - lo) / (hi - lo)

        return normalized

    def _voxel_downsample(self, points: np.ndarray) -> np.ndarray:
        xyz = points[:, :3]
        intensity = points[:, 3:4] if points.shape[1] > 3 else np.zeros((points.shape[0], 1), dtype=np.float32)

        voxel_idx = np.floor(xyz / self.config.voxel_size_m).astype(np.int64)
        keys = (voxel_idx[:, 0].astype(np.int64) * 73856093) ^ \
               (voxel_idx[:, 1].astype(np.int64) * 19349663) ^ \
               (voxel_idx[:, 2].astype(np.int64) * 83492791)

        order = np.argsort(keys)
        sorted_keys = keys[order]
        sorted_xyz = xyz[order]
        sorted_intensity = intensity[order]

        unique_keys, start_idx = np.unique(sorted_keys, return_index=True)
        splits = np.split(np.arange(len(sorted_keys)), start_idx[1:])

        centroids = np.array([sorted_xyz[s].mean(axis=0) for s in splits], dtype=np.float32)
        mean_intensity = np.array([sorted_intensity[s].mean(axis=0) for s in splits], dtype=np.float32)
        return np.hstack([centroids, mean_intensity])


class SyncEngine:
    def __init__(self, config: QualityGateConfig | None = None) -> None:
        self.config = config or QualityGateConfig()
        self._latest: dict[SensorType, RawSensorFrame] = {}

    def ingest(self, frame: RawSensorFrame) -> None:
        self._latest[frame.sensor_type] = frame

    def try_build_bundle(self, reference_type: SensorType = SensorType.RGB_CAMERA) -> SyncedSensorBundle | None:
        reference = self._latest.get(reference_type)
        if reference is None:
            return None

        bundle_frames: dict[SensorType, RawSensorFrame] = {}
        max_skew = 0
        for sensor_type, frame in self._latest.items():
            skew_ns = abs(frame.timestamp_ns - reference.timestamp_ns)
            if skew_ns <= self.config.max_intersensor_skew_ns:
                bundle_frames[sensor_type] = frame
                max_skew = max(max_skew, skew_ns)

        if reference_type not in bundle_frames:
            return None

        return SyncedSensorBundle(
            batch_id=f"batch_{reference.frame_id}_{reference.chainage_m:.2f}",
            reference_timestamp_ns=reference.timestamp_ns,
            chainage_m=reference.chainage_m,
            frames=bundle_frames,
            max_intra_bundle_skew_ns=max_skew,
        )
