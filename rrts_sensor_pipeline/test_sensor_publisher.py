from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field

import numpy as np

from schemas import QualityFlag, RawSensorFrame, SensorType

logger = logging.getLogger("rrts_sensor_pipeline.publisher")


class TopicBus:
    def __init__(self, max_queue_size: int = 256) -> None:
        self._topics: dict[str, asyncio.Queue] = {}
        self._max_queue_size = max_queue_size

    def _queue_for(self, topic: str) -> asyncio.Queue:
        if topic not in self._topics:
            self._topics[topic] = asyncio.Queue(maxsize=self._max_queue_size)
        return self._topics[topic]

    async def publish(self, topic: str, message: RawSensorFrame) -> None:
        queue = self._queue_for(topic)
        if queue.full():
            _ = queue.get_nowait()
            logger.warning("Topic '%s' saturated — dropped oldest frame.", topic)
        await queue.put(message)

    async def subscribe(self, topic: str) -> RawSensorFrame:
        return await self._queue_for(topic).get()

    def topics(self) -> list[str]:
        return list(self._topics.keys())


@dataclass
class FailureInjectionConfig:
    frame_drop_probability: float = 0.0
    blur_probability: float = 0.0
    timestamp_jitter_ns: int = 0
    disconnect_after_n_frames: int | None = None


@dataclass
class SensorChannelConfig:
    sensor_id: str
    sensor_type: SensorType
    rate_hz: float
    topic: str
    failure: FailureInjectionConfig = field(default_factory=FailureInjectionConfig)


def _gen_rgb_frame(blurred: bool) -> np.ndarray:
    if blurred:
        return np.full((1080, 1920, 3), fill_value=128, dtype=np.uint8)
    return np.random.randint(0, 255, size=(1080, 1920, 3), dtype=np.uint8)


def _gen_thermal_frame() -> np.ndarray:
    return (np.random.normal(loc=2500, scale=300, size=(240, 320)).astype(np.int16))


def _gen_lidar_points(n_points: int = 4096) -> np.ndarray:
    xyz = np.random.uniform(low=-15.0, high=15.0, size=(n_points, 3)).astype(np.float32)
    intensity = np.random.uniform(0.0, 1.0, size=(n_points, 1)).astype(np.float32)
    return np.hstack([xyz, intensity])


def _gen_imu_vector() -> np.ndarray:
    return np.array(
        [random.gauss(0, 0.05), random.gauss(0, 0.05), random.gauss(9.81, 0.02),
         random.gauss(0, 0.01), random.gauss(0, 0.01), random.gauss(0, 0.01)],
        dtype=np.float32,
    )


class SyntheticSensorPublisher:
    def __init__(self, bus: TopicBus, channels: list[SensorChannelConfig]) -> None:
        self._bus = bus
        self._channels = channels
        self._chainage_m = 12435.0
        self._chainage_rate_m_s = 0.8

    async def run(self, duration_s: float) -> None:
        tasks = [asyncio.create_task(self._run_channel(ch, duration_s)) for ch in self._channels]
        await asyncio.gather(*tasks)

    async def _run_channel(self, ch: SensorChannelConfig, duration_s: float) -> None:
        period_s = 1.0 / ch.rate_hz
        frame_id = 0
        start = time.monotonic()
        while (time.monotonic() - start) < duration_s:
            frame_id += 1

            if ch.failure.disconnect_after_n_frames is not None and frame_id > ch.failure.disconnect_after_n_frames:
                logger.info("[%s] simulated permanent disconnect after %d frames.", ch.sensor_id, frame_id - 1)
                return

            if random.random() < ch.failure.frame_drop_probability:
                await asyncio.sleep(period_s)
                continue

            blurred = random.random() < ch.failure.blur_probability
            timestamp_ns = time.time_ns() + random.randint(
                -ch.failure.timestamp_jitter_ns, ch.failure.timestamp_jitter_ns
            ) if ch.failure.timestamp_jitter_ns else time.time_ns()

            frame = self._build_frame(ch, frame_id, timestamp_ns, blurred)
            await self._bus.publish(ch.topic, frame)
            await asyncio.sleep(period_s)

    def _build_frame(self, ch: SensorChannelConfig, frame_id: int, timestamp_ns: int, blurred: bool) -> RawSensorFrame:
        self._chainage_m += self._chainage_rate_m_s * (1.0 / ch.rate_hz)

        if ch.sensor_type == SensorType.RGB_CAMERA:
            array = _gen_rgb_frame(blurred)
            laplacian_variance = float(np.var(np.diff(array.astype(np.float32), axis=0))) if not blurred else 8.0
            calibration = [[1050.0, 0, 960.0], [0, 1050.0, 540.0], [0, 0, 1.0]]
        elif ch.sensor_type == SensorType.THERMAL_CAMERA:
            array = _gen_thermal_frame()
            laplacian_variance = None
            calibration = None
        elif ch.sensor_type == SensorType.LIDAR_3D:
            array = _gen_lidar_points()
            laplacian_variance = None
            calibration = None
        elif ch.sensor_type == SensorType.IMU:
            array = _gen_imu_vector()
            laplacian_variance = None
            calibration = None
        else:
            array = np.array([self._chainage_m], dtype=np.float64)
            laplacian_variance = None
            calibration = None

        return RawSensorFrame(
            timestamp_ns=timestamp_ns,
            sensor_id=ch.sensor_id,
            sensor_type=ch.sensor_type,
            frame_id=frame_id,
            chainage_m=round(self._chainage_m, 3),
            quality=QualityFlag.OK,
            raw_bytes_or_array=array,
            calibration_matrix=calibration,
            laplacian_variance=laplacian_variance,
        )


def default_rrts_channel_set() -> list[SensorChannelConfig]:
    return [
        SensorChannelConfig("cam_front_optical_01", SensorType.RGB_CAMERA, rate_hz=20.0, topic="/inspection/camera/front"),
        SensorChannelConfig("cam_front_thermal_01", SensorType.THERMAL_CAMERA, rate_hz=10.0, topic="/inspection/thermal/front"),
        SensorChannelConfig("lidar_main_01", SensorType.LIDAR_3D, rate_hz=10.0, topic="/inspection/lidar/scan"),
        SensorChannelConfig("imu_chassis_01", SensorType.IMU, rate_hz=30.0, topic="/inspection/imu/data"),
        SensorChannelConfig("encoder_odom_01", SensorType.ENCODER_ODOMETRY, rate_hz=20.0, topic="/inspection/odom/chainage"),
    ]
