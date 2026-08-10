import time
import json
import random
import logging
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [RobotPipeline] %(message)s")

class RobotSensorPipeline:
    def __init__(self, ai_service_url="http://127.0.0.1:8001/api/v1/ai/process-frame",
                 backend_telemetry_url="http://127.0.0.1:8000/api/v1/ingest/telemetry"):
        self.robot_id = "ROBOT-01"
        self.line_id = "LINE_ALPHA"
        self.section = "SECT-04"
        self.current_m = 435.0  # 12+435
        self.speed_ms = 0.85
        self.sys_temp = 42.1
        self.battery = 98.5
        self.seq_num = 1000
        
        self.ai_service_url = ai_service_url
        self.backend_telemetry_url = backend_telemetry_url
        self.buffer = []
        self.max_buffer_size = 500

    def format_chainage(self, meters):
        km = int(meters // 1000)
        rem = int(meters % 1000)
        return f"{km}+{rem:03d}"

    def capture_sensor_frame(self):
        self.seq_num += 1
        self.current_m += self.speed_ms * 1.0
        if self.current_m > 600.0:
            self.current_m = 200.0
            
        self.sys_temp += random.uniform(-0.1, 0.1)
        self.sys_temp = round(max(35.0, min(55.0, self.sys_temp)), 1)
        self.battery = round(max(10.0, self.battery - 0.01), 1)

        timestamp = datetime.now(timezone.utc).isoformat()
        chainage_str = self.format_chainage(self.current_m)

        telemetry = {
            "seq_num": self.seq_num,
            "robot_id": self.robot_id,
            "line_id": self.line_id,
            "section": self.section,
            "chainage": chainage_str,
            "chainage_meters": round(self.current_m, 2),
            "speed_ms": round(self.speed_ms + random.uniform(-0.02, 0.02), 2),
            "sys_temp_c": self.sys_temp,
            "battery_pct": self.battery,
            "telemetry_latency_ms": random.randint(110, 140),
            "status": "ACTIVE",
            "timestamp": timestamp
        }

        is_anomaly_candidate = random.random() < 0.35
        
        sensor_frame = {
            "seq_num": self.seq_num,
            "robot_id": self.robot_id,
            "line_id": self.line_id,
            "chainage": chainage_str,
            "chainage_meters": round(self.current_m, 2),
            "timestamp": timestamp,
            "camera_id": "CAM_01_FORWARD_OPTIC",
            "thermal_camera_id": "CAM_02_THERMAL",
            "candidate_flag": is_anomaly_candidate,
            "optical_data_ref": f"frame_{self.seq_num}_vis.jpg",
            "thermal_data_ref": f"frame_{self.seq_num}_therm.jpg",
            "lidar_point_count": random.randint(120000, 150000),
            "vibration_g": round(random.uniform(0.01, 0.45), 3),
            "thermal_peak_c": round(self.sys_temp + (120.0 if is_anomaly_candidate else 5.0), 1)
        }

        return telemetry, sensor_frame

    def transmit_telemetry(self, telemetry):
        try:
            resp = requests.post(self.backend_telemetry_url, json=telemetry, timeout=2.0)
            return resp.status_code == 200
        except Exception:
            return False

    def transmit_to_ai(self, sensor_frame):
        try:
            resp = requests.post(self.ai_service_url, json=sensor_frame, timeout=3.0)
            return resp.status_code == 200
        except Exception:
            return False

    def step(self):
        telemetry, sensor_frame = self.capture_sensor_frame()
        sent_telemetry = self.transmit_telemetry(telemetry)
        if not sent_telemetry and len(self.buffer) < self.max_buffer_size:
            self.buffer.append(("telemetry", telemetry))

        if sensor_frame["candidate_flag"]:
            sent_ai = self.transmit_to_ai(sensor_frame)
            if not sent_ai and len(self.buffer) < self.max_buffer_size:
                self.buffer.append(("ai_frame", sensor_frame))

if __name__ == "__main__":
    p = RobotSensorPipeline()
    print("Executing 3 test steps...")
    for _ in range(3):
        p.step()
        time.sleep(0.5)
    print("Pipeline test complete.")
