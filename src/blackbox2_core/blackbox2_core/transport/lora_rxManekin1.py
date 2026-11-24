#!/usr/bin/env python3
"""
LoRa P2P Receiver Standalone (Pure Python, Tanpa ROS 2) - RAK3272S + Direct Web Forwarder + Local Communication Logger
(1 File Standalone)

Fitur:
  - Mandiri 1 file utuh tanpa ROS 2 (cukup Python 3 + pyserial: `python3 lora_rxManekin1.py`).
  - Sinkron dengan parameter radio lora_txManekin1.py (923.2 MHz, SF6, BW 500 kHz, CR 4/5, Preamble 8).
  - Menerima paket 1-uplink langsung (B2/Z2, JSON*CRC16, Alert*CRC16) maupun frame FRG:i/N:...*CRC16 (kompatibilitas) + validasi CRC16-CCITT.
  - Mendekode 4 jenis payload dari TX:
      1. Biner terkompresi/ringkas B2 & Z2 1-uplink (`packed_b2` / `packed_z2`, <= 238 byte untuk seluruh 109 parameter)
      2. Array JSON 109 parameter sensor (`payload_mode == "array"`, mendukung nilai null)
      3. Web Compact V2 JSON (`payload_mode == "web_v2"`, readingId 1001..1095 / 2001..2095)
      4. Alert kompak 12-byte ASCII ("A1" + seq + severity + event + status + offline_mask)
  - MENYIMPAN DATA KOMUNIKASI LORA & TELEMETRI SECARA LOKAL DI RASPBERRY PI:
      Folder default: <folder_script>/logs/communication/ (atau env `LORA_LOG_DIR` / flag `--log-dir`):
      1. lora_rx_communication.csv : RSSI (dBm), SNR (dB), ukuran byte, fragmen, waktu reassembly (ms),
                                     interval antar-paket / jitter (ms), packet rate (Hz), status CRC16,
                                     total CRC error, frekuensi, SF, BW, CR, jumlah sensor valid, & status ACK Web.
      2. lora_rx_telemetry.csv     : Timestamp, RSSI, SNR, ukuran byte + seluruh 109 kolom nilai sensor.
      3. lora_rx_alerts.csv        : Timestamp, RSSI, SNR + detail alert kompak 12-byte yang diterima.
  - LANGSUNG MENGIRIM KE WEB BACKEND (Be-smart-manekin) pada background thread non-blocking:
      1. Jalur Utama MQTTS Compact V2 (TLS terverifikasi, QoS 1, retain=false):
         Default: mqtts://mannequin-mqtt.stas-rg.com:18883
         (env `WEB_MQTT_URL` / `WEB_MQTT_HOST` / `WEB_MQTT_PORT` / `WEB_MQTT_USER` / `WEB_MQTT_PASS` / `WEB_MQTT_CLIENT_ID`).
         Publish ke topic `smart-mannequin/{device_id}/telemetry`
         & subscribe ACK `smart-mannequin/{device_id}/ack`.
      2. Jalur Opsional HTTP/HTTPS REST Ingest (`http.client` Keep-Alive, default nonaktif):
         Aktifkan dengan `WEB_HTTP_ENABLED=true` atau `--http-enabled true`.
         Default: https://dev-mannequin-api.stas-rg.com (env `WEB_HTTP_URL` / `--http-base-url`).
"""

from collections import deque
import csv
from dataclasses import dataclass
from datetime import datetime
import http.client
import json
import math
import os
import signal
import socket
import ssl
import struct
import sys
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlsplit
import zlib

import serial

try:
    import paho.mqtt.client as paho_mqtt  # type: ignore
except ImportError:
    paho_mqtt = None


# ============================================================
# KONFIGURASI RADIO RAK3272S (IDENTIK lora_txManekin1.py)
# ============================================================

DEFAULT_SERIAL_PORT = "/dev/ttyAMA0"
FALLBACK_PORTS = ("/dev/ttyAMA0", "/dev/serial0", "/dev/ttyAMA3", "/dev/ttyUSB0")

DEFAULT_BAUDRATE = 115200
DEFAULT_FREQUENCY = 923200000
DEFAULT_SPREADING_FACTOR = 6
DEFAULT_BANDWIDTH = 2
DEFAULT_CODING_RATE = 0
DEFAULT_PREAMBLE = 8
DEFAULT_TX_POWER = 20

# ============================================================
# KONFIGURASI LOG LOKAL KOMUNIKASI LORA DI RASPBERRY PI
# ============================================================

DEFAULT_LOG_ENABLED = True


def _resolve_default_log_dir() -> str:
    env_dir = (
        os.environ.get("LORA_LOG_DIR", "").strip()
        or os.environ.get("WEB_LOG_DIR", "").strip()
    )
    if env_dir:
        return os.path.expanduser(env_dir)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(script_dir) == "transport":
        repo_root = os.path.abspath(os.path.join(script_dir, "..", "..", "..", ".."))
        if os.path.isdir(os.path.join(repo_root, "src")):
            return os.path.join(repo_root, "logs", "communication")

    # Standalone mode: simpan langsung di folder logs/communication sejajar dengan file rx ini
    return os.path.join(script_dir, "logs", "communication")


DEFAULT_LOG_DIR = _resolve_default_log_dir()

# ============================================================
# KONFIGURASI DEFAULT PENGIRIMAN KE WEB (Be-smart-manekin)
# ============================================================

DEFAULT_API_BASE_URL = "https://dev-mannequin-api.stas-rg.com"
DEFAULT_MQTT_BROKER_URL_FALLBACK = "mqtts://mannequin-mqtt.stas-rg.com:18883"


def _parse_mqtt_broker_url(raw_url: str) -> Tuple[str, int, bool, str, str]:
    candidate = raw_url.strip() or DEFAULT_MQTT_BROKER_URL_FALLBACK
    if "://" not in candidate:
        candidate = f"mqtts://{candidate}"
    parsed = urlsplit(candidate)
    scheme = (parsed.scheme or "mqtts").lower()
    use_tls = scheme in ("mqtts", "ssl", "tls", "wss")
    host = parsed.hostname or "mannequin-mqtt.stas-rg.com"
    port = parsed.port or (18883 if use_tls else 1883)
    username = unquote(parsed.username) if parsed.username else ""
    password = unquote(parsed.password) if parsed.password else ""
    return host, port, use_tls, username, password


DEFAULT_MQTT_BROKER_URL = (
    os.environ.get("WEB_MQTT_URL", "").strip() or DEFAULT_MQTT_BROKER_URL_FALLBACK
)
_MQTT_URL_HOST, _MQTT_URL_PORT, _MQTT_URL_TLS, _MQTT_URL_USER, _MQTT_URL_PASS = (
    _parse_mqtt_broker_url(DEFAULT_MQTT_BROKER_URL)
)

DEFAULT_MQTT_ENABLED = (
    os.environ.get("WEB_MQTT_ENABLED", "true").strip().lower()
    in ("1", "true", "yes", "on")
)
DEFAULT_MQTT_HOST = os.environ.get("WEB_MQTT_HOST", "").strip() or _MQTT_URL_HOST
DEFAULT_MQTT_PORT = int(os.environ.get("WEB_MQTT_PORT", "").strip() or _MQTT_URL_PORT)
if "WEB_MQTT_TLS" in os.environ:
    DEFAULT_MQTT_TLS = os.environ.get("WEB_MQTT_TLS", "true").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
elif "WEB_MQTT_PORT" in os.environ and "WEB_MQTT_URL" not in os.environ and DEFAULT_MQTT_PORT == 1883:
    DEFAULT_MQTT_TLS = False
else:
    DEFAULT_MQTT_TLS = _MQTT_URL_TLS

DEFAULT_MQTT_CA_CERTS = os.environ.get("WEB_MQTT_CA_CERTS", "").strip()
DEFAULT_MQTT_USERNAME = (
    os.environ.get("WEB_MQTT_USER", "").strip() or _MQTT_URL_USER or "manekin_pi"
)
DEFAULT_MQTT_PASSWORD = (
    os.environ.get("WEB_MQTT_PASS", "")
    or os.environ.get("MQTT_PASSWORD", "")
    or _MQTT_URL_PASS
).strip()
DEFAULT_MQTT_CLIENT_ID = (
    os.environ.get("WEB_MQTT_CLIENT_ID", "").strip() or "manekin-rx-01"
)
DEFAULT_MQTT_ALERTS_ENABLED = (
    os.environ.get("WEB_MQTT_ALERTS_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)

DEFAULT_DEVICE_ID_M1 = (
    os.environ.get("WEB_DEVICE_ID_M1", "").strip() or "AD8121DAD12451E2"
)
DEFAULT_DEVICE_ID_M2 = (
    os.environ.get("WEB_DEVICE_ID_M2", "").strip() or "AD8121DAD12451E1"
)
DEFAULT_AI_WEB_ENABLED = (
    os.environ.get("WEB_AI_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)

DEFAULT_HTTP_ENABLED = (
    os.environ.get("WEB_HTTP_ENABLED", "false").strip().lower()
    in ("1", "true", "yes", "on")
)
DEFAULT_HTTP_BASE_URL = (
    os.environ.get("WEB_HTTP_URL", "").strip() or DEFAULT_API_BASE_URL
).rstrip("/")
DEFAULT_INGEST_API_KEY = (
    os.environ.get("WEB_INGEST_KEY", "") or os.environ.get("INGEST_API_KEY", "")
).strip()


# ============================================================
# SKEMA 109 PARAMETER SENSOR (URUTAN IDENTIK lora_txManekin1.py)
# ============================================================

SENSOR_GROUPS: Dict[str, Tuple[str, ...]] = {
    "arduino": (
        "lidar_raw",
        "lidar_median",
        "mq_nh3",
        "mq_co",
        "mq_co2",
        "smoke",
        "mpu_acc_x_raw",
        "mpu_acc_y_raw",
        "mpu_acc_z_raw",
        "mpu_gyro_x_raw",
        "mpu_gyro_y_raw",
        "mpu_gyro_z_raw",
        "mpu_acc_x_kalman",
        "mpu_acc_y_kalman",
        "mpu_acc_z_kalman",
        "mpu_gyro_x_kalman",
        "mpu_gyro_y_kalman",
        "mpu_gyro_z_kalman",
        "adxl_acc_x_raw",
        "adxl_acc_y_raw",
        "adxl_acc_z_raw",
        "adxl_acc_x_kalman",
        "adxl_acc_y_kalman",
        "adxl_acc_z_kalman",
    ),
    "aurora": (
        "accelx",
        "accely",
        "accelz",
        "gyrox",
        "gyroy",
        "gyroz",
        "bme_temp",
        "bme_hum",
        "bme_pres",
        "bme_alt",
        "sound_dbLeft",
        "sound_dbRight",
    ),
    "loadcell": (
        "loadcell_paka",
        "loadcell_paki",
        "loadcell_leher",
        "loadcell_kaka",
        "loadcell_kaki",
    ),
    "flex": tuple(f"flex{i}" for i in range(1, 17)),
    "mcp": tuple(f"mcp{i}" for i in range(1, 15)),
    "fsr": tuple(f"fsr{i}" for i in range(1, 15)),
    "piezo": tuple(f"piezo{i}" for i in range(1, 15)),
    "thermal": ("thermal",),
    "witmotion": (
        "ax",
        "ay",
        "az",
        "gx",
        "gy",
        "gz",
        "roll",
        "pitch",
        "yaw",
    ),
}

EXPECTED_KEYS: List[str] = [
    key for group_keys in SENSOR_GROUPS.values() for key in group_keys
]
NUM_PARAMS = len(EXPECTED_KEYS)  # 109 field


# ============================================================
# MAPPING WEB COMPACT V2 & POSISI ANATOMIS SMARTSKIN
# ============================================================

WEB_READING_ID_MAP_M1: Dict[str, int] = {
    "lidar_raw": 1001,
    "lidar_median": 1002,
    "mq_nh3": 1003,
    "mq_co": 1004,
    "mq_co2": 1005,
    "smoke": 1006,
    "mpu_acc_x_raw": 1007,
    "mpu_acc_y_raw": 1008,
    "mpu_acc_z_raw": 1009,
    "mpu_gyro_x_raw": 1010,
    "mpu_gyro_y_raw": 1011,
    "mpu_gyro_z_raw": 1012,
    "mpu_acc_x_kalman": 1013,
    "mpu_acc_y_kalman": 1014,
    "mpu_acc_z_kalman": 1015,
    "mpu_gyro_x_kalman": 1016,
    "mpu_gyro_y_kalman": 1017,
    "mpu_gyro_z_kalman": 1018,
    "adxl_acc_x_raw": 1019,
    "adxl_acc_y_raw": 1020,
    "adxl_acc_z_raw": 1021,
    "adxl_acc_x_kalman": 1022,
    "adxl_acc_y_kalman": 1023,
    "adxl_acc_z_kalman": 1024,
    "accelx": 1025,
    "accely": 1026,
    "accelz": 1027,
    "gyrox": 1028,
    "gyroy": 1029,
    "gyroz": 1030,
    "bme_temp": 1031,
    "bme_hum": 1032,
    "bme_pres": 1033,
    "bme_alt": 1034,
    "sound_dbLeft": 1035,
    "sound_dbRight": 1036,
    **{f"piezo{i}": 1036 + i for i in range(1, 15)},
    **{f"fsr{i}": 1050 + i for i in range(1, 15)},
    **{f"mcp{i}": 1064 + i for i in range(1, 15)},
    **{f"flex{i}": 1078 + i for i in range(1, 17)},
    "thermal": 1095,
    "loadcell_leher": 1098,
    "loadcell_paki": 1099,
    "loadcell_paka": 1100,
    "loadcell_kaki": 1101,
    "loadcell_kaka": 1102,
    "ax": 1103,
    "ay": 1104,
    "az": 1105,
    "gx": 1106,
    "gy": 1107,
    "gz": 1108,
    "roll": 1109,
    "pitch": 1110,
    "yaw": 1111,
}
REVERSE_WEB_ID_MAP: Dict[int, str] = {
    rid: key for key, rid in WEB_READING_ID_MAP_M1.items()
}

# Backend reading_identity (reading-id-reference.json) does not bind raw MPU gyro (1010..1012 / 2010..2012)
# or Mannequin 2 smoke (2006); omit them from Compact V2 web payloads so strict validation never rejects packets.
UNBOUND_WEB_READING_IDS = frozenset({1010, 1011, 1012, 2006, 2010, 2011, 2012})

# Urutan 18 fitur Edge AI LSTM-Autoencoder (Bit 14..10 dalam 2 byte AI frame B3/Z3)
AI_TARGET_FEATURES: List[str] = [
    "ADXL345_X", "ADXL345_Y", "ADXL345_Z",
    "Kalman_ADXL_X", "Kalman_ADXL_Y", "Kalman_ADXL_Z",
    "MPU6050_X", "MPU6050_Y", "MPU6050_Z",
    "Kalman_MPU_X", "Kalman_MPU_Y", "Kalman_MPU_Z",
    "LiDAR", "LiDAR_Kalman",
    "CO", "CO2", "NH3", "Gas_Sensor",
]

SMARTSKIN_14_LOCATIONS: Tuple[Tuple[str, int], ...] = (
    ("right arm", 1),
    ("right arm", 2),
    ("left arm", 1),
    ("left arm", 2),
    ("back", 1),
    ("back", 2),
    ("back", 3),
    ("back", 4),
    ("right leg", 1),
    ("right leg", 2),
    ("right leg", 3),
    ("left leg", 1),
    ("left leg", 2),
    ("left leg", 3),
)

SMARTSKIN_16_FLEX_LOCATIONS: Tuple[Tuple[str, int], ...] = (
    ("right shoulder", 1),
    ("right shoulder", 2),
    ("left shoulder", 1),
    ("left shoulder", 2),
    ("right elbow", 1),
    ("right elbow", 2),
    ("left elbow", 1),
    ("left elbow", 2),
    ("right waist", 1),
    ("right waist", 2),
    ("left waist", 1),
    ("left waist", 2),
    ("right knee", 1),
    ("right knee", 2),
    ("left knee", 1),
    ("left knee", 2),
)

LOADCELL_WEB_IDS: Tuple[Tuple[str, int], ...] = (
    ("loadcell_leher", 801),
    ("loadcell_paki", 802),
    ("loadcell_paka", 803),
    ("loadcell_kaki", 804),
    ("loadcell_kaka", 805),
)

# Pemetaan 50 parameter non-SmartSkin untuk 1 request POST /sensor-reading/batch per manekin
# (key, sensorType, location, sensorNumber, processingType)
MODULAR_WEB_BATCH_SPECS: Tuple[Tuple[str, str, str, int, str], ...] = (
    ("lidar_raw", "obstacle_distance", "chest", 901, "RAW"),
    ("lidar_median", "obstacle_distance", "chest", 901, "KALMAN_FILTERED"),
    ("mq_nh3", "gas_nh3", "chest", 101, "CALCULATED"),
    ("mq_co", "gas_co", "chest", 101, "CALCULATED"),
    ("mq_co2", "gas_co2", "chest", 101, "CALCULATED"),
    ("smoke", "gas_smoke", "chest", 101, "CALCULATED"),
    ("mpu_acc_x_raw", "x_acceleration", "chest", 202, "RAW"),
    ("mpu_acc_y_raw", "y_acceleration", "chest", 202, "RAW"),
    ("mpu_acc_z_raw", "z_acceleration", "chest", 202, "RAW"),
    ("mpu_gyro_x_raw", "angular_velocity_x", "chest", 202, "RAW"),
    ("mpu_gyro_y_raw", "angular_velocity_y", "chest", 202, "RAW"),
    ("mpu_gyro_z_raw", "angular_velocity_z", "chest", 202, "RAW"),
    ("mpu_acc_x_kalman", "x_acceleration", "chest", 202, "KALMAN_FILTERED"),
    ("mpu_acc_y_kalman", "y_acceleration", "chest", 202, "KALMAN_FILTERED"),
    ("mpu_acc_z_kalman", "z_acceleration", "chest", 202, "KALMAN_FILTERED"),
    ("mpu_gyro_x_kalman", "angular_velocity_x", "chest", 202, "KALMAN_FILTERED"),
    ("mpu_gyro_y_kalman", "angular_velocity_y", "chest", 202, "KALMAN_FILTERED"),
    ("mpu_gyro_z_kalman", "angular_velocity_z", "chest", 202, "KALMAN_FILTERED"),
    ("adxl_acc_x_raw", "x_acceleration", "chest", 201, "RAW"),
    ("adxl_acc_y_raw", "y_acceleration", "chest", 201, "RAW"),
    ("adxl_acc_z_raw", "z_acceleration", "chest", 201, "RAW"),
    ("adxl_acc_x_kalman", "x_acceleration", "chest", 201, "KALMAN_FILTERED"),
    ("adxl_acc_y_kalman", "y_acceleration", "chest", 201, "KALMAN_FILTERED"),
    ("adxl_acc_z_kalman", "z_acceleration", "chest", 201, "KALMAN_FILTERED"),
    ("accelx", "x_acceleration", "chest", 203, "RAW"),
    ("accely", "y_acceleration", "chest", 203, "RAW"),
    ("accelz", "z_acceleration", "chest", 203, "RAW"),
    ("gyrox", "angular_velocity_x", "chest", 203, "RAW"),
    ("gyroy", "angular_velocity_y", "chest", 203, "RAW"),
    ("gyroz", "angular_velocity_z", "chest", 203, "RAW"),
    ("bme_temp", "temperature", "chest", 1001, "RAW"),
    ("bme_hum", "relative_humidity", "chest", 1001, "RAW"),
    ("bme_pres", "barometric_pressure", "chest", 1001, "RAW"),
    ("bme_alt", "approximate_altitude", "chest", 1001, "CALCULATED"),
    ("sound_dbRight", "sound_intensity", "right ear", 601, "RAW"),
    ("sound_dbLeft", "sound_intensity", "left ear", 602, "RAW"),
    ("loadcell_leher", "joint_load", "neck", 801, "RAW"),
    ("loadcell_paki", "joint_load", "left thigh", 802, "RAW"),
    ("loadcell_paka", "joint_load", "right thigh", 803, "RAW"),
    ("loadcell_kaki", "joint_load", "left foot", 804, "RAW"),
    ("loadcell_kaka", "joint_load", "right foot", 805, "RAW"),
    ("ax", "x_acceleration", "chest", 1010, "RAW"),
    ("ay", "y_acceleration", "chest", 1010, "RAW"),
    ("az", "z_acceleration", "chest", 1010, "RAW"),
    ("gx", "angular_velocity_x", "chest", 1010, "RAW"),
    ("gy", "angular_velocity_y", "chest", 1010, "RAW"),
    ("gz", "angular_velocity_z", "chest", 1010, "RAW"),
    ("roll", "euler_angle", "chest", 1010, "RAW"),
    ("pitch", "euler_angle", "chest", 1010, "CALCULATED"),
    ("yaw", "euler_angle", "chest", 1010, "CALIBRATED_LINEAR"),
)

ALERT_SEVERITY_NAMES: Dict[int, str] = {
    0: "INFO",
    1: "WARNING",
    2: "ERROR",
    3: "CRITICAL",
}

ALERT_EVENT_NAMES: Dict[int, str] = {
    0x01: "SYSTEM_STATUS_CHANGED",
    0x02: "DEVICE_OFFLINE",
    0x03: "DEVICE_RECOVERED",
    0x04: "DEVICE_CALIBRATING",
    0x7F: "UNKNOWN",
}

ALERT_STATUS_NAMES: Dict[int, str] = {
    0: "INITIALIZING",
    1: "OPERATIONAL",
    2: "DEGRADED",
    3: "CALIBRATING",
}

ALERT_DEVICE_BITS: Tuple[Tuple[int, str], ...] = (
    (0, "arduino_uno_q"),
    (1, "cosmic_aurora"),
    (2, "analog_sensors"),
    (3, "mcp9808"),
    (4, "hx711_loadcells"),
    (5, "witmotion_wt901"),
)


# ============================================================
# LOGGER CSV LOKAL KOMUNIKASI LORA (~/blackbox2_ws/logs/communication)
# ============================================================

COMMUNICATION_CSV_HEADER: List[str] = [
    "timestamp",
    "packet_no",
    "mannequin_id",
    "packet_type",
    "payload_mode",
    "rssi_dbm",
    "snr_db",
    "freq_hz",
    "sf",
    "bw_code",
    "cr_code",
    "preamble",
    "bytes",
    "fragments",
    "reassembly_ms",
    "inter_packet_ms",
    "packet_rate_hz",
    "crc_status",
    "crc_hex",
    "crc_errors_total",
    "frag_timeouts_total",
    "valid_fields",
    "total_fields",
    "web_mqtt_sent",
    "web_ack_status",
    "web_http_sent",
    "ai_status",
    "ai_dominant_feature",
    "ai_rmse",
    "web_device_id",
]

TELEMETRY_CSV_HEADER: List[str] = [
    "timestamp",
    "packet_no",
    "mannequin_id",
    "payload_mode",
    "rssi_dbm",
    "snr_db",
    "bytes",
    "fragments",
    "reassembly_ms",
    "inter_packet_ms",
    "valid_fields",
    "ai_status",
    "ai_dominant_feature",
    "ai_rmse",
    *EXPECTED_KEYS,
]

ALERT_CSV_HEADER: List[str] = [
    "timestamp",
    "packet_no",
    "alert_no",
    "mannequin_id",
    "rssi_dbm",
    "snr_db",
    "bytes",
    "sequence",
    "severity",
    "code",
    "system_status",
    "offline_mask",
    "offline_devices",
    "raw_compact",
]


class LocalCommunicationLogger:
    """
    Menyimpan log komunikasi LoRa (RSSI, SNR, CRC, ukuran byte, fragmen, jitter,
    packet rate) serta data 109 sensor ke file CSV lokal di Raspberry Pi.
    """

    def __init__(self, enabled: bool, log_dir: str, logger):
        self.enabled = enabled
        self.log_dir = os.path.expanduser(log_dir)
        self.logger = logger
        self._lock = threading.Lock()

        self.comm_csv_path = os.path.join(self.log_dir, "lora_rx_communication.csv")
        self.telemetry_csv_path = os.path.join(self.log_dir, "lora_rx_telemetry.csv")
        self.alert_csv_path = os.path.join(self.log_dir, "lora_rx_alerts.csv")

        self._comm_file = None
        self._telemetry_file = None
        self._alert_file = None

        self._comm_writer = None
        self._telemetry_writer = None
        self._alert_writer = None

        if self.enabled:
            self._open_files()

    def _open_files(self) -> None:
        try:
            os.makedirs(self.log_dir, exist_ok=True)

            self._comm_file, self._comm_writer = self._init_csv(
                self.comm_csv_path, COMMUNICATION_CSV_HEADER
            )
            self._telemetry_file, self._telemetry_writer = self._init_csv(
                self.telemetry_csv_path, TELEMETRY_CSV_HEADER
            )
            self._alert_file, self._alert_writer = self._init_csv(
                self.alert_csv_path, ALERT_CSV_HEADER
            )
            self.logger.info(
                f"Local LoRa Communication CSV Logger aktif di: {self.log_dir}"
            )
        except Exception as exc:
            self.enabled = False
            self.logger.error(f"Gagal membuka file log komunikasi lokal: {exc}")

    @staticmethod
    def _init_csv(path: str, header: List[str]):
        file_obj = open(path, "a", newline="", encoding="utf-8", buffering=1)
        writer = csv.writer(file_obj)
        if os.path.getsize(path) == 0:
            writer.writerow(header)
            file_obj.flush()
        return file_obj, writer

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def log_communication(
        self,
        packet_no: int,
        mannequin_id: int,
        packet_type: str,
        payload_mode: str,
        rssi: str,
        snr: str,
        radio_cfg: "RadioConfig",
        byte_len: int,
        fragments: int,
        reassembly_ms: float,
        inter_packet_ms: Optional[float],
        packet_rate_hz: Optional[float],
        crc_status: str,
        crc_hex: str,
        crc_errors_total: int,
        frag_timeouts_total: int,
        valid_fields: int,
        total_fields: int,
        web_mqtt_sent: int,
        web_ack_status: str,
        web_http_sent: int,
        ai_detection: Optional[Dict[str, object]] = None,
        web_device_id: str = "",
    ) -> None:
        if not self.enabled or self._comm_writer is None:
            return

        ai_status = "" if not ai_detection else str(ai_detection.get("status", ""))
        ai_dom = "" if not ai_detection else str(ai_detection.get("dominant_feature", ""))
        ai_rmse = "" if not ai_detection else ai_detection.get("rmse", "")

        row = [
            self._now_str(),
            packet_no,
            mannequin_id,
            packet_type,
            payload_mode,
            rssi,
            snr,
            radio_cfg.freq,
            radio_cfg.sf,
            radio_cfg.bw,
            radio_cfg.cr,
            radio_cfg.preamble,
            byte_len,
            fragments,
            round(reassembly_ms, 2),
            "" if inter_packet_ms is None else round(inter_packet_ms, 2),
            "" if packet_rate_hz is None else round(packet_rate_hz, 3),
            crc_status,
            crc_hex,
            crc_errors_total,
            frag_timeouts_total,
            valid_fields,
            total_fields,
            web_mqtt_sent,
            web_ack_status,
            web_http_sent,
            ai_status,
            ai_dom,
            ai_rmse,
            web_device_id,
        ]
        with self._lock:
            try:
                self._comm_writer.writerow(row)
                if self._comm_file is not None:
                    self._comm_file.flush()
            except Exception as exc:
                self.logger.warning(f"Gagal menulis lora_rx_communication.csv: {exc}")

    def log_telemetry(
        self,
        packet_no: int,
        mannequin_id: int,
        payload_mode: str,
        rssi: str,
        snr: str,
        byte_len: int,
        fragments: int,
        reassembly_ms: float,
        inter_packet_ms: Optional[float],
        valid_fields: int,
        sensor_dict: Dict[str, Optional[float]],
        ai_detection: Optional[Dict[str, object]] = None,
    ) -> None:
        if not self.enabled or self._telemetry_writer is None:
            return

        ai_status = "" if not ai_detection else str(ai_detection.get("status", ""))
        ai_dom = "" if not ai_detection else str(ai_detection.get("dominant_feature", ""))
        ai_rmse = "" if not ai_detection else ai_detection.get("rmse", "")

        row = [
            self._now_str(),
            packet_no,
            mannequin_id,
            payload_mode,
            rssi,
            snr,
            byte_len,
            fragments,
            round(reassembly_ms, 2),
            "" if inter_packet_ms is None else round(inter_packet_ms, 2),
            valid_fields,
            ai_status,
            ai_dom,
            ai_rmse,
            *[
                "" if sensor_dict.get(k) is None else sensor_dict.get(k)
                for k in EXPECTED_KEYS
            ],
        ]
        with self._lock:
            try:
                self._telemetry_writer.writerow(row)
                if self._telemetry_file is not None:
                    self._telemetry_file.flush()
            except Exception as exc:
                self.logger.warning(f"Gagal menulis lora_rx_telemetry.csv: {exc}")

    def log_alert(
        self,
        packet_no: int,
        alert_no: int,
        mannequin_id: int,
        rssi: str,
        snr: str,
        byte_len: int,
        decoded_alert: Dict[str, object],
    ) -> None:
        if not self.enabled or self._alert_writer is None:
            return

        offline_devs = decoded_alert.get("offline_devices", [])
        offline_str = (
            ";".join(str(x) for x in offline_devs)
            if isinstance(offline_devs, list)
            else str(offline_devs)
        )
        row = [
            self._now_str(),
            packet_no,
            alert_no,
            mannequin_id,
            rssi,
            snr,
            byte_len,
            decoded_alert.get("sequence", ""),
            decoded_alert.get("severity", ""),
            decoded_alert.get("code", ""),
            decoded_alert.get("status", ""),
            decoded_alert.get("device_mask", ""),
            offline_str,
            decoded_alert.get("raw_compact", ""),
        ]
        with self._lock:
            try:
                self._alert_writer.writerow(row)
                if self._alert_file is not None:
                    self._alert_file.flush()
            except Exception as exc:
                self.logger.warning(f"Gagal menulis lora_rx_alerts.csv: {exc}")

    def close(self) -> None:
        with self._lock:
            for f in (self._comm_file, self._telemetry_file, self._alert_file):
                if f is not None:
                    try:
                        f.flush()
                        f.close()
                    except Exception:
                        pass
            self._comm_file = None
            self._telemetry_file = None
            self._alert_file = None


# ============================================================
# BUILT-IN PURE PYTHON MQTT v3.1.1 CLIENT (FALLBACK TANPA PIP)
# ============================================================

def _encode_mqtt_remaining_length(length: int) -> bytes:
    out = bytearray()
    while True:
        digit = length % 128
        length //= 128
        if length > 0:
            digit |= 0x80
        out.append(digit)
        if length <= 0:
            break
    return bytes(out)


def _encode_mqtt_utf8(text: str) -> bytes:
    raw = text.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def build_mqtt_ssl_context(ca_certs: str = "") -> ssl.SSLContext:
    """Membuat SSLContext dengan verifikasi sertifikat & hostname broker aktif."""
    cafile = ca_certs.strip() or None
    ssl_ctx = ssl.create_default_context(cafile=cafile)
    ssl_ctx.check_hostname = True
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    return ssl_ctx


MQTT_CONNACK_REASONS: Dict[int, str] = {
    1: "versi protokol MQTT tidak didukung",
    2: "client_id ditolak broker",
    3: "server broker tidak tersedia",
    4: "username/password MQTT salah",
    5: "tidak diizinkan (unauthorized/ACL)",
}


class SimpleSocketMqttClient:
    """Client MQTT v3.1.1 murni berbasis socket/TLS standar Python (tanpa instalasi pip)."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
        client_id: str = "manekin-rx-01",
        keepalive: int = 30,
        use_tls: bool = True,
        ca_certs: str = "",
        subscribe_topics: Tuple[str, ...] = ("smart-mannequin/+/ack",),
        on_message: Optional[Callable[[str, bytes], None]] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client_id = client_id
        self.keepalive = keepalive
        self.use_tls = use_tls
        self.ca_certs = ca_certs
        self.subscribe_topics = subscribe_topics
        self.on_message = on_message
        self._sock: Optional[socket.socket] = None
        self._pkt_id: int = 1
        self._last_io: float = 0.0

    @property
    def is_connected(self) -> bool:
        return self._sock is not None

    def _next_pkt_id(self) -> int:
        self._pkt_id = (self._pkt_id % 65535) + 1
        return self._pkt_id

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = sock.recv(size - len(chunks))
            if not chunk:
                raise ConnectionError("Koneksi MQTT ditutup oleh broker")
            chunks.extend(chunk)
        return bytes(chunks)

    def _read_packet(
        self, sock: socket.socket, first_byte_timeout: Optional[float] = None
    ) -> Tuple[int, int, bytes]:
        old_timeout = sock.gettimeout()
        if first_byte_timeout is not None:
            sock.settimeout(first_byte_timeout)
        try:
            first = sock.recv(1)
        finally:
            if first_byte_timeout is not None:
                sock.settimeout(old_timeout)
        if not first:
            raise ConnectionError("Koneksi MQTT ditutup oleh broker")

        b0 = first[0]
        pkt_type = b0 >> 4
        flags = b0 & 0x0F

        multiplier = 1
        rem_len = 0
        while True:
            b = self._recv_exact(sock, 1)[0]
            rem_len += (b & 0x7F) * multiplier
            if (b & 0x80) == 0:
                break
            multiplier *= 128
            if multiplier > 128 * 128 * 128:
                raise ConnectionError("MQTT Remaining Length tidak valid")

        payload = self._recv_exact(sock, rem_len) if rem_len > 0 else b""
        return pkt_type, flags, payload

    def _dispatch_incoming_publish(
        self, sock: socket.socket, flags: int, payload: bytes
    ) -> None:
        if len(payload) < 2:
            return
        topic_len = struct.unpack("!H", payload[:2])[0]
        pos = 2 + topic_len
        if len(payload) < pos:
            return
        topic = payload[2:pos].decode("utf-8", errors="ignore")
        qos = (flags >> 1) & 0x03
        if qos > 0:
            if len(payload) < pos + 2:
                return
            pkt_id = struct.unpack("!H", payload[pos:pos + 2])[0]
            pos += 2
            # Kirim PUBACK untuk pesan QoS 1 dari broker
            sock.sendall(b"\x40\x02" + struct.pack("!H", pkt_id))
            self._last_io = time.monotonic()

        if self.on_message is not None:
            self.on_message(topic, payload[pos:])

    def connect(self) -> bool:
        self.close()
        raw_sock = socket.create_connection((self.host, self.port), timeout=5.0)
        sock: socket.socket
        if self.use_tls:
            try:
                ssl_ctx = build_mqtt_ssl_context(self.ca_certs)
                sock = ssl_ctx.wrap_socket(raw_sock, server_hostname=self.host)
            except Exception:
                try:
                    raw_sock.close()
                except Exception:
                    pass
                raise
        else:
            sock = raw_sock
        sock.settimeout(3.0)

        connect_flags = 0x02  # Clean Session
        payload = _encode_mqtt_utf8(self.client_id)
        if self.username:
            connect_flags |= 0x80
            payload += _encode_mqtt_utf8(self.username)
            if self.password:
                connect_flags |= 0x40
                payload += _encode_mqtt_utf8(self.password)

        var_header = _encode_mqtt_utf8("MQTT") + struct.pack(
            "!BBH", 4, connect_flags, self.keepalive
        )
        body = var_header + payload
        packet = b"\x10" + _encode_mqtt_remaining_length(len(body)) + body
        sock.sendall(packet)

        pkt_type, _flags, connack = self._read_packet(sock)
        if pkt_type != 2 or len(connack) < 2 or connack[1] != 0:
            rc = connack[1] if len(connack) >= 2 else -1
            reason = MQTT_CONNACK_REASONS.get(rc, f"rc={rc}")
            sock.close()
            raise ConnectionError(f"MQTT CONNACK ditolak ({reason})")

        if self.subscribe_topics:
            sub_id = self._next_pkt_id()
            sub_body = bytearray(struct.pack("!H", sub_id))
            for topic in self.subscribe_topics:
                sub_body.extend(_encode_mqtt_utf8(topic))
                sub_body.append(1)  # QoS 1
            sub_pkt = b"\x82" + _encode_mqtt_remaining_length(len(sub_body)) + bytes(sub_body)
            sock.sendall(sub_pkt)
            ack_type, _ack_flags, _ack_body = self._read_packet(sock)
            if ack_type != 9:
                sock.close()
                raise ConnectionError(f"MQTT SUBACK tidak valid (pkt_type={ack_type})")

        self._sock = sock
        self._last_io = time.monotonic()
        return True

    def publish(
        self, topic: str, payload_str: str, qos: int = 1, retain: bool = False
    ) -> bool:
        if self._sock is None:
            self.connect()
        assert self._sock is not None

        topic_bytes = _encode_mqtt_utf8(topic)
        payload_bytes = payload_str.encode("utf-8")

        header_byte = 0x30 | ((1 if qos == 1 else 0) << 1) | (1 if retain else 0)
        if qos == 1:
            pkt_id = self._next_pkt_id()
            body = topic_bytes + struct.pack("!H", pkt_id) + payload_bytes
        else:
            pkt_id = 0
            body = topic_bytes + payload_bytes

        packet = bytes([header_byte]) + _encode_mqtt_remaining_length(len(body)) + body
        self._sock.sendall(packet)
        self._last_io = time.monotonic()

        if qos == 1:
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                rem = max(0.2, deadline - time.monotonic())
                pkt_type, flags, resp_body = self._read_packet(
                    self._sock, first_byte_timeout=rem
                )
                if pkt_type == 4 and len(resp_body) >= 2:
                    ack_id = struct.unpack("!H", resp_body[:2])[0]
                    if ack_id == pkt_id:
                        return True
                elif pkt_type == 3:
                    self._dispatch_incoming_publish(self._sock, flags, resp_body)
            raise TimeoutError(f"MQTT PUBACK timeout ({topic})")
        return True

    def poll_incoming(self, timeout: float = 0.02) -> None:
        if self._sock is None:
            return
        try:
            pkt_type, flags, body = self._read_packet(
                self._sock, first_byte_timeout=timeout
            )
            if pkt_type == 3:
                self._dispatch_incoming_publish(self._sock, flags, body)
        except (socket.timeout, TimeoutError, BlockingIOError):
            pass
        except Exception:
            self.close()

    def ping_if_needed(self) -> None:
        if self._sock is None:
            return
        if time.monotonic() - self._last_io >= (self.keepalive * 0.7):
            try:
                self._sock.sendall(b"\xC0\x00")
                deadline = time.monotonic() + 2.5
                while time.monotonic() < deadline:
                    pkt_type, flags, body = self._read_packet(
                        self._sock, first_byte_timeout=max(0.2, deadline - time.monotonic())
                    )
                    if pkt_type == 13:  # PINGRESP
                        self._last_io = time.monotonic()
                        return
                    if pkt_type == 3:
                        self._dispatch_incoming_publish(self._sock, flags, body)
                raise TimeoutError("PINGRESP timeout")
            except Exception:
                self.close()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.sendall(b"\xE0\x00")
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None


# ============================================================
# WEB TELEMETRY FORWARDER (NON-BLOCKING MQTTS V2 + OPSIONAL HTTP)
# ============================================================

@dataclass(frozen=True)
class WebForwarderConfig:
    """Konfigurasi pengiriman telemetry dari Receiver ke Web Backend (Be-smart-manekin)."""

    mqtt_enabled: bool = DEFAULT_MQTT_ENABLED
    mqtt_broker_url: str = DEFAULT_MQTT_BROKER_URL
    mqtt_host: str = DEFAULT_MQTT_HOST
    mqtt_port: int = DEFAULT_MQTT_PORT
    mqtt_tls: bool = DEFAULT_MQTT_TLS
    mqtt_ca_certs: str = DEFAULT_MQTT_CA_CERTS
    mqtt_username: str = DEFAULT_MQTT_USERNAME
    mqtt_password: str = DEFAULT_MQTT_PASSWORD
    mqtt_client_id: str = DEFAULT_MQTT_CLIENT_ID
    mqtt_alerts_enabled: bool = DEFAULT_MQTT_ALERTS_ENABLED
    device_id: str = ""
    device_id_m1: str = DEFAULT_DEVICE_ID_M1
    device_id_m2: str = DEFAULT_DEVICE_ID_M2
    ai_web_enabled: bool = DEFAULT_AI_WEB_ENABLED
    http_enabled: bool = DEFAULT_HTTP_ENABLED
    http_base_url: str = DEFAULT_HTTP_BASE_URL
    ingest_api_key: str = DEFAULT_INGEST_API_KEY
    http_timeout_sec: float = 2.5
    http_legacy_endpoints: bool = False

    def effective_mqtt_scheme(self) -> str:
        return "mqtts" if self.mqtt_tls else "mqtt"

    def resolve_device_id(self, mannequin_id: int) -> str:
        if mannequin_id == 1:
            return self.device_id_m1.strip() or DEFAULT_DEVICE_ID_M1
        if mannequin_id == 2:
            return self.device_id_m2.strip() or DEFAULT_DEVICE_ID_M2
        if self.device_id.strip():
            return self.device_id.strip()
        return self.device_id_m1.strip() or DEFAULT_DEVICE_ID_M1

    def mannequin_label_for_device(self, device_id: str) -> str:
        norm = (device_id or "").strip()
        if norm == (self.device_id_m1.strip() or DEFAULT_DEVICE_ID_M1):
            return "M1"
        if norm == (self.device_id_m2.strip() or DEFAULT_DEVICE_ID_M2):
            return "M2"
        return "-"

    def ack_topics(self) -> Tuple[str, ...]:
        seen: List[str] = []
        for dev in (
            self.device_id_m1.strip() or DEFAULT_DEVICE_ID_M1,
            self.device_id_m2.strip() or DEFAULT_DEVICE_ID_M2,
        ):
            topic = f"smart-mannequin/{dev}/ack"
            if topic not in seen:
                seen.append(topic)
        return tuple(seen)


class WebTelemetryForwarder:
    """
    Pengirim data dari Receiver LoRa ke Web Backend (Be-smart-manekin) di background thread:
      1. MQTTS Compact V2 -> topic `smart-mannequin/{deviceIdentifier}/telemetry` (QoS 1, retain=False)
         + subscribe Application ACK `smart-mannequin/{deviceIdentifier}/ack`
      2. HTTP REST Ingest (opsional) -> `POST /sensor-reading/batch`
    """

    def __init__(self, config: WebForwarderConfig, logger):
        self.config = config
        self.logger = logger
        self._queue: deque = deque(maxlen=64)
        self._cond = threading.Condition()
        self._running = True

        self.mqtt_sent_count = 0
        self.mqtt_ack_count = 0
        self.mqtt_fail_count = 0
        self.http_sent_count = 0
        self.http_fail_count = 0
        self.last_ack_status = "-"
        self._stats_lock = threading.Lock()
        self._stats_by_dev: Dict[str, Dict[str, object]] = {}

        self._paho_client = None
        self._paho_connected = False
        self._paho_connect_event = threading.Event()
        self._last_mqtt_error: str = ""
        self._mqtt_err_warn_at: float = -999.0
        self._simple_mqtt: Optional[SimpleSocketMqttClient] = None
        self._http_conn: Optional[http.client.HTTPConnection] = None
        self._http_auth_warn_at: float = -999.0
        self._http_err_warn_at: float = -999.0

        raw_http_url = (self.config.http_base_url or DEFAULT_API_BASE_URL).strip()
        parsed_url = urlsplit(
            raw_http_url if "://" in raw_http_url else f"https://{raw_http_url}"
        )
        self._http_scheme = (parsed_url.scheme or "https").lower()
        self._http_netloc = parsed_url.netloc or urlsplit(DEFAULT_API_BASE_URL).netloc
        self._http_base_path = parsed_url.path.rstrip("/")

        if self.config.mqtt_enabled:
            self._init_mqtt()

        self._worker = threading.Thread(
            target=self._worker_loop, name="web_telemetry_forwarder", daemon=True
        )
        self._worker.start()

    def _dev_stats_unlocked(self, device_id: str) -> Dict[str, object]:
        st = self._stats_by_dev.get(device_id)
        if st is None:
            st = {
                "device_id": device_id,
                "mqtt_sent": 0,
                "mqtt_fail": 0,
                "mqtt_ack": 0,
                "last_ack": "-",
                "http_sent": 0,
                "http_fail": 0,
            }
            self._stats_by_dev[device_id] = st
        return st

    def get_stats(self, mannequin_id: int) -> Dict[str, object]:
        device_id = self.config.resolve_device_id(mannequin_id)
        with self._stats_lock:
            return dict(self._dev_stats_unlocked(device_id))

    def _init_mqtt(self) -> None:
        client_id = (self.config.mqtt_client_id or "").strip() or "manekin-rx-01"
        scheme = self.config.effective_mqtt_scheme()
        if self.config.mqtt_username and not self.config.mqtt_password:
            self.logger.warning(
                f"Password MQTT untuk user '{self.config.mqtt_username}' belum diisi "
                f"(set env WEB_MQTT_PASS / MQTT_PASSWORD atau param ROS2 mqtt_password)."
            )

        if paho_mqtt is not None:
            try:
                try:
                    client = paho_mqtt.Client(
                        paho_mqtt.CallbackAPIVersion.VERSION2,
                        client_id=client_id,
                        transport="tcp",
                        protocol=paho_mqtt.MQTTv311,
                    )
                except AttributeError:
                    client = paho_mqtt.Client(
                        client_id=client_id,
                        transport="tcp",
                        protocol=paho_mqtt.MQTTv311,
                    )

                if self.config.mqtt_tls:
                    ssl_ctx = build_mqtt_ssl_context(self.config.mqtt_ca_certs)
                    client.tls_set_context(ssl_ctx)

                if self.config.mqtt_username:
                    client.username_pw_set(
                        self.config.mqtt_username,
                        self.config.mqtt_password or None,
                    )

                if hasattr(client, "reconnect_delay_set"):
                    client.reconnect_delay_set(min_delay=1, max_delay=30)

                client.on_connect = self._on_paho_connect
                client.on_disconnect = self._on_paho_disconnect
                client.on_message = self._on_paho_message
                client.connect_async(self.config.mqtt_host, self.config.mqtt_port, 30)
                client.loop_start()
                self._paho_client = client
                self.logger.info(
                    f"Web MQTT client (paho-mqtt) aktif -> "
                    f"{scheme}://{self.config.mqtt_host}:{self.config.mqtt_port} "
                    f"(client_id={client_id}, tls={self.config.mqtt_tls}, verify=True)"
                )
                return
            except Exception as exc:
                self.logger.warning(
                    f"Gagal inisialisasi paho-mqtt ({exc}), beralih ke socket MQTT bawaan."
                )

        self._simple_mqtt = SimpleSocketMqttClient(
            host=self.config.mqtt_host,
            port=self.config.mqtt_port,
            username=self.config.mqtt_username,
            password=self.config.mqtt_password,
            client_id=client_id,
            use_tls=self.config.mqtt_tls,
            ca_certs=self.config.mqtt_ca_certs,
            on_message=self._handle_ack_message,
            subscribe_topics=self.config.ack_topics(),
        )
        self.logger.info(
            f"Web MQTT client (built-in socket) siap -> "
            f"{scheme}://{self.config.mqtt_host}:{self.config.mqtt_port} "
            f"(client_id={client_id}, tls={self.config.mqtt_tls}, verify=True)"
        )

    def _on_paho_connect(self, client, userdata, flags, rc, *args) -> None:
        is_failure = bool(getattr(rc, "is_failure", False))
        rc_val = getattr(rc, "value", rc)
        if not is_failure and rc_val == 0:
            self._paho_connected = True
            self._last_mqtt_error = ""
            self._paho_connect_event.set()
            ack_topics = self.config.ack_topics()
            for ack_topic in ack_topics:
                client.subscribe(ack_topic, qos=1)
            self.logger.info(
                f"Terhubung ke Web MQTT Broker "
                f"({self.config.effective_mqtt_scheme()}://{self.config.mqtt_host}:{self.config.mqtt_port}) "
                f"[client_id={self.config.mqtt_client_id}] & subscribe {', '.join(ack_topics)}"
            )
        else:
            self._paho_connected = False
            reason = (
                MQTT_CONNACK_REASONS.get(rc_val, str(rc))
                if isinstance(rc_val, int)
                else str(rc)
            )
            self._last_mqtt_error = f"CONNACK ditolak ({reason})"
            self._paho_connect_event.set()
            self.logger.warning(f"Koneksi Web MQTT gagal: {self._last_mqtt_error}")

    def _on_paho_disconnect(self, client, userdata, *args) -> None:
        self._paho_connected = False

    def _handle_ack_message(self, _topic: str, payload_bytes: bytes) -> None:
        try:
            ack = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
            if isinstance(ack, dict):
                self.mqtt_ack_count += 1
                status = str(ack.get("status", "UNKNOWN"))
                self.last_ack_status = status
                dev_id = str(ack.get("deviceIdentifier") or "").strip()
                if not dev_id and _topic:
                    topic_parts = [p for p in str(_topic).split("/") if p]
                    if len(topic_parts) >= 3 and topic_parts[-1] == "ack":
                        dev_id = topic_parts[-2]
                if not dev_id:
                    dev_id = "-"
                mannequin_lbl = self.config.mannequin_label_for_device(dev_id)
                with self._stats_lock:
                    st = self._dev_stats_unlocked(dev_id)
                    st["mqtt_ack"] = int(st["mqtt_ack"]) + 1
                    st["last_ack"] = status
                seq = ack.get("s", "-")
                readings_cnt = ack.get("readingsCount", 0)
                if status == "REJECTED":
                    err_code = ack.get("errorCode", "UNKNOWN_ERROR")
                    self.logger.error(
                        f"[WEB-ACK REJECTED] [{mannequin_lbl}] dev={dev_id} s={seq} errorCode={err_code}"
                    )
                else:
                    self.logger.info(
                        f"[WEB-ACK #{self.mqtt_ack_count:04d}] [{mannequin_lbl}] dev={dev_id} s={seq} "
                        f"status={status} readings={readings_cnt}"
                    )
        except Exception:
            pass

    def _on_paho_message(self, client, userdata, msg) -> None:
        self._handle_ack_message(getattr(msg, "topic", ""), getattr(msg, "payload", b""))

    def enqueue_telemetry(
        self,
        mannequin_id: int,
        web_v2_obj: Dict[str, object],
        sensor_dict: Dict[str, Optional[float]],
    ) -> None:
        if not self.config.mqtt_enabled and not self.config.http_enabled:
            return
        with self._cond:
            self._queue.append(("telemetry", mannequin_id, web_v2_obj, sensor_dict))
            self._cond.notify()

    def enqueue_alert(self, mannequin_id: int, alert_payload: Dict[str, object]) -> None:
        if not (self.config.mqtt_enabled and self.config.mqtt_alerts_enabled) and not self.config.http_enabled:
            return
        with self._cond:
            self._queue.append(("alert", mannequin_id, alert_payload, None))
            self._cond.notify()

    def _worker_loop(self) -> None:
        while self._running:
            item = None
            with self._cond:
                if not self._queue:
                    self._cond.wait(timeout=1.0)
                if self._queue:
                    item = self._queue.popleft()

            if item is None:
                if self._simple_mqtt is not None:
                    self._simple_mqtt.poll_incoming()
                    self._simple_mqtt.ping_if_needed()
                continue

            kind, mannequin_id, payload_obj, sensor_dict = item
            device_id = self.config.resolve_device_id(mannequin_id)

            if kind == "telemetry":
                if self.config.mqtt_enabled:
                    self._send_mqtt_telemetry(device_id, payload_obj)
                if self.config.http_enabled and isinstance(sensor_dict, dict):
                    self._send_http_telemetry(mannequin_id, sensor_dict)
            elif kind == "alert":
                if self.config.mqtt_enabled and self.config.mqtt_alerts_enabled:
                    self._send_mqtt_alert(device_id, payload_obj)

    def _send_mqtt_telemetry(
        self, device_id: str, web_v2_obj: Dict[str, object]
    ) -> None:
        raw_readings = web_v2_obj.get("r", [])
        if isinstance(raw_readings, list):
            non_null = [
                pair for pair in raw_readings
                if isinstance(pair, (list, tuple)) and len(pair) >= 2 and pair[1] is not None
            ]
            outbound_r = non_null if non_null else raw_readings
        else:
            outbound_r = []

        if not outbound_r:
            return

        wire_obj = {
            "v": 2,
            "b": int(web_v2_obj.get("b", 1)),
            "s": int(web_v2_obj.get("s", 1)),
            "t": int(web_v2_obj.get("t", int(time.time() * 1000))),
            "r": outbound_r,
        }
        topic = f"smart-mannequin/{device_id}/telemetry"
        payload_str = json.dumps(wire_obj, separators=(",", ":"), allow_nan=False)

        ok = self._publish_mqtt_raw(topic, payload_str)
        if ok:
            self.mqtt_sent_count += 1
        else:
            self.mqtt_fail_count += 1
        with self._stats_lock:
            st = self._dev_stats_unlocked(device_id)
            if ok:
                st["mqtt_sent"] = int(st["mqtt_sent"]) + 1
            else:
                st["mqtt_fail"] = int(st["mqtt_fail"]) + 1

    def _send_mqtt_alert(
        self, device_id: str, alert_payload: Dict[str, object]
    ) -> None:
        topic = f"smart-mannequin/{device_id}/alerts"
        payload_str = json.dumps(alert_payload, separators=(",", ":"), allow_nan=False)
        self._publish_mqtt_raw(topic, payload_str)

    def _log_mqtt_warning_rate_limited(self, message: str) -> None:
        now = time.monotonic()
        if now - self._mqtt_err_warn_at >= 15.0:
            self._mqtt_err_warn_at = now
            self.logger.warning(message)

    def _publish_mqtt_raw(self, topic: str, payload_str: str) -> bool:
        if self._paho_client is not None:
            if not self._paho_connected and not self._paho_connect_event.is_set():
                self._paho_connect_event.wait(timeout=2.0)
            if not self._paho_connected:
                detail = (
                    self._last_mqtt_error
                    or "belum terhubung ke broker (periksa TLS/hostname/username/password)"
                )
                self._log_mqtt_warning_rate_limited(
                    f"Web MQTT publish tertunda/gagal ({topic}): {detail}"
                )
                return False
            try:
                info = self._paho_client.publish(topic, payload_str, qos=1, retain=False)
                return info.rc == 0
            except Exception as exc:
                self._log_mqtt_warning_rate_limited(
                    f"Web MQTT publish gagal ({topic}): {exc}"
                )
                return False

        if self._simple_mqtt is not None:
            try:
                return self._simple_mqtt.publish(topic, payload_str, qos=1, retain=False)
            except Exception as exc:
                self._simple_mqtt.close()
                self._log_mqtt_warning_rate_limited(
                    f"Web MQTT socket gagal ({self.config.effective_mqtt_scheme()}://"
                    f"{self.config.mqtt_host}:{self.config.mqtt_port}): {exc}"
                )
                return False
        return False

    def _get_http_connection(self) -> http.client.HTTPConnection:
        if self._http_conn is None:
            timeout = self.config.http_timeout_sec
            if self._http_scheme == "https":
                self._http_conn = http.client.HTTPSConnection(
                    self._http_netloc, timeout=timeout
                )
            else:
                self._http_conn = http.client.HTTPConnection(
                    self._http_netloc, timeout=timeout
                )
        return self._http_conn

    def _close_http_connection(self) -> None:
        if self._http_conn is not None:
            try:
                self._http_conn.close()
            except Exception:
                pass
            self._http_conn = None

    def _post_http_json(self, path: str, body_obj: object) -> Tuple[bool, bool]:
        """Mengirim JSON via HTTP/HTTPS Keep-Alive. Mengembalikan (sukses, auth_blocked)."""
        rel_path = path if path.startswith("/") else f"/{path}"
        full_path = f"{self._http_base_path}{rel_path}" or "/"
        url = f"{self._http_scheme}://{self._http_netloc}{full_path}"
        data = json.dumps(body_obj, separators=(",", ":"), allow_nan=False).encode("utf-8")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Connection": "keep-alive",
        }
        if self.config.ingest_api_key:
            headers["x-ingest-key"] = self.config.ingest_api_key

        last_exc: Optional[Exception] = None
        for attempt in range(2):
            conn = self._get_http_connection()
            try:
                conn.request("POST", full_path, body=data, headers=headers)
                resp = conn.getresponse()
                resp_body = resp.read()
                status = resp.status
                if 200 <= status < 300:
                    return True, False

                snippet = resp_body.decode("utf-8", errors="ignore")[:180].strip()
                now = time.monotonic()
                if status in (401, 403, 503):
                    if now - self._http_auth_warn_at >= 30.0:
                        self._http_auth_warn_at = now
                        self.logger.warning(
                            f"HTTP POST {url} ditolak ({status}: {snippet or 'Unauthorized'}). "
                            "Pastikan INGEST_API_KEY cocok: set parameter ROS2 "
                            "ingest_api_key:=<key> atau env WEB_INGEST_KEY=<key>."
                        )
                    return False, True

                if now - self._http_err_warn_at >= 15.0:
                    self._http_err_warn_at = now
                    self.logger.warning(
                        f"HTTP POST {url} ditolak ({status}: {snippet})"
                    )
                return False, False
            except (http.client.HTTPException, OSError) as exc:
                last_exc = exc
                self._close_http_connection()
                if attempt == 0:
                    continue
                break
            except Exception as exc:
                last_exc = exc
                self._close_http_connection()
                break

        now = time.monotonic()
        if now - self._http_err_warn_at >= 15.0:
            self._http_err_warn_at = now
            self.logger.warning(f"HTTP POST {url} gagal: {last_exc}")
        return False, False

    def _send_http_telemetry(
        self, mannequin_id: int, d: Dict[str, Optional[float]]
    ) -> None:
        endpoints: List[Tuple[str, object]] = []

        readings: List[Dict[str, object]] = []
        for idx, (loc, pt) in enumerate(SMARTSKIN_14_LOCATIONS, start=1):
            v_piezo = d.get(f"piezo{idx}")
            if v_piezo is not None:
                readings.append({
                    "sensorType": "vibration",
                    "location": loc,
                    "sensorNumber": pt,
                    "processingType": "RAW",
                    "value": v_piezo,
                })
            v_fsr = d.get(f"fsr{idx}")
            if v_fsr is not None:
                readings.append({
                    "sensorType": "pressure",
                    "location": loc,
                    "sensorNumber": pt,
                    "processingType": "CALIBRATED_LINEAR",
                    "value": v_fsr,
                })
            v_mcp = d.get(f"mcp{idx}")
            if v_mcp is not None:
                readings.append({
                    "sensorType": "temperature",
                    "location": loc,
                    "sensorNumber": pt,
                    "processingType": "RAW",
                    "value": v_mcp,
                })

        for idx, (loc, pt) in enumerate(SMARTSKIN_16_FLEX_LOCATIONS, start=1):
            v_flex = d.get(f"flex{idx}")
            if v_flex is not None:
                readings.append({
                    "sensorType": "flex",
                    "location": loc,
                    "sensorNumber": pt,
                    "processingType": "RAW",
                    "value": v_flex,
                })

        if d.get("thermal") is not None:
            readings.append({
                "sensorType": "thermal_temperature",
                "location": "chest",
                "sensorNumber": 1,
                "processingType": "RAW",
                "value": d["thermal"],
            })

        for key, sensor_type, location, sensor_num, proc_type in MODULAR_WEB_BATCH_SPECS:
            val = d.get(key)
            if val is not None:
                readings.append({
                    "sensorType": sensor_type,
                    "location": location,
                    "sensorNumber": sensor_num,
                    "processingType": proc_type,
                    "value": val,
                })

        if readings:
            endpoints.append((
                "/sensor-reading/batch",
                {"mannequinId": mannequin_id, "readings": readings},
            ))

        if self.config.http_legacy_endpoints:
            if d.get("lidar_raw") is not None or d.get("lidar_median") is not None:
                lidar_body: Dict[str, object] = {"sensor_id": 901, "mannequin_id": mannequin_id}
                if d.get("lidar_raw") is not None:
                    lidar_body["value"] = d["lidar_raw"]
                if d.get("lidar_median") is not None:
                    lidar_body["kalmanvalue"] = d["lidar_median"]
                endpoints.append(("/sensor/lidar", lidar_body))

            if any(d.get(k) is not None for k in ("mq_co", "mq_co2", "smoke", "mq_nh3")):
                mq_body: Dict[str, object] = {"sensor_id": 101, "mannequin_id": mannequin_id}
                if d.get("mq_nh3") is not None:
                    mq_body["value"] = d["mq_nh3"]
                if d.get("mq_co") is not None:
                    mq_body["co"] = d["mq_co"]
                if d.get("mq_co2") is not None:
                    mq_body["co2"] = d["mq_co2"]
                if d.get("smoke") is not None:
                    mq_body["smoke"] = d["smoke"]
                endpoints.append(("/sensor/mq", mq_body))

            if any(
                d.get(k) is not None
                for k in (
                    "adxl_acc_x_raw",
                    "adxl_acc_y_raw",
                    "adxl_acc_z_raw",
                    "adxl_acc_x_kalman",
                    "adxl_acc_y_kalman",
                    "adxl_acc_z_kalman",
                )
            ):
                adxl_body: Dict[str, object] = {"sensor_id": 201, "mannequin_id": mannequin_id}
                for src_k, dst_k in (
                    ("adxl_acc_x_raw", "x"),
                    ("adxl_acc_y_raw", "y"),
                    ("adxl_acc_z_raw", "z"),
                    ("adxl_acc_x_kalman", "x_kalman"),
                    ("adxl_acc_y_kalman", "y_kalman"),
                    ("adxl_acc_z_kalman", "z_kalman"),
                ):
                    if d.get(src_k) is not None:
                        adxl_body[dst_k] = d[src_k]
                endpoints.append(("/sensor/adxl", adxl_body))

            if any(d.get(k) is not None for k in ("bme_temp", "bme_pres", "bme_hum")):
                bme_body: Dict[str, object] = {"mannequin_id": mannequin_id}
                if d.get("bme_temp") is not None:
                    bme_body["temperature"] = d["bme_temp"]
                if d.get("bme_pres") is not None:
                    bme_body["pressure"] = d["bme_pres"]
                if d.get("bme_hum") is not None:
                    bme_body["humidity"] = d["bme_hum"]
                endpoints.append(("/sensor/bme", bme_body))

            for lc_key, lc_id in LOADCELL_WEB_IDS:
                if d.get(lc_key) is not None:
                    endpoints.append((
                        "/sensor/loadcell",
                        {"sensor_id": lc_id, "value": d[lc_key], "mannequin_id": mannequin_id},
                    ))

            if any(d.get(k) is not None for k in ("roll", "pitch", "yaw")):
                wit_body: Dict[str, object] = {"mannequin_id": mannequin_id}
                if d.get("roll") is not None:
                    wit_body["x_angle"] = d["roll"]
                if d.get("pitch") is not None:
                    wit_body["y_angle"] = d["pitch"]
                if d.get("yaw") is not None:
                    wit_body["z_angle"] = d["yaw"]
                endpoints.append(("/sensor/witsensor", wit_body))

            if d.get("sound_dbRight") is not None:
                endpoints.append((
                    "/sensor/ky",
                    {"sensor_id": 601, "value": d["sound_dbRight"], "mannequin_id": mannequin_id},
                ))
            if d.get("sound_dbLeft") is not None:
                endpoints.append((
                    "/sensor/ky",
                    {"sensor_id": 602, "value": d["sound_dbLeft"], "mannequin_id": mannequin_id},
                ))

        if not endpoints:
            return

        ok_any = False
        for path, body_obj in endpoints:
            ok, auth_blocked = self._post_http_json(path, body_obj)
            if ok:
                ok_any = True
            if auth_blocked:
                break

        device_id = self.config.resolve_device_id(mannequin_id)
        if ok_any:
            self.http_sent_count += 1
        else:
            self.http_fail_count += 1
        with self._stats_lock:
            st = self._dev_stats_unlocked(device_id)
            if ok_any:
                st["http_sent"] = int(st["http_sent"]) + 1
            else:
                st["http_fail"] = int(st["http_fail"]) + 1

    def close(self) -> None:
        self._running = False
        with self._cond:
            self._cond.notify_all()
        self._close_http_connection()
        if self._paho_client is not None:
            try:
                self._paho_client.loop_stop()
                self._paho_client.disconnect()
            except Exception:
                pass
        if self._simple_mqtt is not None:
            self._simple_mqtt.close()


# ============================================================
# KONFIGURASI & DRIVER SERIAL RAK3272S
# ============================================================

@dataclass(frozen=True)
class RadioConfig:
    """Parameter konfigurasi modul RAK3272S mode LoRa P2P."""

    port: str
    baudrate: int = DEFAULT_BAUDRATE
    freq: int = DEFAULT_FREQUENCY
    sf: int = DEFAULT_SPREADING_FACTOR
    bw: int = DEFAULT_BANDWIDTH
    cr: int = DEFAULT_CODING_RATE
    preamble: int = DEFAULT_PREAMBLE
    tx_power: int = DEFAULT_TX_POWER

    def p2p_command(self, bw_override: Optional[int] = None) -> str:
        bw = self.bw if bw_override is None else bw_override
        return (
            f"AT+P2P={self.freq}:{self.sf}:{bw}:"
            f"{self.cr}:{self.preamble}:{self.tx_power}"
        )


class StandaloneLogger:
    """Logger console standalone pengganti ROS 2 logger."""

    def __init__(self, name: str = "lora_rx_manekin"):
        self.name = name
        self._lock = threading.Lock()

    def _emit(self, level: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        stream = sys.stderr if level in ("WARN", "ERROR") else sys.stdout
        with self._lock:
            print(f"[{level}] [{ts}] [{self.name}]: {message}", file=stream, flush=True)

    def info(self, message: str) -> None:
        self._emit("INFO", str(message))

    def warning(self, message: str) -> None:
        self._emit("WARN", str(message))

    def error(self, message: str) -> None:
        self._emit("ERROR", str(message))


@dataclass
class _ParamValue:
    value: object


class StandaloneParamStore:
    """Penyimpan parameter CLI/env standalone (kompatibel flag --key val maupun -p key:=val)."""

    def __init__(self, cli_overrides: Optional[Dict[str, str]] = None):
        self._overrides: Dict[str, str] = dict(cli_overrides or {})
        self._values: Dict[str, object] = {}
        self._logger = StandaloneLogger()

    def get_logger(self) -> StandaloneLogger:
        return self._logger

    @staticmethod
    def _coerce_like(raw: str, default: object) -> object:
        if isinstance(default, bool):
            return raw.strip().lower() in ("1", "true", "yes", "on")
        if isinstance(default, int):
            return int(raw.strip())
        if isinstance(default, float):
            return float(raw.strip())
        return raw

    def declare_parameter(self, name: str, default: object) -> None:
        if name in self._overrides:
            try:
                self._values[name] = self._coerce_like(self._overrides[name], default)
                return
            except ValueError:
                pass
        self._values[name] = default

    def get_parameter(self, name: str) -> _ParamValue:
        return _ParamValue(self._values.get(name))


def declare_radio_config(node: StandaloneParamStore, default_port: str) -> RadioConfig:
    node.declare_parameter("port", default_port)
    node.declare_parameter("baudrate", DEFAULT_BAUDRATE)
    node.declare_parameter("freq", DEFAULT_FREQUENCY)
    node.declare_parameter("sf", DEFAULT_SPREADING_FACTOR)
    node.declare_parameter("bw", DEFAULT_BANDWIDTH)
    node.declare_parameter("cr", DEFAULT_CODING_RATE)
    node.declare_parameter("preamble", DEFAULT_PREAMBLE)
    node.declare_parameter("tx_power", DEFAULT_TX_POWER)

    return RadioConfig(
        port=str(node.get_parameter("port").value),
        baudrate=int(node.get_parameter("baudrate").value),
        freq=int(node.get_parameter("freq").value),
        sf=int(node.get_parameter("sf").value),
        bw=int(node.get_parameter("bw").value),
        cr=int(node.get_parameter("cr").value),
        preamble=int(node.get_parameter("preamble").value),
        tx_power=int(node.get_parameter("tx_power").value),
    )


def declare_web_config(node: StandaloneParamStore) -> WebForwarderConfig:
    node.declare_parameter("mqtt_enabled", DEFAULT_MQTT_ENABLED)
    node.declare_parameter("mqtt_broker_url", DEFAULT_MQTT_BROKER_URL)
    node.declare_parameter("mqtt_host", DEFAULT_MQTT_HOST)
    node.declare_parameter("mqtt_port", DEFAULT_MQTT_PORT)
    node.declare_parameter("mqtt_tls", DEFAULT_MQTT_TLS)
    node.declare_parameter("mqtt_ca_certs", DEFAULT_MQTT_CA_CERTS)
    node.declare_parameter("mqtt_username", DEFAULT_MQTT_USERNAME)
    node.declare_parameter("mqtt_password", DEFAULT_MQTT_PASSWORD)
    node.declare_parameter("mqtt_client_id", DEFAULT_MQTT_CLIENT_ID)
    node.declare_parameter("mqtt_alerts_enabled", DEFAULT_MQTT_ALERTS_ENABLED)
    node.declare_parameter("device_id", "")
    node.declare_parameter("device_id_m1", DEFAULT_DEVICE_ID_M1)
    node.declare_parameter("device_id_m2", DEFAULT_DEVICE_ID_M2)
    node.declare_parameter("ai_web_enabled", DEFAULT_AI_WEB_ENABLED)
    node.declare_parameter("http_enabled", DEFAULT_HTTP_ENABLED)
    node.declare_parameter("http_base_url", DEFAULT_HTTP_BASE_URL)
    node.declare_parameter("ingest_api_key", DEFAULT_INGEST_API_KEY)
    node.declare_parameter("http_timeout_sec", 2.5)
    node.declare_parameter("http_legacy_endpoints", False)

    broker_url = str(node.get_parameter("mqtt_broker_url").value).strip() or DEFAULT_MQTT_BROKER_URL
    mqtt_host = str(node.get_parameter("mqtt_host").value).strip()
    mqtt_port = int(node.get_parameter("mqtt_port").value)
    mqtt_tls = bool(node.get_parameter("mqtt_tls").value)
    mqtt_user = str(node.get_parameter("mqtt_username").value).strip()
    mqtt_pass = str(node.get_parameter("mqtt_password").value).strip()

    if (
        broker_url != DEFAULT_MQTT_BROKER_URL
        and mqtt_host == DEFAULT_MQTT_HOST
        and mqtt_port == DEFAULT_MQTT_PORT
    ):
        u_host, u_port, u_tls, u_user, u_pass = _parse_mqtt_broker_url(broker_url)
        mqtt_host = u_host
        mqtt_port = u_port
        mqtt_tls = u_tls
        if u_user and mqtt_user == DEFAULT_MQTT_USERNAME:
            mqtt_user = u_user
        if u_pass and not mqtt_pass:
            mqtt_pass = u_pass
    elif (
        mqtt_port == 1883
        and mqtt_tls == DEFAULT_MQTT_TLS
        and "WEB_MQTT_TLS" not in os.environ
        and broker_url == DEFAULT_MQTT_BROKER_URL
    ):
        mqtt_tls = False

    return WebForwarderConfig(
        mqtt_enabled=bool(node.get_parameter("mqtt_enabled").value),
        mqtt_broker_url=broker_url,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_tls=mqtt_tls,
        mqtt_ca_certs=str(node.get_parameter("mqtt_ca_certs").value).strip(),
        mqtt_username=mqtt_user,
        mqtt_password=mqtt_pass,
        mqtt_client_id=str(node.get_parameter("mqtt_client_id").value).strip() or "manekin-rx-01",
        mqtt_alerts_enabled=bool(node.get_parameter("mqtt_alerts_enabled").value),
        device_id=str(node.get_parameter("device_id").value),
        device_id_m1=str(node.get_parameter("device_id_m1").value),
        device_id_m2=str(node.get_parameter("device_id_m2").value),
        ai_web_enabled=bool(node.get_parameter("ai_web_enabled").value),
        http_enabled=bool(node.get_parameter("http_enabled").value),
        http_base_url=str(node.get_parameter("http_base_url").value),
        ingest_api_key=str(node.get_parameter("ingest_api_key").value),
        http_timeout_sec=float(node.get_parameter("http_timeout_sec").value),
        http_legacy_endpoints=bool(node.get_parameter("http_legacy_endpoints").value),
    )


class RakP2PReceiver:
    """Mengelola koneksi serial, konfigurasi AT P2P, dan pembacaan RX modul RAK3272S."""

    def __init__(self, config: RadioConfig, fallback_ports: Tuple[str, ...], logger):
        self.config = config
        self.fallback_ports = fallback_ports
        self.logger = logger
        self.serial_port: Optional[serial.Serial] = None
        self.active_port: str = config.port
        self._rx_buffer: str = ""
        self._last_reconnect_try: float = 0.0

    @property
    def is_connected(self) -> bool:
        return self.serial_port is not None and self.serial_port.is_open

    def _select_port(self) -> str:
        candidates = (self.config.port,) + tuple(
            p for p in self.fallback_ports if p != self.config.port
        )
        for candidate in candidates:
            if os.path.exists(candidate):
                return candidate
        return self.config.port

    def connect(self) -> bool:
        chosen_port = self._select_port()
        try:
            self.serial_port = serial.Serial(
                port=chosen_port,
                baudrate=self.config.baudrate,
                timeout=0.1,
                write_timeout=1.0,
                rtscts=False,
            )
            self.active_port = chosen_port
            time.sleep(0.5)
            self.serial_port.reset_input_buffer()
            self.serial_port.reset_output_buffer()
            self.logger.info(f"RAK connected: {self.active_port}")
            self.configure()
            return True
        except Exception as exc:
            self.logger.error(f"RAK connection failed on {chosen_port}: {exc}")
            self.serial_port = None
            return False

    def reconnect_if_due(self, cooldown: float = 3.0) -> bool:
        now = time.monotonic()
        if now - self._last_reconnect_try < cooldown:
            return False
        self._last_reconnect_try = now
        self.close(stop_rx=False)
        self.logger.warning("Mencoba reconnect ke port RAK3272S...")
        return self.connect()

    def send_at(self, command: str, timeout: float = 2.0) -> bool:
        if not self.is_connected:
            return False

        try:
            self.serial_port.reset_input_buffer()
            self.serial_port.write((command + "\r\n").encode())
            self.serial_port.flush()

            deadline = time.monotonic() + timeout
            response = ""
            while time.monotonic() < deadline:
                waiting = self.serial_port.in_waiting
                if waiting:
                    response += self.serial_port.read(waiting).decode(errors="ignore")
                    if "ERROR" in response:
                        self.logger.error(f"AT error: {command} | {response.strip()}")
                        return False
                    if "OK" in response:
                        self.logger.info(f"AT OK: {command}")
                        return True
                time.sleep(0.02)

            self.logger.warning(f"AT timeout: {command} | {response.strip()}")
            return False
        except Exception as exc:
            self.logger.error(f"AT command failed ({command}): {exc}")
            return False

    def configure(self) -> None:
        if not self.is_connected:
            return

        self.logger.info("Configuring RAK LoRa P2P Receiver...")
        self.send_at("AT", timeout=1.0)
        self._ensure_p2p_mode()
        self.send_at("AT+PRECV=0", timeout=1.0)

        if not self.send_at(self.config.p2p_command(), timeout=2.0) and self.config.bw == 2:
            self.send_at(self.config.p2p_command(bw_override=500), timeout=2.0)

        if not self.send_at("AT+PRECV=65534", timeout=2.0):
            self.logger.info("Fallback AT+PRECV=65535...")
            self.send_at("AT+PRECV=65535", timeout=2.0)

        self.logger.info("RAK Receiver configuration sequence completed.")

    def _ensure_p2p_mode(self) -> None:
        self.serial_port.reset_input_buffer()
        self.serial_port.write(b"AT+NWM=?\r\n")
        self.serial_port.flush()
        time.sleep(0.3)
        res_nwm = self.serial_port.read_all().decode(errors="ignore")

        if "=0" not in res_nwm:
            self.logger.info("Mengubah RAK ke mode LoRa P2P (AT+NWM=0)...")
            self.send_at("AT+NWM=0", timeout=2.0)
            time.sleep(2.0)
            self.send_at("AT", timeout=1.0)

    def read_lines(self) -> List[str]:
        if not self.is_connected:
            return []

        waiting = self.serial_port.in_waiting
        if waiting <= 0:
            return []

        data = self.serial_port.read(waiting)
        self._rx_buffer += data.decode("ascii", errors="replace")

        lines: List[str] = []
        while "\n" in self._rx_buffer:
            line, self._rx_buffer = self._rx_buffer.split("\n", 1)
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

        if len(self._rx_buffer) > 16384:
            self._rx_buffer = ""

        return lines

    def close(self, stop_rx: bool = True) -> None:
        if self.serial_port is None:
            return
        if self.serial_port.is_open:
            if stop_rx:
                try:
                    self.serial_port.write(b"AT+PRECV=0\r\n")
                    time.sleep(0.1)
                except Exception:
                    pass
            try:
                self.serial_port.close()
            except Exception:
                pass
        self.serial_port = None


# ============================================================
# HELPER PARSING, CRC16, FRAGMENT, DECODER & FORMATTING
# ============================================================

def crc16_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def valid_number(value) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) else None


def parse_rx_event(line: str) -> Optional[Tuple[str, str, str]]:
    if "RXP2P" not in line:
        return None

    idx = line.find("RXP2P")
    after = line[idx + len("RXP2P"):].strip().lstrip(":, ")
    tokens = [t.strip() for t in after.split(":") if t.strip()]
    if len(tokens) < 3 and "," in after:
        tokens = [t.strip() for t in after.split(",") if t.strip()]

    if len(tokens) >= 3:
        rssi, snr = tokens[0], tokens[1]
        hex_str = ":".join(tokens[2:]).strip()
    else:
        parts = line.split()
        if len(parts) < 3:
            return None
        rssi = parts[-3].strip() if len(parts) >= 4 else "-"
        snr = parts[-2].strip() if len(parts) >= 4 else "-"
        hex_str = parts[-1].strip()

    if hex_str.upper() in {"TIMEOUT", "ERROR", "RECEIVE TIMEOUT"}:
        return None
    return rssi, snr, hex_str


class FragmentAssembler:
    """Menangani reassembly paket berheader FRG:index/total secara aman."""

    def __init__(self, timeout_sec: float):
        self.timeout_sec = timeout_sec
        self._chunks: Dict[int, bytes] = {}
        self._total: int = 0
        self._start_time: float = 0.0

    def add_fragment(self, raw_bytes: bytes) -> Optional[Tuple[bytes, int, float]]:
        meta, chunk = raw_bytes[4:].split(b":", 1)
        idx_str, tot_str = meta.decode("ascii").split("/")
        idx, total = int(idx_str), int(tot_str)
        now = time.monotonic()

        if idx == 1 or total != self._total:
            self._chunks.clear()
            self._total = total
            self._start_time = now

        self._chunks[idx] = chunk
        if len(self._chunks) < self._total:
            return None

        complete_data = b"".join(self._chunks[i] for i in range(1, self._total + 1))
        elapsed_ms = (now - self._start_time) * 1000.0
        completed_total = self._total
        self.reset()
        return complete_data, completed_total, elapsed_ms

    def expire_if_stale(self) -> Optional[Tuple[int, int]]:
        if not self._chunks:
            return None
        if (time.monotonic() - self._start_time) <= self.timeout_sec:
            return None
        dropped = (len(self._chunks), self._total)
        self.reset()
        return dropped

    def reset(self) -> None:
        self._chunks.clear()
        self._total = 0


def split_and_verify_crc(complete_data: bytes) -> Tuple[bytes, str]:
    """
    Memeriksa akhiran CRC16-CCITT (*XXXX) bila tersedia dan mengembalikan (payload_bytes, crc_hex).

    Melempar ValueError jika checksum *XXXX ada tetapi tidak cocok.
    """
    if b"*" not in complete_data:
        return complete_data, "NONE"

    candidate_body, crc_bytes = complete_data.rsplit(b"*", 1)
    crc_text = crc_bytes.strip()
    if len(crc_text) != 4:
        return complete_data, "NONE"

    try:
        crc_hex = crc_text.decode("ascii").upper()
        expected_crc = int(crc_hex, 16)
    except (ValueError, UnicodeDecodeError):
        return complete_data, "NONE"

    actual_crc = crc16_ccitt(candidate_body)
    if actual_crc != expected_crc:
        raise ValueError(
            f"CRC16 mismatch (dihitung {actual_crc:04X}, diterima {expected_crc:04X})"
        )
    return candidate_body, crc_hex


def decode_compact_alert(text: str) -> Dict[str, object]:
    if len(text) == 12 and text.startswith("A1"):
        mannequin_id: Optional[int] = None
        body = text[2:]
    elif len(text) == 13 and text.startswith("A2"):
        raw_mid = int(text[2:3], 16)
        mannequin_id = raw_mid if raw_mid > 0 else None
        body = text[3:]
    else:
        raise ValueError(f"Format alert kompak tidak valid: {text!r}")

    sequence = int(body[0:4], 16)
    severity_code = int(body[4:5], 16)
    event_code = int(body[5:7], 16)
    status_code = int(body[7:8], 16)
    device_mask = int(body[8:10], 16)

    severity = ALERT_SEVERITY_NAMES.get(severity_code, f"SEVERITY_{severity_code}")
    code = ALERT_EVENT_NAMES.get(event_code, f"EVENT_{event_code:02X}")
    status = ALERT_STATUS_NAMES.get(status_code, f"STATUS_{status_code}")
    offline_devices = [
        name for bit, name in ALERT_DEVICE_BITS if device_mask & (1 << bit)
    ]

    return {
        "mannequin_id": mannequin_id,
        "sequence": sequence,
        "severity": severity,
        "code": code,
        "status": status,
        "device_mask": device_mask,
        "offline_devices": offline_devices,
        "raw_compact": text,
    }


def decode_array_telemetry(
    values: list,
) -> Tuple[Dict[str, Optional[float]], Optional[str]]:
    warning_msg: Optional[str] = None
    if len(values) != NUM_PARAMS:
        warning_msg = (
            f"Jumlah field array tidak standar: diterima {len(values)}, "
            f"seharusnya {NUM_PARAMS}"
        )

    sensor_dict: Dict[str, Optional[float]] = {key: None for key in EXPECTED_KEYS}
    for key, raw_val in zip(EXPECTED_KEYS, values):
        sensor_dict[key] = valid_number(raw_val)

    return sensor_dict, warning_msg


def decode_web_v2_telemetry(
    payload_obj: dict, default_mannequin_id: int
) -> Tuple[int, Dict[str, Optional[float]]]:
    sensor_dict: Dict[str, Optional[float]] = {key: None for key in EXPECTED_KEYS}
    readings = payload_obj.get("r", [])

    inferred_id = default_mannequin_id
    for entry in readings:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        raw_id, raw_val = entry[0], entry[1]
        if isinstance(raw_id, bool):
            continue
        try:
            rid = int(raw_id)
        except (ValueError, TypeError):
            continue

        if 2000 <= rid < 3000:
            inferred_id = 2
            base_id = rid - 1000
        elif 1000 <= rid < 2000:
            inferred_id = 1
            base_id = rid
        else:
            base_id = rid

        key = REVERSE_WEB_ID_MAP.get(base_id)
        if key is not None:
            sensor_dict[key] = valid_number(raw_val)

    return inferred_id, sensor_dict


PRESSED_TEMP_INDICES = frozenset({30, *range(57, 71), 99})   # int16 * 100
PRESSED_HUM_INDICES = frozenset({31})                        # int16 * 100
PRESSED_PRES_INDICES = frozenset({32})                       # uint16 * 50
PRESSED_LIDAR_INDICES = frozenset({0, 1})                    # int16 * 10
PRESSED_ALT_INDICES = frozenset({33})                        # int16 * 2
PRESSED_RPY_INDICES = frozenset({106, 107, 108})             # int16 * 100
F16_NAN_BYTES = b"\xFF\x7F"
INT16_SENTINEL = -32768
UINT16_SENTINEL = 0xFFFF


def decode_pressed_telemetry(
    complete_data: bytes, default_mannequin_id: int
) -> Tuple[int, Dict[str, Optional[float]], str, str, Optional[Dict[str, object]]]:
    """
    Mendekode paket terkompresi (press) biner B2/Z2 maupun B3/Z3 (dengan Edge AI 2B).
    Mengembalikan (mannequin_id, sensor_dict, payload_mode, crc_hex, ai_detection).
    """
    if len(complete_data) < 4:
        raise ValueError(f"Panjang paket biner terlalu pendek: {len(complete_data)}B")

    body_with_magic = complete_data[:-2]
    expected_crc = struct.unpack(">H", complete_data[-2:])[0]
    actual_crc = crc16_ccitt(body_with_magic)
    if actual_crc != expected_crc:
        raise ValueError(
            f"CRC16 biner mismatch (dihitung {actual_crc:04X}, diterima {expected_crc:04X})"
        )
    crc_hex = f"{expected_crc:04X}"

    has_ai = False
    if body_with_magic.startswith(b"Z3"):
        try:
            raw_body = zlib.decompress(body_with_magic[2:])
        except zlib.error as exc:
            raise ValueError(f"Gagal dekompresi Z3 zlib: {exc}") from exc
        payload_mode = "packed_z3"
        has_ai = True
    elif body_with_magic.startswith(b"B3"):
        raw_body = body_with_magic[2:]
        payload_mode = "packed_b3"
        has_ai = True
    elif body_with_magic.startswith(b"Z2"):
        try:
            raw_body = zlib.decompress(body_with_magic[2:])
        except zlib.error as exc:
            raise ValueError(f"Gagal dekompresi Z2 zlib: {exc}") from exc
        payload_mode = "packed_z2"
    elif body_with_magic.startswith(b"B2"):
        raw_body = body_with_magic[2:]
        payload_mode = "packed_b2"
    else:
        raise ValueError("Header paket biner bukan B2/Z2/B3/Z3")

    if len(raw_body) < 14:
        raise ValueError(f"Body biner tidak lengkap: {len(raw_body)}B (<14B)")

    bitmask = raw_body[:14]
    encoded_mid = (bitmask[13] >> 5) & 0x07
    mannequin_id = encoded_mid if encoded_mid > 0 else default_mannequin_id

    active_indices = [
        idx for idx in range(NUM_PARAMS) if (bitmask[idx >> 3] & (1 << (idx & 7)))
    ]
    needed_primary = 14 + 2 * len(active_indices)
    ai_len = 2 if has_ai else 0
    if len(raw_body) < needed_primary + ai_len:
        raise ValueError(
            f"Ukuran body biner kurang ({len(raw_body)}B < {needed_primary + ai_len}B)"
        )
    if (len(raw_body) - needed_primary - ai_len) % 5 != 0:
        raise ValueError("Ukuran overflow tail biner tidak valid")

    sensor_dict: Dict[str, Optional[float]] = {key: None for key in EXPECTED_KEYS}

    offset = 14
    for idx in active_indices:
        chunk = raw_body[offset:offset + 2]
        offset += 2
        key = EXPECTED_KEYS[idx]

        if idx in PRESSED_PRES_INDICES:
            raw_u16 = struct.unpack("<H", chunk)[0]
            if raw_u16 != UINT16_SENTINEL:
                sensor_dict[key] = round(raw_u16 / 50.0, 2)
        elif idx in PRESSED_TEMP_INDICES or idx in PRESSED_HUM_INDICES or idx in PRESSED_RPY_INDICES:
            raw_i16 = struct.unpack("<h", chunk)[0]
            if raw_i16 != INT16_SENTINEL:
                sensor_dict[key] = round(raw_i16 / 100.0, 2)
        elif idx in PRESSED_LIDAR_INDICES:
            raw_i16 = struct.unpack("<h", chunk)[0]
            if raw_i16 != INT16_SENTINEL:
                sensor_dict[key] = round(raw_i16 / 10.0, 1)
        elif idx in PRESSED_ALT_INDICES:
            raw_i16 = struct.unpack("<h", chunk)[0]
            if raw_i16 != INT16_SENTINEL:
                sensor_dict[key] = round(raw_i16 / 2.0, 1)
        else:
            if chunk != F16_NAN_BYTES:
                fval = struct.unpack("<e", chunk)[0]
                if math.isfinite(fval):
                    sensor_dict[key] = round(float(fval), 4)

    ai_detection: Optional[Dict[str, object]] = None
    if has_ai:
        ai_u16 = struct.unpack("<H", raw_body[offset:offset + 2])[0]
        offset += 2
        is_anomaly = bool((ai_u16 >> 15) & 0x01)
        dom_idx = (ai_u16 >> 10) & 0x1F
        rmse_val = round((ai_u16 & 0x03FF) / 100.0, 2)
        dom_feat = (
            AI_TARGET_FEATURES[dom_idx]
            if 0 <= dom_idx < len(AI_TARGET_FEATURES)
            else "NONE"
        )
        ai_detection = {
            "is_anomaly": is_anomaly,
            "status": "ANOMALI" if is_anomaly else "NORMAL",
            "dominant_idx": dom_idx,
            "dominant_feature": dom_feat,
            "rmse": rmse_val,
        }

    ov_offset = needed_primary + ai_len
    while ov_offset + 5 <= len(raw_body):
        idx, fval = struct.unpack_from("<Bf", raw_body, ov_offset)
        ov_offset += 5
        if idx < NUM_PARAMS:
            valid_f = valid_number(fval)
            if valid_f is not None:
                sensor_dict[EXPECTED_KEYS[idx]] = round(valid_f, 4)

    return mannequin_id, sensor_dict, payload_mode, crc_hex, ai_detection


def build_web_v2_payload(
    sensor_dict: Dict[str, Optional[float]],
    mannequin_id: int,
    boot_count: int,
    seq: int,
    ai_detection: Optional[Dict[str, object]] = None,
    include_ai: bool = False,
) -> Dict[str, object]:
    id_offset = 1000 if mannequin_id == 2 else 0
    readings: List[List[object]] = [
        [base_id + id_offset, sensor_dict.get(key)]
        for key, base_id in WEB_READING_ID_MAP_M1.items()
        if (base_id + id_offset) not in UNBOUND_WEB_READING_IDS
    ]
    if include_ai and ai_detection is not None:
        readings.append([1112 + id_offset, 1 if ai_detection.get("is_anomaly") else 0])
        readings.append([1113 + id_offset, int(ai_detection.get("dominant_idx", 31))])
        readings.append([1114 + id_offset, float(ai_detection.get("rmse", 0.0))])
    return {
        "v": 2,
        "b": boot_count,
        "s": seq,
        "t": int(time.time() * 1000),
        "r": readings,
    }


def format_manekin_horizontal(d: Dict[str, Optional[float]]) -> str:
    def val(k: str) -> object:
        v = d.get(k)
        return "-" if v is None else v

    parts = [
        f"Lidar:(raw:{val('lidar_raw')},med:{val('lidar_median')})",
        f"MQ(NH3:{val('mq_nh3')},CO:{val('mq_co')},CO2:{val('mq_co2')})",
        f"Smoke:{val('smoke')}",
        (
            f"MPU_Raw[A:({val('mpu_acc_x_raw')},{val('mpu_acc_y_raw')},{val('mpu_acc_z_raw')}) "
            f"G:({val('mpu_gyro_x_raw')},{val('mpu_gyro_y_raw')},{val('mpu_gyro_z_raw')})]"
        ),
        (
            f"MPU_Klm[A:({val('mpu_acc_x_kalman')},{val('mpu_acc_y_kalman')},{val('mpu_acc_z_kalman')}) "
            f"G:({val('mpu_gyro_x_kalman')},{val('mpu_gyro_y_kalman')},{val('mpu_gyro_z_kalman')})]"
        ),
        f"ADXL_Raw:({val('adxl_acc_x_raw')},{val('adxl_acc_y_raw')},{val('adxl_acc_z_raw')})",
        f"ADXL_Klm:({val('adxl_acc_x_kalman')},{val('adxl_acc_y_kalman')},{val('adxl_acc_z_kalman')})",
        (
            f"LSM6DS3[A:({val('accelx')},{val('accely')},{val('accelz')}) "
            f"G:({val('gyrox')},{val('gyroy')},{val('gyroz')})]"
        ),
        f"BME280({val('bme_temp')}C,{val('bme_hum')}%,{val('bme_pres')}hPa,{val('bme_alt')}m)",
        f"Sound(L:{val('sound_dbLeft')},R:{val('sound_dbRight')}dB)",
        f"Loadcell({','.join(str(val(k)) for k in SENSOR_GROUPS['loadcell'])})",
        f"Flex[{','.join(str(val(k)) for k in SENSOR_GROUPS['flex'])}]",
        f"MCP[{','.join(str(val(k)) for k in SENSOR_GROUPS['mcp'])}]",
        f"FSR[{','.join(str(val(k)) for k in SENSOR_GROUPS['fsr'])}]",
        f"Piezo[{','.join(str(val(k)) for k in SENSOR_GROUPS['piezo'])}]",
        f"Thermal:{val('thermal')}",
        (
            f"WT901[A:({val('ax')},{val('ay')},{val('az')}) "
            f"G:({val('gx')},{val('gy')},{val('gz')}) "
            f"RPY:({val('roll')},{val('pitch')},{val('yaw')})]"
        ),
    ]
    return " | ".join(parts)


# ============================================================
# RECEIVER STANDALONE (+ WEB FORWARDER + LOCAL CSV LOGGER)
# ============================================================

def _resolve_auto_boot_count(log_dir: str, log_enabled: bool = True) -> int:
    """Menghasilkan boot_count monotonik agar restart RX tidak bentrok deduplikasi DB."""
    epoch_boot = max(1, int(time.time()) - 1_700_000_000)
    prev_boot = 0
    state_file = os.path.join(os.path.expanduser(log_dir), ".web_boot_count")
    try:
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as fh:
                prev_boot = int((fh.read() or "0").strip())
    except Exception:
        prev_boot = 0

    boot = min(2_147_483_647, max(1, epoch_boot, prev_boot + 1))
    if log_enabled:
        try:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            with open(state_file, "w", encoding="utf-8") as fh:
                fh.write(f"{boot}\n")
        except Exception:
            pass
    return boot


class LoRaManekin1ReceiverNode(StandaloneParamStore):
    """Receiver LoRa P2P Pure Python Standalone + Web Forwarder + Local CSV Communication Logger."""

    def __init__(
        self,
        default_port: str = DEFAULT_SERIAL_PORT,
        cli_overrides: Optional[Dict[str, str]] = None,
    ):
        super().__init__(cli_overrides=cli_overrides)
        self._stop_event = threading.Event()

        self.radio_config = declare_radio_config(self, default_port)
        web_config = declare_web_config(self)

        self.declare_parameter("frag_timeout", 3.0)
        self.declare_parameter("mannequin_id", 1)
        self.declare_parameter("boot_count", 0)
        self.declare_parameter("log_enabled", DEFAULT_LOG_ENABLED)
        self.declare_parameter("log_dir", DEFAULT_LOG_DIR)

        frag_timeout = float(self.get_parameter("frag_timeout").value)
        self.default_mannequin_id = int(self.get_parameter("mannequin_id").value)
        configured_boot = int(self.get_parameter("boot_count").value)
        log_enabled = bool(self.get_parameter("log_enabled").value)
        log_dir = str(self.get_parameter("log_dir").value)
        self.boot_count = (
            configured_boot
            if configured_boot > 0
            else _resolve_auto_boot_count(log_dir, log_enabled=log_enabled)
        )

        self.packet_count = 0
        self.alert_count = 0
        self.crc_error_count = 0
        self.frag_timeout_count = 0
        self.web_seq = 0
        self.web_seq_by_mannequin: Dict[int, int] = {}
        self.last_rssi = "-"
        self.last_snr = "-"
        self.last_packet_monotonic: Optional[float] = None
        self.last_packet_by_mannequin: Dict[int, float] = {}
        self.assembler = FragmentAssembler(frag_timeout)

        # Logger CSV Lokal (<folder_script>/logs/communication/)
        self.comm_logger = LocalCommunicationLogger(
            enabled=log_enabled,
            log_dir=log_dir,
            logger=self.get_logger(),
        )

        # Pengirim langsung ke Web Backend (MQTTS Compact V2 & Opsional HTTP REST)
        self.web_forwarder = WebTelemetryForwarder(
            config=web_config,
            logger=self.get_logger(),
        )

        self.radio = RakP2PReceiver(
            config=self.radio_config,
            fallback_ports=FALLBACK_PORTS,
            logger=self.get_logger(),
        )
        self.radio.connect()

        key_state = "SET" if web_config.ingest_api_key else "EMPTY"
        mqtt_pass_state = "SET" if web_config.mqtt_password else "EMPTY"
        mqtt_scheme = web_config.effective_mqtt_scheme()
        self.get_logger().info(
            f"LoRa Manekin Receiver (Standalone) started on {self.radio.active_port} "
            f"@ {self.radio_config.baudrate} | Freq:{self.radio_config.freq} SF{self.radio_config.sf} "
            f"BW{self.radio_config.bw} CR{self.radio_config.cr} | {NUM_PARAMS} params | boot={self.boot_count} | "
            f"LocalLog:{self.comm_logger.log_dir} | "
            f"WebMQTT:{web_config.mqtt_enabled} ({mqtt_scheme}://{web_config.mqtt_host}:{web_config.mqtt_port}, "
            f"tls={web_config.mqtt_tls}, client_id={web_config.mqtt_client_id}, "
            f"user={web_config.mqtt_username or '-'}, pass={mqtt_pass_state}, qos=1, retain=False) | "
            f"WebRoutes:[M1->{web_config.device_id_m1}, M2->{web_config.device_id_m2}, AI_Web={web_config.ai_web_enabled}] | "
            f"WebHTTP:{web_config.http_enabled} ({web_config.http_base_url}, key={key_state})"
        )

    def _compute_inter_packet_timing(
        self, mannequin_id: Optional[int] = None
    ) -> Tuple[Optional[float], Optional[float]]:
        now = time.monotonic()
        self.last_packet_monotonic = now
        if mannequin_id is None:
            return None, None
        by_mid = getattr(self, "last_packet_by_mannequin", None)
        if by_mid is None:
            by_mid = {}
            self.last_packet_by_mannequin = by_mid
        prev = by_mid.get(mannequin_id)
        by_mid[mannequin_id] = now
        if prev is None:
            return None, None
        delta_sec = now - prev
        inter_ms = delta_sec * 1000.0
        rate_hz = (1.0 / delta_sec) if delta_sec > 0 else None
        return inter_ms, rate_hz

    def _log_comm_event(
        self,
        *,
        mannequin_id: Optional[int] = None,
        packet_type: str,
        payload_mode: str,
        byte_len: int,
        fragments: int,
        reassembly_ms: float = 0.0,
        inter_packet_ms: Optional[float] = None,
        packet_rate_hz: Optional[float] = None,
        crc_status: str = "OK",
        crc_hex: str = "NONE",
        valid_fields: int = 0,
    ) -> None:
        target_mid = self.default_mannequin_id if mannequin_id is None else mannequin_id
        dev_stats = self.web_forwarder.get_stats(target_mid)
        self.comm_logger.log_communication(
            packet_no=self.packet_count,
            mannequin_id=target_mid,
            packet_type=packet_type,
            payload_mode=payload_mode,
            rssi=self.last_rssi,
            snr=self.last_snr,
            radio_cfg=self.radio_config,
            byte_len=byte_len,
            fragments=fragments,
            reassembly_ms=reassembly_ms,
            inter_packet_ms=inter_packet_ms,
            packet_rate_hz=packet_rate_hz,
            crc_status=crc_status,
            crc_hex=crc_hex,
            crc_errors_total=self.crc_error_count,
            frag_timeouts_total=self.frag_timeout_count,
            valid_fields=valid_fields,
            total_fields=NUM_PARAMS,
            web_device_id=str(dev_stats["device_id"]),
            web_mqtt_sent=int(dev_stats["mqtt_sent"]),
            web_ack_status=str(dev_stats["last_ack"]),
            web_http_sent=int(dev_stats["http_sent"]),
        )

    def read_serial_data(self) -> None:
        if not self.radio.is_connected:
            self.radio.reconnect_if_due()
            return

        try:
            lines = self.radio.read_lines()
        except (serial.SerialException, OSError) as exc:
            self.get_logger().error(f"Serial read error: {exc}")
            self.radio.reconnect_if_due()
            return

        for line in lines:
            parsed = parse_rx_event(line)
            if parsed is None:
                continue
            rssi, snr, hex_str = parsed
            try:
                raw_bytes = bytes.fromhex(hex_str)
            except ValueError:
                self.packet_count += 1
                self.last_rssi = rssi or "-"
                self.last_snr = snr or "-"
                self._log_comm_event(
                    packet_type="rx_error",
                    payload_mode="invalid_hex",
                    byte_len=len(hex_str),
                    fragments=1,
                    crc_status="HEX_ERROR",
                )
                self.get_logger().error(f"Gagal konversi hex: {hex_str[:30]}")
                continue
            self.handle_received_bytes(raw_bytes, rssi, snr)

    def handle_received_bytes(self, raw_bytes: bytes, rssi: str, snr: str) -> None:
        self.last_rssi = rssi or "-"
        self.last_snr = snr or "-"

        if raw_bytes.startswith(b"FRG:"):
            try:
                assembled = self.assembler.add_fragment(raw_bytes)
            except Exception as exc:
                self.packet_count += 1
                self._log_comm_event(
                    packet_type="frag_error",
                    payload_mode="invalid_fragment",
                    byte_len=len(raw_bytes),
                    fragments=1,
                    crc_status="FRAG_ERROR",
                )
                self.get_logger().error(f"Gagal memproses fragmen FRG: {exc}")
                return
            if assembled is not None:
                complete_data, total_frags, elapsed_ms = assembled
                self.process_complete_data(complete_data, total_frags, elapsed_ms)
            else:
                self._log_comm_event(
                    packet_type="fragment",
                    payload_mode=f"frag_{len(self.assembler._chunks)}_of_{self.assembler._total}",
                    byte_len=len(raw_bytes),
                    fragments=self.assembler._total or 1,
                    crc_status="FRAG_PART",
                )
            return

        self.process_complete_data(raw_bytes, total_frags=1, elapsed_ms=0.0)

    def process_complete_data(
        self, complete_data: bytes, total_frags: int, elapsed_ms: float
    ) -> None:
        self.packet_count += 1

        if complete_data.startswith((b"B2", b"Z2", b"B3", b"Z3")):
            try:
                mannequin_id, sensor_dict, payload_mode, crc_hex, ai_detection = (
                    decode_pressed_telemetry(complete_data, self.default_mannequin_id)
                )
            except ValueError as exc:
                self.crc_error_count += 1
                self._log_comm_event(
                    packet_type="crc_error",
                    payload_mode="invalid_pressed_crc",
                    byte_len=len(complete_data),
                    fragments=total_frags,
                    reassembly_ms=elapsed_ms,
                    crc_status="CRC_ERROR",
                    crc_hex="MISMATCH",
                )
                self.get_logger().warning(
                    f"[RX #{self.packet_count:04d}] {exc}. Paket dibuang."
                )
                return

            inter_ms, rate_hz = self._compute_inter_packet_timing(mannequin_id)
            self._publish_telemetry(
                sensor_dict=sensor_dict,
                mannequin_id=mannequin_id,
                payload_mode=payload_mode,
                byte_len=len(complete_data),
                total_frags=total_frags,
                elapsed_ms=elapsed_ms,
                inter_ms=inter_ms,
                rate_hz=rate_hz,
                crc_hex=crc_hex,
                web_v2_obj=None,
                ai_detection=ai_detection,
            )
            return

        try:
            payload_bytes, crc_hex = split_and_verify_crc(complete_data)
        except ValueError as exc:
            self.crc_error_count += 1
            self._log_comm_event(
                packet_type="crc_error",
                payload_mode="invalid_crc",
                byte_len=len(complete_data),
                fragments=total_frags,
                reassembly_ms=elapsed_ms,
                crc_status="CRC_ERROR",
                crc_hex="MISMATCH",
            )
            self.get_logger().warning(
                f"[RX #{self.packet_count:04d}] {exc}. Paket dibuang."
            )
            return

        try:
            text = payload_bytes.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            self._log_comm_event(
                packet_type="rx_error",
                payload_mode="invalid_utf8",
                byte_len=len(complete_data),
                fragments=total_frags,
                reassembly_ms=elapsed_ms,
                crc_status="UTF8_ERROR",
                crc_hex=crc_hex,
            )
            self.get_logger().warning(
                f"[RX #{self.packet_count:04d}] Payload bukan UTF-8 valid ({exc})."
            )
            return

        if (text.startswith("A1") and len(text) == 12) or (text.startswith("A2") and len(text) == 13):
            self._handle_alert(
                text, len(complete_data), total_frags, elapsed_ms, None, None, crc_hex
            )
            return

        self._handle_json_payload(
            text, len(complete_data), total_frags, elapsed_ms, None, None, crc_hex
        )

    def _handle_alert(
        self,
        text: str,
        byte_len: int,
        total_frags: int,
        elapsed_ms: float,
        inter_ms: Optional[float],
        rate_hz: Optional[float],
        crc_hex: str,
    ) -> None:
        try:
            decoded = decode_compact_alert(text)
        except ValueError as exc:
            self._log_comm_event(
                packet_type="rx_error",
                payload_mode="invalid_alert",
                byte_len=byte_len,
                fragments=total_frags,
                reassembly_ms=elapsed_ms,
                crc_status="ALERT_ERROR",
                crc_hex=crc_hex,
            )
            self.get_logger().warning(f"[RX #{self.packet_count:04d}] {exc}")
            return

        alert_mid = int(decoded.get("mannequin_id") or self.default_mannequin_id)
        if inter_ms is None and rate_hz is None:
            inter_ms, rate_hz = self._compute_inter_packet_timing(alert_mid)

        self.alert_count += 1
        alert_payload = {
            "source": "lora_rx_manekin",
            "mannequin_id": alert_mid,
            "sequence": decoded["sequence"],
            "severity": decoded["severity"],
            "code": decoded["code"],
            "details": {
                "current_status": decoded["status"],
                "offline_mask": decoded["device_mask"],
                "system_status": {
                    "status": decoded["status"],
                    "offline_devices": decoded["offline_devices"],
                },
            },
            "raw_compact": decoded["raw_compact"],
            "rssi": self.last_rssi,
            "snr": self.last_snr,
            "received_at_ms": int(time.time() * 1000),
        }
        self.web_forwarder.enqueue_alert(alert_mid, alert_payload)

        # Simpan ke CSV komunikasi & CSV alert lokal
        self._log_comm_event(
            mannequin_id=alert_mid,
            packet_type="alert",
            payload_mode="alert",
            byte_len=byte_len,
            fragments=total_frags,
            reassembly_ms=elapsed_ms,
            inter_packet_ms=inter_ms,
            packet_rate_hz=rate_hz,
            crc_status="OK",
            crc_hex=crc_hex,
            valid_fields=0,
        )
        self.comm_logger.log_alert(
            packet_no=self.packet_count,
            alert_no=self.alert_count,
            mannequin_id=alert_mid,
            rssi=self.last_rssi,
            snr=self.last_snr,
            byte_len=byte_len,
            decoded_alert=decoded,
        )

        pkt_label = "1Paket" if total_frags == 1 else f"{total_frags}Frg"
        self.get_logger().warning(
            f"[RX #{self.packet_count:04d}] [M{alert_mid} ALERT #{self.alert_count:04d}] "
            f"[{byte_len}B {pkt_label}] [{self.last_rssi}dBm {self.last_snr}dB] "
            f"seq={decoded['sequence']} sev={decoded['severity']} code={decoded['code']} "
            f"status={decoded['status']} offline={decoded['offline_devices']}"
        )

    def _handle_json_payload(
        self,
        text: str,
        byte_len: int,
        total_frags: int,
        elapsed_ms: float,
        inter_ms: Optional[float],
        rate_hz: Optional[float],
        crc_hex: str,
    ) -> None:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            self._log_comm_event(
                packet_type="rx_error",
                payload_mode="invalid_json",
                byte_len=byte_len,
                fragments=total_frags,
                reassembly_ms=elapsed_ms,
                crc_status="JSON_ERROR",
                crc_hex=crc_hex,
            )
            self.get_logger().warning(
                f"[RX #{self.packet_count:04d}] Gagal parse JSON ({exc}): {text[:80]}"
            )
            return

        mannequin_id = self.default_mannequin_id
        web_v2_obj: Optional[Dict[str, object]] = None

        if isinstance(parsed, list):
            payload_mode = "array"
            sensor_dict, len_warn = decode_array_telemetry(parsed)
            if len_warn:
                self.get_logger().warning(f"[RX #{self.packet_count:04d}] {len_warn}")
        elif isinstance(parsed, dict) and parsed.get("v") == 2 and isinstance(parsed.get("r"), list):
            payload_mode = "web_v2"
            mannequin_id, sensor_dict = decode_web_v2_telemetry(
                parsed, self.default_mannequin_id
            )
            web_v2_obj = parsed
        elif isinstance(parsed, dict):
            payload_mode = "dict"
            source_obj = parsed.get("data") if isinstance(parsed.get("data"), dict) else parsed
            sensor_dict = {
                key: valid_number(source_obj.get(key)) for key in EXPECTED_KEYS
            }
        else:
            self._log_comm_event(
                packet_type="rx_error",
                payload_mode=f"unsupported_json_{type(parsed).__name__}",
                byte_len=byte_len,
                fragments=total_frags,
                reassembly_ms=elapsed_ms,
                crc_status="UNSUPPORTED_JSON",
                crc_hex=crc_hex,
            )
            self.get_logger().warning(
                f"[RX #{self.packet_count:04d}] Tipe JSON tidak didukung: {type(parsed).__name__}"
            )
            return

        if inter_ms is None and rate_hz is None:
            inter_ms, rate_hz = self._compute_inter_packet_timing(mannequin_id)

        self._publish_telemetry(
            sensor_dict=sensor_dict,
            mannequin_id=mannequin_id,
            payload_mode=payload_mode,
            byte_len=byte_len,
            total_frags=total_frags,
            elapsed_ms=elapsed_ms,
            inter_ms=inter_ms,
            rate_hz=rate_hz,
            crc_hex=crc_hex,
            web_v2_obj=web_v2_obj,
        )

    def _publish_telemetry(
        self,
        sensor_dict: Dict[str, Optional[float]],
        mannequin_id: int,
        payload_mode: str,
        byte_len: int,
        total_frags: int,
        elapsed_ms: float,
        inter_ms: Optional[float],
        rate_hz: Optional[float],
        crc_hex: str,
        web_v2_obj: Optional[Dict[str, object]] = None,
        ai_detection: Optional[Dict[str, object]] = None,
    ) -> None:
        valid_fields = sum(1 for v in sensor_dict.values() if v is not None)
        seq_map = getattr(self, "web_seq_by_mannequin", None)
        if seq_map is None:
            seq_map = {}
            self.web_seq_by_mannequin = seq_map
        next_seq = (seq_map.get(mannequin_id, 0) + 1) & 0x7FFFFFFF
        seq_map[mannequin_id] = next_seq
        self.web_seq = (getattr(self, "web_seq", 0) + 1) & 0x7FFFFFFF

        if web_v2_obj is None:
            web_v2_obj = build_web_v2_payload(
                sensor_dict=sensor_dict,
                mannequin_id=mannequin_id,
                boot_count=self.boot_count,
                seq=next_seq,
                ai_detection=ai_detection,
                include_ai=self.web_forwarder.config.ai_web_enabled,
            )

        # 1. KIRIM LANGSUNG KE WEB BACKEND (MQTT Compact V2 & HTTP REST)
        self.web_forwarder.enqueue_telemetry(
            mannequin_id=mannequin_id,
            web_v2_obj=web_v2_obj,
            sensor_dict=sensor_dict,
        )
        dev_stats = self.web_forwarder.get_stats(mannequin_id)

        # 2. SIMPAN LOKAL DI RASPBERRY PI (<folder_script>/logs/communication/)
        self.comm_logger.log_communication(
            packet_no=self.packet_count,
            mannequin_id=mannequin_id,
            packet_type="telemetry",
            payload_mode=payload_mode,
            rssi=self.last_rssi,
            snr=self.last_snr,
            radio_cfg=self.radio_config,
            byte_len=byte_len,
            fragments=total_frags,
            reassembly_ms=elapsed_ms,
            inter_packet_ms=inter_ms,
            packet_rate_hz=rate_hz,
            crc_status="OK",
            crc_hex=crc_hex,
            crc_errors_total=self.crc_error_count,
            frag_timeouts_total=self.frag_timeout_count,
            valid_fields=valid_fields,
            total_fields=NUM_PARAMS,
            web_device_id=str(dev_stats["device_id"]),
            web_mqtt_sent=int(dev_stats["mqtt_sent"]),
            web_ack_status=str(dev_stats["last_ack"]),
            web_http_sent=int(dev_stats["http_sent"]),
            ai_detection=ai_detection,
        )
        self.comm_logger.log_telemetry(
            packet_no=self.packet_count,
            mannequin_id=mannequin_id,
            payload_mode=payload_mode,
            rssi=self.last_rssi,
            snr=self.last_snr,
            byte_len=byte_len,
            fragments=total_frags,
            reassembly_ms=elapsed_ms,
            inter_packet_ms=inter_ms,
            valid_fields=valid_fields,
            sensor_dict=sensor_dict,
            ai_detection=ai_detection,
        )

        # 3. Log 1 baris horizontal bersih
        pkt_label = "1Paket" if total_frags == 1 else f"{total_frags}Frg"
        web_parts = []
        if self.web_forwarder.config.http_enabled:
            web_parts.append(
                f"HTTP:{dev_stats['http_sent']}"
                f"(fail:{dev_stats['http_fail']})"
            )
        if self.web_forwarder.config.mqtt_enabled or not web_parts:
            web_parts.append(
                f"MQTT:{dev_stats['device_id']}:{dev_stats['mqtt_sent']}"
                f"(fail:{dev_stats['mqtt_fail']},ACK:{dev_stats['last_ack']})"
            )
        web_info = " ".join(web_parts)
        ai_tag = ""
        if ai_detection:
            ai_tag = (
                f" [AI:{ai_detection['status']}|RMSE:{ai_detection['rmse']:.2f}"
                f"|Dom:{ai_detection['dominant_feature']}]"
            )

        self.get_logger().info(
            f"[RX #{self.packet_count:04d}] [M{mannequin_id} {payload_mode}] "
            f"[{byte_len}B {pkt_label} {elapsed_ms:.0f}ms] "
            f"[{self.last_rssi}dBm {self.last_snr}dB] [{valid_fields}/{NUM_PARAMS} valid]"
            f"{ai_tag} [{web_info}] -> {format_manekin_horizontal(sensor_dict)}"
        )

    def check_frag_timeout(self) -> None:
        dropped = self.assembler.expire_if_stale()
        if dropped is not None:
            received, total = dropped
            self.frag_timeout_count += 1
            self._log_comm_event(
                packet_type="frag_timeout",
                payload_mode=f"incomplete_{received}_of_{total}",
                byte_len=0,
                fragments=received,
                reassembly_ms=self.assembler.timeout_sec * 1000.0,
                crc_status="FRAG_TIMEOUT",
                crc_hex="NONE",
            )
            self.get_logger().warning(
                f"Fragmen tidak lengkap dalam {self.assembler.timeout_sec}s "
                f"({received}/{total}), dibatalkan."
            )

    def run(self) -> None:
        next_frag_check = time.monotonic() + 0.5
        while not self._stop_event.is_set():
            self.read_serial_data()
            now = time.monotonic()
            if now >= next_frag_check:
                self.check_frag_timeout()
                next_frag_check = now + 0.5
            time.sleep(0.005)

    def stop(self) -> None:
        self._stop_event.set()

    def destroy_node(self) -> None:
        self.stop()
        self.web_forwarder.close()
        self.comm_logger.close()
        self.radio.close(stop_rx=True)


def parse_cli_args(argv: Optional[List[str]] = None) -> Tuple[str, Dict[str, str]]:
    args = list(sys.argv[1:] if argv is None else argv)
    cli_port = DEFAULT_SERIAL_PORT
    overrides: Dict[str, str] = {}

    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--ros-args":
            i += 1
            continue
        if arg in ("-p", "--param") and i + 1 < len(args):
            kv = args[i + 1]
            if ":=" in kv:
                k, v = kv.split(":=", 1)
                overrides[k.strip()] = v.strip()
            elif "=" in kv:
                k, v = kv.split("=", 1)
                overrides[k.strip()] = v.strip()
            i += 2
            continue
        if arg.startswith("--"):
            key_raw = arg[2:]
            if "=" in key_raw:
                k, v = key_raw.split("=", 1)
                overrides[k.strip().replace("-", "_")] = v.strip()
                i += 1
            elif i + 1 < len(args):
                overrides[key_raw.strip().replace("-", "_")] = args[i + 1].strip()
                i += 2
            else:
                overrides[key_raw.strip().replace("-", "_")] = "true"
                i += 1
            continue
        if arg.startswith("/dev/") or arg.upper().startswith("COM"):
            cli_port = arg
        i += 1

    return cli_port, overrides


def main(argv: Optional[List[str]] = None) -> None:
    cli_port, cli_overrides = parse_cli_args(argv)
    node = LoRaManekin1ReceiverNode(default_port=cli_port, cli_overrides=cli_overrides)

    def _handle_sig(_signum, _frame):
        node.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_sig)
        except Exception:
            pass

    try:
        node.run()
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()


if __name__ == "__main__":
    main()
