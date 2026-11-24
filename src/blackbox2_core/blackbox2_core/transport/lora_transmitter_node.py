
#!/usr/bin/env python3

import csv
from datetime import datetime
import getpass
import json
import math
import os
import random
import re
import socket
import struct
import time
import threading
import zlib
from collections import deque
from typing import Optional, Tuple

import serial
import rclpy

from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# RAK LORA CONFIGURATION
# ============================================================

SERIAL_PORT = "/dev/ttyAMA3"
BAUD_RATE = 115200

FREQUENCY = 920600000
SPREADING_FACTOR = 9
BANDWIDTH = 0
CODING_RATE = 3
PREAMBLE = 8
TX_POWER = 22
CAD_ENABLED = True
CAD_MAX_RETRIES = 3

# Jeda antar paket telemetry (detik) agar aman di SF9/BW125kHz/CR4-8 & anti-tabrakan M1/M2
TRANSMIT_INTERVAL = 2.5

# Batas keras payload RAK3272S / RUI3 AT+PSEND = 239 byte (478 hex char).
# TX wajib mengirim 1 uplink tunggal (<= 238 byte) tanpa dipecah fragmen FRG.
# Mode "packed" (B2/Z2) memadatkan 109 parameter menjadi <= 236..238 byte.
MAX_SINGLE_LORA_PAYLOAD = 238
MAX_PAYLOAD = MAX_SINGLE_LORA_PAYLOAD

# Pertahankan nilai terakhir yang valid.
# Tidak pernah membuat nilai dummy.
HOLD_LAST_VALUE = True

# Simpan 1 snapshot telemetry terbaru di TX queue agar tidak mengirim burst beruntun
MAX_QUEUE_SIZE = 1

# Folder log komunikasi LoRa lokal di Raspberry Pi
DEFAULT_COMM_LOG_DIR = os.path.expanduser(
    "~/blackbox2_ws/logs/communication"
)


# ============================================================
# FIELD ORDER - TOTAL 109 FIELD (DISAMAKAN DENGAN WEB)
# URUTAN INI TIDAK BOLEH DIUBAH TANPA MENYESUAIKAN RECEIVER
# ============================================================

EXPECTED_KEYS = [

    # --------------------------------------------------------
    # 1. ARDUINO UNO Q (24) -> Web readingId 1001..1024 (M1) / 2001..2024 (M2)
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 2. COSMIC AURORA (12)
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 3. LOAD CELL (5)
    # --------------------------------------------------------

    "loadcell_paka",
    "loadcell_paki",
    "loadcell_leher",
    "loadcell_kaka",
    "loadcell_kaki",

    # --------------------------------------------------------
    # 4. FLEX (16)
    # --------------------------------------------------------

    "flex1",
    "flex2",
    "flex3",
    "flex4",
    "flex5",
    "flex6",
    "flex7",
    "flex8",
    "flex9",
    "flex10",
    "flex11",
    "flex12",
    "flex13",
    "flex14",
    "flex15",
    "flex16",

    # --------------------------------------------------------
    # 5. MCP9808 (14)
    # --------------------------------------------------------

    "mcp1",
    "mcp2",
    "mcp3",
    "mcp4",
    "mcp5",
    "mcp6",
    "mcp7",
    "mcp8",
    "mcp9",
    "mcp10",
    "mcp11",
    "mcp12",
    "mcp13",
    "mcp14",

    # --------------------------------------------------------
    # 6. FSR (14)
    # --------------------------------------------------------

    "fsr1",
    "fsr2",
    "fsr3",
    "fsr4",
    "fsr5",
    "fsr6",
    "fsr7",
    "fsr8",
    "fsr9",
    "fsr10",
    "fsr11",
    "fsr12",
    "fsr13",
    "fsr14",

    # --------------------------------------------------------
    # 7. PIEZO (14)
    # --------------------------------------------------------

    "piezo1",
    "piezo2",
    "piezo3",
    "piezo4",
    "piezo5",
    "piezo6",
    "piezo7",
    "piezo8",
    "piezo9",
    "piezo10",
    "piezo11",
    "piezo12",
    "piezo13",
    "piezo14",

    # --------------------------------------------------------
    # 8. AMG8833 (1)
    # --------------------------------------------------------

    "thermal",

    # --------------------------------------------------------
    # 9. WITMOTION WT901 (9)
    # --------------------------------------------------------

    "ax",
    "ay",
    "az",

    "gx",
    "gy",
    "gz",

    "roll",
    "pitch",
    "yaw",
]


# ============================================================
# ARDUINO FIELD MAPPING
# ============================================================

ARDUINO_MAP = {

    "lidar_distance_cm": "lidar_raw",
    "lidar_median_cm": "lidar_median",

    "mq135_nh3_ppm": "mq_nh3",
    "mq135_co_ppm": "mq_co",
    "mq135_co2_ppm": "mq_co2",
    "mq2_smoke_index": "smoke",

    "mpu_accel_x": "mpu_acc_x_raw",
    "mpu_accel_y": "mpu_acc_y_raw",
    "mpu_accel_z": "mpu_acc_z_raw",

    "mpu_gyro_x": "mpu_gyro_x_raw",
    "mpu_gyro_y": "mpu_gyro_y_raw",
    "mpu_gyro_z": "mpu_gyro_z_raw",

    "mpu_accel_kalman_x": "mpu_acc_x_kalman",
    "mpu_accel_kalman_y": "mpu_acc_y_kalman",
    "mpu_accel_kalman_z": "mpu_acc_z_kalman",

    "mpu_gyro_kalman_x": "mpu_gyro_x_kalman",
    "mpu_gyro_kalman_y": "mpu_gyro_y_kalman",
    "mpu_gyro_kalman_z": "mpu_gyro_z_kalman",

    "adxl_accel_x": "adxl_acc_x_raw",
    "adxl_accel_y": "adxl_acc_y_raw",
    "adxl_accel_z": "adxl_acc_z_raw",

    "adxl_accel_kalman_x": "adxl_acc_x_kalman",
    "adxl_accel_kalman_y": "adxl_acc_y_kalman",
    "adxl_accel_kalman_z": "adxl_acc_z_kalman",
}


# ============================================================
# COSMIC AURORA FIELD MAPPING
# ============================================================

AURORA_MAP = {

    "accel_x": "accelx",
    "accel_y": "accely",
    "accel_z": "accelz",

    "gyro_x": "gyrox",
    "gyro_y": "gyroy",
    "gyro_z": "gyroz",

    "bme_temperature": "bme_temp",
    "bme_humidity": "bme_hum",
    "bme_pressure": "bme_pres",
    "bme_altitude": "bme_alt",

    "sound_db_left": "sound_dbLeft",
    "sound_db_right": "sound_dbRight",
}


# ============================================================
# URUTAN LOADCELL DARI HX711_NODE (/sensors/loadcell)
# hx711_node mengirim {"loadcell": [[raw, gram, gram_kalman, newton, newton_kalman], ...]}
# dengan urutan: [0]=leher, [1]=paha_kanan, [2]=paha_kiri, [3]=kaki_kanan, [4]=kaki_kiri
# ============================================================

LOADCELL_INDEX_KEYS = [
    "loadcell_leher",
    "loadcell_paka",
    "loadcell_paki",
    "loadcell_kaka",
    "loadcell_kaki",
]


# ============================================================
# WEB COMPACT V2 READING ID MAPPING (Be-smart-manekin)
# Mannequin 1: 1001..1095, 1098..1111 | Mannequin 2: otomatis +1000
# ============================================================

WEB_READING_ID_MAP_M1 = {
    # 1. Arduino Uno Q (1001..1024)
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

    # 2. Cosmic Aurora - LSM6DS3, BME280, SEN0232 (1025..1036)
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

    # 3. SmartSkin Piezo 1..14 (1037..1050)
    **{f"piezo{i}": 1036 + i for i in range(1, 15)},

    # 4. SmartSkin FSR 1..14 (1051..1064)
    **{f"fsr{i}": 1050 + i for i in range(1, 15)},

    # 5. SmartSkin MCP9808 1..14 (1065..1078)
    **{f"mcp{i}": 1064 + i for i in range(1, 15)},

    # 6. SmartSkin Flex 1..16 (1079..1094)
    **{f"flex{i}": 1078 + i for i in range(1, 17)},

    # 7. Thermal (1095)
    "thermal": 1095,

    # 8. Loadcell YZC-133 (1098..1102; 1096..1097 cadangan thermal)
    "loadcell_leher": 1098,
    "loadcell_paki": 1099,
    "loadcell_paka": 1100,
    "loadcell_kaki": 1101,
    "loadcell_kaka": 1102,

    # 9. WitMotion WITSENSOR_AHRS (1103..1111)
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

GRAVITY_MS2 = 9.80665

# ============================================================
# BINARY PRESSED CODEC CONSTANTS (MODE "packed" <= 236 BYTE)
# ============================================================

PRESSED_TEMP_INDICES = frozenset({30, *range(57, 71), 99})   # int16 * 100 (0.01 C)
PRESSED_HUM_INDICES = frozenset({31})                        # int16 * 100 (0.01 %)
PRESSED_PRES_INDICES = frozenset({32})                       # uint16 * 50 (0.02 hPa)
PRESSED_LIDAR_INDICES = frozenset({0, 1})                    # int16 * 10  (0.1 cm)
PRESSED_ALT_INDICES = frozenset({33})                        # int16 * 2   (0.5 m)
PRESSED_RPY_INDICES = frozenset({106, 107, 108})             # int16 * 100 (0.01 deg)

F16_NAN_BYTES = b"\xFF\x7F"
INT16_SENTINEL = -32768
UINT16_SENTINEL = 0xFFFF

# Urutan 18 fitur Edge AI LSTM-Autoencoder (Bit 14..10 dalam 2 byte AI frame B3/Z3)
AI_TARGET_FEATURES = [
    "ADXL345_X", "ADXL345_Y", "ADXL345_Z",
    "Kalman_ADXL_X", "Kalman_ADXL_Y", "Kalman_ADXL_Z",
    "MPU6050_X", "MPU6050_Y", "MPU6050_Z",
    "Kalman_MPU_X", "Kalman_MPU_Y", "Kalman_MPU_Z",
    "LiDAR", "LiDAR_Kalman",
    "CO", "CO2", "NH3", "Gas_Sensor",
]


def encode_pressed_parts(values, mannequin_id: int = 1):
    """
    Menyiapkan komponen biner B2:
      - bitmask (14 byte: bit 0..108 valid/null, bit 109..111 mannequin_id 1..7)
      - primary_bytes (2 byte per nilai aktif, total maks 218 byte)
      - overflow_chunks (list entry <uint8 idx, float32 val> 5 byte)
    """
    bitmask = bytearray(14)
    bitmask[13] = (mannequin_id & 0x07) << 5

    primary_chunks = []
    overflow_chunks = []

    for idx, raw_val in enumerate(values):
        val = valid_number(raw_val)
        if val is None:
            continue

        bitmask[idx >> 3] |= (1 << (idx & 7))

        if idx in PRESSED_PRES_INDICES:
            scaled = int(round(val * 50.0))
            if 0 <= scaled < UINT16_SENTINEL:
                primary_chunks.append(struct.pack("<H", scaled))
            else:
                primary_chunks.append(struct.pack("<H", UINT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif idx in PRESSED_TEMP_INDICES or idx in PRESSED_HUM_INDICES or idx in PRESSED_RPY_INDICES:
            scaled = int(round(val * 100.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif idx in PRESSED_LIDAR_INDICES:
            scaled = int(round(val * 10.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif idx in PRESSED_ALT_INDICES:
            scaled = int(round(val * 2.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        else:
            try:
                packed_f16 = struct.pack("<e", val)
                check_f16 = struct.unpack("<e", packed_f16)[0]
                if packed_f16 != F16_NAN_BYTES and math.isfinite(check_f16):
                    primary_chunks.append(packed_f16)
                else:
                    primary_chunks.append(F16_NAN_BYTES)
                    overflow_chunks.append(struct.pack("<Bf", idx, val))
            except (OverflowError, ValueError):
                primary_chunks.append(F16_NAN_BYTES)
                overflow_chunks.append(struct.pack("<Bf", idx, val))

    return bytes(bitmask), b"".join(primary_chunks), overflow_chunks


def encode_pressed_body(values, mannequin_id: int = 1) -> bytes:
    """
    Memadatkan (press) 109 nilai telemetry menjadi biner kompak B2:
      - 14 byte bitmask + 2 byte per nilai aktif + overflow tail <Bf>.
    Ukuran tanpa overflow saat 109 field penuh = 14 + 218 = 232 byte body.
    """
    bitmask, primary_bytes, overflow_chunks = encode_pressed_parts(
        values, mannequin_id=mannequin_id
    )
    return bitmask + primary_bytes + b"".join(overflow_chunks)


def _pack_pressed_candidate(raw_body: bytes, allow_zlib: bool) -> bytes:
    """Membungkus body menjadi frame B3 (atau Z3 zlib bila lebih kecil)."""
    b3_head_body = b"B3" + raw_body
    b3_frame = b3_head_body + struct.pack(">H", crc16_ccitt(b3_head_body))

    if allow_zlib:
        try:
            z_body = zlib.compress(raw_body, 9)
            z3_head_body = b"Z3" + z_body
            z3_frame = z3_head_body + struct.pack(">H", crc16_ccitt(z3_head_body))
            if len(z3_frame) < len(b3_frame):
                return z3_frame
        except Exception:
            pass

    return b3_frame


def build_pressed_frame(
    values,
    ai_bytes: bytes = b"\x00\x7C",
    mannequin_id: int = 1,
    allow_zlib: bool = True,
    max_bytes: int = MAX_SINGLE_LORA_PAYLOAD,
) -> bytes:
    """
    Membungkus body biner menjadi 1 frame uplink tunggal siap kirim LoRa:
      - B3 + bitmask(14B) + primary(<=218B) + ai_bytes(2B) + overflow + CRC16(2B) [maks 238 byte tanpa overflow]
      - Z3 + zlib(body) + CRC16 [dipakai otomatis jika hasil kompresi lebih kecil]
    Jika max_bytes diset (default 238 byte), overflow tail yang melebihi budget
    dipangkas otomatis (binary search jumlah maksimal tail) agar selalu <= 1 uplink.
    """
    bitmask, primary_bytes, overflow_chunks = encode_pressed_parts(
        values, mannequin_id=mannequin_id
    )
    base_bytes = bitmask + primary_bytes + ai_bytes

    full_frame = _pack_pressed_candidate(
        base_bytes + b"".join(overflow_chunks),
        allow_zlib=allow_zlib,
    )
    if max_bytes is None or len(full_frame) <= max_bytes:
        return full_frame

    if not overflow_chunks:
        return full_frame

    best_frame = _pack_pressed_candidate(base_bytes, allow_zlib=allow_zlib)
    lo = 1
    hi = len(overflow_chunks) - 1

    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = _pack_pressed_candidate(
            base_bytes + b"".join(overflow_chunks[:mid]),
            allow_zlib=allow_zlib,
        )
        if len(candidate) <= max_bytes:
            best_frame = candidate
            lo = mid + 1
        else:
            hi = mid - 1

    return best_frame


# ============================================================
# CRC16-CCITT
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


# ============================================================
# NUMERIC VALIDATION
# ============================================================

def valid_number(value):

    if isinstance(value, bool):
        return None

    if value is None:
        return None

    try:

        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    except (ValueError, TypeError):
        return None


# ============================================================
# LORA TRANSMITTER NODE
# ============================================================

_MANNEQUIN_NAME_RE = re.compile(
    r"(?:manekin|mannequin|raspy|rpi)[_-]?([12])(?:\b|[^0-9])",
    re.IGNORECASE,
)


def _resolve_auto_mannequin_id() -> Tuple[int, str]:
    """Deteksi otomatis ID manekin (1/2) dari env, file lokal mesin, atau user/hostname Raspy."""
    env_mid = os.environ.get("MANNEQUIN_ID", "").strip()
    if env_mid.isdigit():
        val = int(env_mid)
        if 1 <= val <= 7:
            return val, f"env:MANNEQUIN_ID={val}"

    for id_path in (
        os.path.expanduser("~/.mannequin_id"),
        os.path.expanduser("~/blackbox2_ws/.mannequin_id"),
        "/etc/mannequin_id",
    ):
        try:
            if os.path.isfile(id_path):
                raw = open(id_path, "r", encoding="utf-8").read().strip()
                if raw.isdigit():
                    val = int(raw)
                    if 1 <= val <= 7:
                        return val, f"file:{id_path}"
        except Exception:
            pass

    candidates = []
    try:
        candidates.append(("host", socket.gethostname()))
    except Exception:
        pass
    try:
        candidates.append(("user", getpass.getuser()))
    except Exception:
        pass
    home_name = os.path.basename(os.path.expanduser("~").rstrip("/\\"))
    if home_name:
        candidates.append(("home", home_name))

    for kind, label in candidates:
        m = _MANNEQUIN_NAME_RE.search(label or "")
        if m:
            return int(m.group(1)), f"{kind}:{label}"

    return 1, "default"


class LoRaTransmitterNode(Node):

    def __init__(self, default_mannequin_id: Optional[int] = None):

        super().__init__("lora_transmitter_node")

        if default_mannequin_id is not None:
            auto_mid = int(default_mannequin_id)
            mid_source = f"entrypoint:{auto_mid}"
        else:
            auto_mid, mid_source = _resolve_auto_mannequin_id()

        self.declare_parameter("mannequin_id", auto_mid)
        self.declare_parameter("boot_count", 1)
        self.declare_parameter("payload_mode", "packed")
        self.declare_parameter("freq", FREQUENCY)
        self.declare_parameter("cad_enabled", CAD_ENABLED)
        self.declare_parameter("transmit_interval", TRANSMIT_INTERVAL)
        self.declare_parameter("log_enabled", True)
        self.declare_parameter("log_dir", DEFAULT_COMM_LOG_DIR)

        self.mannequin_id = int(self.get_parameter("mannequin_id").value)
        if self.mannequin_id != auto_mid:
            mid_source = f"ros_param:{self.mannequin_id}"
        self._mannequin_id_source = mid_source
        self.boot_count = int(self.get_parameter("boot_count").value)
        self.payload_mode = str(self.get_parameter("payload_mode").value).lower()
        self.freq = int(self.get_parameter("freq").value)
        self.cad_enabled = bool(self.get_parameter("cad_enabled").value)
        self.cad_active = False
        self.transmit_interval = max(
            0.2, float(self.get_parameter("transmit_interval").value)
        )
        self.log_enabled = bool(self.get_parameter("log_enabled").value)
        self.log_dir = os.path.expanduser(
            str(self.get_parameter("log_dir").value)
        )
        self.packet_sequence = 0

        self.lock = threading.RLock()

        # Data terbaru yang sudah diterima
        self.latest_values = {}

        # Data valid terakhir
        self.last_values = {}

        self.tx_queue = deque()

        self.serial_port = None
        self.tx_active = False
        self.rak_ready = False
        self.rak_retry_interval = 10.0

        self.transmitted_count = 0
        self.failed_count = 0
        self._last_single_uplink_warn = 0.0
        # Stagger fase TX agar M1 (+0.0s) dan M2 (+setengah interval) tidak bertabrakan di 1 RX
        initial_offset = (
            (self.transmit_interval * 0.5) if (self.mannequin_id % 2 == 0) else 0.0
        )
        self._next_telemetry_at = time.monotonic() + initial_offset

        self.received_sources = set()

        # Alert queue has priority over regular telemetry packets.
        self.alert_sequence = 0
        self.alert_tx_queue = deque(maxlen=10)

        # Status Edge AI LSTM-Autoencoder terbaru (dikemas ke 2 byte B3/Z3)
        self.latest_ai_state = {
            "is_anomaly": False,
            "dominant_idx": 31,
            "rmse_x100": 0,
        }

        # Inisialisasi CSV Logger Lokal di ~/blackbox2_ws/logs/communication/
        self.comm_csv_file = None
        self.comm_csv_writer = None
        self.telemetry_csv_file = None
        self.telemetry_csv_writer = None
        self.initialize_local_csv_logs()

        # Publisher JSON Compact v2 siap kirim langsung ke Web (Be-smart-manekin)
        self.web_v2_pub = self.create_publisher(
            String,
            "/telemetri/web/v2",
            10
        )

        # ====================================================
        # ROS SUBSCRIBERS
        # ====================================================

        # Arduino JSON
        self.arduino_sub = self.create_subscription(
            String,
            "/telemetri/arduino/data",
            self.arduino_callback,
            10
        )

        # Cosmic Aurora JSON
        self.aurora_sub = self.create_subscription(
            String,
            "/telemetri/aurora/data",
            self.aurora_callback,
            10
        )

        # Analog FSR, Piezo, Flex
        self.analog_sub = self.create_subscription(
            String,
            "/sensors/analog_data",
            self.analog_callback,
            10
        )

        # MCP9808
        self.mcp_sub = self.create_subscription(
            String,
            "/sensors/mcp9808",
            self.mcp_callback,
            10
        )

        # Load cell
        self.loadcell_sub = self.create_subscription(
            String,
            "/sensors/loadcell",
            self.loadcell_callback,
            10
        )

        # WitMotion WT901 JSON (/sensors/witmotion)
        self.witmotion_sub = self.create_subscription(
            String,
            "/sensors/witmotion",
            self.witmotion_callback,
            10
        )

        # Compact system alerts from Alert Manager
        self.alert_sub = self.create_subscription(
            String,
            "/system/alerts",
            self.alert_callback,
            10
        )

        # Edge AI LSTM-Autoencoder status (/ai/anomaly_status)
        self.ai_sub = self.create_subscription(
            String,
            "/ai/anomaly_status",
            self.ai_status_callback,
            10
        )

        # ====================================================
        # TIMERS
        # ====================================================

        self.transmit_timer = self.create_timer(
            0.05,
            self.transmit_callback
        )

        self.queue_timer = self.create_timer(
            0.01,
            self.process_queue
        )

        # Retry RAK initialization while acquisition/ROS continues.
        self.rak_retry_timer = self.create_timer(
            self.rak_retry_interval,
            self.retry_rak_connection
        )

        # ====================================================
        # SERIAL INITIALIZATION
        # ====================================================

        self.initialize_serial()

        self.get_logger().info(
            f"LoRa node initialized | "
            f"mannequin_id={self.mannequin_id} ({self._mannequin_id_source}) | "
            f"Freq:{self.freq} SF{SPREADING_FACTOR} BW{BANDWIDTH} CR{CODING_RATE} Pwr:{TX_POWER}dBm | "
            f"CAD:{self.cad_active} | Fields: {len(EXPECTED_KEYS)} | "
            f"Interval: {self.transmit_interval}s"
        )

    # ========================================================
    # LOCAL COMMUNICATION CSV LOGGER (~/blackbox2_ws/logs/communication)
    # ========================================================

    def initialize_local_csv_logs(self):
        if not self.log_enabled:
            return

        try:
            os.makedirs(self.log_dir, exist_ok=True)

            comm_path = os.path.join(
                self.log_dir,
                "lora_tx_communication.csv"
            )
            telemetry_path = os.path.join(
                self.log_dir,
                "lora_tx_telemetry.csv"
            )

            self.comm_csv_file = open(
                comm_path,
                "a",
                newline="",
                encoding="utf-8",
                buffering=1
            )
            self.comm_csv_writer = csv.writer(self.comm_csv_file)
            if os.path.getsize(comm_path) == 0:
                self.comm_csv_writer.writerow([
                    "timestamp",
                    "tx_no",
                    "failed_total",
                    "mannequin_id",
                    "packet_type",
                    "payload_mode",
                    "freq_hz",
                    "sf",
                    "bw_code",
                    "cr_code",
                    "preamble",
                    "tx_power_dbm",
                    "fragment_bytes",
                    "crc_hex",
                    "tx_duration_ms",
                    "tx_status",
                    "valid_fields",
                    "total_fields",
                    "active_sources",
                ])

            self.telemetry_csv_file = open(
                telemetry_path,
                "a",
                newline="",
                encoding="utf-8",
                buffering=1
            )
            self.telemetry_csv_writer = csv.writer(self.telemetry_csv_file)
            if os.path.getsize(telemetry_path) == 0:
                self.telemetry_csv_writer.writerow([
                    "timestamp",
                    "tx_no",
                    "mannequin_id",
                    "tx_status",
                    "fragment_bytes",
                    "tx_duration_ms",
                    "valid_fields",
                    *EXPECTED_KEYS,
                ])

            self.get_logger().info(
                f"Local LoRa TX Communication log aktif di: {self.log_dir}"
            )

        except Exception as error:
            self.log_enabled = False
            self.get_logger().error(
                f"Gagal menyiapkan log komunikasi lokal: {error}"
            )

    def write_local_tx_log(
        self,
        fragment: bytes,
        packet_type: str,
        tx_status: str,
        tx_duration_ms: float
    ):
        if not self.log_enabled:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        crc_hex = "NONE"
        if fragment.startswith((b"B2", b"Z2", b"B3", b"Z3")) and len(fragment) >= 4:
            crc_hex = f"{struct.unpack('>H', fragment[-2:])[0]:04X}"
        elif b"*" in fragment:
            try:
                crc_hex = fragment.rsplit(b"*", 1)[1].decode("ascii", errors="ignore").strip()
            except Exception:
                crc_hex = "NONE"

        if packet_type == "alert":
            logged_mode = "alert"
        elif fragment.startswith(b"Z3"):
            logged_mode = "packed_z3"
        elif fragment.startswith(b"B3"):
            logged_mode = "packed_b3"
        elif fragment.startswith(b"Z2"):
            logged_mode = "packed_z2"
        elif fragment.startswith(b"B2"):
            logged_mode = "packed_b2"
        else:
            logged_mode = self.payload_mode

        snapshot = {}
        with self.lock:
            for key in EXPECTED_KEYS:
                val = self.latest_values.get(key)
                if val is None and HOLD_LAST_VALUE:
                    val = self.last_values.get(key)
                snapshot[key] = val
            sources_str = ";".join(sorted(self.received_sources))

        valid_fields = sum(1 for v in snapshot.values() if v is not None)

        try:
            if self.comm_csv_writer is not None:
                self.comm_csv_writer.writerow([
                    timestamp,
                    self.transmitted_count,
                    self.failed_count,
                    self.mannequin_id,
                    packet_type,
                    logged_mode,
                    self.freq,
                    SPREADING_FACTOR,
                    BANDWIDTH,
                    CODING_RATE,
                    PREAMBLE,
                    TX_POWER,
                    len(fragment),
                    crc_hex,
                    round(tx_duration_ms, 2),
                    tx_status,
                    valid_fields,
                    len(EXPECTED_KEYS),
                    sources_str,
                ])

            if packet_type == "telemetry" and self.telemetry_csv_writer is not None:
                self.telemetry_csv_writer.writerow([
                    timestamp,
                    self.transmitted_count,
                    self.mannequin_id,
                    tx_status,
                    len(fragment),
                    round(tx_duration_ms, 2),
                    valid_fields,
                    *[
                        "" if snapshot.get(k) is None else snapshot.get(k)
                        for k in EXPECTED_KEYS
                    ],
                ])
        except Exception as error:
            self.get_logger().warning(
                f"Gagal menulis CSV log komunikasi TX: {error}"
            )

    # ========================================================
    # SERIAL INITIALIZATION
    # ========================================================

    def initialize_serial(self):
        """Open UART3 and configure the RAK. Failure does not stop ROS."""
        try:
            if self.serial_port is None or not self.serial_port.is_open:
                self.serial_port = serial.Serial(
                    port=SERIAL_PORT,
                    baudrate=BAUD_RATE,
                    timeout=0.1,
                    write_timeout=1
                )
                # Give the UART/module time to settle after opening.
                time.sleep(1.0)
                self.get_logger().info(
                    f"UART opened: {SERIAL_PORT} @ {BAUD_RATE} baud"
                )

            self.serial_port.reset_input_buffer()
            self.serial_port.reset_output_buffer()
            self.rak_ready = self.configure_rak()
            return self.rak_ready

        except Exception as error:
            self.rak_ready = False
            self.get_logger().error(
                f"RAK UART initialization failed: {error}"
            )
            return False

    def retry_rak_connection(self):
        """Retry configuration periodically without blocking sensor nodes."""
        if self.rak_ready:
            return
        self.get_logger().warning(
            f"RAK not ready; retrying {SERIAL_PORT} initialization..."
        )
        self.initialize_serial()

    # ========================================================
    # SEND AT COMMAND
    # ========================================================

    def send_at(self, command, timeout=3.0):
        if self.serial_port is None or not self.serial_port.is_open:
            return False

        try:
            self.serial_port.reset_input_buffer()
            self.serial_port.write((command + "\r\n").encode("ascii"))
            self.serial_port.flush()

            start = time.monotonic()
            response = ""
            while time.monotonic() - start < timeout:
                waiting = self.serial_port.in_waiting
                if waiting:
                    response += self.serial_port.read(waiting).decode(
                        errors="ignore"
                    )
                    upper_response = response.upper()
                    if "ERROR" in upper_response:
                        self.get_logger().error(
                            f"AT error [{command}]: {response.strip()}"
                        )
                        return False
                    if "OK" in upper_response:
                        self.get_logger().info(f"AT OK: {command}")
                        return True
                time.sleep(0.02)

            self.get_logger().warning(
                f"AT timeout: {command} | response={response.strip()!r}"
            )
            return False

        except Exception as error:
            self.get_logger().error(f"AT command error [{command}]: {error}")
            return False

    # ========================================================
    # CONFIGURE RAK
    # ========================================================

    def configure_rak(self):
        if self.serial_port is None or not self.serial_port.is_open:
            self.rak_ready = False
            return False

        commands = [
            "AT",
            "AT+NWM=0",
            "AT+PRECV=0",
            (
                f"AT+P2P={self.freq}:"
                f"{SPREADING_FACTOR}:"
                f"{BANDWIDTH}:"
                f"{CODING_RATE}:"
                f"{PREAMBLE}:"
                f"{TX_POWER}"
            ),
        ]

        self.rak_ready = False
        self.cad_active = False
        self.get_logger().info(
            f"Configuring RAK P2P on {SERIAL_PORT} "
            f"({self.freq} Hz, SF{SPREADING_FACTOR}, BW{BANDWIDTH}, CR{CODING_RATE}, Pwr{TX_POWER})..."
        )

        for command in commands:
            success = False
            # A brief retry helps with module wake-up/boot timing.
            for attempt in range(1, 4):
                if self.send_at(command, timeout=3.0):
                    success = True
                    break
                if attempt < 3:
                    self.get_logger().warning(
                        f"Retry AT command {attempt + 1}/3: {command}"
                    )
                    time.sleep(0.5)

            if not success and command.startswith("AT+P2P="):
                bw_fallback = {0: 125, 1: 250, 2: 500}.get(BANDWIDTH)
                if bw_fallback is not None:
                    fallback_cmd = (
                        f"AT+P2P={self.freq}:{SPREADING_FACTOR}:{bw_fallback}:"
                        f"{CODING_RATE}:{PREAMBLE}:{TX_POWER}"
                    )
                    self.get_logger().warning(
                        f"Fallback P2P command (kHz): {fallback_cmd}"
                    )
                    success = self.send_at(fallback_cmd, timeout=3.0)

            if not success:
                self.get_logger().error(
                    f"RAK configuration failed at: {command}; "
                    f"TX is disabled until configuration succeeds"
                )
                self.rak_ready = False
                return False

            time.sleep(0.25)

        if self.cad_enabled:
            if self.send_at("AT+CAD=1", timeout=1.5):
                self.cad_active = True
                self.get_logger().info("CAD (Listen-Before-Talk) aktif: AT+CAD=1")
            else:
                self.cad_active = False
                self.get_logger().warning(
                    "AT+CAD=1 tidak didukung firmware modul; lanjut TX tanpa CAD."
                )

        self.rak_ready = True
        self.get_logger().info(
            "RAK P2P READY. Telemetry and alert transmission enabled."
        )
        return True

    # ========================================================
    # PARSE JSON
    # ========================================================

    def parse_json(self, raw_data):

        try:

            result = json.loads(raw_data)

            if isinstance(result, dict):
                return result

            if isinstance(result, list):
                return result

            return None

        except (json.JSONDecodeError, TypeError):
            return None

    # ========================================================
    # UPDATE ONE FIELD
    # ========================================================

    def update_field(self, key, value):

        if key not in EXPECTED_KEYS:
            return False

        number = valid_number(value)

        if number is None:
            return False

        with self.lock:

            self.latest_values[key] = number
            self.last_values[key] = number

        return True

    # ========================================================
    # UPDATE MULTIPLE FIELDS USING MAP
    # ========================================================

    def update_mapped_values(self, data, mapping):

        if not isinstance(data, dict):
            return

        updated = 0

        for source_key, target_key in mapping.items():

            if source_key not in data:
                continue

            if self.update_field(
                target_key,
                data[source_key]
            ):
                updated += 1

        if updated > 0:
            return updated

        return 0

    # ========================================================
    # EXTRACT NORMALIZED JSON DATA
    # ========================================================

    def extract_sensor_data(self, payload):

        if not isinstance(payload, dict):
            return None

        # Struktur normal:
        # {
        #   "source": "...",
        #   "status": "...",
        #   "field_count": 25,
        #   "data": {...}
        # }

        nested = payload.get("data")

        if isinstance(nested, dict):
            return nested

        # Mendukung JSON flat bila suatu saat digunakan
        return payload

    # ========================================================
    # ARDUINO CALLBACK
    # ========================================================

    def arduino_callback(self, msg):

        payload = self.parse_json(msg.data)

        if payload is None:
            self.get_logger().warning(
                "Invalid Arduino JSON"
            )
            return

        data = self.extract_sensor_data(payload)

        if not isinstance(data, dict):
            return

        updated = self.update_mapped_values(
            data,
            ARDUINO_MAP
        )

        if updated:

            self.received_sources.add("arduino")

    # ========================================================
    # COSMIC AURORA CALLBACK
    # ========================================================

    def aurora_callback(self, msg):

        payload = self.parse_json(msg.data)

        if payload is None:
            self.get_logger().warning(
                "Invalid Cosmic Aurora JSON"
            )
            return

        data = self.extract_sensor_data(payload)

        if not isinstance(data, dict):
            return

        updated = self.update_mapped_values(
            data,
            AURORA_MAP
        )

        if updated:

            self.received_sources.add("aurora")

    # ========================================================
    # ANALOG CALLBACK
    # ========================================================

    def analog_callback(self, msg):

        payload = self.parse_json(msg.data)

        if not isinstance(payload, dict):
            self.get_logger().warning(
                "Invalid analog JSON"
            )
            return

        # Struktur:
        # {
        #   "fsr": [...14...],
        #   "piezo": [...14...],
        #   "flex": [...16...]
        # }

        updated = 0

        updated += self.update_array(
            payload.get("flex"),
            "flex"
        )

        updated += self.update_array(
            payload.get("fsr"),
            "fsr"
        )

        updated += self.update_array(
            payload.get("piezo"),
            "piezo"
        )

        if updated:
            self.received_sources.add("analog")

    # ========================================================
    # ARRAY MAPPING
    # ========================================================

    def update_array(self, values, prefix):

        if not isinstance(values, list):
            return 0

        updated = 0

        for index, value in enumerate(values):

            key = f"{prefix}{index + 1}"

            if key not in EXPECTED_KEYS:
                continue

            if self.update_field(key, value):
                updated += 1

        return updated

    # ========================================================
    # MCP9808 CALLBACK
    # ========================================================

    def mcp_callback(self, msg):

        payload = self.parse_json(msg.data)

        if not isinstance(payload, list):
            self.get_logger().warning(
                "MCP9808 data must be a JSON array"
            )
            return

        updated = self.update_array(
            payload,
            "mcp"
        )

        if updated:
            self.received_sources.add("mcp9808")

    # ========================================================
    # LOAD CELL CALLBACK
    # ========================================================

    def loadcell_callback(self, msg):

        payload = self.parse_json(msg.data)

        if not isinstance(payload, dict):
            return

        # 1. Format terbaru dari hx711_node.py:
        #    {"loadcell": [[raw, gram, gram_kalman, newton, newton_kalman], ...]}
        #    Urutan LOADCELLS di hx711_node.py:
        #    [0]=leher, [1]=paha_kanan, [2]=paha_kiri, [3]=kaki_kanan, [4]=kaki_kiri
        loadcell_list = payload.get("loadcell")

        if isinstance(loadcell_list, list):
            updated = 0

            for idx, target_key in enumerate(LOADCELL_INDEX_KEYS):
                if idx >= len(loadcell_list):
                    break

                entry = loadcell_list[idx]

                if isinstance(entry, list):
                    # Prioritaskan gram_kalman (index 2), fallback ke gram (index 1) atau raw (index 0)
                    if len(entry) >= 3:
                        val = entry[2]
                    elif len(entry) >= 2:
                        val = entry[1]
                    elif len(entry) >= 1:
                        val = entry[0]
                    else:
                        continue
                else:
                    val = entry

                if self.update_field(target_key, val):
                    updated += 1

            if updated:
                self.received_sources.add("loadcell")
            return

        # 2. Format dictionary (fallback):
        data = payload.get("data", payload)

        if not isinstance(data, dict):
            return

        mapping = {
            "loadcell_paka": "loadcell_paka",
            "loadcell_paki": "loadcell_paki",
            "loadcell_leher": "loadcell_leher",
            "loadcell_kaka": "loadcell_kaka",
            "loadcell_kaki": "loadcell_kaki",
            "leher": "loadcell_leher",
            "paha_kanan": "loadcell_paka",
            "paha_kiri": "loadcell_paki",
            "kaki_kanan": "loadcell_kaka",
            "kaki_kaki": "loadcell_kaki",
            "kaki_kiri": "loadcell_kaki",
        }

        updated = self.update_mapped_values(
            data,
            mapping
        )

        if updated:
            self.received_sources.add("loadcell")

    # ========================================================
    # WITMOTION JSON CALLBACK (/sensors/witmotion)
    # ========================================================

    def witmotion_callback(self, msg):

        payload = self.parse_json(msg.data)

        if not isinstance(payload, dict):
            self.get_logger().warning(
                "Invalid WitMotion JSON"
            )
            return

        # Saat masih auto-calibration (status == "calibrating"),
        # witmotion_node hanya mengirim progress kalibrasi tanpa field data.
        status = str(payload.get("status", "")).lower()
        if status == "calibrating":
            return

        data = self.extract_sensor_data(payload)

        if not isinstance(data, dict):
            return

        accel = data.get("accel")
        gyro = data.get("gyro")
        euler = data.get("euler")

        candidates = {}

        # Web Backend menggunakan satuan:
        # - Akselerasi: g (-16 .. 16 g). Jika witmotion_node mengirim m/s2, bagi 9.80665.
        # - Gyroscope : deg/s (-2000 .. 2000 °/s). Jika witmotion_node mengirim rad/s, konversi degrees().
        # - Euler     : degree (-180 .. 180 °).
        if isinstance(accel, dict):
            accel_unit = str(accel.get("unit", "m/s2")).lower()
            for axis, key in (("x", "ax"), ("y", "ay"), ("z", "az")):
                raw_val = valid_number(accel.get(axis))
                if raw_val is not None:
                    if "m/s" in accel_unit:
                        candidates[key] = round(raw_val / GRAVITY_MS2, 4)
                    else:
                        candidates[key] = round(raw_val, 4)
        else:
            candidates["ax"] = data.get("accel_x", data.get("ax"))
            candidates["ay"] = data.get("accel_y", data.get("ay"))
            candidates["az"] = data.get("accel_z", data.get("az"))

        if isinstance(gyro, dict):
            gyro_unit = str(gyro.get("unit", "rad/s")).lower()
            for axis, key in (("x", "gx"), ("y", "gy"), ("z", "gz")):
                raw_val = valid_number(gyro.get(axis))
                if raw_val is not None:
                    if "rad" in gyro_unit:
                        candidates[key] = round(math.degrees(raw_val), 4)
                    else:
                        candidates[key] = round(raw_val, 4)
        else:
            candidates["gx"] = data.get("gyro_x", data.get("gx"))
            candidates["gy"] = data.get("gyro_y", data.get("gy"))
            candidates["gz"] = data.get("gyro_z", data.get("gz"))

        if isinstance(euler, dict):
            for axis in ("roll", "pitch", "yaw"):
                raw_val = valid_number(euler.get(axis))
                if raw_val is not None:
                    candidates[axis] = round(raw_val, 3)
        else:
            candidates["roll"] = data.get("roll")
            candidates["pitch"] = data.get("pitch")
            candidates["yaw"] = data.get("yaw")

        updated = 0

        for key, val in candidates.items():
            if self.update_field(key, val):
                updated += 1

        if updated:
            self.received_sources.add("witmotion")

    # ========================================================
    # COMPACT ALERT ENCODER
    # ========================================================

    def alert_callback(self, msg):
        """Encode a JSON alert as a compact 13-byte ASCII payload (A2 with mannequin_id).

        Layout: A2 + mannequin_id(1 hex) + sequence(4 hex) + severity(1 hex) +
                event(2 hex) + status(1 hex) + offline-mask(2 hex).
        CRC16 is appended by fragment_packet() (1 single uplink, no FRG split).
        """
        try:
            alert = json.loads(msg.data)
            if not isinstance(alert, dict):
                raise ValueError("Alert JSON must be an object")

            severity_map = {"INFO": 0, "WARNING": 1, "ERROR": 2, "CRITICAL": 3}
            event_map = {
                "SYSTEM_STATUS_CHANGED": 0x01,
                "DEVICE_OFFLINE": 0x02,
                "DEVICE_RECOVERED": 0x03,
                "DEVICE_CALIBRATING": 0x04,
            }
            status_map = {
                "INITIALIZING": 0,
                "OPERATIONAL": 1,
                "DEGRADED": 2,
                "CALIBRATING": 3,
            }
            device_bits = {
                "arduino_uno_q": 0,
                "cosmic_aurora": 1,
                "analog_sensors": 2,
                "mcp9808": 3,
                "hx711_loadcells": 4,
                "witmotion_wt901": 5,
            }

            severity_name = str(alert.get("severity", "INFO")).upper()
            event_name = str(alert.get("code", "UNKNOWN")).upper()
            details = alert.get("details") or {}
            if not isinstance(details, dict):
                details = {}
            if (details.get("device_event") or {}).get("event_type") == "AI_ANOMALY_DETECTED":
                return
            system = details.get("system_status") or {}
            if not isinstance(system, dict):
                system = {}

            status_name = system.get("status") or details.get("current_status") or "INITIALIZING"
            severity = severity_map.get(severity_name, 0)
            event_code = event_map.get(event_name, 0x7F)
            status = status_map.get(str(status_name).upper(), 0)

            offline_devices = system.get("offline_devices", [])
            if not isinstance(offline_devices, list):
                offline_devices = []
            # Device-specific events may identify one device directly.
            device_name = details.get("device") or details.get("device_name")
            if device_name:
                offline_devices = list(offline_devices) + [device_name]

            device_mask = 0
            for device in offline_devices:
                bit = device_bits.get(str(device))
                if bit is not None:
                    device_mask |= (1 << bit)

            self.alert_sequence = (self.alert_sequence + 1) & 0xFFFF
            mid_nibble = max(1, min(7, int(self.mannequin_id)))
            compact = (
                f"A2{mid_nibble:X}{self.alert_sequence:04X}{severity:X}"
                f"{event_code:02X}{status:X}{device_mask:02X}"
            )
            if len(compact) != 13:
                raise ValueError(f"Invalid compact alert length: {len(compact)}")

            fragment = self.fragment_packet(compact)
            if len(self.alert_tx_queue) == self.alert_tx_queue.maxlen:
                self.get_logger().warning("Alert TX queue full; oldest alert will be discarded")
            self.alert_tx_queue.append(fragment)
            self.get_logger().info(
                f"Alert queued: {compact} ({len(compact)} payload bytes)"
            )

        except Exception as error:
            self.get_logger().error(f"Alert encoding failed: {error}")

    # ========================================================
    # BUILD FIXED JSON ARRAY
    # ========================================================

    def _collect_ordered_values(self):

        values = []

        with self.lock:

            for key in EXPECTED_KEYS:

                value = self.latest_values.get(key)

                if value is None and HOLD_LAST_VALUE:
                    value = self.last_values.get(key)

                values.append(value)

        return values

    def build_packet(self):

        values = self._collect_ordered_values()

        # None akan menjadi null di JSON.
        # Tidak ada dummy atau angka buatan.

        return json.dumps(
            values,
            separators=(",", ":"),
            allow_nan=False
        )

    # ========================================================
    # EDGE AI ANOMALY STATUS CALLBACK (/ai/anomaly_status)
    # ========================================================

    def ai_status_callback(self, msg):
        try:
            data = json.loads(msg.data)
            if not isinstance(data, dict):
                return
            is_anom = bool(data.get("is_anomaly", False))
            rmse = float(data.get("rmse", 0.0))
            if not math.isfinite(rmse):
                rmse = 0.0
            dom_feat = str(data.get("dominant_feature", ""))
            dom_idx = (
                AI_TARGET_FEATURES.index(dom_feat)
                if dom_feat in AI_TARGET_FEATURES
                else 31
            )
            rmse_scaled = max(0, min(1023, int(round(rmse * 100.0))))
            with self.lock:
                self.latest_ai_state = {
                    "is_anomaly": is_anom,
                    "dominant_idx": dom_idx,
                    "rmse_x100": rmse_scaled,
                }
                self.received_sources.add("edge_ai")
        except Exception as error:
            self.get_logger().warning(f"Invalid AI status JSON: {error}")

    def encode_ai_2bytes(self) -> bytes:
        with self.lock:
            st = dict(self.latest_ai_state)
        packed_u16 = (
            ((1 if st["is_anomaly"] else 0) << 15)
            | ((st["dominant_idx"] & 0x1F) << 10)
            | (st["rmse_x100"] & 0x03FF)
        )
        return struct.pack("<H", packed_u16)

    def build_pressed_packet(self):

        values = self._collect_ordered_values()

        return build_pressed_frame(
            values,
            ai_bytes=self.encode_ai_2bytes(),
            mannequin_id=self.mannequin_id,
            allow_zlib=True,
            max_bytes=MAX_SINGLE_LORA_PAYLOAD,
        )

    # ========================================================
    # BUILD WEB COMPACT V2 PACKET (Be-smart-manekin)
    # {"v":2,"b":...,"s":...,"t":...,"r":[[readingId,value],...]}
    # ========================================================

    def build_web_v2_packet(self):

        id_offset = 1000 if self.mannequin_id == 2 else 0
        readings = []

        with self.lock:

            for key, base_id in WEB_READING_ID_MAP_M1.items():

                value = self.latest_values.get(key)

                if value is None and HOLD_LAST_VALUE:
                    value = self.last_values.get(key)

                readings.append([base_id + id_offset, value])

            self.packet_sequence = (self.packet_sequence + 1) & 0x7FFFFFFF
            seq = self.packet_sequence

        payload = {
            "v": 2,
            "b": self.boot_count,
            "s": seq,
            "t": int(time.time() * 1000),
            "r": readings,
        }

        return json.dumps(
            payload,
            separators=(",", ":"),
            allow_nan=False
        )

    # ========================================================
    # SINGLE-UPLINK LORA FRAME BUILDER (TANPA MULTI-FRAGMEN FRG)
    # ========================================================

    def _warn_single_uplink_fallback(self, reason: str):
        now = time.monotonic()
        last_warn = getattr(self, "_last_single_uplink_warn", 0.0)
        if now - last_warn >= 30.0:
            self._last_single_uplink_warn = now
            self.get_logger().warning(reason)

    @staticmethod
    def _frame_ascii_payload(payload: str) -> bytes:
        raw_bytes = payload.encode("utf-8")
        crc = crc16_ccitt(raw_bytes)
        return raw_bytes + f"*{crc:04X}".encode("ascii")

    def fragment_packet(self, payload):
        """Membungkus 1 paket uplink utuh (<= 238 byte), tanpa dipecah fragmen."""
        if isinstance(payload, (bytes, bytearray)):
            complete_data = bytes(payload)
        else:
            complete_data = self._frame_ascii_payload(payload)

        if len(complete_data) > MAX_SINGLE_LORA_PAYLOAD:
            raise ValueError(
                f"Payload melebihi batas 1 uplink LoRa "
                f"({len(complete_data)} > {MAX_SINGLE_LORA_PAYLOAD} bytes)"
            )

        return complete_data

    def build_fragments(self, payload):
        """Selalu mengembalikan 1 uplink tunggal (tidak pernah memecah ke multi-fragmen)."""
        return [self.fragment_packet(payload)]

    def build_single_uplink_packet(self, web_v2_json: str = None) -> bytes:
        """Menghasilkan tepat 1 paket uplink LoRa (<= 238 byte)."""
        if self.payload_mode in ("packed", "compressed", "binary", "b2", "b3", "z3"):
            return self.build_pressed_packet()

        raw_json = (
            web_v2_json if (self.payload_mode == "web_v2" and web_v2_json is not None)
            else (self.build_web_v2_packet() if self.payload_mode == "web_v2" else self.build_packet())
        )
        framed_json = self._frame_ascii_payload(raw_json)
        if len(framed_json) <= MAX_SINGLE_LORA_PAYLOAD:
            return framed_json

        self._warn_single_uplink_fallback(
            f"payload_mode='{self.payload_mode}' ({len(framed_json)}B) melebihi batas "
            f"{MAX_SINGLE_LORA_PAYLOAD}B dan fragmentasi TX dinonaktifkan; "
            "otomatis dikirim dalam 1 uplink packed (B2/Z2)."
        )
        return self.build_pressed_packet()

    # ========================================================
    # TRANSMIT TIMER
    # ========================================================

    def transmit_callback(self):

        now = time.monotonic()
        if now < getattr(self, "_next_telemetry_at", 0.0):
            return

        # Jangan kirim paket kosong sebelum menerima data sensor.
        with self.lock:

            if not self.latest_values:
                return

        self._next_telemetry_at = now + self.transmit_interval + random.uniform(-0.12, 0.12)

        try:

            # Publish format Compact v2 Web ke topic ROS /telemetri/web/v2
            web_v2_json = self.build_web_v2_packet()
            web_msg = String()
            web_msg.data = web_v2_json
            self.web_v2_pub.publish(web_msg)

            if self.serial_port is None:
                return

            packet = self.build_single_uplink_packet(web_v2_json=web_v2_json)

            with self.lock:

                while len(self.tx_queue) >= MAX_QUEUE_SIZE and self.tx_queue:

                    self.tx_queue.popleft()

                    self.get_logger().warning(
                        "Oldest packet removed from TX queue"
                    )

                self.tx_queue.append(packet)

        except Exception as error:

            self.failed_count += 1

            self.get_logger().error(
                f"Packet creation failed: {error}"
            )

    # ========================================================
    # PROCESS TX QUEUE
    # ========================================================

    def process_queue(self):

        if self.serial_port is None or not self.rak_ready:
            return

        if self.tx_active:
            return

        # Alert packets take priority; telemetry queue remains unchanged.
        if self.alert_tx_queue:
            fragment = self.alert_tx_queue.popleft()
            packet_type = "alert"
        elif self.tx_queue:
            fragment = self.tx_queue.popleft()
            packet_type = "telemetry"
        else:
            return

        self.tx_active = True
        tx_start = time.monotonic()

        try:

            hex_data = fragment.hex().upper()

            command = f"AT+PSEND={hex_data}"

            self.get_logger().info(
                f"TX SIZE: {len(fragment)} bytes | "
                f"AT command: {len(command)} chars"
            )

            tx_result = self._send_p2p_with_cad_retry(command)
            tx_duration_ms = (time.monotonic() - tx_start) * 1000.0

            if tx_result == "DONE":

                self.transmitted_count += 1

                self.write_local_tx_log(
                    fragment=fragment,
                    packet_type=packet_type,
                    tx_status="SUCCESS",
                    tx_duration_ms=tx_duration_ms
                )

                self.get_logger().info(
                    f"TX success: {self.transmitted_count} | "
                    f"Bytes: {len(fragment)} | "
                    f"Time: {tx_duration_ms:.1f}ms | "
                    f"Sources: {sorted(self.received_sources)}"
                )

            else:

                self.failed_count += 1
                tx_status = "CAD_BUSY" if tx_result == "BUSY" else "FAILED"

                self.write_local_tx_log(
                    fragment=fragment,
                    packet_type=packet_type,
                    tx_status=tx_status,
                    tx_duration_ms=tx_duration_ms
                )

                self.get_logger().warning(
                    f"TX failed ({tx_status}): {self.failed_count}"
                )

        except Exception as error:

            self.failed_count += 1
            tx_duration_ms = (time.monotonic() - tx_start) * 1000.0

            self.write_local_tx_log(
                fragment=fragment,
                packet_type=packet_type,
                tx_status="ERROR",
                tx_duration_ms=tx_duration_ms
            )

            self.get_logger().error(
                f"LoRa TX error: {error}"
            )

        finally:

            self.tx_active = False

    def _send_p2p_with_cad_retry(self, command: str) -> str:
        """Mengirim AT+PSEND dengan retry backoff singkat bila CAD mendeteksi channel sibuk."""
        max_attempts = 1 + (CAD_MAX_RETRIES if self.cad_enabled else 0)
        last_status = "ERROR"
        for attempt in range(1, max_attempts + 1):
            self.serial_port.reset_input_buffer()
            self.serial_port.write((command + "\r\n").encode("ascii"))
            self.serial_port.flush()

            last_status = self.wait_tx_done(timeout=4.5)
            if last_status != "BUSY" or attempt >= max_attempts:
                return last_status

            backoff_s = random.uniform(0.10, 0.25)
            self.get_logger().warning(
                f"CAD channel sibuk (AT_BUSY_ERROR); retry {attempt}/{CAD_MAX_RETRIES} "
                f"dalam {backoff_s * 1000:.0f}ms"
            )
            time.sleep(backoff_s)
        return last_status

    # ========================================================
    # WAIT TRANSMISSION CONFIRMATION
    # ========================================================

    def wait_tx_done(self, timeout=4.5) -> str:

        start = time.monotonic()

        response = ""

        while time.monotonic() - start < timeout:

            if self.serial_port.in_waiting:

                data = self.serial_port.read(
                    self.serial_port.in_waiting
                )

                response += data.decode(
                    errors="ignore"
                )
                upper_resp = response.upper()

                if "+EVT:TXP2P DONE" in upper_resp:
                    return "DONE"

                if "AT_BUSY_ERROR" in upper_resp:
                    return "BUSY"

                if "ERROR" in upper_resp:
                    self.get_logger().error(
                        response.strip()
                    )
                    return "ERROR"

            time.sleep(0.02)

        self.get_logger().warning(
            f"TX confirmation timeout: {response.strip()}"
        )

        return "TIMEOUT"

    # ========================================================
    # SHUTDOWN
    # ========================================================

    def destroy_node(self):

        self.rak_ready = False

        for csv_f in (self.comm_csv_file, self.telemetry_csv_file):
            if csv_f is not None:
                try:
                    csv_f.flush()
                    csv_f.close()
                except Exception:
                    pass

        if self.serial_port is not None:

            try:
                self.serial_port.close()
            except Exception:
                pass

        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None, default_mannequin_id: Optional[int] = None):

    rclpy.init(args=args)

    node = None

    try:

        node = LoRaTransmitterNode(default_mannequin_id=default_mannequin_id)

        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:
        print(f"LoRa node failed: {error}")

    finally:

        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
