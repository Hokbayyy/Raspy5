#!/usr/bin/env python3
"""
Dummy Simulator LoRa TX -> lora_rxManekin1 -> Web Backend (Be-smart-manekin)

Menguji pipeline lengkap `lora_rxManekin1.py` tanpa perangkat keras LoRa/RAK3272S:
  1. Menghasilkan data dummy realistis untuk seluruh 109 parameter sensor manekin
     (mendukung skenario normal, hazard/darurat, partial/null, dan sweep).
  2. Mengemas data menjadi frame LoRa persis seperti transmitter asli:
     - `packed`     : biner B2/Z2 + CRC16 (<= 238 byte, 1 uplink)
     - `array`      : JSON array 109 sensor + *CRC16
     - `web_v2`     : JSON Compact V2 + *CRC16
     - `fragmented` : FRG:i/N:...*CRC16 (menguji FragmentAssembler)
     - `alert`      : Alert kompak 13-byte A2 + *CRC16
     - `mixed`      : Campuran seluruh mode di atas + alert berkala
  3. Mengumpankan event `+EVT:RXP2P:<rssi>:<snr>:<HEX>` ke `LoRaManekin1ReceiverNode`
     (mode `rx-pipeline`) ATAU langsung menembak MQTTS Compact V2 berkecepatan tinggi
     (mode `direct-mqtt`) untuk stress test / kirim data sebanyak mungkin ke web.

Contoh pemakaian:
  # 1. Dry-run lokal (tanpa kirim jaringan, cek decode & CSV log):
  python simulate_lora_rx_to_web.py --dry-run --count 20 --payload-mode mixed

  # 2. Kirim 500 paket Manekin 1 & 2 ke Web MQTT via pipeline penuh lora_rxManekin1.py:
  python simulate_lora_rx_to_web.py --mqtt-pass "<PASSWORD>" --mannequin both --count 500 --rate 20

  # 3. Stress test / kirim sebanyak mungkin (direct-mqtt, 2000 paket secepat mungkin):
  python simulate_lora_rx_to_web.py --mode direct-mqtt --mqtt-pass "<PASSWORD>" --mannequin both --count 2000 --rate 0 --quiet

  # 4. Uji HTTP REST ingest (/sensor-reading/batch) bersamaan atau terpisah:
  python simulate_lora_rx_to_web.py --mqtt-enabled false --http-enabled true --ingest-key "<KEY>" --count 100 --rate 10
"""

from __future__ import annotations

import argparse
from collections import Counter, deque
import json
import math
import os
import random
import signal
import struct
import sys
import threading
import time
import types
from typing import Dict, List, Optional, Tuple
import zlib


def _ensure_serial_stub() -> bool:
    """Menyediakan stub module `serial` jika pyserial belum terpasang (mis. di Windows)."""
    try:
        import serial  # noqa: F401
        return False
    except ImportError:
        stub = types.ModuleType("serial")

        class SerialException(OSError):
            pass

        class DummySerial:
            def __init__(self, *args, **kwargs):
                self.is_open = False
                self.in_waiting = 0

            def reset_input_buffer(self):
                pass

            def reset_output_buffer(self):
                pass

            def write(self, data):
                return len(data)

            def flush(self):
                pass

            def read(self, size=1):
                return b""

            def read_all(self):
                return b""

            def close(self):
                self.is_open = False

        stub.Serial = DummySerial
        stub.SerialException = SerialException
        sys.modules["serial"] = stub
        return True


_USED_SERIAL_STUB = _ensure_serial_stub()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import lora_rxManekin1 as rx  # noqa: E402


# ============================================================
# ENCODER B2/Z2 & ALERT KOMPAK (IDENTIK lora_transmitter_node.py)
# ============================================================

MAX_SINGLE_LORA_PAYLOAD = 238


def _encode_pressed_parts(
    values: List[Optional[float]], mannequin_id: int = 1
) -> Tuple[bytearray, bytes, List[bytes]]:
    bitmask = bytearray(14)
    bitmask[13] = (mannequin_id & 0x07) << 5

    primary_chunks: List[bytes] = []
    overflow_chunks: List[bytes] = []

    for idx, raw_val in enumerate(values):
        val = rx.valid_number(raw_val)
        if val is None:
            continue

        bitmask[idx >> 3] |= 1 << (idx & 7)

        if idx in rx.PRESSED_PRES_INDICES:
            scaled = int(round(val * 50.0))
            if 0 <= scaled < rx.UINT16_SENTINEL:
                primary_chunks.append(struct.pack("<H", scaled))
            else:
                primary_chunks.append(struct.pack("<H", rx.UINT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif (
            idx in rx.PRESSED_TEMP_INDICES
            or idx in rx.PRESSED_HUM_INDICES
            or idx in rx.PRESSED_RPY_INDICES
        ):
            scaled = int(round(val * 100.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", rx.INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif idx in rx.PRESSED_LIDAR_INDICES:
            scaled = int(round(val * 10.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", rx.INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        elif idx in rx.PRESSED_ALT_INDICES:
            scaled = int(round(val * 2.0))
            if -32767 <= scaled <= 32767:
                primary_chunks.append(struct.pack("<h", scaled))
            else:
                primary_chunks.append(struct.pack("<h", rx.INT16_SENTINEL))
                overflow_chunks.append(struct.pack("<Bf", idx, val))
        else:
            try:
                packed_f16 = struct.pack("<e", val)
                check_f16 = struct.unpack("<e", packed_f16)[0]
                if packed_f16 != rx.F16_NAN_BYTES and math.isfinite(check_f16):
                    primary_chunks.append(packed_f16)
                else:
                    primary_chunks.append(rx.F16_NAN_BYTES)
                    overflow_chunks.append(struct.pack("<Bf", idx, val))
            except (OverflowError, ValueError):
                primary_chunks.append(rx.F16_NAN_BYTES)
                overflow_chunks.append(struct.pack("<Bf", idx, val))

    return bitmask, b"".join(primary_chunks), overflow_chunks


def _pack_pressed_candidate(raw_body: bytes, allow_zlib: bool = True) -> bytes:
    b2_head_body = b"B2" + raw_body
    b2_frame = b2_head_body + struct.pack(">H", rx.crc16_ccitt(b2_head_body))
    if allow_zlib:
        try:
            z_body = zlib.compress(raw_body, 9)
            z2_head_body = b"Z2" + z_body
            z2_frame = z2_head_body + struct.pack(">H", rx.crc16_ccitt(z2_head_body))
            if len(z2_frame) < len(b2_frame):
                return z2_frame
        except Exception:
            pass
    return b2_frame


def build_pressed_frame(
    values: List[Optional[float]],
    mannequin_id: int = 1,
    allow_zlib: bool = True,
    max_bytes: int = MAX_SINGLE_LORA_PAYLOAD,
) -> bytes:
    bitmask, primary_bytes, overflow_chunks = _encode_pressed_parts(
        values, mannequin_id=mannequin_id
    )
    base_bytes = bytes(bitmask) + primary_bytes
    full_frame = _pack_pressed_candidate(
        base_bytes + b"".join(overflow_chunks), allow_zlib=allow_zlib
    )
    if max_bytes <= 0 or len(full_frame) <= max_bytes or not overflow_chunks:
        return full_frame

    best_frame = _pack_pressed_candidate(base_bytes, allow_zlib=allow_zlib)
    lo, hi = 1, len(overflow_chunks) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = _pack_pressed_candidate(
            base_bytes + b"".join(overflow_chunks[:mid]), allow_zlib=allow_zlib
        )
        if len(candidate) <= max_bytes:
            best_frame = candidate
            lo = mid + 1
        else:
            hi = mid - 1
    return best_frame


def wrap_crc16_ascii(payload_bytes: bytes) -> bytes:
    crc_hex = f"{rx.crc16_ccitt(payload_bytes):04X}".encode("ascii")
    return payload_bytes + b"*" + crc_hex


def build_lora_frames(
    sensor_dict: Dict[str, Optional[float]],
    mannequin_id: int,
    payload_mode: str,
    boot_count: int,
    seq: int,
    rng: random.Random,
    frag_chunk_size: int = 180,
) -> List[bytes]:
    """Menghasilkan 1 (atau lebih jika fragmented) frame raw bytes siap masuk ke RXP2P."""
    values = [sensor_dict.get(k) for k in rx.EXPECTED_KEYS]

    if payload_mode == "packed":
        return [build_pressed_frame(values, mannequin_id=mannequin_id, allow_zlib=True)]

    if payload_mode == "array":
        rounded = [None if v is None else round(float(v), 3) for v in values]
        raw = json.dumps(rounded, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return [wrap_crc16_ascii(raw)]

    if payload_mode == "web_v2":
        v2_obj = rx.build_web_v2_payload(
            sensor_dict=sensor_dict,
            mannequin_id=mannequin_id,
            boot_count=boot_count,
            seq=seq,
        )
        raw = json.dumps(v2_obj, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return [wrap_crc16_ascii(raw)]

    if payload_mode == "fragmented":
        rounded = [None if v is None else round(float(v), 3) for v in values]
        full_payload = wrap_crc16_ascii(
            json.dumps(rounded, separators=(",", ":"), allow_nan=False).encode("utf-8")
        )
        chunks = [
            full_payload[i : i + frag_chunk_size]
            for i in range(0, len(full_payload), frag_chunk_size)
        ]
        total = len(chunks)
        return [
            f"FRG:{idx}/{total}:".encode("ascii") + chunk
            for idx, chunk in enumerate(chunks, start=1)
        ]

    if payload_mode == "alert":
        mid_nibble = mannequin_id & 0x0F
        severity = rng.choice([0, 1, 2, 3])
        event_code = rng.choice([0x01, 0x02, 0x03, 0x04])
        status = 2 if severity >= 2 else 1
        mask = rng.choice([0x00, 0x01, 0x04, 0x10, 0x15]) if status == 2 else 0x00
        compact = f"A2{mid_nibble:X}{seq & 0xFFFF:04X}{severity:X}{event_code:02X}{status:X}{mask:02X}"
        return [wrap_crc16_ascii(compact.encode("ascii"))]

    raise ValueError(f"Mode payload tidak dikenal: {payload_mode}")


# ============================================================
# GENERATOR DATA DUMMY 109 SENSOR REALISTIS
# ============================================================

def generate_dummy_sensors(
    step: int,
    mannequin_id: int,
    scenario: str,
    rng: random.Random,
) -> Dict[str, Optional[float]]:
    """Menghasilkan 109 nilai sensor realistis yang berubah dinamis per langkah."""
    phase = step * 0.18 + (1.3 if mannequin_id == 2 else 0.0)
    s1 = math.sin(phase)
    c1 = math.cos(phase * 0.7)

    def j(base: float, amp: float, noise: float = 0.05, digits: int = 2) -> float:
        val = base + amp * s1 + rng.uniform(-noise, noise)
        return round(val, digits)

    if scenario == "hazard":
        temp_base = 41.5 + 3.0 * abs(s1)
        nh3_base = 38.0
        co_base = 55.0
        co2_base = 1650.0
        smoke_base = 42.0
        vib_amp = 2.4
        tilt_amp = 38.0
        lidar_base = 35.0
    elif scenario == "sweep":
        t_norm = (step % 100) / 100.0
        temp_base = 25.0 + 20.0 * t_norm
        nh3_base = 2.0 + 48.0 * t_norm
        co_base = 5.0 + 60.0 * t_norm
        co2_base = 420.0 + 1400.0 * t_norm
        smoke_base = 1.0 + 45.0 * t_norm
        vib_amp = 0.2 + 2.5 * t_norm
        tilt_amp = 5.0 + 40.0 * t_norm
        lidar_base = 20.0 + 180.0 * t_norm
    else:
        temp_base = 33.0
        nh3_base = 4.2
        co_base = 7.5
        co2_base = 520.0
        smoke_base = 2.1
        vib_amp = 0.35
        tilt_amp = 8.0
        lidar_base = 115.0

    lidar_raw = max(5.0, round(lidar_base + 18.0 * c1 + rng.uniform(-1.5, 1.5), 1))
    lidar_med = max(5.0, round(lidar_raw + rng.uniform(-0.4, 0.4), 1))

    ax_raw = round(0.12 * s1 + rng.uniform(-0.02, 0.02), 3)
    ay_raw = round(0.08 * c1 + rng.uniform(-0.02, 0.02), 3)
    az_raw = round(0.98 + 0.04 * s1 + rng.uniform(-0.01, 0.01), 3)

    gx_raw = round(4.5 * s1 + rng.uniform(-0.4, 0.4), 2)
    gy_raw = round(3.8 * c1 + rng.uniform(-0.4, 0.4), 2)
    gz_raw = round(1.8 * math.sin(phase * 0.5) + rng.uniform(-0.3, 0.3), 2)

    data: Dict[str, Optional[float]] = {
        # arduino (24)
        "lidar_raw": lidar_raw,
        "lidar_median": lidar_med,
        "mq_nh3": max(0.1, j(nh3_base, 2.5, 0.3, 2)),
        "mq_co": max(0.1, j(co_base, 3.0, 0.4, 2)),
        "mq_co2": max(350.0, j(co2_base, 45.0, 5.0, 1)),
        "smoke": max(0.1, j(smoke_base, 1.8, 0.2, 2)),
        "mpu_acc_x_raw": ax_raw,
        "mpu_acc_y_raw": ay_raw,
        "mpu_acc_z_raw": az_raw,
        "mpu_gyro_x_raw": gx_raw,
        "mpu_gyro_y_raw": gy_raw,
        "mpu_gyro_z_raw": gz_raw,
        "mpu_acc_x_kalman": round(ax_raw * 0.92, 3),
        "mpu_acc_y_kalman": round(ay_raw * 0.92, 3),
        "mpu_acc_z_kalman": round(az_raw, 3),
        "mpu_gyro_x_kalman": round(gx_raw * 0.88, 2),
        "mpu_gyro_y_kalman": round(gy_raw * 0.88, 2),
        "mpu_gyro_z_kalman": round(gz_raw * 0.88, 2),
        "adxl_acc_x_raw": round(ax_raw + rng.uniform(-0.01, 0.01), 3),
        "adxl_acc_y_raw": round(ay_raw + rng.uniform(-0.01, 0.01), 3),
        "adxl_acc_z_raw": round(az_raw + rng.uniform(-0.01, 0.01), 3),
        "adxl_acc_x_kalman": round(ax_raw * 0.94, 3),
        "adxl_acc_y_kalman": round(ay_raw * 0.94, 3),
        "adxl_acc_z_kalman": round(az_raw, 3),
        # aurora (12)
        "accelx": round(ax_raw * 0.95, 3),
        "accely": round(ay_raw * 0.95, 3),
        "accelz": round(az_raw, 3),
        "gyrox": round(gx_raw * 0.8, 2),
        "gyroy": round(gy_raw * 0.8, 2),
        "gyroz": round(gz_raw * 0.8, 2),
        "bme_temp": round(temp_base - 4.0 + 1.2 * c1 + rng.uniform(-0.15, 0.15), 2),
        "bme_hum": round(max(20.0, min(95.0, 58.0 + 8.0 * s1 + rng.uniform(-0.5, 0.5))), 2),
        "bme_pres": round(1009.5 + 1.4 * c1 + rng.uniform(-0.1, 0.1), 2),
        "bme_alt": round(725.0 + 4.0 * s1 + rng.uniform(-0.5, 0.5), 1),
        "sound_dbLeft": round(58.0 + 12.0 * abs(s1) + rng.uniform(-1.5, 1.5), 2),
        "sound_dbRight": round(59.0 + 12.0 * abs(c1) + rng.uniform(-1.5, 1.5), 2),
        # loadcell (5)
        "loadcell_paka": max(0.0, j(12.5, 3.2, 0.2, 2)),
        "loadcell_paki": max(0.0, j(12.2, 3.0, 0.2, 2)),
        "loadcell_leher": max(0.0, j(4.8, 1.1, 0.1, 2)),
        "loadcell_kaka": max(0.0, j(18.4, 4.5, 0.25, 2)),
        "loadcell_kaki": max(0.0, j(18.1, 4.3, 0.25, 2)),
        # thermal (1)
        "thermal": round(temp_base + 1.1 + 0.8 * s1 + rng.uniform(-0.15, 0.15), 2),
        # witmotion (9)
        "ax": round(ax_raw * 0.9, 3),
        "ay": round(ay_raw * 0.9, 3),
        "az": round(az_raw, 3),
        "gx": round(gx_raw * 0.6, 2),
        "gy": round(gy_raw * 0.6, 2),
        "gz": round(gz_raw * 0.6, 2),
        "roll": round(tilt_amp * s1 + rng.uniform(-0.3, 0.3), 2),
        "pitch": round(tilt_amp * 0.7 * c1 + rng.uniform(-0.3, 0.3), 2),
        "yaw": round(((step * 3.5) % 360.0) - 180.0, 2),
    }

    for i in range(1, 17):
        data[f"flex{i}"] = round(max(0.0, 35.0 + 25.0 * math.sin(phase + i * 0.25) + rng.uniform(-1.0, 1.0)), 2)

    for i in range(1, 15):
        data[f"mcp{i}"] = round(temp_base + 0.4 * math.sin(phase + i * 0.3) + rng.uniform(-0.15, 0.15), 2)
        data[f"fsr{i}"] = round(max(0.0, 28.0 + 22.0 * abs(math.sin(phase + i * 0.35)) + rng.uniform(-1.0, 1.0)), 2)
        data[f"piezo{i}"] = round(max(0.0, 0.4 + vib_amp * abs(math.cos(phase + i * 0.4)) + rng.uniform(0.0, 0.15)), 3)

    if scenario == "partial":
        drop_keys = rng.sample(rx.EXPECTED_KEYS, k=max(1, len(rx.EXPECTED_KEYS) // 5))
        for k in drop_keys:
            data[k] = None

    return data


# ============================================================
# VIRTUAL RAK3272S RECEIVER (TANPA HARDWARE SERIAL)
# ============================================================

class VirtualRakP2PReceiver(rx.RakP2PReceiver):
    """Pengganti RakP2PReceiver yang membaca frame RXP2P simulasi dari memori."""

    def __init__(self, config: rx.RadioConfig, fallback_ports, logger):
        super().__init__(config, fallback_ports, logger)
        self._queue: deque = deque()
        self._lock = threading.Lock()
        self._connected = True
        self.active_port = "VIRTUAL-SIM-RAK3272S"

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self) -> bool:
        self._connected = True
        self.logger.info("Virtual RAK3272S Receiver aktif (mode simulasi dummy tanpa serial).")
        return True

    def configure(self) -> None:
        pass

    def push_rx_frame(self, raw_bytes: bytes, rssi: int, snr: int) -> None:
        line = f"+EVT:RXP2P:{rssi}:{snr}:{raw_bytes.hex().upper()}"
        with self._lock:
            self._queue.append(line)

    def read_lines(self) -> List[str]:
        with self._lock:
            if not self._queue:
                return []
            out = list(self._queue)
            self._queue.clear()
            return out

    def close(self, stop_rx: bool = True) -> None:
        self._connected = False


# ============================================================
# MODE 1: FULL RX PIPELINE (lora_rxManekin1.py)
# ============================================================

def _build_node_overrides(args: argparse.Namespace) -> Dict[str, str]:
    overrides: Dict[str, str] = {
        "mqtt_enabled": str(args.mqtt_enabled).lower(),
        "http_enabled": str(args.http_enabled).lower(),
        "log_enabled": str(not args.no_log).lower(),
        "mqtt_alerts_enabled": str(args.mqtt_alerts).lower(),
    }
    if args.mqtt_url:
        overrides["mqtt_broker_url"] = args.mqtt_url
    if args.mqtt_host:
        overrides["mqtt_host"] = args.mqtt_host
    if args.mqtt_port is not None:
        overrides["mqtt_port"] = str(args.mqtt_port)
    if args.mqtt_tls is not None:
        overrides["mqtt_tls"] = str(args.mqtt_tls).lower()
    if args.mqtt_ca_certs:
        overrides["mqtt_ca_certs"] = args.mqtt_ca_certs
    if args.mqtt_user:
        overrides["mqtt_username"] = args.mqtt_user
    if args.mqtt_pass:
        overrides["mqtt_password"] = args.mqtt_pass
    if args.mqtt_client_id:
        overrides["mqtt_client_id"] = args.mqtt_client_id
    if args.device_id:
        overrides["device_id"] = args.device_id
    if args.boot_count > 0:
        overrides["boot_count"] = str(args.boot_count)
    if args.http_url:
        overrides["http_base_url"] = args.http_url
    if args.ingest_key:
        overrides["ingest_api_key"] = args.ingest_key
    if args.log_dir:
        overrides["log_dir"] = args.log_dir

    if args.dry_run:
        overrides["mqtt_enabled"] = "false"
        overrides["http_enabled"] = "false"

    return overrides


def _wait_mqtt_ready(forwarder: rx.WebTelemetryForwarder, timeout_sec: float = 8.0) -> None:
    if not forwarder.config.mqtt_enabled:
        return
    if forwarder._paho_client is not None:
        forwarder._paho_connect_event.wait(timeout=timeout_sec)
        if forwarder._paho_connected:
            forwarder.logger.info("Koneksi MQTT siap; memulai injeksi simulasi...")
        else:
            forwarder.logger.warning(
                f"MQTT belum terhubung setelah {timeout_sec:.1f}s "
                f"({forwarder._last_mqtt_error or 'timeout'}). Tetap melanjutkan simulasi."
            )


def _drain_forwarder_and_acks(
    forwarder: rx.WebTelemetryForwarder,
    expected_mqtt: int,
    ack_wait_sec: float,
    drain_timeout_sec: float = 30.0,
) -> None:
    deadline = time.monotonic() + drain_timeout_sec
    while time.monotonic() < deadline:
        with forwarder._cond:
            q_len = len(forwarder._queue)
        if q_len == 0:
            break
        time.sleep(0.05)

    if forwarder.config.mqtt_enabled and ack_wait_sec > 0:
        ack_deadline = time.monotonic() + ack_wait_sec
        while time.monotonic() < ack_deadline:
            if expected_mqtt > 0 and forwarder.mqtt_ack_count >= expected_mqtt:
                break
            time.sleep(0.05)


def run_rx_pipeline_simulation(args: argparse.Namespace) -> int:
    rng = random.Random(args.seed)
    overrides = _build_node_overrides(args)

    orig_rak_cls = rx.RakP2PReceiver
    rx.RakP2PReceiver = VirtualRakP2PReceiver
    try:
        node = rx.LoRaManekin1ReceiverNode(
            default_port="VIRTUAL-SIM-RAK3272S",
            cli_overrides=overrides,
        )
    finally:
        rx.RakP2PReceiver = orig_rak_cls

    # Perbesar kapasitas antrean agar burst tinggi tidak diam-diam membuang paket di deque(maxlen=64)
    if not args.allow_drop:
        new_maxlen = max(4096, (args.count * 2) if args.count > 0 else 16384)
        with node.web_forwarder._cond:
            node.web_forwarder._queue = deque(node.web_forwarder._queue, maxlen=new_maxlen)

    ack_stats: Counter = Counter()
    orig_handle_ack = node.web_forwarder._handle_ack_message

    def _tracked_ack(topic: str, payload_bytes: bytes) -> None:
        try:
            ack = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
            if isinstance(ack, dict):
                ack_stats[str(ack.get("status", "UNKNOWN"))] += 1
        except Exception:
            pass
        if not args.quiet:
            orig_handle_ack(topic, payload_bytes)
        else:
            # Tetap hitung mqtt_ack_count & last_ack_status tanpa spam log per ACK
            try:
                ack = json.loads(payload_bytes.decode("utf-8", errors="ignore"))
                if isinstance(ack, dict):
                    node.web_forwarder.mqtt_ack_count += 1
                    node.web_forwarder.last_ack_status = str(ack.get("status", "UNKNOWN"))
            except Exception:
                pass

    node.web_forwarder._handle_ack_message = _tracked_ack
    if node.web_forwarder._simple_mqtt is not None:
        node.web_forwarder._simple_mqtt.on_message = _tracked_ack

    # Matikan log per-paket yang sangat panjang jika --quiet
    orig_info = node.get_logger().info
    if args.quiet:
        def _quiet_info(msg: str) -> None:
            if msg.startswith("[RX #"):
                return
            orig_info(msg)
        node.get_logger().info = _quiet_info

    stop_flag = threading.Event()

    def _on_sig(_sig, _frame):
        stop_flag.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_sig)
        except Exception:
            pass

    mannequins = [1, 2] if args.mannequin == "both" else [int(args.mannequin)]
    mixed_cycle = ["packed", "packed", "array", "web_v2", "fragmented"]

    _wait_mqtt_ready(node.web_forwarder)

    tx_packets = 0
    tx_frames = 0
    start_t = time.monotonic()
    next_tick = start_t
    period = (1.0 / args.rate) if args.rate > 0 else 0.0
    progress_every = max(1, args.progress_every)

    try:
        step = 1
        while not stop_flag.is_set():
            if args.count > 0 and tx_packets >= args.count:
                break

            mannequin_id = mannequins[(step - 1) % len(mannequins)]
            if args.payload_mode == "mixed":
                if args.alert_every > 0 and step % args.alert_every == 0:
                    mode_now = "alert"
                else:
                    mode_now = mixed_cycle[(step - 1) % len(mixed_cycle)]
            else:
                mode_now = args.payload_mode

            sensor_dict = generate_dummy_sensors(
                step=step,
                mannequin_id=mannequin_id,
                scenario=args.scenario,
                rng=rng,
            )
            frames = build_lora_frames(
                sensor_dict=sensor_dict,
                mannequin_id=mannequin_id,
                payload_mode=mode_now,
                boot_count=node.boot_count,
                seq=step,
                rng=rng,
                frag_chunk_size=args.frag_size,
            )

            # Backpressure ringan jika antrean mendekati kapasitas agar tidak drop
            if not args.allow_drop:
                q_cap = node.web_forwarder._queue.maxlen or 4096
                while not stop_flag.is_set():
                    with node.web_forwarder._cond:
                        q_len = len(node.web_forwarder._queue)
                    if q_len < max(16, int(q_cap * 0.85)):
                        break
                    time.sleep(0.01)

            rssi = rng.randint(-82, -38)
            snr = rng.randint(5, 13)
            for fr in frames:
                node.radio.push_rx_frame(fr, rssi=rssi, snr=snr)
                tx_frames += 1

            node.read_serial_data()
            tx_packets += 1
            step += 1

            if args.quiet and (tx_packets % progress_every == 0 or tx_packets == args.count):
                elapsed = max(1e-6, time.monotonic() - start_t)
                orig_info(
                    f"[PROGRESS] TX={tx_packets} RX={node.packet_count} "
                    f"MQTT_sent={node.web_forwarder.mqtt_sent_count} "
                    f"ACK={node.web_forwarder.mqtt_ack_count} "
                    f"HTTP_sent={node.web_forwarder.http_sent_count} "
                    f"({tx_packets / elapsed:.1f} pkt/s)"
                )

            if period > 0:
                next_tick += period
                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_tick = time.monotonic()

        expected_mqtt = (
            node.packet_count - node.alert_count
            if node.web_forwarder.config.mqtt_enabled and not node.web_forwarder.config.mqtt_alerts_enabled
            else node.packet_count
        )
        _drain_forwarder_and_acks(
            node.web_forwarder,
            expected_mqtt=expected_mqtt,
            ack_wait_sec=args.ack_wait,
        )
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = max(1e-6, time.monotonic() - start_t)
        summary = (
            "\n=== RINGKASAN SIMULASI (rx-pipeline) ===\n"
            f"Durasi           : {elapsed:.2f} s ({tx_packets / elapsed:.2f} pkt/s)\n"
            f"TX Dummy Dikirim : {tx_packets} paket ({tx_frames} LoRa frame)\n"
            f"RX Terproses     : {node.packet_count} paket (alerts={node.alert_count}, "
            f"crc_err={node.crc_error_count})\n"
            f"Web MQTT         : sent={node.web_forwarder.mqtt_sent_count}, "
            f"fail={node.web_forwarder.mqtt_fail_count}, "
            f"ack_total={node.web_forwarder.mqtt_ack_count} {dict(ack_stats)}\n"
            f"Web HTTP         : sent={node.web_forwarder.http_sent_count}, "
            f"fail={node.web_forwarder.http_fail_count}\n"
            f"CSV Log Dir      : {node.comm_logger.log_dir if node.comm_logger.enabled else 'DISABLED'}\n"
        )
        print(summary, flush=True)
        node.destroy_node()

    return 0


# ============================================================
# MODE 2: DIRECT MQTT STRESS TEST (MAKSIMAL THROUGHPUT KE WEB)
# ============================================================

def run_direct_mqtt_simulation(args: argparse.Namespace) -> int:
    if args.dry_run:
        print("[INFO] --dry-run aktif pada mode direct-mqtt: hanya membangun payload tanpa koneksi broker.")
        rng = random.Random(args.seed)
        mannequins = [1, 2] if args.mannequin == "both" else [int(args.mannequin)]
        boot_count = args.boot_count if args.boot_count > 0 else max(1, int(time.time()) - 1_700_000_000)
        count = args.count if args.count > 0 else 10
        start_t = time.monotonic()
        total_bytes = 0
        for step in range(1, count + 1):
            mid = mannequins[(step - 1) % len(mannequins)]
            d = generate_dummy_sensors(step, mid, args.scenario, rng)
            v2 = rx.build_web_v2_payload(d, mid, boot_count, step)
            v2["r"] = [p for p in v2["r"] if p[1] is not None]
            raw = json.dumps(v2, separators=(",", ":"), allow_nan=False)
            total_bytes += len(raw.encode("utf-8"))
        elapsed = max(1e-6, time.monotonic() - start_t)
        print(
            f"[DRY-RUN direct-mqtt] built={count} payloads, total={total_bytes}B, "
            f"time={elapsed:.3f}s ({count / elapsed:.1f} pkt/s)"
        )
        return 0

    try:
        import paho.mqtt.client as paho_mqtt
    except ImportError:
        print(
            "[WARN] paho-mqtt tidak tersedia; beralih ke mode rx-pipeline (built-in socket MQTT).",
            file=sys.stderr,
        )
        return run_rx_pipeline_simulation(args)

    overrides = _build_node_overrides(args)
    store = rx.StandaloneParamStore(overrides)
    web_cfg = rx.declare_web_config(store)
    boot_count = (
        args.boot_count
        if args.boot_count > 0
        else rx._resolve_auto_boot_count(
            args.log_dir or rx.DEFAULT_LOG_DIR, log_enabled=not args.no_log
        )
    )

    if web_cfg.mqtt_username and not web_cfg.mqtt_password:
        print(
            f"[WARN] Password MQTT untuk user '{web_cfg.mqtt_username}' masih kosong "
            "(gunakan --mqtt-pass atau env WEB_MQTT_PASS).",
            file=sys.stderr,
        )

    client_id = web_cfg.mqtt_client_id or "manekin-sim-01"
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

    if hasattr(client, "max_inflight_messages_set"):
        client.max_inflight_messages_set(max(20, args.max_inflight))
    if hasattr(client, "max_queued_messages_set"):
        client.max_queued_messages_set(0)

    if web_cfg.mqtt_tls:
        client.tls_set_context(rx.build_mqtt_ssl_context(web_cfg.mqtt_ca_certs))
    if web_cfg.mqtt_username:
        client.username_pw_set(web_cfg.mqtt_username, web_cfg.mqtt_password or None)

    connected_evt = threading.Event()
    connect_err: List[str] = []
    puback_count = 0
    puback_lock = threading.Lock()
    ack_stats: Counter = Counter()

    def _on_connect(c, _u, _f, rc, *extra):
        rc_val = getattr(rc, "value", rc)
        is_fail = bool(getattr(rc, "is_failure", False))
        if not is_fail and rc_val == 0:
            c.subscribe("smart-mannequin/+/ack", qos=1)
            connected_evt.set()
        else:
            connect_err.append(f"CONNACK error rc={rc}")
            connected_evt.set()

    def _on_publish(_c, _u, _mid, *extra):
        nonlocal puback_count
        with puback_lock:
            puback_count += 1

    def _on_message(_c, _u, msg):
        try:
            ack = json.loads(msg.payload.decode("utf-8", errors="ignore"))
            if isinstance(ack, dict):
                status = str(ack.get("status", "UNKNOWN"))
                ack_stats[status] += 1
                if not args.quiet or status == "REJECTED":
                    print(
                        f"[WEB-ACK] dev={ack.get('deviceIdentifier')} s={ack.get('s')} "
                        f"status={status} readings={ack.get('readingsCount')} "
                        f"err={ack.get('errorCode', '-')}",
                        flush=True,
                    )
        except Exception:
            pass

    client.on_connect = _on_connect
    client.on_publish = _on_publish
    client.on_message = _on_message

    scheme = web_cfg.effective_mqtt_scheme()
    print(
        f"[DIRECT-MQTT] Connecting to {scheme}://{web_cfg.mqtt_host}:{web_cfg.mqtt_port} "
        f"(client_id={client_id}, boot={boot_count}, max_inflight={args.max_inflight})...",
        flush=True,
    )
    client.connect(web_cfg.mqtt_host, web_cfg.mqtt_port, keepalive=30)
    client.loop_start()

    if not connected_evt.wait(timeout=10.0) or connect_err:
        print(
            f"[ERROR] Gagal terhubung ke MQTT broker: {connect_err or ['timeout']}",
            file=sys.stderr,
        )
        client.loop_stop()
        client.disconnect()
        return 1

    stop_flag = threading.Event()

    def _on_sig(_sig, _frame):
        stop_flag.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_sig)
        except Exception:
            pass

    rng = random.Random(args.seed)
    mannequins = [1, 2] if args.mannequin == "both" else [int(args.mannequin)]
    seq_by_mid: Dict[int, int] = {1: 0, 2: 0}
    sent_ok = 0
    sent_fail = 0
    progress_every = max(1, args.progress_every)
    period = (1.0 / args.rate) if args.rate > 0 else 0.0
    start_t = time.monotonic()
    next_tick = start_t

    try:
        step = 1
        while not stop_flag.is_set():
            if args.count > 0 and step > args.count:
                break

            mid = mannequins[(step - 1) % len(mannequins)]
            seq_by_mid[mid] += 1
            seq = seq_by_mid[mid]

            sensor_dict = generate_dummy_sensors(step, mid, args.scenario, rng)
            v2_obj = rx.build_web_v2_payload(sensor_dict, mid, boot_count, seq)
            v2_obj["r"] = [p for p in v2_obj["r"] if p[1] is not None]
            if not v2_obj["r"]:
                step += 1
                continue

            device_id = web_cfg.resolve_device_id(mid)
            topic = f"smart-mannequin/{device_id}/telemetry"
            payload_str = json.dumps(v2_obj, separators=(",", ":"), allow_nan=False)

            info = client.publish(topic, payload_str, qos=1, retain=False)
            if info.rc == paho_mqtt.MQTT_ERR_SUCCESS:
                sent_ok += 1
            else:
                sent_fail += 1

            if not args.quiet:
                print(
                    f"[TX #{step:05d}] M{mid} dev={device_id} s={seq} "
                    f"readings={len(v2_obj['r'])} bytes={len(payload_str)}",
                    flush=True,
                )
            elif step % progress_every == 0 or step == args.count:
                elapsed = max(1e-6, time.monotonic() - start_t)
                with puback_lock:
                    pa = puback_count
                print(
                    f"[PROGRESS] queued={sent_ok} puback={pa} web_ack={sum(ack_stats.values())} "
                    f"({sent_ok / elapsed:.1f} pkt/s)",
                    flush=True,
                )

            step += 1
            if period > 0:
                next_tick += period
                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                else:
                    next_tick = time.monotonic()

        # Tunggu PUBACK & Application ACK dari Be-smart-manekin
        pub_deadline = time.monotonic() + 20.0
        while time.monotonic() < pub_deadline:
            with puback_lock:
                if puback_count >= sent_ok:
                    break
            time.sleep(0.05)

        if args.ack_wait > 0:
            ack_deadline = time.monotonic() + args.ack_wait
            while time.monotonic() < ack_deadline:
                if sum(ack_stats.values()) >= sent_ok:
                    break
                time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = max(1e-6, time.monotonic() - start_t)
        with puback_lock:
            pa = puback_count
        print(
            "\n=== RINGKASAN SIMULASI (direct-mqtt) ===\n"
            f"Durasi         : {elapsed:.2f} s ({sent_ok / elapsed:.2f} pkt/s)\n"
            f"MQTT Queued    : {sent_ok} (fail={sent_fail})\n"
            f"Broker PUBACK  : {pa}\n"
            f"Web App ACK    : {sum(ack_stats.values())} {dict(ack_stats)}\n",
            flush=True,
        )
        client.loop_stop()
        client.disconnect()

    return 0


# ============================================================
# CLI PARSER
# ============================================================

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Dummy Simulator LoRa RX -> Web (Be-smart-manekin) untuk pengujian volume tinggi."
    )
    p.add_argument(
        "--mode",
        choices=("rx-pipeline", "direct-mqtt"),
        default="rx-pipeline",
        help="rx-pipeline: uji penuh lora_rxManekin1.py (encode->RXP2P->decode->CSV->Web); "
             "direct-mqtt: tembak langsung MQTTS Compact V2 throughput maksimal.",
    )
    p.add_argument(
        "--count",
        type=int,
        default=100,
        help="Jumlah paket dummy yang dikirim (0 = jalan terus sampai Ctrl+C). Default: 100",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Kecepatan kirim dalam paket/detik (Hz). Isi 0 untuk secepat mungkin (burst). Default: 10",
    )
    p.add_argument(
        "--mannequin",
        choices=("1", "2", "both"),
        default="1",
        help="Target manekin: 1, 2, atau both (bergantian M1 & M2). Default: 1",
    )
    p.add_argument(
        "--payload-mode",
        choices=("packed", "array", "web_v2", "fragmented", "alert", "mixed"),
        default="packed",
        help="Format payload LoRa pada mode rx-pipeline. Default: packed (B2/Z2)",
    )
    p.add_argument(
        "--scenario",
        choices=("normal", "hazard", "partial", "sweep"),
        default="normal",
        help="Pola data 109 sensor dummy. Default: normal",
    )
    p.add_argument("--alert-every", type=int, default=25, help="Pada mode mixed, kirim alert tiap N paket.")
    p.add_argument("--frag-size", type=int, default=180, help="Ukuran byte per fragmen FRG untuk mode fragmented.")
    p.add_argument("--seed", type=int, default=42, help="Random seed agar reproduktif.")
    p.add_argument("--dry-run", action="store_true", help="Nonaktifkan kirim MQTT/HTTP (uji encode/decode/CSV saja).")
    p.add_argument("--quiet", action="store_true", help="Ringkas output terminal (tampilkan progres per N paket).")
    p.add_argument("--progress-every", type=int, default=50, help="Interval cetak progres saat --quiet.")
    p.add_argument("--ack-wait", type=float, default=4.0, help="Detik menunggu sisa Web ACK setelah kirim selesai.")
    p.add_argument("--allow-drop", action="store_true", help="Pertahankan deque(maxlen=64) asli tanpa backpressure.")
    p.add_argument("--max-inflight", type=int, default=100, help="Batas inflight QoS 1 pada mode direct-mqtt.")

    # Override konfigurasi Web / MQTT / HTTP / Logging
    p.add_argument("--mqtt-enabled", type=lambda s: s.lower() in ("1", "true", "yes", "on"), default=rx.DEFAULT_MQTT_ENABLED)
    p.add_argument("--mqtt-url", default="", help="URL broker, mis. mqtts://mannequin-mqtt.stas-rg.com:18883")
    p.add_argument("--mqtt-host", default="")
    p.add_argument("--mqtt-port", type=int, default=None)
    p.add_argument("--mqtt-tls", type=lambda s: s.lower() in ("1", "true", "yes", "on"), default=None)
    p.add_argument("--mqtt-ca-certs", default="")
    p.add_argument("--mqtt-user", default="")
    p.add_argument("--mqtt-pass", default="")
    p.add_argument("--mqtt-client-id", default="manekin-sim-01")
    p.add_argument("--mqtt-alerts", action="store_true", help="Aktifkan forward alert ke MQTT topic alerts.")
    p.add_argument("--device-id", default="", help="Override deviceIdentifier tunggal untuk semua paket.")
    p.add_argument("--boot-count", type=int, default=0, help="Override nilai boot counter 'b' (0 = auto-increment).")

    p.add_argument("--http-enabled", type=lambda s: s.lower() in ("1", "true", "yes", "on"), default=rx.DEFAULT_HTTP_ENABLED)
    p.add_argument("--http-url", default="")
    p.add_argument("--ingest-key", default="")

    p.add_argument("--log-dir", default="", help="Folder output CSV simulasi.")
    p.add_argument("--no-log", action="store_true", help="Nonaktifkan penulisan file CSV lokal.")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.mode == "direct-mqtt":
        return run_direct_mqtt_simulation(args)
    return run_rx_pipeline_simulation(args)


if __name__ == "__main__":
    raise SystemExit(main())
