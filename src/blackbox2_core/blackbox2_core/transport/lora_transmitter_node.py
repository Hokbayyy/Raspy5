
#!/usr/bin/env python3
"""
SMART MANNEQUIN - LoRa P2P TRANSMITTER
RAK3272S + ROS 2 Jazzy

35 parameter:
- Arduino UNO Q: 23 parameter
- Cosmic Aurora V3: 12 parameter

Features:
- Real sensor data priority
- Dummy fallback for missing parameters
- Quality: valid / stale / simulated
- JSON envelope + CRC16
- Fragmentation
- AT+PSEND sequential transmission
- Wait for TXP2P DONE
- Automatic RAK reconnect
"""

import json
import time
import random
from collections import deque

import serial

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# 35 EXPECTED PARAMETERS
# ============================================================

EXPECTED_KEYS = [
    # Arduino UNO Q (23)
    "lidar_raw",
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

    # Cosmic Aurora V3 (12)
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
]


# ============================================================
# CRC16 CCITT
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
# HORIZONTAL LOG FORMAT
# ============================================================

def format_horizontal(data):

    g = lambda key: data.get(key, "-")

    parts = [

        f"Lidar:{g('lidar_raw')}",

        f"MQ(NH3:{g('mq_nh3')},"
        f"CO:{g('mq_co')},"
        f"CO2:{g('mq_co2')})",

        f"Smoke:{g('smoke')}",

        f"MPU_Raw[A:({g('mpu_acc_x_raw')},"
        f"{g('mpu_acc_y_raw')},"
        f"{g('mpu_acc_z_raw')}) "
        f"G:({g('mpu_gyro_x_raw')},"
        f"{g('mpu_gyro_y_raw')},"
        f"{g('mpu_gyro_z_raw')})]",

        f"MPU_Klm[A:({g('mpu_acc_x_kalman')},"
        f"{g('mpu_acc_y_kalman')},"
        f"{g('mpu_acc_z_kalman')}) "
        f"G:({g('mpu_gyro_x_kalman')},"
        f"{g('mpu_gyro_y_kalman')},"
        f"{g('mpu_gyro_z_kalman')})]",

        f"ADXL_Raw:({g('adxl_acc_x_raw')},"
        f"{g('adxl_acc_y_raw')},"
        f"{g('adxl_acc_z_raw')})",

        f"ADXL_Klm:({g('adxl_acc_x_kalman')},"
        f"{g('adxl_acc_y_kalman')},"
        f"{g('adxl_acc_z_kalman')})",

        f"LSM6DS3[A:({g('accelx')},"
        f"{g('accely')},"
        f"{g('accelz')}) "
        f"G:({g('gyrox')},"
        f"{g('gyroy')},"
        f"{g('gyroz')})]",

        f"BME280({g('bme_temp')}C,"
        f"{g('bme_hum')}%,"
        f"{g('bme_pres')}hPa,"
        f"{g('bme_alt')}m)",

        f"Sound(L:{g('sound_dbLeft')},"
        f"R:{g('sound_dbRight')}dB)",
    ]

    return " | ".join(parts)


# ============================================================
# NODE
# ============================================================

class LoRaTransmitterNode(Node):

    def __init__(self):

        super().__init__("lora_transmitter_node")

        # ----------------------------------------------------
        # GENERAL PARAMETERS
        # ----------------------------------------------------

        self.declare_parameter("port", "/dev/ttyAMA3")
        self.declare_parameter("baudrate", 115200)

        self.declare_parameter("interval", 1.0)
        self.declare_parameter("stale_timeout", 3.0)
        self.declare_parameter("hold_last", True)

        # Dummy fallback
        self.declare_parameter("dummy_enabled", True)

        # ----------------------------------------------------
        # FRAGMENTATION
        # ----------------------------------------------------

        self.declare_parameter("chunk_size", 185)
        self.declare_parameter("append_crc", True)

        self.declare_parameter("tx_timeout", 1.5)
        self.declare_parameter("max_payload", 255)

        # ----------------------------------------------------
        # RADIO P2P CONFIGURATION
        # ----------------------------------------------------

        self.declare_parameter("configure_radio", True)

        self.declare_parameter("freq", 923200000)
        self.declare_parameter("sf", 6)
        self.declare_parameter("bw", 2)
        self.declare_parameter("cr", 0)

        self.declare_parameter("preamble", 8)
        self.declare_parameter("tx_power", 20)

        # ----------------------------------------------------
        # READ PARAMETERS
        # ----------------------------------------------------

        gp = lambda name: self.get_parameter(name).value

        self.port = gp("port")
        self.baud = int(gp("baudrate"))

        self.interval = float(gp("interval"))
        self.stale_timeout = float(gp("stale_timeout"))

        self.hold_last = bool(gp("hold_last"))
        self.dummy_enabled = bool(gp("dummy_enabled"))

        self.chunk_size = int(gp("chunk_size"))
        self.append_crc = bool(gp("append_crc"))

        self.tx_timeout = float(gp("tx_timeout"))
        self.max_payload = int(gp("max_payload"))

        # ----------------------------------------------------
        # SOURCE STATE
        # ----------------------------------------------------

        self.packet_seq = 0
        self.start_time = time.monotonic()

        self.sources = {

            "arduino": {
                "raw": None,
                "received_at": None
            },

            "aurora": {
                "raw": None,
                "received_at": None
            }

        }

        # ----------------------------------------------------
        # SERIAL STATE
        # ----------------------------------------------------

        self.ser = None

        self.rx_buffer = ""

        self.last_reconnect_try = 0.0

        # ----------------------------------------------------
        # TX QUEUE
        # ----------------------------------------------------

        self.tx_queue = deque()

        self.tx_busy = False

        self.tx_started_at = 0.0

        self.next_send_at = 0.0

        self.cur_msg = None

        # ----------------------------------------------------
        # CONNECT RAK
        # ----------------------------------------------------

        if self.connect_rak(self.port, self.baud):

            if bool(gp("configure_radio")):

                if not self.init_radio():

                    self.get_logger().error(
                        "Radio initialization failed"
                    )

        # ----------------------------------------------------
        # ROS SUBSCRIPTIONS
        # ----------------------------------------------------

        self.create_subscription(
            String,
            "/telemetri/arduino",
            lambda msg: self.source_callback("arduino", msg),
            10
        )

        self.create_subscription(
            String,
            "/telemetri/aurora",
            lambda msg: self.source_callback("aurora", msg),
            10
        )

        # ----------------------------------------------------
        # TIMERS
        # ----------------------------------------------------

        self.create_timer(
            0.005,
            self.read_rak_response
        )

        self.create_timer(
            0.005,
            self.pump_queue
        )

        self.create_timer(
            self.interval,
            self.transmit
        )

        self.get_logger().info(
            "LoRa transmitter started | "
            f"Interval={self.interval}s | "
            f"Dummy={self.dummy_enabled} | "
            f"Chunk={self.chunk_size}B"
        )

    # ========================================================
    # DUMMY GENERATOR
    # ========================================================

    def generate_dummy_parameters(self, seq):

        rng = random.Random(seq)

        return {

            # Arduino UNO Q - 23

            "lidar_raw": round(rng.uniform(50, 200), 2),

            "mq_nh3": round(rng.uniform(5, 30), 2),
            "mq_co": round(rng.uniform(1, 15), 2),
            "mq_co2": round(rng.uniform(350, 900), 2),

            "smoke": round(rng.uniform(0, 100), 2),

            "mpu_acc_x_raw": round(rng.uniform(-2, 2), 3),
            "mpu_acc_y_raw": round(rng.uniform(-2, 2), 3),
            "mpu_acc_z_raw": round(rng.uniform(8, 11), 3),

            "mpu_gyro_x_raw": round(rng.uniform(-180, 180), 3),
            "mpu_gyro_y_raw": round(rng.uniform(-180, 180), 3),
            "mpu_gyro_z_raw": round(rng.uniform(-180, 180), 3),

            "mpu_acc_x_kalman": round(rng.uniform(-2, 2), 3),
            "mpu_acc_y_kalman": round(rng.uniform(-2, 2), 3),
            "mpu_acc_z_kalman": round(rng.uniform(8, 11), 3),

            "mpu_gyro_x_kalman": round(rng.uniform(-180, 180), 3),
            "mpu_gyro_y_kalman": round(rng.uniform(-180, 180), 3),
            "mpu_gyro_z_kalman": round(rng.uniform(-180, 180), 3),

            "adxl_acc_x_raw": round(rng.uniform(-2, 2), 3),
            "adxl_acc_y_raw": round(rng.uniform(-2, 2), 3),
            "adxl_acc_z_raw": round(rng.uniform(8, 11), 3),

            "adxl_acc_x_kalman": round(rng.uniform(-2, 2), 3),
            "adxl_acc_y_kalman": round(rng.uniform(-2, 2), 3),
            "adxl_acc_z_kalman": round(rng.uniform(8, 11), 3),

            # Cosmic Aurora - 12

            "accelx": round(rng.uniform(-2, 2), 3),
            "accely": round(rng.uniform(-2, 2), 3),
            "accelz": round(rng.uniform(8, 11), 3),

            "gyrox": round(rng.uniform(-180, 180), 3),
            "gyroy": round(rng.uniform(-180, 180), 3),
            "gyroz": round(rng.uniform(-180, 180), 3),

            "bme_temp": round(rng.uniform(25, 35), 2),
            "bme_hum": round(rng.uniform(40, 80), 2),
            "bme_pres": round(rng.uniform(990, 1020), 2),
            "bme_alt": round(rng.uniform(0, 100), 2),

            "sound_dbLeft": round(rng.uniform(30, 80), 2),
            "sound_dbRight": round(rng.uniform(30, 80), 2),

        }

    # ========================================================
    # AURORA ALIASES
    # ========================================================

    @staticmethod
    def normalize_source_data(data):

        if not isinstance(data, dict):
            return data

        aliases = {

            "accel_x": "accelx",
            "accel_y": "accely",
            "accel_z": "accelz",

            "gyro_x": "gyrox",
            "gyro_y": "gyroy",
            "gyro_z": "gyroz",

            "temperature": "bme_temp",
            "humidity": "bme_hum",
            "pressure": "bme_pres",
            "altitude": "bme_alt",

            "sound_left": "sound_dbLeft",
            "sound_right": "sound_dbRight",

        }

        normalized = dict(data)

        for old_key, new_key in aliases.items():

            if old_key in data and new_key not in normalized:

                normalized[new_key] = data[old_key]

        return normalized

    # ========================================================
    # SERIAL CONNECTION
    # ========================================================

    def connect_rak(self, port, baud):

        try:

            self.ser = serial.Serial(
                port=port,
                baudrate=baud,
                timeout=0,
                write_timeout=0.2
            )

            self.get_logger().info(
                f"RAK connected: {port} @ {baud}"
            )

            return True

        except (serial.SerialException, OSError) as exc:

            self.ser = None

            self.get_logger().error(
                f"RAK connection failed: {exc}"
            )

            return False

    # ========================================================
    # RECONNECT
    # ========================================================

    def try_reconnect(self):

        now = time.monotonic()

        if now - self.last_reconnect_try < 3.0:
            return

        self.last_reconnect_try = now

        if self.ser is not None:

            try:
                self.ser.close()
            except Exception:
                pass

            self.ser = None

        self.get_logger().warning(
            "Attempting RAK reconnect..."
        )

        if self.connect_rak(self.port, self.baud):

            self.reset_tx_state()

            self.rx_buffer = ""

            if bool(
                self.get_parameter("configure_radio").value
            ):

                self.init_radio()

    def reset_tx_state(self):

        self.tx_queue.clear()

        self.tx_busy = False

        self.cur_msg = None

    # ========================================================
    # AT COMMAND
    # ========================================================

    def send_at(self, cmd, timeout=1.5):

        self.ser.reset_input_buffer()

        self.ser.write(
            (cmd + "\r\n").encode("ascii")
        )

        self.ser.flush()

        start = time.monotonic()

        responses = []

        buf = ""

        while time.monotonic() - start < timeout:

            n = self.ser.in_waiting

            if n > 0:

                buf += self.ser.read(n).decode(
                    "ascii",
                    errors="ignore"
                )

                while "\n" in buf:

                    line, buf = buf.split("\n", 1)

                    line = line.strip()

                    if not line:
                        continue

                    responses.append(line)

                    if (
                        "OK" in line
                        or "AT_ERROR" in line
                        or "AT_PARAM_ERROR" in line
                    ):

                        return responses

            time.sleep(0.02)

        return responses

    @staticmethod
    def has_ok(lines):

        return any(
            "OK" in line
            for line in lines
        )

    # ========================================================
    # RADIO INITIALIZATION
    # ========================================================

    def init_radio(self):

        log = self.get_logger()

        log.info("Initializing RAK3272S...")

        if not self.has_ok(
            self.send_at("AT", 1.0)
        ):

            time.sleep(0.5)

            if not self.has_ok(
                self.send_at("AT", 1.0)
            ):

                log.error(
                    "RAK does not respond to AT"
                )

                return False

        log.info("RAK AT OK")

        res = self.send_at(
            "AT+NWM=?",
            1.0
        )

        mode = None

        for line in res:

            if "AT+NWM=" in line:

                mode = line.split("=")[-1].strip()

        if mode != "0":

            log.warning(
                "Changing RAK to P2P mode..."
            )

            self.ser.write(
                b"AT+NWM=0\r\n"
            )

            self.ser.flush()

            time.sleep(2.5)

            self.ser.reset_input_buffer()

            if not self.has_ok(
                self.send_at("AT", 1.0)
            ):

                log.error(
                    "Failed to initialize P2P mode"
                )

                return False

        self.send_at(
            "AT+PRECV=0",
            0.5
        )

        gp = lambda name: int(
            self.get_parameter(name).value
        )

        cmd = (
            f"AT+P2P={gp('freq')}:"
            f"{gp('sf')}:"
            f"{gp('bw')}:"
            f"{gp('cr')}:"
            f"{gp('preamble')}:"
            f"{gp('tx_power')}"
        )

        log.info(
            f"Setting radio: {cmd}"
        )

        res = self.send_at(
            cmd,
            1.5
        )

        if not self.has_ok(res):

            log.error(
                f"Failed to configure P2P: {res}"
            )

            return False

        log.info(
            "RAK P2P configuration: "
            + " ".join(
                self.send_at("AT+P2P=?", 1.0)
            )
        )

        log.info(
            "RAK3272S ready to transmit"
        )

        return True

    # ========================================================
    # ROS CALLBACK
    # ========================================================

    def source_callback(self, name, msg):

        self.sources[name]["raw"] = msg.data

        self.sources[name]["received_at"] = (
            time.monotonic()
        )

    # ========================================================
    # BUILD COMPLETE PACKET WITH DUMMY FALLBACK
    # ========================================================

    def build_packet(self):

        now = time.monotonic()

        sources_payload = {}

        merged = {}

        quality = {}

        have_any = False

        for name, src in self.sources.items():

            raw = src["raw"]

            ts = src["received_at"]

            if raw is None or ts is None:

                sources_payload[name] = {
                    "available": False,
                    "age_ms": None,
                    "data": None
                }

                continue

            age_ms = int(
                (now - ts) * 1000
            )

            fresh = (
                now - ts
            ) <= self.stale_timeout

            if not fresh and not self.hold_last:

                sources_payload[name] = {
                    "available": False,
                    "age_ms": age_ms,
                    "data": None
                }

                continue

            try:

                data = json.loads(raw)

            except (ValueError, TypeError):

                data = None

            if not isinstance(data, dict):

                sources_payload[name] = {
                    "available": False,
                    "age_ms": age_ms,
                    "data": None
                }

                continue

            data = self.normalize_source_data(data)

            valid_fields = {}

            for key, value in data.items():

                if key not in EXPECTED_KEYS:
                    continue

                if value is None or isinstance(value, bool):
                    continue

                if not isinstance(value, (int, float)):
                    continue

                merged[key] = value

                quality[key] = (
                    "valid" if fresh else "stale"
                )

                valid_fields[key] = value

            if valid_fields:
                have_any = True

            sources_payload[name] = {
                "available": fresh,
                "age_ms": age_ms,
                "data": data
            }

        # ----------------------------------------------------
        # NO DATA + DUMMY DISABLED
        # ----------------------------------------------------

        if not have_any and not self.dummy_enabled:

            return None, None

        next_seq = self.packet_seq + 1

        # ----------------------------------------------------
        # DUMMY FALLBACK
        # ----------------------------------------------------

        dummy_values = self.generate_dummy_parameters(
            next_seq
        )

        dummy_count = 0

        if self.dummy_enabled:

            for key in EXPECTED_KEYS:

                if key not in merged:

                    merged[key] = dummy_values[key]

                    quality[key] = "simulated"

                    dummy_count += 1

        # ----------------------------------------------------
        # COMPLETE PARAMETER OBJECT
        # ----------------------------------------------------

        parameters = {}

        for key in EXPECTED_KEYS:

            parameters[key] = merged.get(key)

            if key not in quality:

                quality[key] = "unavailable"

        self.packet_seq = next_seq

        packet = {

            "version": 2,

            "seq": self.packet_seq,

            "uptime_ms": int(
                (now - self.start_time) * 1000
            ),

            "simulation": dummy_count > 0,

            "dummy_count": dummy_count,

            "parameter_count": len(EXPECTED_KEYS),

            "sources": sources_payload,

            "parameters": parameters,

            "parameter_quality": quality,

        }

        payload = json.dumps(
            packet,
            separators=(",", ":"),
            ensure_ascii=True
        )

        return payload, merged

    # ========================================================
    # PREPARE FRAGMENTS
    # ========================================================

    def transmit(self):

        if self.ser is None or not self.ser.is_open:

            self.try_reconnect()

            return

        if self.tx_queue or self.tx_busy:

            self.get_logger().warning(
                "Previous TX cycle not finished; skipping cycle"
            )

            return

        payload, merged = self.build_packet()

        if payload is None:

            self.get_logger().warning(
                "No telemetry available and dummy disabled"
            )

            return

        body = payload.encode("utf-8")

        if self.append_crc:

            crc = crc16_ccitt(body)

            body += f"*{crc:04X}".encode("ascii")

        chunks = [

            body[i:i + self.chunk_size]

            for i in range(
                0,
                len(body),
                self.chunk_size
            )

        ]

        total = len(chunks)

        for idx, chunk in enumerate(
            chunks,
            start=1
        ):

            fragment = (
                f"FRG:{idx}/{total}:".encode("ascii")
                + chunk
            )

            if len(fragment) > self.max_payload:

                self.get_logger().error(
                    f"Fragment too large: {len(fragment)} bytes"
                )

                self.tx_queue.clear()

                return

            self.tx_queue.append(fragment)

        missing = [
            key
            for key in EXPECTED_KEYS
            if merged.get(key) is None
        ]

        simulated = [
            key
            for key in EXPECTED_KEYS
            if key in merged
            and key not in [
                k for k, v in merged.items()
                if v is not None
            ]
        ]

        self.cur_msg = {

            "seq": self.packet_seq,

            "bytes": len(body),

            "frags": total,

            "t_start": time.monotonic(),

            "line": format_horizontal(merged),

            "missing": missing,

            "count": len([
                key for key in EXPECTED_KEYS
                if merged.get(key) is not None
            ]),

        }

    # ========================================================
    # PUMP TX QUEUE
    # ========================================================

    def pump_queue(self):

        if (
            self.ser is None
            or not self.ser.is_open
            or not self.tx_queue
        ):

            return

        now = time.monotonic()

        if self.tx_busy:

            if now - self.tx_started_at > self.tx_timeout:

                self.abort_message(
                    "TX DONE timeout"
                )

            return

        if now < self.next_send_at:
            return

        fragment = self.tx_queue[0]

        command = (
            "AT+PSEND="
            + fragment.hex().upper()
            + "\r\n"
        )

        try:

            self.ser.write(
                command.encode("ascii")
            )

            self.ser.flush()

            self.tx_busy = True

            self.tx_started_at = now

        except (
            serial.SerialException,
            serial.SerialTimeoutException,
            OSError
        ) as exc:

            self.get_logger().error(
                f"LoRa TX error: {exc}"
            )

            self.reset_tx_state()

            self.try_reconnect()

    # ========================================================
    # MESSAGE FINISHED
    # ========================================================

    def finish_message(self):

        msg = self.cur_msg

        if msg is None:
            return

        elapsed = (
            time.monotonic() - msg["t_start"]
        ) * 1000

        self.get_logger().info(

            f"[TX #{msg['seq']:04d}] "

            f"[{msg['bytes']}B "

            f"{msg['frags']} fragments "

            f"{elapsed:.0f}ms] "

            f"[{msg['count']}/35 parameters]"

        )

        self.get_logger().info(
            msg["line"]
        )

        if msg["missing"]:

            self.get_logger().warning(
                "Unavailable parameters: "
                + ", ".join(msg["missing"])
            )

        self.cur_msg = None

    # ========================================================
    # ABORT MESSAGE
    # ========================================================

    def abort_message(self, reason):

        seq = (
            self.cur_msg["seq"]
            if self.cur_msg
            else "?"
        )

        self.get_logger().warning(
            f"[TX #{seq}] Message aborted: {reason}"
        )

        self.reset_tx_state()

    # ========================================================
    # READ RAK RESPONSE
    # ========================================================

    def read_rak_response(self):

        if self.ser is None or not self.ser.is_open:
            return

        try:

            n = self.ser.in_waiting

            if n <= 0:
                return

            self.rx_buffer += self.ser.read(n).decode(
                "ascii",
                errors="replace"
            )

            while "\n" in self.rx_buffer:

                line, self.rx_buffer = (
                    self.rx_buffer.split("\n", 1)
                )

                line = line.strip()

                if not line:
                    continue

                if (
                    "+EVT:TXP2P DONE" in line
                    or "+EVT:TX_DONE" in line
                ):

                    if self.tx_busy and self.tx_queue:

                        self.tx_queue.popleft()

                        self.tx_busy = False

                        if not self.tx_queue:

                            self.finish_message()

                elif "AT_BUSY_ERROR" in line:

                    self.tx_busy = False

                    self.next_send_at = (
                        time.monotonic() + 0.05
                    )

                elif (
                    "AT_PARAM_ERROR" in line
                    or "AT_ERROR" in line
                ):

                    self.get_logger().error(
                        f"RAK response: {line}"
                    )

                    self.abort_message(line)

                else:

                    self.get_logger().debug(
                        f"RAK response: {line}"
                    )

            if len(self.rx_buffer) > 4096:

                self.rx_buffer = ""

        except (
            serial.SerialException,
            OSError
        ) as exc:

            self.get_logger().error(
                f"RAK read error: {exc}"
            )

            self.try_reconnect()

    # ========================================================
    # CLEANUP
    # ========================================================

    def destroy_node(self):

        if self.ser is not None and self.ser.is_open:

            try:

                self.ser.write(
                    b"AT+PRECV=0\r\n"
                )

                time.sleep(0.1)

            except Exception:
                pass

            self.ser.close()

        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None):

    rclpy.init(args=args)

    node = LoRaTransmitterNode()

    try:

        rclpy.spin(node)

    except KeyboardInterrupt:

        pass

    finally:

        node.destroy_node()

        rclpy.shutdown()


if __name__ == "__main__":

    main()
