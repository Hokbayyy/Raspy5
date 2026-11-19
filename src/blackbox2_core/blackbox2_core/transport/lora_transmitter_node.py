
#!/usr/bin/env python3

import json
import math
import time
import threading
from collections import deque

import serial
import rclpy

from rclpy.node import Node
from sensor_msgs.msg import Imu
from std_msgs.msg import String


# ============================================================
# RAK LORA CONFIGURATION
# ============================================================

SERIAL_PORT = "/dev/ttyAMA3"
BAUD_RATE = 115200

FREQUENCY = 923200000
SPREADING_FACTOR = 6
BANDWIDTH = 2
CODING_RATE = 0
PREAMBLE = 8
TX_POWER = 20

# Kirim satu paket setiap 1 detik
TRANSMIT_INTERVAL = 1.0

# Batas pemeriksaan ukuran paket di sisi gateway.
# Pastikan juga sesuai batas payload dan konfigurasi RAK.
MAX_PAYLOAD = 700

# Pertahankan nilai terakhir yang valid.
# Tidak pernah membuat nilai dummy.
HOLD_LAST_VALUE = True

# Maksimal antrean paket
MAX_QUEUE_SIZE = 3


# ============================================================
# FIELD ORDER - TOTAL 105 FIELD
# URUTAN INI TIDAK BOLEH DIUBAH TANPA MENYESUAIKAN RECEIVER
# ============================================================

EXPECTED_KEYS = [

    # --------------------------------------------------------
    # 1. ARDUINO UNO Q (23)
    # --------------------------------------------------------

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
    # 9. WITMOTION (6)
    # --------------------------------------------------------

    "ax",
    "ay",
    "az",

    "gx",
    "gy",
    "gz",
]


# ============================================================
# ARDUINO FIELD MAPPING
# ============================================================

ARDUINO_MAP = {

    "lidar_distance_cm": "lidar_raw",

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

class LoRaTransmitterNode(Node):

    def __init__(self):

        super().__init__("lora_transmitter_node")

        self.lock = threading.RLock()

        # Data terbaru yang sudah diterima
        self.latest_values = {}

        # Data valid terakhir
        self.last_values = {}

        self.tx_queue = deque()

        self.serial_port = None
        self.tx_active = False

        self.transmitted_count = 0
        self.failed_count = 0

        self.received_sources = set()

        # Alert queue has priority over regular telemetry packets.
        self.alert_sequence = 0
        self.alert_tx_queue = deque(maxlen=10)

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

        # WitMotion IMU (ax, ay, az, gx, gy, gz)
        self.witmotion_sub = self.create_subscription(
            Imu,
            "witmotion_imu",
            self.witmotion_imu_callback,
            10
        )

        # Compact system alerts from Alert Manager
        self.alert_sub = self.create_subscription(
            String,
            "/system/alerts",
            self.alert_callback,
            10
        )

        # ====================================================
        # TIMERS
        # ====================================================

        self.transmit_timer = self.create_timer(
            TRANSMIT_INTERVAL,
            self.transmit_callback
        )

        self.queue_timer = self.create_timer(
            0.01,
            self.process_queue
        )

        # ====================================================
        # SERIAL INITIALIZATION
        # ====================================================

        self.initialize_serial()

        self.get_logger().info(
            f"LoRa node initialized | "
            f"Fields: {len(EXPECTED_KEYS)} | "
            f"Interval: {TRANSMIT_INTERVAL}s"
        )

    # ========================================================
    # SERIAL INITIALIZATION
    # ========================================================

    def initialize_serial(self):

        try:

            self.serial_port = serial.Serial(
                port=SERIAL_PORT,
                baudrate=BAUD_RATE,
                timeout=0.1,
                write_timeout=1
            )

            time.sleep(1)

            self.serial_port.reset_input_buffer()
            self.serial_port.reset_output_buffer()

            self.get_logger().info(
                f"RAK connected: {SERIAL_PORT}"
            )

            self.configure_rak()

        except Exception as error:

            self.serial_port = None

            self.get_logger().error(
                f"RAK connection failed: {error}"
            )

    # ========================================================
    # SEND AT COMMAND
    # ========================================================

    def send_at(self, command, timeout=2.0):

        if self.serial_port is None:
            return False

        try:

            self.serial_port.reset_input_buffer()

            self.serial_port.write(
                (command + "\r\n").encode()
            )

            self.serial_port.flush()

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

                    if "ERROR" in response:
                        self.get_logger().error(
                            f"AT error: {response.strip()}"
                        )
                        return False

                    if "OK" in response:
                        self.get_logger().info(
                            f"AT OK: {command}"
                        )
                        return True

                time.sleep(0.02)

            self.get_logger().warning(
                f"AT timeout: {command} | {response.strip()}"
            )

            return False

        except Exception as error:

            self.get_logger().error(
                f"AT command error: {error}"
            )

            return False

    # ========================================================
    # CONFIGURE RAK
    # ========================================================

    def configure_rak(self):

        if self.serial_port is None:
            return

        commands = [

            "AT",

            "AT+NWM=0",

            "AT+PRECV=0",

            (
                f"AT+P2P={FREQUENCY}:"
                f"{SPREADING_FACTOR}:"
                f"{BANDWIDTH}:"
                f"{CODING_RATE}:"
                f"{PREAMBLE}:"
                f"{TX_POWER}"
            ),
        ]

        self.get_logger().info(
            "Configuring RAK P2P..."
        )

        for command in commands:

            success = self.send_at(command)

            if not success:

                self.get_logger().error(
                    f"RAK configuration failed at: {command}"
                )

                return

            time.sleep(0.2)

        self.get_logger().info(
            "RAK P2P configuration completed."
        )

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

        if isinstance(payload, dict):

            data = payload.get("data", payload)

            if not isinstance(data, dict):
                return

            mapping = {

                "loadcell_paka": "loadcell_paka",
                "loadcell_paki": "loadcell_paki",
                "loadcell_leher": "loadcell_leher",
                "loadcell_kaka": "loadcell_kaka",
                "loadcell_kaki": "loadcell_kaki",
            }

            updated = self.update_mapped_values(
                data,
                mapping
            )

            if updated:
                self.received_sources.add("loadcell")

    # ========================================================
    # WITMOTION IMU CALLBACK
    # ========================================================

    def witmotion_imu_callback(self, msg):

        try:

            ax = round(msg.linear_acceleration.x / 9.80665, 4)
            ay = round(msg.linear_acceleration.y / 9.80665, 4)
            az = round(msg.linear_acceleration.z / 9.80665, 4)

            gx = round(math.degrees(msg.angular_velocity.x), 4)
            gy = round(math.degrees(msg.angular_velocity.y), 4)
            gz = round(math.degrees(msg.angular_velocity.z), 4)

            updated = 0

            for key, val in (
                ("ax", ax),
                ("ay", ay),
                ("az", az),
                ("gx", gx),
                ("gy", gy),
                ("gz", gz),
            ):
                if self.update_field(key, val):
                    updated += 1

            if updated:
                self.received_sources.add("witmotion")

        except Exception as error:

            self.get_logger().warning(
                f"Invalid WitMotion IMU message: {error}"
            )

    # ========================================================
    # COMPACT ALERT ENCODER
    # ========================================================

    def alert_callback(self, msg):
        """Encode a JSON alert as a compact 12-byte ASCII payload.

        Layout: A1 + sequence(4 hex) + severity(1 hex) +
                event(2 hex) + status(1 hex) + offline-mask(2 hex).
        CRC16 and FRG header are added by fragment_packet().
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
            compact = (
                f"A1{self.alert_sequence:04X}{severity:X}"
                f"{event_code:02X}{status:X}{device_mask:02X}"
            )
            if len(compact) != 12:
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

    def build_packet(self):

        values = []

        with self.lock:

            for key in EXPECTED_KEYS:

                value = self.latest_values.get(key)

                if value is None and HOLD_LAST_VALUE:
                    value = self.last_values.get(key)

                values.append(value)

        # None akan menjadi null di JSON.
        # Tidak ada dummy atau angka buatan.

        return json.dumps(
            values,
            separators=(",", ":"),
            allow_nan=False
        )

    # ========================================================
    # BUILD ONE LORA FRAGMENT
    # ========================================================

    def fragment_packet(self, payload):

        json_bytes = payload.encode("utf-8")

        crc = crc16_ccitt(json_bytes)

        complete_data = (
            json_bytes +
            f"*{crc:04X}".encode("ascii")
        )

        header = b"FRG:1/1:"

        fragment = header + complete_data

        if len(fragment) > MAX_PAYLOAD:

            raise ValueError(
                f"Payload terlalu besar: {len(fragment)} bytes"
            )

        return fragment

    # ========================================================
    # TRANSMIT TIMER
    # ========================================================

    def transmit_callback(self):

        if self.serial_port is None:
            return

        # Jangan kirim paket kosong sebelum menerima data sensor.
        with self.lock:

            if not self.latest_values:
                return

        try:

            payload = self.build_packet()

            fragment = self.fragment_packet(payload)

            with self.lock:

                if len(self.tx_queue) >= MAX_QUEUE_SIZE:

                    self.tx_queue.popleft()

                    self.get_logger().warning(
                        "Oldest packet removed from TX queue"
                    )

                self.tx_queue.append(fragment)

        except Exception as error:

            self.failed_count += 1

            self.get_logger().error(
                f"Packet creation failed: {error}"
            )

    # ========================================================
    # PROCESS TX QUEUE
    # ========================================================

    def process_queue(self):

        if self.serial_port is None:
            return

        if self.tx_active:
            return

        # Alert packets take priority; telemetry queue remains unchanged.
        if self.alert_tx_queue:
            fragment = self.alert_tx_queue.popleft()
        elif self.tx_queue:
            fragment = self.tx_queue.popleft()
        else:
            return

        self.tx_active = True

        try:

            hex_data = fragment.hex().upper()

            command = f"AT+PSEND={hex_data}"

            self.serial_port.reset_input_buffer()

            self.serial_port.write(
                (command + "\r\n").encode("ascii")
            )

            self.serial_port.flush()

            success = self.wait_tx_done(timeout=2.0)

            if success:

                self.transmitted_count += 1

                self.get_logger().info(
                    f"TX success: {self.transmitted_count} | "
                    f"Sources: {sorted(self.received_sources)}"
                )

            else:

                self.failed_count += 1

                self.get_logger().warning(
                    f"TX failed: {self.failed_count}"
                )

        except Exception as error:

            self.failed_count += 1

            self.get_logger().error(
                f"LoRa TX error: {error}"
            )

        finally:

            self.tx_active = False

    # ========================================================
    # WAIT TRANSMISSION CONFIRMATION
    # ========================================================

    def wait_tx_done(self, timeout=2.0):

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

                if "+EVT:TXP2P DONE" in response:
                    return True

                if "ERROR" in response:
                    self.get_logger().error(
                        response.strip()
                    )
                    return False

            time.sleep(0.02)

        self.get_logger().warning(
            f"TX confirmation timeout: {response.strip()}"
        )

        return False

    # ========================================================
    # SHUTDOWN
    # ========================================================

    def destroy_node(self):

        if self.serial_port is not None:

            try:
                self.serial_port.close()
            except Exception:
                pass

        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None):

    rclpy.init(args=args)

    node = None

    try:

        node = LoRaTransmitterNode()

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
