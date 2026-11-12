
#!/usr/bin/env python3

import json
import time
import serial

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

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
# GENERIC LORA TRANSMITTER
# ============================================================

class LoRaTransmitterNode(Node):

    def __init__(self):
        super().__init__("lora_transmitter_node")

        # ----------------------------------------------------
        # PARAMETERS
        # ----------------------------------------------------

        self.declare_parameter("port", "/dev/ttyAMA3")
        self.declare_parameter("baudrate", 115200)
        self.declare_parameter("interval", 0.25)
        self.declare_parameter("stale_timeout", 1.0)

        port = self.get_parameter("port").value
        baud = int(self.get_parameter("baudrate").value)

        self.interval = float(
            self.get_parameter("interval").value
        )

        self.stale_timeout = float(
            self.get_parameter("stale_timeout").value
        )

        # ----------------------------------------------------
        # INTERNAL STATE
        # ----------------------------------------------------

        self.packet_seq = 0
        self.start_time = time.monotonic()

        # Menyimpan payload asli setiap sumber.
        # Tidak ada parsing field sensor.
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

        self.ser = None
        self.rx_buffer = ""

        # ----------------------------------------------------
        # RAK SERIAL CONNECTION
        # ----------------------------------------------------

        self.connect_rak(port, baud)

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
            0.01,
            self.read_rak_response
        )

        self.create_timer(
            self.interval,
            self.transmit
        )

        self.get_logger().info(
            "Generic LoRa transmitter started"
        )

    # ========================================================
    # CONNECT RAK
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

        except (serial.SerialException, OSError) as exc:

            self.ser = None

            self.get_logger().error(
                f"RAK connection failed: {exc}"
            )

    # ========================================================
    # RECEIVE RAW ROS DATA
    # ========================================================

    def source_callback(self, source_name, msg):

        raw = msg.data

        self.sources[source_name]["raw"] = raw
        self.sources[source_name]["received_at"] = (
            time.monotonic()
        )

    # ========================================================
    # BUILD GENERIC ENVELOPE
    # ========================================================

    def build_packet(self):

        now = time.monotonic()

        sources_payload = {}

        available_count = 0

        for name, source in self.sources.items():

            raw = source["raw"]
            received_at = source["received_at"]

            available = (
                raw is not None
                and received_at is not None
                and now - received_at <= self.stale_timeout
            )

            if available:
                age_ms = int(
                    (now - received_at) * 1000
                )

                available_count += 1

            else:
                age_ms = None

            sources_payload[name] = {
                "available": available,
                "age_ms": age_ms,
                "raw": raw if available else None
            }

        if available_count == 0:
            return None

        self.packet_seq += 1

        packet = {
            "version": 1,
            "seq": self.packet_seq,
            "uptime_ms": int(
                (now - self.start_time) * 1000
            ),
            "sources": sources_payload
        }

        # Compact JSON agar payload tidak terlalu besar.
        return json.dumps(
            packet,
            separators=(",", ":"),
            ensure_ascii=True
        )

    # ========================================================
    # TRANSMIT THROUGH RAK
    # ========================================================

    def transmit(self):

        if self.ser is None or not self.ser.is_open:
            return

        payload = self.build_packet()

        if payload is None:
            self.get_logger().warning(
                "Belum ada telemetry yang tersedia"
            )
            return

        body = payload.encode("utf-8")

        crc = crc16_ccitt(body)

        # CRC ditambahkan sebagai suffix.
        packet = body + f"*{crc:04X}".encode("ascii")

        # Ubah seluruh paket menjadi HEX.
        payload_hex = packet.hex().upper()

        command = (
            f"AT+PSEND={payload_hex}\r\n"
        )

        try:

            self.ser.write(command.encode("ascii"))
            self.ser.flush()

            self.get_logger().info(
                f"LoRa TX seq={self.packet_seq} | "
                f"{len(packet)} bytes | "
                f"HEX={len(payload_hex)} chars"
            )

        except (
            serial.SerialException,
            serial.SerialTimeoutException,
            OSError
        ) as exc:

            self.get_logger().error(
                f"LoRa TX error: {exc}"
            )

    # ========================================================
    # READ RAK RESPONSE
    # ========================================================

    def read_rak_response(self):

        if self.ser is None or not self.ser.is_open:
            return

        try:

            waiting = self.ser.in_waiting

            if waiting <= 0:
                return

            data = self.ser.read(waiting).decode(
                "ascii",
                errors="replace"
            )

            self.rx_buffer += data

            while "\n" in self.rx_buffer:

                line, self.rx_buffer = (
                    self.rx_buffer.split("\n", 1)
                )

                line = line.strip()

                if line:
                    self.get_logger().info(
                        f"RAK RX: {line}"
                    )

            # Hindari buffer tumbuh tanpa batas.
            if len(self.rx_buffer) > 4096:
                self.rx_buffer = ""

                self.get_logger().warning(
                    "RAK response buffer reset"
                )

        except (
            serial.SerialException,
            OSError
        ) as exc:

            self.get_logger().error(
                f"RAK read error: {exc}"
            )

    # ========================================================
    # CLEANUP
    # ========================================================

    def destroy_node(self):

        if self.ser is not None and self.ser.is_open:
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