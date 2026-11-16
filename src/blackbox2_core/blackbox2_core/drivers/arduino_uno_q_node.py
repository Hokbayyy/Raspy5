
#!/usr/bin/env python3

import math
import serial
import rclpy

from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# CONFIGURATION
# ============================================================

SERIAL_PORT = "/dev/ttyAMA0"
BAUDRATE = 115200

TOPIC_RAW = "/telemetri/arduino"

EXPECTED_FIELDS = 8
READ_INTERVAL = 0.001
MAX_BUFFER_SIZE = 1024


# ============================================================
# ARDUINO UNO Q UART NODE
# ============================================================

class ArduinoNode(Node):

    def __init__(self):
        super().__init__("arduino_uno_q_node")

        # Publisher: raw Arduino payload
        self.pub = self.create_publisher(
            String,
            TOPIC_RAW,
            10
        )

        # UART
        self.ser = serial.Serial(
            port=SERIAL_PORT,
            baudrate=BAUDRATE,
            timeout=0
        )

        self.buf = bytearray()
        self.count = 0
        self.invalid_count = 0

        self.timer = self.create_timer(
            READ_INTERVAL,
            self.read_uart
        )

        self.get_logger().info(
            f"Arduino UNO Q connected: {SERIAL_PORT}"
        )

        self.get_logger().info(
            f"Publishing raw data to {TOPIC_RAW}"
        )

    # ========================================================
    # VALIDATE FRAME
    # ========================================================

    def validate_frame(self, frame):

        if not frame.startswith(b"<"):
            return False

        if not frame.endswith(b">"):
            return False

        try:
            content = frame[1:-1].decode("ascii")

            fields = content.split(",")

            if len(fields) != EXPECTED_FIELDS:
                return False

            # Validasi angka saja.
            # Nilai tidak digunakan untuk menyusun ulang payload.
            for field in fields:

                value = float(field.strip())

                if not math.isfinite(value):
                    return False

            return True

        except (ValueError, UnicodeDecodeError):
            return False

    # ========================================================
    # READ UART
    # ========================================================

    def read_uart(self):

        try:

            available = self.ser.in_waiting

            if available <= 0:
                return

            incoming = self.ser.read(available)

            self.buf.extend(incoming)

            # Proteksi buffer jika frame rusak
            if len(self.buf) > MAX_BUFFER_SIZE:

                self.get_logger().warning(
                    "UART buffer overflow, clearing buffer."
                )

                self.buf.clear()
                self.invalid_count += 1
                return

            # Ambil frame lengkap berdasarkan newline
            while b"\n" in self.buf:

                frame, _, remaining = self.buf.partition(b"\n")

                self.buf = bytearray(remaining)

                frame = frame.strip()

                if not frame:
                    continue

                if not self.validate_frame(frame):

                    self.invalid_count += 1
                    continue

                # ============================================
                # PUBLISH ORIGINAL FRAME
                # ============================================

                msg = String()

                # Tidak membongkar atau menyusun ulang data.
                msg.data = frame.decode("ascii")

                self.pub.publish(msg)

                self.count += 1

                if self.count % 100 == 0:

                    self.get_logger().info(
                        f"Arduino packets published: {self.count}"
                    )

        except serial.SerialException as error:

            self.get_logger().error(
                f"UART error: {error}"
            )

        except Exception as error:

            self.get_logger().error(
                f"Read error: {error}"
            )

    # ========================================================
    # SHUTDOWN
    # ========================================================

    def destroy_node(self):

        if hasattr(self, "ser") and self.ser.is_open:
            self.ser.close()

        self.get_logger().info(
            f"Arduino stopped. "
            f"Published: {self.count}, "
            f"Invalid: {self.invalid_count}"
        )

        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None):

    rclpy.init(args=args)

    node = ArduinoNode()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:

        node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
