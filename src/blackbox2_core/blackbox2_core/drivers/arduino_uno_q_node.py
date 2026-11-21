
import json
import math
import serial
import rclpy

from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# FIELD MAP: SESUAI URUTAN OUTPUT ARDUINO UNO Q
# ============================================================

FIELD_NAMES = [
    "timestamp_ms",
    "lidar_distance_cm",
    "lidar_median_cm",

    "mq135_nh3_ppm",
    "mq135_co_ppm",
    "mq135_co2_ppm",
    "mq2_smoke_index",

    "mpu_accel_x",
    "mpu_accel_y",
    "mpu_accel_z",

    "mpu_gyro_x",
    "mpu_gyro_y",
    "mpu_gyro_z",

    "mpu_accel_kalman_x",
    "mpu_accel_kalman_y",
    "mpu_accel_kalman_z",

    "mpu_gyro_kalman_x",
    "mpu_gyro_kalman_y",
    "mpu_gyro_kalman_z",

    "adxl_accel_x",
    "adxl_accel_y",
    "adxl_accel_z",

    "adxl_accel_kalman_x",
    "adxl_accel_kalman_y",
    "adxl_accel_kalman_z",
]


class ArduinoUnoQNode(Node):

    def __init__(self):
        super().__init__('arduino_uno_q_node')

        # ----------------------------------------------------
        # SERIAL CONFIGURATION
        # ----------------------------------------------------
        self.port = '/dev/ttyAMA0'
        self.baudrate = 115200
        self.ser = serial.Serial(
            self.port,
            self.baudrate,
            timeout=0.01
        )

        self.buffer = bytearray()
        self.max_buffer_size = 4096

        # ----------------------------------------------------
        # ROS 2 PUBLISHERS
        # ----------------------------------------------------

        # Original Arduino frame (compatibility)
        self.raw_pub = self.create_publisher(
            String,
            '/telemetri/arduino',
            10
        )

        # Normalized dynamic JSON
        self.data_pub = self.create_publisher(
            String,
            '/telemetri/arduino/data',
            10
        )

        self.frame_count = 0
        self.error_count = 0

        self.timer = self.create_timer(
            0.005,
            self.read_serial
        )

        self.get_logger().info(
            f'Arduino UNO Q connected: '
            f'{self.port} @ {self.baudrate}'
        )

        self.get_logger().info(
            f'Expected initial fields: {len(FIELD_NAMES)}'
        )

    # ========================================================
    # SERIAL READER
    # ========================================================

    def read_serial(self):
        try:
            waiting = self.ser.in_waiting
            incoming = self.ser.read(waiting if waiting else 1)

            if not incoming:
                return

            self.buffer.extend(incoming)

            # Protect against unlimited buffer growth
            if len(self.buffer) > self.max_buffer_size:
                self.buffer.clear()
                self.error_count += 1

                self.get_logger().warning(
                    'UART buffer overflow; buffer cleared'
                )
                return

            self.process_frames()

        except serial.SerialException as e:
            self.get_logger().error(
                f'Serial error: {e}'
            )

        except Exception as e:
            self.get_logger().error(
                f'Reader error: {e}'
            )

    # ========================================================
    # FRAME EXTRACTION: < ... >
    # ========================================================

    def process_frames(self):

        while True:

            start = self.buffer.find(b'<')

            if start == -1:
                self.buffer.clear()
                return

            if start > 0:
                del self.buffer[:start]

            end = self.buffer.find(b'>')

            if end == -1:
                return

            frame = bytes(self.buffer[:end + 1])
            del self.buffer[:end + 1]

            try:
                text = frame.decode('ascii').strip()

                self.publish_raw(text)
                self.parse_and_publish(text)

            except UnicodeDecodeError:
                self.error_count += 1

                self.get_logger().warning(
                    'Non-ASCII UART frame rejected'
                )

    # ========================================================
    # RAW TOPIC
    # ========================================================

    def publish_raw(self, frame):

        msg = String()
        msg.data = frame

        self.raw_pub.publish(msg)

    # ========================================================
    # DYNAMIC DATA PARSER
    # ========================================================

    def parse_and_publish(self, frame):

        payload = frame[1:-1].strip()

        if not payload:
            self.error_count += 1
            return

        parts = payload.split(',')

        values = []

        try:
            for part in parts:

                value = float(part.strip())

                if not math.isfinite(value):
                    raise ValueError(
                        f'Invalid numeric value: {part}'
                    )

                values.append(value)

        except ValueError as e:
            self.error_count += 1

            self.get_logger().warning(
                f'Invalid frame: {e}'
            )
            return

        # ----------------------------------------------------
        # AUTOMATIC FIELD MAPPING
        # ----------------------------------------------------

        data = {}

        for index, value in enumerate(values):

            if index < len(FIELD_NAMES):

                field_name = FIELD_NAMES[index]

            else:
                # Automatically accept newly appended fields
                field_name = f'extra_{index + 1}'

            # Timestamp is represented as integer
            if index == 0:
                data[field_name] = int(value)
            else:
                data[field_name] = value

        # ----------------------------------------------------
        # STATUS
        # ----------------------------------------------------

        if len(values) < len(FIELD_NAMES):
            status = 'partial'

        elif len(values) == len(FIELD_NAMES):
            status = 'complete'

        else:
            status = 'extended'

        output = {
            'source': 'arduino_uno_q',
            'status': status,
            'field_count': len(values),
            'data': data
        }

        # ----------------------------------------------------
        # PUBLISH JSON
        # ----------------------------------------------------

        msg = String()
        msg.data = json.dumps(
            output,
            separators=(',', ':'),
            allow_nan=False
        )

        self.data_pub.publish(msg)

        self.frame_count += 1

        # Avoid excessive logging at high data rates
        if self.frame_count % 100 == 0:

            self.get_logger().info(
                f'Frames: {self.frame_count} | '
                f'Fields: {len(values)} | '
                f'Status: {status}'
            )

    # ========================================================
    # CLEAN SHUTDOWN
    # ========================================================

    def destroy_node(self):

        if hasattr(self, 'ser') and self.ser.is_open:
            self.ser.close()

        super().destroy_node()


def main(args=None):

    rclpy.init(args=args)

    node = ArduinoUnoQNode()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
