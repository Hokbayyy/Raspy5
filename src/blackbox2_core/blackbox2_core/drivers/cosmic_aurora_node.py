
import json
import math
import serial

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


class CosmicAuroraNode(Node):

    FIELD_NAMES = [
        'accel_x',
        'accel_y',
        'accel_z',
        'gyro_x',
        'gyro_y',
        'gyro_z',
        'bme_temperature',
        'bme_humidity',
        'bme_pressure',
        'bme_altitude',
        'sound_db_left',
        'sound_db_right'
    ]

    def __init__(self):
        super().__init__('cosmic_aurora_node')

        self.port_name = '/dev/ttyAMA2'
        self.baudrate = 115200

        self.ser = None
        self.buffer = bytearray()

        self.raw_pub = self.create_publisher(
            String, '/telemetri/aurora', 10
        )

        self.data_pub = self.create_publisher(
            String, '/telemetri/aurora/data', 10
        )

        self.total_frames = 0
        self.valid_frames = 0
        self.invalid_frames = 0

        self.connect_serial()

        self.timer = self.create_timer(
            0.005, self.read_serial
        )

    def connect_serial(self):
        try:
            self.ser = serial.Serial(
                self.port_name,
                self.baudrate,
                timeout=0
            )

            self.get_logger().info(
                f'Connected: {self.port_name} '
                f'@ {self.baudrate}'
            )

        except serial.SerialException as e:
            self.ser = None
            self.get_logger().error(
                f'Serial connection failed: {e}'
            )

    def read_serial(self):
        if self.ser is None or not self.ser.is_open:
            return

        try:
            waiting = self.ser.in_waiting

            if waiting <= 0:
                return

            self.buffer.extend(self.ser.read(waiting))

            if len(self.buffer) > 8192:
                self.buffer.clear()
                self.invalid_frames += 1
                self.get_logger().warning(
                    'Serial buffer overflow'
                )
                return

            while b'\n' in self.buffer:
                line, _, remaining = self.buffer.partition(b'\n')
                self.buffer = bytearray(remaining)

                self.process_frame(line)

        except serial.SerialException as e:
            self.get_logger().error(
                f'Serial read error: {e}'
            )

    def process_frame(self, raw_line):
        self.total_frames += 1

        try:
            line = raw_line.decode('ascii').strip()

            if not line:
                return

            values = [
                float(item.strip())
                for item in line.split(',')
            ]

            if not values:
                raise ValueError('Empty frame')

            if not all(math.isfinite(v) for v in values):
                raise ValueError('Invalid numeric value')

            # Raw CSV publisher
            raw_msg = String()
            raw_msg.data = line
            self.raw_pub.publish(raw_msg)

            # Dynamic mapping
            data = {}

            for index, value in enumerate(values):
                if index < len(self.FIELD_NAMES):
                    name = self.FIELD_NAMES[index]
                else:
                    name = f'extra_{index + 1}'

                data[name] = value

            status = (
                'complete' if len(values) == 12
                else 'partial' if len(values) < 12
                else 'extended'
            )

            payload = {
                'source': 'cosmic_aurora',
                'status': status,
                'field_count': len(values),
                'data': data
            }

            json_msg = String()
            json_msg.data = json.dumps(
                payload,
                separators=(',', ':')
            )

            self.data_pub.publish(json_msg)

            self.valid_frames += 1

            if self.valid_frames % 100 == 0:
                self.get_logger().info(
                    f'Valid frames: {self.valid_frames} | '
                    f'Invalid: {self.invalid_frames}'
                )

        except (ValueError, UnicodeDecodeError):
            self.invalid_frames += 1
            # Pesan ERR atau teks debug tidak diteruskan.
            return

    def destroy_node(self):
        if self.ser is not None and self.ser.is_open:
            self.ser.close()

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = CosmicAuroraNode()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
