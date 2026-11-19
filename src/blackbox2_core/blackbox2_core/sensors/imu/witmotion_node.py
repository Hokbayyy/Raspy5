
#!/usr/bin/env python3

import os
import csv
import json
import math
import struct
import serial

from datetime import datetime, timezone

import rclpy
from rclpy.node import Node

from sensor_msgs.msg import Imu
from geometry_msgs.msg import Vector3
from std_msgs.msg import String


class WitMotionNode(Node):

    FRAME_HEADER = b'\x55\x61'
    FRAME_LENGTH = 28

    ACCEL_SCALE = 16.0
    GYRO_SCALE = 2000.0
    EULER_SCALE = 180.0

    GRAVITY = 9.80665

    def __init__(self):
        super().__init__('witmotion_hwt901b_node')

        # =====================================================
        # PARAMETERS
        # =====================================================

        self.declare_parameter('port', '/dev/ttyACM0')
        self.declare_parameter('baudrate', 115200)
        self.declare_parameter('polling_rate', 20.0)
        self.declare_parameter('serial_timeout', 0.01)

        self.port = self.get_parameter(
            'port'
        ).value

        self.baudrate = self.get_parameter(
            'baudrate'
        ).value

        self.polling_rate = self.get_parameter(
            'polling_rate'
        ).value

        self.serial_timeout = self.get_parameter(
            'serial_timeout'
        ).value

        # =====================================================
        # SERIAL
        # =====================================================

        self.serial_port = None
        self.rx_buffer = bytearray()

        self.connect_serial()

        # =====================================================
        # ROS PUBLISHERS
        # =====================================================

        self.imu_pub = self.create_publisher(
            Imu,
            'witmotion_imu',
            10
        )

        self.euler_pub = self.create_publisher(
            Vector3,
            'witmotion_euler',
            10
        )

        self.json_pub = self.create_publisher(
            String,
            '/sensors/witmotion',
            10
        )

        # =====================================================
        # CSV LOGGER
        # =====================================================

        self.csv_dir = os.path.expanduser(
            '~/blackbox2_ws/logs/imu'
        )

        os.makedirs(
            self.csv_dir,
            exist_ok=True
        )

        self.csv_path = os.path.join(
            self.csv_dir,
            'witmotion.csv'
        )

        self.csv_file = open(
            self.csv_path,
            'a',
            newline='',
            buffering=1
        )

        self.csv_writer = csv.writer(
            self.csv_file
        )

        if os.path.getsize(self.csv_path) == 0:
            self.csv_writer.writerow([
                'timestamp',
                'accel_x', 'accel_y', 'accel_z',
                'gyro_x', 'gyro_y', 'gyro_z',
                'roll', 'pitch', 'yaw'
            ])

        # =====================================================
        # FRAME CONFIGURATION
        # =====================================================

        self.frame_id = 'imu_link'

        self.frame_count = 0
        self.error_count = 0

        timer_period = 1.0 / max(
            float(self.polling_rate), 1.0
        )

        self.timer = self.create_timer(
            timer_period,
            self.read_serial
        )

        self.get_logger().info(
            'WT901SDCL-BT50 node started'
        )

        self.get_logger().info(
            f'Serial: {self.port} @ {self.baudrate}'
        )

        self.get_logger().info(
            f'CSV: {self.csv_path}'
        )

        self.get_logger().info(
            'JSON topic: /sensors/witmotion'
        )

    # =========================================================
    # SERIAL CONNECTION
    # =========================================================

    def connect_serial(self):

        try:
            self.serial_port = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.serial_timeout,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE
            )

            self.serial_port.reset_input_buffer()

            self.get_logger().info(
                f'Connected to {self.port}'
            )

        except serial.SerialException as error:

            self.serial_port = None

            self.get_logger().error(
                f'Serial connection failed: {error}'
            )

    # =========================================================
    # SERIAL READER
    # =========================================================

    def read_serial(self):

        if self.serial_port is None:
            return

        try:

            waiting = self.serial_port.in_waiting

            if waiting > 0:

                incoming = self.serial_port.read(
                    waiting
                )

                self.rx_buffer.extend(incoming)

                # Prevent unlimited buffer growth
                if len(self.rx_buffer) > 4096:
                    self.rx_buffer = self.rx_buffer[-128:]

                self.process_rx_buffer()

        except serial.SerialException as error:

            self.error_count += 1

            self.get_logger().error(
                f'Serial read error: {error}'
            )

            try:
                self.serial_port.close()
            except Exception:
                pass

            self.serial_port = None

    # =========================================================
    # FRAME PARSER
    # =========================================================

    def process_rx_buffer(self):

        while True:

            header_index = self.rx_buffer.find(
                self.FRAME_HEADER
            )

            # Header not found
            if header_index < 0:

                # Preserve possible first header byte
                if self.rx_buffer[-1:] == b'\x55':
                    self.rx_buffer = bytearray(b'\x55')
                else:
                    self.rx_buffer.clear()

                return

            # Remove bytes before header
            if header_index > 0:

                del self.rx_buffer[:header_index]

            # Wait until complete frame
            if len(self.rx_buffer) < self.FRAME_LENGTH:
                return

            frame = bytes(
                self.rx_buffer[:self.FRAME_LENGTH]
            )

            # Consume frame
            del self.rx_buffer[:self.FRAME_LENGTH]

            if frame[:2] != self.FRAME_HEADER:
                continue

            self.parse_frame(frame)

    # =========================================================
    # FRAME DECODER
    # =========================================================

    def parse_frame(self, frame):

        try:

            # 2-byte header + 18-byte sensor data
            values = struct.unpack(
                '<9h',
                frame[2:20]
            )

            # ---------------------------------------------
            # ACCELEROMETER
            # ---------------------------------------------

            ax_raw = values[0]
            ay_raw = values[1]
            az_raw = values[2]

            ax = (
                ax_raw / 32768.0
            ) * self.ACCEL_SCALE * self.GRAVITY

            ay = (
                ay_raw / 32768.0
            ) * self.ACCEL_SCALE * self.GRAVITY

            az = (
                az_raw / 32768.0
            ) * self.ACCEL_SCALE * self.GRAVITY

            # ---------------------------------------------
            # GYROSCOPE
            # ---------------------------------------------

            gx_raw = values[3]
            gy_raw = values[4]
            gz_raw = values[5]

            gx_deg = (
                gx_raw / 32768.0
            ) * self.GYRO_SCALE

            gy_deg = (
                gy_raw / 32768.0
            ) * self.GYRO_SCALE

            gz_deg = (
                gz_raw / 32768.0
            ) * self.GYRO_SCALE

            gx = math.radians(gx_deg)
            gy = math.radians(gy_deg)
            gz = math.radians(gz_deg)

            # ---------------------------------------------
            # EULER ANGLES
            # ---------------------------------------------

            roll = (
                values[6] / 32768.0
            ) * self.EULER_SCALE

            pitch = (
                values[7] / 32768.0
            ) * self.EULER_SCALE

            yaw = (
                values[8] / 32768.0
            ) * self.EULER_SCALE

            # ---------------------------------------------
            # TIMESTAMP
            # ---------------------------------------------

            timestamp = datetime.now(
                timezone.utc
            ).astimezone().isoformat(
                timespec='milliseconds'
            )

            # ---------------------------------------------
            # PUBLISH
            # ---------------------------------------------

            self.publish_imu(
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            self.publish_euler(
                roll, pitch, yaw
            )

            self.publish_json(
                timestamp,
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            self.write_csv(
                timestamp,
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            self.frame_count += 1

            if self.frame_count % 100 == 0:

                self.get_logger().info(
                    f'Frames received: {self.frame_count}'
                )

        except (struct.error, ValueError) as error:

            self.error_count += 1

            self.get_logger().warning(
                f'Frame parsing error: {error}'
            )

    # =========================================================
    # EULER TO QUATERNION
    # =========================================================

    @staticmethod
    def euler_to_quaternion(
        roll_deg,
        pitch_deg,
        yaw_deg
    ):

        roll = math.radians(roll_deg)
        pitch = math.radians(pitch_deg)
        yaw = math.radians(yaw_deg)

        cy = math.cos(yaw * 0.5)
        sy = math.sin(yaw * 0.5)

        cp = math.cos(pitch * 0.5)
        sp = math.sin(pitch * 0.5)

        cr = math.cos(roll * 0.5)
        sr = math.sin(roll * 0.5)

        qw = (
            cr * cp * cy +
            sr * sp * sy
        )

        qx = (
            sr * cp * cy -
            cr * sp * sy
        )

        qy = (
            cr * sp * cy +
            sr * cp * sy
        )

        qz = (
            cr * cp * sy -
            sr * sp * cy
        )

        return qx, qy, qz, qw

    # =========================================================
    # ROS IMU PUBLISHER
    # =========================================================

    def publish_imu(
        self,
        ax, ay, az,
        gx, gy, gz,
        roll, pitch, yaw
    ):

        msg = Imu()

        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = self.frame_id

        qx, qy, qz, qw = self.euler_to_quaternion(
            roll,
            pitch,
            yaw
        )

        msg.orientation.x = qx
        msg.orientation.y = qy
        msg.orientation.z = qz
        msg.orientation.w = qw

        msg.angular_velocity.x = gx
        msg.angular_velocity.y = gy
        msg.angular_velocity.z = gz

        msg.linear_acceleration.x = ax
        msg.linear_acceleration.y = ay
        msg.linear_acceleration.z = az

        self.imu_pub.publish(msg)

    # =========================================================
    # ROS EULER PUBLISHER
    # =========================================================

    def publish_euler(
        self,
        roll,
        pitch,
        yaw
    ):

        msg = Vector3()

        # Degrees, preserving previous convention
        msg.x = float(roll)
        msg.y = float(pitch)
        msg.z = float(yaw)

        self.euler_pub.publish(msg)

    # =========================================================
    # JSON PUBLISHER
    # =========================================================

    def publish_json(
        self,
        timestamp,
        ax, ay, az,
        gx, gy, gz,
        roll, pitch, yaw
    ):

        payload = {
            'source': 'witmotion_wt901sdcl_bt50',
            'timestamp': timestamp,
            'status': 'ok',
            'field_count': 9,
            'data': {
                'accel': {
                    'x': round(ax, 5),
                    'y': round(ay, 5),
                    'z': round(az, 5),
                    'unit': 'm/s2'
                },
                'gyro': {
                    'x': round(gx, 5),
                    'y': round(gy, 5),
                    'z': round(gz, 5),
                    'unit': 'rad/s'
                },
                'euler': {
                    'roll': round(roll, 3),
                    'pitch': round(pitch, 3),
                    'yaw': round(yaw, 3),
                    'unit': 'degree'
                }
            }
        }

        msg = String()

        msg.data = json.dumps(
            payload,
            separators=(',', ':')
        )

        self.json_pub.publish(msg)

    # =========================================================
    # CSV LOGGER
    # =========================================================

    def write_csv(
        self,
        timestamp,
        ax, ay, az,
        gx, gy, gz,
        roll, pitch, yaw
    ):

        self.csv_writer.writerow([
            timestamp,
            round(ax, 5),
            round(ay, 5),
            round(az, 5),
            round(gx, 5),
            round(gy, 5),
            round(gz, 5),
            round(roll, 3),
            round(pitch, 3),
            round(yaw, 3)
        ])

    # =========================================================
    # CLEANUP
    # =========================================================

    def destroy_node(self):

        self.get_logger().info(
            'Stopping WitMotion node...'
        )

        if hasattr(self, 'csv_file') and not self.csv_file.closed:
            self.csv_file.flush()
            self.csv_file.close()

        if self.serial_port is not None:

            try:
                self.serial_port.close()
            except Exception:
                pass

        super().destroy_node()


# =============================================================
# MAIN
# =============================================================

def main(args=None):

    rclpy.init(args=args)

    node = WitMotionNode()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:

        node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
