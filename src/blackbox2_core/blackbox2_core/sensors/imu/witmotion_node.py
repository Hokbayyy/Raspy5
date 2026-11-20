
#!/usr/bin/env python3

import os
import csv
import json
import math
import time
import struct
import serial

from datetime import datetime, timezone

import rclpy
from rclpy.node import Node

from sensor_msgs.msg import Imu
from geometry_msgs.msg import Vector3
from std_msgs.msg import String


class WitMotionNode(Node):

    # =========================================================
    # WT901 FRAME
    # =========================================================

    FRAME_HEADER = b'\x55\x61'
    FRAME_LENGTH = 28

    ACCEL_SCALE = 16.0
    GYRO_SCALE = 2000.0
    EULER_SCALE = 180.0

    GRAVITY = 9.80665

    # =========================================================
    # INITIALIZATION
    # =========================================================

    def __init__(self):

        super().__init__('witmotion_hwt901b_node')

        # -----------------------------------------------------
        # PARAMETERS
        # -----------------------------------------------------

        self.declare_parameter('port', '/dev/ttyACM0')
        self.declare_parameter('baudrate', 9600)
        self.declare_parameter('polling_rate', 20.0)
        self.declare_parameter('serial_timeout', 0.01)

        self.declare_parameter('calibration_duration', 120.0)
        self.declare_parameter('gyro_motion_threshold', 10.0)
        self.declare_parameter('accel_tolerance', 2.0)

        self.port = self.get_parameter('port').value
        self.baudrate = self.get_parameter('baudrate').value
        self.polling_rate = self.get_parameter('polling_rate').value
        self.serial_timeout = self.get_parameter('serial_timeout').value

        self.calibration_duration = float(
            self.get_parameter('calibration_duration').value
        )

        self.gyro_motion_threshold = float(
            self.get_parameter('gyro_motion_threshold').value
        )

        self.accel_tolerance = float(
            self.get_parameter('accel_tolerance').value
        )

        # -----------------------------------------------------
        # SERIAL
        # -----------------------------------------------------

        self.serial_port = None
        self.rx_buffer = bytearray()

        self.connect_serial()

        # -----------------------------------------------------
        # ROS PUBLISHERS
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # CSV
        # -----------------------------------------------------

        self.csv_dir = os.path.expanduser(
            '~/blackbox2_ws/logs/imu'
        )

        os.makedirs(self.csv_dir, exist_ok=True)

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

        self.csv_writer = csv.writer(self.csv_file)

        if os.path.getsize(self.csv_path) == 0:

            self.csv_writer.writerow([
                'timestamp',
                'accel_x', 'accel_y', 'accel_z',
                'gyro_x', 'gyro_y', 'gyro_z',
                'roll', 'pitch', 'yaw'
            ])

        # -----------------------------------------------------
        # CALIBRATION STORAGE
        # -----------------------------------------------------

        self.config_dir = os.path.expanduser(
            '~/.config/smart_mannequin'
        )

        os.makedirs(self.config_dir, exist_ok=True)

        self.calibration_path = os.path.join(
            self.config_dir,
            'witmotion_calibration.json'
        )

        # -----------------------------------------------------
        # CALIBRATION STATE
        # -----------------------------------------------------

        self.calibration_state = 'waiting_for_sensor'

        self.calibration_start_time = None

        self.gyro_sum = [0.0, 0.0, 0.0]
        self.gyro_sample_count = 0

        self.gyro_bias = [0.0, 0.0, 0.0]

        self.calibration_progress = 0.0
        self.calibration_completed = False

        self.last_status_publish = 0.0

        self.frame_count = 0
        self.error_count = 0

        self.frame_id = 'imu_link'

        # -----------------------------------------------------
        # TIMER
        # -----------------------------------------------------

        timer_period = 1.0 / max(
            float(self.polling_rate),
            1.0
        )

        self.timer = self.create_timer(
            timer_period,
            self.read_serial
        )

        self.get_logger().info(
            'WT901 Startup Auto-Calibration initialized'
        )

        self.get_logger().info(
            f'USB: {self.port} @ {self.baudrate}'
        )

        self.get_logger().info(
            f'Calibration duration: {self.calibration_duration:.0f} seconds'
        )

        self.get_logger().info(
            'Waiting for sensor data...'
        )

    # =========================================================
    # SERIAL CONNECTION
    # =========================================================

    def connect_serial(self):

        if self.serial_port is not None:
            return

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

            self.get_logger().warning(
                f'Waiting for USB sensor: {error}'
            )

    # =========================================================
    # SERIAL READER
    # =========================================================

    def read_serial(self):

        if self.serial_port is None:

            self.connect_serial()
            return

        try:

            waiting = self.serial_port.in_waiting

            if waiting > 0:

                incoming = self.serial_port.read(waiting)

                self.rx_buffer.extend(incoming)

                # Prevent unlimited buffer growth
                if len(self.rx_buffer) > 4096:
                    self.rx_buffer = self.rx_buffer[-128:]

                self.process_rx_buffer()

        except serial.SerialException as error:

            self.error_count += 1

            self.get_logger().error(
                f'Serial error: {error}'
            )

            try:
                self.serial_port.close()
            except Exception:
                pass

            self.serial_port = None
            self.rx_buffer.clear()

    # =========================================================
    # FRAME PARSER
    # =========================================================

    def process_rx_buffer(self):

        while True:

            header_index = self.rx_buffer.find(
                self.FRAME_HEADER
            )

            if header_index < 0:

                if self.rx_buffer[-1:] == b'\x55':
                    self.rx_buffer = bytearray(b'\x55')
                else:
                    self.rx_buffer.clear()

                return

            if header_index > 0:
                del self.rx_buffer[:header_index]

            if len(self.rx_buffer) < self.FRAME_LENGTH:
                return

            frame = bytes(
                self.rx_buffer[:self.FRAME_LENGTH]
            )

            del self.rx_buffer[:self.FRAME_LENGTH]

            if frame[:2] != self.FRAME_HEADER:
                continue

            self.parse_frame(frame)

    # =========================================================
    # FRAME DECODER
    # =========================================================

    def parse_frame(self, frame):

        try:

            values = struct.unpack(
                '<9h',
                frame[2:20]
            )

            # -------------------------------------------------
            # ACCELEROMETER
            # -------------------------------------------------

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

            # -------------------------------------------------
            # GYROSCOPE RAW CONVERSION
            # -------------------------------------------------

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

            gyro_deg = [
                gx_deg,
                gy_deg,
                gz_deg
            ]

            # -------------------------------------------------
            # EULER
            # -------------------------------------------------

            roll = (
                values[6] / 32768.0
            ) * self.EULER_SCALE

            pitch = (
                values[7] / 32768.0
            ) * self.EULER_SCALE

            yaw = (
                values[8] / 32768.0
            ) * self.EULER_SCALE

            # -------------------------------------------------
            # STARTUP CALIBRATION
            # -------------------------------------------------

            if not self.calibration_completed:

                self.process_calibration(
                    ax, ay, az,
                    gx_deg, gy_deg, gz_deg
                )

                self.publish_calibration_status()

                return

            # -------------------------------------------------
            # APPLY GYRO BIAS
            # -------------------------------------------------

            gx = math.radians(
                gx_deg - self.gyro_bias[0]
            )

            gy = math.radians(
                gy_deg - self.gyro_bias[1]
            )

            gz = math.radians(
                gz_deg - self.gyro_bias[2]
            )

            # -------------------------------------------------
            # TIMESTAMP
            # -------------------------------------------------

            timestamp = datetime.now(
                timezone.utc
            ).astimezone().isoformat(
                timespec='milliseconds'
            )

            # -------------------------------------------------
            # PUBLISH ROS
            # -------------------------------------------------

            self.publish_imu(
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            self.publish_euler(
                roll, pitch, yaw
            )

            # -------------------------------------------------
            # PUBLISH JSON
            # -------------------------------------------------

            self.publish_json(
                timestamp,
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            # -------------------------------------------------
            # CSV
            # -------------------------------------------------

            self.write_csv(
                timestamp,
                ax, ay, az,
                gx, gy, gz,
                roll, pitch, yaw
            )

            self.frame_count += 1

        except (struct.error, ValueError, OverflowError) as error:

            self.error_count += 1

            self.get_logger().warning(
                f'Frame parsing error: {error}'
            )

    # =========================================================
    # CALIBRATION STABILITY CHECK
    # =========================================================

    def is_sensor_still(
        self,
        ax, ay, az,
        gx_deg, gy_deg, gz_deg
    ):

        # Gyroscope movement threshold
        gyro_stable = (
            abs(gx_deg) <= self.gyro_motion_threshold
            and abs(gy_deg) <= self.gyro_motion_threshold
            and abs(gz_deg) <= self.gyro_motion_threshold
        )

        # Accelerometer magnitude should remain near gravity
        accel_magnitude = math.sqrt(
            ax * ax +
            ay * ay +
            az * az
        )

        accel_stable = (
            abs(accel_magnitude - self.GRAVITY)
            <= self.accel_tolerance
        )

        return gyro_stable and accel_stable

    # =========================================================
    # CALIBRATION RESET
    # =========================================================

    def reset_calibration_window(self):

        self.calibration_start_time = None

        self.gyro_sum = [0.0, 0.0, 0.0]
        self.gyro_sample_count = 0

        self.calibration_progress = 0.0

    # =========================================================
    # AUTO CALIBRATION
    # =========================================================

    def process_calibration(
        self,
        ax, ay, az,
        gx_deg, gy_deg, gz_deg
    ):

        if self.calibration_state == 'waiting_for_sensor':

            self.calibration_state = 'calibrating'

            self.get_logger().info(
                'Sensor detected. Starting auto-calibration.'
            )

        # Check whether the sensor is stationary
        if not self.is_sensor_still(
            ax, ay, az,
            gx_deg, gy_deg, gz_deg
        ):

            if self.calibration_start_time is not None:

                self.get_logger().warning(
                    'Movement detected. Restarting calibration timer.'
                )

            self.reset_calibration_window()

            self.calibration_state = 'calibrating'

            return

        # Start stable calibration window
        if self.calibration_start_time is None:

            self.calibration_start_time = time.monotonic()

            self.gyro_sum = [0.0, 0.0, 0.0]
            self.gyro_sample_count = 0

            self.get_logger().info(
                'Sensor stable. Calibration timer started.'
            )

        # Accumulate gyro samples in degrees/second
        self.gyro_sum[0] += gx_deg
        self.gyro_sum[1] += gy_deg
        self.gyro_sum[2] += gz_deg

        self.gyro_sample_count += 1

        elapsed = (
            time.monotonic() -
            self.calibration_start_time
        )

        self.calibration_progress = min(
            100.0,
            (elapsed / self.calibration_duration) * 100.0
        )

        # Wait for complete calibration duration
        if elapsed < self.calibration_duration:
            return

        # Require enough samples before accepting calibration
        if self.gyro_sample_count < 100:

            self.get_logger().warning(
                'Not enough calibration samples. Restarting.'
            )

            self.reset_calibration_window()
            return

        # Calculate average gyro bias
        self.gyro_bias = [
            value / self.gyro_sample_count
            for value in self.gyro_sum
        ]

        self.calibration_completed = True
        self.calibration_state = 'ready'
        self.calibration_progress = 100.0

        self.save_calibration()

        self.get_logger().info(
            '========================================'
        )

        self.get_logger().info(
            'WT901 AUTO-CALIBRATION COMPLETED'
        )

        self.get_logger().info(
            f'Gyro bias X: {self.gyro_bias[0]:.5f} deg/s'
        )

        self.get_logger().info(
            f'Gyro bias Y: {self.gyro_bias[1]:.5f} deg/s'
        )

        self.get_logger().info(
            f'Gyro bias Z: {self.gyro_bias[2]:.5f} deg/s'
        )

        self.get_logger().info(
            'Telemetry is now enabled.'
        )

        self.get_logger().info(
            '========================================'
        )

    # =========================================================
    # SAVE CALIBRATION
    # =========================================================

    def save_calibration(self):

        data = {
            'source': 'witmotion_wt901sdcl_bt50',
            'timestamp': datetime.now(
                timezone.utc
            ).isoformat(timespec='seconds'),
            'calibration_type': 'startup_gyro_bias',
            'duration_seconds': self.calibration_duration,
            'sample_count': self.gyro_sample_count,
            'gyro_bias_deg_s': {
                'x': self.gyro_bias[0],
                'y': self.gyro_bias[1],
                'z': self.gyro_bias[2]
            }
        }

        temp_path = self.calibration_path + '.tmp'

        try:

            with open(temp_path, 'w') as file:

                json.dump(
                    data,
                    file,
                    indent=4
                )

                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temp_path,
                self.calibration_path
            )

            self.get_logger().info(
                f'Calibration saved: {self.calibration_path}'
            )

        except OSError as error:

            self.get_logger().error(
                f'Cannot save calibration: {error}'
            )

    # =========================================================
    # CALIBRATION STATUS JSON
    # =========================================================

    def publish_calibration_status(self):

        now = time.monotonic()

        # Publish progress once per second
        if now - self.last_status_publish < 1.0:
            return

        self.last_status_publish = now

        if self.calibration_completed:
            return

        elapsed = 0.0

        if self.calibration_start_time is not None:

            elapsed = (
                now - self.calibration_start_time
            )

        remaining = max(
            0.0,
            self.calibration_duration - elapsed
        )

        payload = {
            'source': 'witmotion_wt901sdcl_bt50',
            'timestamp': datetime.now(
                timezone.utc
            ).astimezone().isoformat(
                timespec='milliseconds'
            ),
            'status': 'calibrating',
            'calibration': {
                'completed': False,
                'progress': round(
                    self.calibration_progress,
                    1
                ),
                'duration_seconds': self.calibration_duration,
                'elapsed_seconds': round(elapsed, 1),
                'remaining_seconds': round(remaining, 1),
                'sample_count': self.gyro_sample_count,
                'gyro': 'sampling',
                'instruction': 'Keep mannequin stationary'
            }
        }

        msg = String()

        msg.data = json.dumps(
            payload,
            separators=(',', ':')
        )

        self.json_pub.publish(msg)

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

        msg.x = float(roll)
        msg.y = float(pitch)
        msg.z = float(yaw)

        self.euler_pub.publish(msg)

    # =========================================================
    # JSON TELEMETRY
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
            'status': 'ready',
            'field_count': 9,
            'calibration': {
                'completed': True,
                'type': 'startup_gyro_bias'
            },
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
            'Stopping WT901 node...'
        )

        if hasattr(self, 'csv_file'):

            if not self.csv_file.closed:

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
