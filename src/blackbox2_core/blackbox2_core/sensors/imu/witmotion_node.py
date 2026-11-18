
import rclpy
from rclpy.node import Node

from sensor_msgs.msg import Imu
from geometry_msgs.msg import Vector3

import serial
import struct
import math


class WitMotionModbusNode(Node):

    def __init__(self):
        super().__init__('witmotion_hwt901b_node')

        # ==========================================================
        # 1. PARAMETERS
        # ==========================================================
        self.declare_parameter('port', '/dev/ttyACM0')
        self.declare_parameter('baudrate', 115200)
        self.declare_parameter('polling_rate', 20.0)
        self.declare_parameter('serial_timeout', 0.01)

        self.port = self.get_parameter('port').value
        self.baudrate = self.get_parameter('baudrate').value
        self.polling_rate = self.get_parameter('polling_rate').value
        self.serial_timeout = self.get_parameter('serial_timeout').value

        # ==========================================================
        # 2. MODBUS CONFIGURATION
        # ==========================================================
        self.slave_address = 0x50
        self.function_code = 0x03
        self.start_register = 0x0034
        self.register_count = 12

        self.expected_byte_count = self.register_count * 2
        self.expected_frame_length = 5 + self.expected_byte_count

        self.modbus_cmd = bytearray([
            self.slave_address,
            self.function_code,
            (self.start_register >> 8) & 0xFF,
            self.start_register & 0xFF,
            (self.register_count >> 8) & 0xFF,
            self.register_count & 0xFF
        ])

        self.modbus_cmd += self.calculate_crc(self.modbus_cmd)

        # Buffer untuk menerima frame serial
        self.rx_buffer = bytearray()

        self.serial_connected = False
        self.request_count = 0
        self.valid_frame_count = 0
        self.invalid_frame_count = 0

        # ==========================================================
        # 3. SERIAL CONNECTION
        # ==========================================================
        self.ser = None

        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.serial_timeout
            )

            self.ser.dtr = True
            self.ser.rts = True

            self.serial_connected = True

            self.get_logger().info(
                f"WitMotion terhubung: {self.port} "
                f"({self.baudrate} baud)"
            )

        except serial.SerialException as e:
            self.get_logger().error(
                f"Gagal membuka serial {self.port}: {e}"
            )
            raise SystemExit

        # ==========================================================
        # 4. ROS PUBLISHERS
        # ==========================================================
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

        # ==========================================================
        # 5. TIMER
        # ==========================================================
        if self.polling_rate <= 0:
            self.get_logger().error(
                "polling_rate harus lebih besar dari 0"
            )
            raise ValueError("Invalid polling_rate")

        timer_period = 1.0 / self.polling_rate

        self.timer = self.create_timer(
            timer_period,
            self.polling_callback
        )

        self.get_logger().info(
            f"Polling WitMotion: {self.polling_rate:.1f} Hz"
        )

    # ==============================================================
    # MODBUS CRC16
    # ==============================================================
    @staticmethod
    def calculate_crc(data):
        crc = 0xFFFF

        for byte in data:
            crc ^= byte

            for _ in range(8):
                if crc & 0x0001:
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1

        return crc.to_bytes(2, byteorder='little')

    # ==============================================================
    # CRC VALIDATION
    # ==============================================================
    def validate_crc(self, frame):
        if len(frame) < 3:
            return False

        calculated_crc = self.calculate_crc(frame[:-2])
        received_crc = frame[-2:]

        return calculated_crc == received_crc

    # ==============================================================
    # SERIAL POLLING
    # ==============================================================
    def polling_callback(self):

        if self.ser is None or not self.ser.is_open:
            return

        try:
            # ------------------------------------------------------
            # Kirim request Modbus
            # ------------------------------------------------------
            self.ser.write(self.modbus_cmd)
            self.request_count += 1

            # ------------------------------------------------------
            # Baca data serial yang tersedia
            # ------------------------------------------------------
            available = self.ser.in_waiting

            if available > 0:
                incoming = self.ser.read(available)
                self.rx_buffer.extend(incoming)

            # ------------------------------------------------------
            # Proses buffer
            # ------------------------------------------------------
            self.process_rx_buffer()

        except serial.SerialException as e:
            self.get_logger().error(
                f"Serial error WitMotion: {e}"
            )

        except Exception as e:
            self.get_logger().error(
                f"Error polling WitMotion: {e}"
            )

    # ==============================================================
    # FRAME BUFFER PROCESSOR
    # ==============================================================
    def process_rx_buffer(self):

        while len(self.rx_buffer) >= 5:

            # ------------------------------------------------------
            # Sinkronisasi slave address
            # ------------------------------------------------------
            if self.rx_buffer[0] != self.slave_address:
                del self.rx_buffer[0]
                continue

            # ------------------------------------------------------
            # Validasi function code
            # ------------------------------------------------------
            if self.rx_buffer[1] != self.function_code:

                # Modbus exception response
                if self.rx_buffer[1] & 0x80:
                    self.get_logger().warning(
                        f"Modbus exception: "
                        f"{self.rx_buffer[2]:02X}"
                    )

                del self.rx_buffer[0]
                continue

            # ------------------------------------------------------
            # Byte count
            # ------------------------------------------------------
            byte_count = self.rx_buffer[2]

            if byte_count != self.expected_byte_count:
                del self.rx_buffer[0]
                continue

            frame_length = 3 + byte_count + 2

            # Tunggu sampai frame lengkap
            if len(self.rx_buffer) < frame_length:
                return

            frame = bytes(self.rx_buffer[:frame_length])

            # Hapus frame dari buffer
            del self.rx_buffer[:frame_length]

            # ------------------------------------------------------
            # CRC validation
            # ------------------------------------------------------
            if not self.validate_crc(frame):
                self.invalid_frame_count += 1

                self.get_logger().warning(
                    "CRC response WitMotion tidak valid"
                )
                continue

            self.valid_frame_count += 1

            # ------------------------------------------------------
            # Parsing dan publish
            # ------------------------------------------------------
            self.parsing_and_publish(frame)

    # ==============================================================
    # EULER TO QUATERNION
    # ==============================================================
    @staticmethod
    def euler_to_quaternion(roll_deg, pitch_deg, yaw_deg):

        roll = math.radians(roll_deg)
        pitch = math.radians(pitch_deg)
        yaw = math.radians(yaw_deg)

        cy = math.cos(yaw * 0.5)
        sy = math.sin(yaw * 0.5)

        cp = math.cos(pitch * 0.5)
        sp = math.sin(pitch * 0.5)

        cr = math.cos(roll * 0.5)
        sr = math.sin(roll * 0.5)

        qw = cy * cp * cr + sy * sp * sr
        qx = cy * cp * sr - sy * sp * cr
        qy = sy * cp * sr + cy * sp * cr
        qz = sy * cp * cr - cy * sp * sr

        return qw, qx, qy, qz

    # ==============================================================
    # PARSING & PUBLISH
    # ==============================================================
    def parsing_and_publish(self, frame):

        try:
            # 12 signed 16-bit registers
            registers = struct.unpack(
                '>12h',
                frame[3:27]
            )

        except struct.error:
            self.invalid_frame_count += 1

            self.get_logger().warning(
                "Payload WitMotion tidak valid"
            )
            return

        # ==========================================================
        # ACCELEROMETER
        # Range ±16 g
        # ==========================================================
        ax = registers[0] / 32768.0 * 16.0
        ay = registers[1] / 32768.0 * 16.0
        az = registers[2] / 32768.0 * 16.0

        # ==========================================================
        # GYROSCOPE
        # Range ±2000 degree/s
        # ==========================================================
        gx = registers[3] / 32768.0 * 2000.0
        gy = registers[4] / 32768.0 * 2000.0
        gz = registers[5] / 32768.0 * 2000.0

        # ==========================================================
        # EULER ANGLE
        # ==========================================================
        roll = registers[9] / 32768.0 * 180.0
        pitch = registers[10] / 32768.0 * 180.0
        yaw = registers[11] / 32768.0 * 180.0

        # ==========================================================
        # ROS IMU MESSAGE
        # ==========================================================
        imu_msg = Imu()

        imu_msg.header.stamp = self.get_clock().now().to_msg()
        imu_msg.header.frame_id = 'imu_link'

        # ----------------------------------------------------------
        # Acceleration: g -> m/s²
        # ----------------------------------------------------------
        imu_msg.linear_acceleration.x = ax * 9.80665
        imu_msg.linear_acceleration.y = ay * 9.80665
        imu_msg.linear_acceleration.z = az * 9.80665

        # ----------------------------------------------------------
        # Gyroscope: degree/s -> rad/s
        # ----------------------------------------------------------
        imu_msg.angular_velocity.x = math.radians(gx)
        imu_msg.angular_velocity.y = math.radians(gy)
        imu_msg.angular_velocity.z = math.radians(gz)

        # ----------------------------------------------------------
        # Orientation: Euler -> Quaternion
        # ----------------------------------------------------------
        qw, qx, qy, qz = self.euler_to_quaternion(
            roll,
            pitch,
            yaw
        )

        imu_msg.orientation.w = qw
        imu_msg.orientation.x = qx
        imu_msg.orientation.y = qy
        imu_msg.orientation.z = qz

        # Covariance belum dikalibrasi
        # Jangan mengarang covariance sensor.
        # ----------------------------------------------------------

        self.imu_pub.publish(imu_msg)

        # ==========================================================
        # EULER MESSAGE
        # ==========================================================
        euler_msg = Vector3()

        euler_msg.x = roll
        euler_msg.y = pitch
        euler_msg.z = yaw

        self.euler_pub.publish(euler_msg)

    # ==============================================================
    # NODE CLEANUP
    # ==============================================================
    def destroy_node(self):

        try:
            if self.ser is not None and self.ser.is_open:
                self.ser.close()

                self.get_logger().info(
                    "Serial WitMotion ditutup"
                )

        except Exception as e:
            self.get_logger().warning(
                f"Error saat menutup serial: {e}"
            )

        super().destroy_node()


# ==================================================================
# MAIN
# ==================================================================
def main(args=None):

    rclpy.init(args=args)

    node = None

    try:
        node = WitMotionModbusNode()
        rclpy.spin(node)

    except KeyboardInterrupt:
        if node is not None:
            node.get_logger().info(
                "Menghentikan WitMotion node..."
            )

    finally:
        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
