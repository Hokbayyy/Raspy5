
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import serial
import time


class CosmicAuroraNode(Node):
    def __init__(self):
        super().__init__('cosmic_aurora_node')

        self.publisher_ = self.create_publisher(
            String,
            '/telemetri/aurora',
            10
        )

        self.port_name = '/dev/ttyAMA2'
        self.baudrate = 115200

        self.serial_port = None
        self.buffer = bytearray()

        self.last_retry = 0.0
        self.retry_interval = 3.0

        self.connect_serial()

        self.timer = self.create_timer(
            0.02,
            self.read_serial
        )

    def connect_serial(self):
        try:
            self.serial_port = serial.Serial(
                port=self.port_name,
                baudrate=self.baudrate,
                timeout=0,
                write_timeout=1
            )

            self.buffer.clear()

            self.get_logger().info(
                f'Aurora connected: {self.port_name} '
                f'@ {self.baudrate}'
            )

        except (serial.SerialException, OSError) as e:
            self.serial_port = None
            self.last_retry = time.monotonic()

            self.get_logger().error(
                f'Aurora connection failed: {e}'
            )

    def read_serial(self):
        if self.serial_port is None or not self.serial_port.is_open:
            now = time.monotonic()

            if now - self.last_retry >= self.retry_interval:
                self.connect_serial()

            return

        try:
            waiting = self.serial_port.in_waiting

            if waiting <= 0:
                return

            data = self.serial_port.read(waiting)

            if not data:
                return

            self.buffer.extend(data)

            # Pisahkan data berdasarkan newline.
            while b'\n' in self.buffer:
                raw_line, _, remaining = self.buffer.partition(b'\n')
                self.buffer = bytearray(remaining)

                raw_line = raw_line.strip(b'\r \t')

                if not raw_line:
                    continue

                decoded = raw_line.decode(
                    'utf-8',
                    errors='replace'
                )

                msg = String()
                msg.data = decoded

                self.publisher_.publish(msg)

                self.get_logger().info(
                    f'Aurora RX: {decoded}'
                )

            # Hindari buffer tumbuh tanpa batas jika tidak ada newline.
            if len(self.buffer) > 8192:
                self.get_logger().warning(
                    'Aurora buffer terlalu besar, reset buffer'
                )
                self.buffer.clear()

        except (serial.SerialException, OSError) as e:
            self.get_logger().error(
                f'Aurora serial disconnected: {e}'
            )

            try:
                self.serial_port.close()
            except Exception:
                pass

            self.serial_port = None
            self.buffer.clear()
            self.last_retry = time.monotonic()

    def destroy_node(self):
        if self.serial_port is not None:
            self.serial_port.close()

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
