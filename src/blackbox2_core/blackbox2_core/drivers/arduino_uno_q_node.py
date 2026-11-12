
import rclpy
import serial

from rclpy.node import Node
from std_msgs.msg import String


class ArduinoNode(Node):
    def __init__(self):
        super().__init__('arduino_uno_q_node')

        self.pub = self.create_publisher(
            String, '/telemetri/arduino', 10
        )

        self.ser = serial.Serial(
            '/dev/ttyAMA0', 115200, timeout=0
        )

        self.buf = bytearray()
        self.count = 0

        self.timer = self.create_timer(0.001, self.read_uart)

        self.get_logger().info('Arduino UART ready')

    def read_uart(self):
        n = self.ser.in_waiting
        if not n:
            return

        self.buf.extend(self.ser.read(n))

        while b'\n' in self.buf:
            line, _, rest = self.buf.partition(b'\n')
            self.buf = bytearray(rest)

            line = line.strip()
            if not line.startswith(b'<') or not line.endswith(b'>'):
                continue

            try:
                v = line[1:-1].decode('ascii').split(',')
                if len(v) != 8:
                    continue

                values = [float(x) for x in v]
                if not all(map(lambda x: abs(x) < float('inf'), values)):
                    continue

                msg = String()
                msg.data = line.decode('ascii')
                self.pub.publish(msg)
                self.count += 1

            except (ValueError, UnicodeDecodeError):
                continue

    def destroy_node(self):
        self.ser.close()
        super().destroy_node()


def main():
    rclpy.init()
    node = ArduinoNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
