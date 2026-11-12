
import json

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.adc.ads1115_driver import ADS1115
from blackbox2_core.sensors.analog_mux.analog_mux_driver import AnalogMuxDriver
from blackbox2_core.sensors.analog_mux.analog_sensor_manager import AnalogSensorManager


class AnalogSensorNode(Node):

    def __init__(self):
        super().__init__('analog_sensor_node')

        self.i2c = None
        self.mux = None

        try:
            self.get_logger().info('Initializing analog acquisition...')

            self.i2c = I2CManager(bus_number=1)

            self.adc = ADS1115(
                i2c_manager=self.i2c,
                address=0x48,
                gain=1,
                data_rate=860
            )

            self.mux = AnalogMuxDriver(
                s0=6,
                s1=13,
                s2=19,
                s3=26,
                gpiochip=4,
                settle_time=0.001
            )

            self.sensor_manager = AnalogSensorManager(
                self.mux,
                self.adc
            )

            self.publisher = self.create_publisher(
                String,
                '/sensors/analog_data',
                10
            )

            self.timer = self.create_timer(
                1.0,
                self.publish_batch
            )

            self.get_logger().info(
                'Analog sensor node ready. Publishing 44 channels at 1 Hz.'
            )

        except Exception:
            self.close_hardware()
            raise

    def publish_batch(self):

        try:
            data, elapsed = self.sensor_manager.read_all()

            payload = json.dumps(
                data,
                separators=(',', ':'),
                allow_nan=False
            )

            msg = String()
            msg.data = payload

            self.publisher.publish(msg)

            self.get_logger().info(
                f'Published 44 analog values | '
                f'Scan: {elapsed * 1000:.2f} ms | '
                f'Payload: {len(payload.encode("utf-8"))} bytes'
            )

        except Exception as error:
            self.get_logger().error(
                f'Analog acquisition failed: {error}'
            )

    def close_hardware(self):

        if self.mux is not None:
            self.mux.close()
            self.mux = None

        if self.i2c is not None:
            self.i2c.close()
            self.i2c = None

    def destroy_node(self):
        self.close_hardware()
        super().destroy_node()


def main(args=None):

    rclpy.init(args=args)
    node = None

    try:
        node = AnalogSensorNode()
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:
        print(f'Analog node error: {error}')

    finally:
        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
