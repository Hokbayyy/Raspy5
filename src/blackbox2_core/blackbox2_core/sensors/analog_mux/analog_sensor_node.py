
#!/usr/bin/env python3

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
        super().__init__("analog_sensor_node")

        self.i2c = None
        self.mux = None

        self.publisher = None
        self.timer = None

        self.scan_count = 0
        self.failed_count = 0

        try:
            self.get_logger().info(
                "Initializing analog sensor acquisition..."
            )

            # ================================================
            # I2C MANAGER
            # ================================================

            self.i2c = I2CManager(
                bus_number=1
            )

            # ================================================
            # ADS1115
            # ================================================

            self.adc = ADS1115(
                i2c_manager=self.i2c,
                address=0x48,
                gain=1,
                data_rate=860
            )

            # ================================================
            # ANALOG MULTIPLEXER
            # ================================================

            self.mux = AnalogMuxDriver(
                s0=6,
                s1=13,
                s2=19,
                s3=26,
                gpiochip=4,
                settle_time=0.002
            )

            # ================================================
            # SENSOR MANAGER
            # ================================================

            self.sensor_manager = AnalogSensorManager(
                mux=self.mux,
                adc=self.adc
            )

            # ================================================
            # ROS PUBLISHER
            # ================================================

            self.publisher = self.create_publisher(
                String,
                "/sensors/analog_data",
                10
            )

            # ================================================
            # TIMER
            # ================================================

            self.timer = self.create_timer(
                1.0,
                self.publish_batch
            )

            self.get_logger().info(
                "Analog sensor node ready."
            )

            self.get_logger().info(
                "FSR: 14 channels | ADS A0"
            )

            self.get_logger().info(
                "Piezo: 14 channels | ADS A1"
            )

            self.get_logger().info(
                "Flex: 16 channels | ADS A2"
            )

            self.get_logger().info(
                "Publishing: /sensors/analog_data"
            )

        except Exception:
            self.close_hardware()
            raise

    # ========================================================
    # PUBLISH SENSOR DATA
    # ========================================================

    def publish_batch(self):

        try:

            data, elapsed = self.sensor_manager.read_all()

            payload = json.dumps(
                data,
                separators=(",", ":"),
                allow_nan=False
            )

            message = String()
            message.data = payload

            self.publisher.publish(message)

            self.scan_count += 1

            if self.scan_count % 10 == 0:

                self.get_logger().info(
                    f"Analog scans: {self.scan_count} | "
                    f"Scan time: {elapsed * 1000:.2f} ms"
                )

        except Exception as error:

            self.failed_count += 1

            self.get_logger().error(
                f"Analog acquisition failed: {error}"
            )

    # ========================================================
    # HARDWARE CLEANUP
    # ========================================================

    def close_hardware(self):

        if self.mux is not None:

            try:
                self.mux.close()
            except Exception:
                pass

            self.mux = None

        if self.i2c is not None:

            try:
                self.i2c.close()
            except Exception:
                pass

            self.i2c = None

    # ========================================================
    # NODE SHUTDOWN
    # ========================================================

    def destroy_node(self):

        self.close_hardware()

        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None):

    rclpy.init(args=args)

    node = None

    try:

        node = AnalogSensorNode()

        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:

        print(
            f"Analog sensor node failed: {error}"
        )

    finally:

        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
