
#!/usr/bin/env python3

import json

import rclpy

from rclpy.node import Node
from std_msgs.msg import String

from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.i2c.mcp9808_manager import MCP9808Manager


class MCP9808Node(Node):

    def __init__(self):

        super().__init__("mcp9808_node")

        self.declare_parameter(
            "bus_number",
            1
        )

        self.declare_parameter(
            "publish_rate_hz",
            1.0
        )

        self.declare_parameter(
            "topic",
            "/sensors/mcp9808"
        )

        bus_number = self.get_parameter(
            "bus_number"
        ).value

        publish_rate = self.get_parameter(
            "publish_rate_hz"
        ).value

        topic = self.get_parameter(
            "topic"
        ).value

        if publish_rate <= 0:
            raise ValueError(
                "publish_rate_hz harus lebih dari 0"
            )

        self.i2c = None
        self.timer = None

        try:

            # ================================================
            # I2C MANAGER
            # ================================================

            self.i2c = I2CManager(
                bus_number=bus_number
            )

            # ================================================
            # REGISTER TCA9548A
            # ================================================

            self.i2c.register_mux(
                "tca_1",
                0x70
            )

            self.i2c.register_mux(
                "tca_2",
                0x71
            )

            # ================================================
            # MCP9808 MANAGER
            # ================================================

            self.mcp = MCP9808Manager(
                self.i2c,
                address=0x18
            )

            # ================================================
            # ROS PUBLISHER
            # ================================================

            self.publisher = self.create_publisher(
                String,
                topic,
                10
            )

            # ================================================
            # TIMER
            # ================================================

            self.timer = self.create_timer(
                1.0 / publish_rate,
                self.acquire_data
            )

            self.get_logger().info(
                "MCP9808 node initialized."
            )

            self.get_logger().info(
                f"I2C bus: {bus_number}"
            )

            self.get_logger().info(
                "TCA9548A: 0x70 and 0x71"
            )

            self.get_logger().info(
                "Sensors: 14 MCP9808"
            )

            self.get_logger().info(
                f"Publishing: {topic}"
            )

        except Exception:

            self.close_hardware()
            raise

    # ========================================================
    # ACQUIRE AND PUBLISH
    # ========================================================

    def acquire_data(self):

        try:

            values = self.mcp.read_all()

            message = String()

            message.data = json.dumps(
                values,
                separators=(",", ":"),
                allow_nan=False
            )

            self.publisher.publish(message)

            valid_count = sum(
                value is not None
                for value in values
            )

            self.get_logger().info(
                f"MCP9808: {valid_count}/14 valid readings"
            )

        except Exception as error:

            self.get_logger().error(
                f"MCP9808 acquisition failed: {error}"
            )

    # ========================================================
    # CLEANUP
    # ========================================================

    def close_hardware(self):

        if self.i2c is not None:

            try:
                self.i2c.close()
            except Exception:
                pass

            self.i2c = None

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

        node = MCP9808Node()

        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:

        print(
            f"MCP9808 node failed: {error}"
        )

    finally:

        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
