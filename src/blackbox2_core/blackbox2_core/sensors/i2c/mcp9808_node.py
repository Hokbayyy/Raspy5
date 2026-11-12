import json

import rclpy
from rclpy.node import Node
from std_msgs.msg import String

from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.i2c.tca9548a_driver import TCA9548A
from blackbox2_core.sensors.i2c.mcp9808_manager import MCP9808Manager


class MCP9808Node(Node):

    def __init__(self):
        super().__init__("mcp9808_node")

        # ROS parameters
        self.declare_parameter("bus_number", 1)
        self.declare_parameter("publish_rate_hz", 1.0)
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

        topic = self.get_parameter("topic").value

        if publish_rate <= 0:
            raise ValueError("publish_rate_hz harus lebih dari 0")

        # I2C initialization
        self.i2c = I2CManager(bus_number=bus_number)

        # Register parallel TCA9548A
        self.tca_1 = TCA9548A(
            self.i2c,
            "tca_1",
            0x70
        )

        self.tca_2 = TCA9548A(
            self.i2c,
            "tca_2",
            0x71
        )

        # MCP9808 manager
        self.mcp = MCP9808Manager(self.i2c)

        # ROS publisher
        self.publisher = self.create_publisher(
            String,
            topic,
            10
        )

        # Acquisition timer
        self.timer = self.create_timer(
            1.0 / publish_rate,
            self.acquire_data
        )

        self.get_logger().info(
            "MCP9808 Node started"
        )

        self.get_logger().info(
            f"I2C bus: {bus_number}"
        )

        self.get_logger().info(
            f"Publishing: {topic}"
        )

        self.get_logger().info(
            f"Configured sensors: {len(self.mcp.SENSOR_CONFIG)}"
        )

    def acquire_data(self):
        try:
            values = self.mcp.read_all()

            message = String()

            # JSON compact: tanpa spasi tambahan
            message.data = json.dumps(
                values,
                separators=(",", ":"),
                allow_nan=False
            )

            self.publisher.publish(message)

            self.get_logger().info(
                f"MCP9808 published: {len(values)} values"
            )

        except Exception as error:
            self.get_logger().error(
                f"Acquisition error: {error}"
            )

    def destroy_node(self):
        if hasattr(self, "i2c"):
            self.i2c.close()

        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)

    node = None

    try:
        node = MCP9808Node()
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:
        print(f"MCP9808 Node failed: {error}")

    finally:
        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
