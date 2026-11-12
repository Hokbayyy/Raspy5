
#!/usr/bin/env python3

import json
import math
import time
import rclpy

from rclpy.node import Node
from std_msgs.msg import String


class DummyLoRaTest(Node):

    def __init__(self):
        super().__init__('dummy_lora_test')

        self.pub_arduino = self.create_publisher(
            String,
            '/telemetri/arduino',
            10
        )

        self.pub_aurora = self.create_publisher(
            String,
            '/telemetri/aurora',
            10
        )

        self.seq = 0
        self.start_time = time.monotonic()

        self.timer = self.create_timer(1.0, self.publish_dummy)

        self.get_logger().info(
            'DUMMY LORA TEST STARTED - 35 PARAMETERS'
        )

    def generate_arduino(self, t):

        # 23 parameter Arduino UNO Q
        return {
            "lidar_raw": round(1200 + 50 * math.sin(t), 2),

            "mq_nh3": round(120 + 5 * math.sin(t), 2),
            "mq_co": round(85 + 4 * math.cos(t), 2),
            "mq_co2": round(420 + 10 * math.sin(t), 2),

            "smoke": 0,

            "mpu_acc_x_raw": round(0.12 * math.sin(t), 3),
            "mpu_acc_y_raw": round(0.15 * math.cos(t), 3),
            "mpu_acc_z_raw": round(9.81 + 0.05 * math.sin(t), 3),

            "mpu_gyro_x_raw": round(0.5 * math.sin(t), 3),
            "mpu_gyro_y_raw": round(0.4 * math.cos(t), 3),
            "mpu_gyro_z_raw": round(0.3 * math.sin(t), 3),

            "mpu_acc_x_kalman": round(0.10 * math.sin(t), 3),
            "mpu_acc_y_kalman": round(0.13 * math.cos(t), 3),
            "mpu_acc_z_kalman": round(9.80 + 0.03 * math.sin(t), 3),

            "mpu_gyro_x_kalman": round(0.45 * math.sin(t), 3),
            "mpu_gyro_y_kalman": round(0.35 * math.cos(t), 3),
            "mpu_gyro_z_kalman": round(0.25 * math.sin(t), 3),

            "adxl_acc_x_raw": round(0.20 * math.sin(t), 3),
            "adxl_acc_y_raw": round(0.18 * math.cos(t), 3),
            "adxl_acc_z_raw": round(9.79 + 0.04 * math.sin(t), 3),

            "adxl_acc_x_kalman": round(0.18 * math.sin(t), 3),
            "adxl_acc_y_kalman": round(0.16 * math.cos(t), 3),
            "adxl_acc_z_kalman": round(9.80 + 0.02 * math.sin(t), 3)
        }

    def generate_aurora(self, t):

        # 12 parameter Cosmic Aurora V3
        return {
            "accelx": round(0.25 * math.sin(t), 3),
            "accely": round(0.20 * math.cos(t), 3),
            "accelz": round(9.81 + 0.03 * math.sin(t), 3),

            "gyrox": round(0.5 * math.sin(t), 3),
            "gyroy": round(0.4 * math.cos(t), 3),
            "gyroz": round(0.3 * math.sin(t), 3),

            "bme_temp": round(29.5 + 0.5 * math.sin(t), 2),
            "bme_hum": round(65.0 + 2.0 * math.cos(t), 2),
            "bme_pres": round(1008.5 + 0.3 * math.sin(t), 2),
            "bme_alt": round(38.0 + 0.5 * math.cos(t), 2),

            "sound_dbLeft": round(42.0 + 3.0 * math.sin(t), 2),
            "sound_dbRight": round(43.0 + 3.0 * math.cos(t), 2)
        }

    def publish_dummy(self):

        self.seq += 1
        t = time.monotonic() - self.start_time

        arduino = self.generate_arduino(t)
        aurora = self.generate_aurora(t)

        msg_arduino = String()
        msg_arduino.data = json.dumps(
            arduino,
            separators=(',', ':')
        )

        msg_aurora = String()
        msg_aurora.data = json.dumps(
            aurora,
            separators=(',', ':')
        )

        self.pub_arduino.publish(msg_arduino)
        self.pub_aurora.publish(msg_aurora)

        self.get_logger().info(
            f'DUMMY #{self.seq} | '
            f'Arduino: {len(arduino)}/23 | '
            f'Aurora: {len(aurora)}/12 | '
            f'Total: {len(arduino) + len(aurora)}/35'
        )


def main(args=None):

    rclpy.init(args=args)
    node = DummyLoRaTest()

    try:
        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
