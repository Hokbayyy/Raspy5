
#!/usr/bin/env python3

import json
import math
import time
from pathlib import Path

import lgpio
import rclpy

from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# KALMAN FILTER
# ============================================================

class KalmanFilter:

    def __init__(self, q=0.01, r=0.1):

        self.q = q
        self.r = r

        self.x = None
        self.p = 1.0

    def update(self, measurement):

        if self.x is None:
            self.x = measurement
            return self.x

        self.p += self.q

        k = self.p / (self.p + self.r)

        self.x += k * (measurement - self.x)

        self.p *= (1.0 - k)

        return self.x


# ============================================================
# HX711 DRIVER
# ============================================================

class HX711:

    def __init__(
        self,
        handle,
        dt_pin=20,
        sck_pin=21,
        read_timeout=1.0
    ):

        self.handle = handle

        self.dt_pin = dt_pin
        self.sck_pin = sck_pin

        self.read_timeout = read_timeout

        self.dt_claimed = False
        self.sck_claimed = False

        try:

            lgpio.gpio_claim_input(
                self.handle,
                self.dt_pin
            )

            self.dt_claimed = True

            lgpio.gpio_claim_output(
                self.handle,
                self.sck_pin,
                0
            )

            self.sck_claimed = True

        except Exception:

            self.close()
            raise

    def read_raw(self):

        start_time = time.monotonic()

        # ----------------------------------------------------
        # WAIT DATA READY
        # ----------------------------------------------------

        while lgpio.gpio_read(
            self.handle,
            self.dt_pin
        ) == 1:

            if (
                time.monotonic() - start_time
                > self.read_timeout
            ):

                raise TimeoutError(
                    "HX711 timeout: DT tetap HIGH"
                )

            time.sleep(0.0001)

        value = 0

        try:

            # ------------------------------------------------
            # READ 24-BIT DATA
            # ------------------------------------------------

            for _ in range(24):

                lgpio.gpio_write(
                    self.handle,
                    self.sck_pin,
                    1
                )

                time.sleep(0.000002)

                bit = lgpio.gpio_read(
                    self.handle,
                    self.dt_pin
                )

                value = (value << 1) | bit

                lgpio.gpio_write(
                    self.handle,
                    self.sck_pin,
                    0
                )

                time.sleep(0.000002)

            # ------------------------------------------------
            # 25TH PULSE
            # CHANNEL A, GAIN 128
            # ------------------------------------------------

            lgpio.gpio_write(
                self.handle,
                self.sck_pin,
                1
            )

            time.sleep(0.000002)

            lgpio.gpio_write(
                self.handle,
                self.sck_pin,
                0
            )

            time.sleep(0.000002)

        finally:

            # Pastikan SCK tidak tertinggal HIGH.
            lgpio.gpio_write(
                self.handle,
                self.sck_pin,
                0
            )

        # ----------------------------------------------------
        # SIGNED 24-BIT CONVERSION
        # ----------------------------------------------------

        if value & 0x800000:
            value -= 0x1000000

        return value

    def close(self):

        if self.handle is None:
            return

        if self.sck_claimed:

            try:
                lgpio.gpio_write(
                    self.handle,
                    self.sck_pin,
                    0
                )

                lgpio.gpio_free(
                    self.handle,
                    self.sck_pin
                )

            except Exception:
                pass

            self.sck_claimed = False

        if self.dt_claimed:

            try:
                lgpio.gpio_free(
                    self.handle,
                    self.dt_pin
                )

            except Exception:
                pass

            self.dt_claimed = False


# ============================================================
# LOAD CELL ROS 2 NODE
# ============================================================

class HX711Node(Node):

    def __init__(self):

        super().__init__("hx711_node")

        # ----------------------------------------------------
        # PARAMETERS
        # ----------------------------------------------------

        self.declare_parameter(
            "gpiochip",
            0
        )

        self.declare_parameter(
            "dt_pin",
            20
        )

        self.declare_parameter(
            "sck_pin",
            21
        )

        self.declare_parameter(
            "read_timeout",
            1.0
        )

        self.declare_parameter(
            "publish_interval",
            0.05
        )

        self.declare_parameter(
            "kalman_q",
            0.01
        )

        self.declare_parameter(
            "kalman_r",
            0.1
        )

        self.declare_parameter(
            "sensor_id",
            "LC_TEST"
        )

        self.declare_parameter(
            "topic",
            "/sensors/loadcell"
        )

        default_calibration = str(
            Path.home()
            / "blackbox2_ws"
            / "src"
            / "blackbox2_core"
            / "config"
            / "loadcell_calibration.json"
        )

        self.declare_parameter(
            "calibration_file",
            default_calibration
        )

        # ----------------------------------------------------
        # READ PARAMETERS
        # ----------------------------------------------------

        gpiochip = self.get_parameter(
            "gpiochip"
        ).value

        dt_pin = self.get_parameter(
            "dt_pin"
        ).value

        sck_pin = self.get_parameter(
            "sck_pin"
        ).value

        read_timeout = self.get_parameter(
            "read_timeout"
        ).value

        interval = self.get_parameter(
            "publish_interval"
        ).value

        kalman_q = self.get_parameter(
            "kalman_q"
        ).value

        kalman_r = self.get_parameter(
            "kalman_r"
        ).value

        self.sensor_id = self.get_parameter(
            "sensor_id"
        ).value

        topic = self.get_parameter(
            "topic"
        ).value

        calibration_file = self.get_parameter(
            "calibration_file"
        ).value

        if interval <= 0:
            raise ValueError(
                "publish_interval harus lebih dari 0"
            )

        if read_timeout <= 0:
            raise ValueError(
                "read_timeout harus lebih dari 0"
            )

        if kalman_q <= 0 or kalman_r <= 0:
            raise ValueError(
                "Kalman Q dan R harus lebih dari 0"
            )

        self.offset = None
        self.scale = None

        self.handle = None
        self.hx711 = None

        self.filter = KalmanFilter(
            q=kalman_q,
            r=kalman_r
        )

        self.read_count = 0
        self.error_count = 0

        try:

            # ================================================
            # LOAD CALIBRATION
            # ================================================

            self.load_calibration(
                calibration_file
            )

            # ================================================
            # OPEN GPIO
            # ================================================

            self.handle = lgpio.gpiochip_open(
                gpiochip
            )

            # ================================================
            # INITIALIZE HX711
            # ================================================

            self.hx711 = HX711(
                handle=self.handle,
                dt_pin=dt_pin,
                sck_pin=sck_pin,
                read_timeout=read_timeout
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
                interval,
                self.acquire_data
            )

            self.get_logger().info(
                "HX711 node initialized."
            )

            self.get_logger().info(
                f"DT: GPIO{dt_pin}"
            )

            self.get_logger().info(
                f"SCK: GPIO{sck_pin}"
            )

            self.get_logger().info(
                f"Sensor: {self.sensor_id}"
            )

            self.get_logger().info(
                f"Calibration offset: {self.offset}"
            )

            self.get_logger().info(
                f"Calibration scale: {self.scale}"
            )

            self.get_logger().info(
                f"Publishing: {topic}"
            )

        except Exception:

            self.close_hardware()
            raise

    # ========================================================
    # LOAD CALIBRATION
    # ========================================================

    def load_calibration(self, filename):

        path = Path(filename).expanduser()

        if not path.is_file():

            raise FileNotFoundError(
                f"Calibration file tidak ditemukan: {path}"
            )

        with path.open("r") as file:

            calibration_data = json.load(file)

        sensors = calibration_data.get(
            "sensors",
            {}
        )

        if self.sensor_id not in sensors:

            raise ValueError(
                f"Kalibrasi {self.sensor_id} tidak ditemukan"
            )

        sensor_calibration = sensors[
            self.sensor_id
        ]

        self.offset = float(
            sensor_calibration["offset"]
        )

        self.scale = float(
            sensor_calibration["scale"]
        )

        if not math.isfinite(self.offset):
            raise ValueError("Offset tidak valid")

        if (
            not math.isfinite(self.scale)
            or self.scale == 0
        ):

            raise ValueError(
                "Scale tidak valid atau bernilai nol"
            )

    # ========================================================
    # ACQUIRE DATA
    # ========================================================

    def acquire_data(self):

        try:

            # Read raw HX711
            raw = self.hx711.read_raw()

            # Convert to gram
            weight = (
                raw - self.offset
            ) / self.scale

            if not math.isfinite(weight):

                raise ValueError(
                    "Hasil konversi berat tidak valid"
                )

            # Kalman filter
            filtered = self.filter.update(
                weight
            )

            self.read_count += 1

            # ================================================
            # JSON OUTPUT
            # ================================================

            payload = {

                "source": "hx711",

                "sensor_id": self.sensor_id,

                "data": {

                    "raw": raw,

                    "weight_gram": round(
                        weight,
                        2
                    ),

                    "weight_kalman_gram": round(
                        filtered,
                        2
                    )
                }
            }

            message = String()

            message.data = json.dumps(
                payload,
                separators=(",", ":"),
                allow_nan=False
            )

            self.publisher.publish(message)

            if self.read_count % 20 == 0:

                self.get_logger().info(
                    f"{self.sensor_id} | "
                    f"RAW: {raw} | "
                    f"Weight: {weight:.2f} g | "
                    f"Kalman: {filtered:.2f} g"
                )

        except TimeoutError as error:

            self.error_count += 1

            self.get_logger().warning(
                f"HX711 timeout: {error}"
            )

        except Exception as error:

            self.error_count += 1

            self.get_logger().error(
                f"HX711 acquisition failed: {error}"
            )

    # ========================================================
    # CLEANUP
    # ========================================================

    def close_hardware(self):

        if self.hx711 is not None:

            try:
                self.hx711.close()
            except Exception:
                pass

            self.hx711 = None

        if self.handle is not None:

            try:
                lgpio.gpiochip_close(
                    self.handle
                )
            except Exception:
                pass

            self.handle = None

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

        node = HX711Node()

        rclpy.spin(node)

    except KeyboardInterrupt:
        pass

    except Exception as error:

        print(
            f"HX711 node failed: {error}"
        )

    finally:

        if node is not None:
            node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
