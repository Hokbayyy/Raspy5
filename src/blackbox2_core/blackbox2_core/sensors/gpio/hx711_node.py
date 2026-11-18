
#!/usr/bin/env python3

import json
import time
import csv
from datetime import datetime
from pathlib import Path

import lgpio
import rclpy

from rclpy.node import Node
from std_msgs.msg import String
from std_srvs.srv import Trigger


# ============================================================
# CONFIGURATION
# ============================================================

GPIO_CHIP = 4

READ_TIMEOUT = 1.0
PUBLISH_INTERVAL = 0.05

GRAVITY = 9.80665

# AUTO-TARE
TARE_SAMPLES = 150
TARE_SETTLE_TIME = 0.5
TARE_SAMPLE_DELAY = 0.005

# Trimmed mean: buang 10% data ekstrem di masing-masing sisi
TARE_TRIM_PERCENT = 0.10

KALMAN_Q = 0.01
KALMAN_R = 0.1


# ============================================================
# LOAD CELL PIN CONFIGURATION (BCM)
# ============================================================

LOADCELLS = [
    ("loadcell_1", "leher",       17, 27),
    ("loadcell_2", "paha_kanan",  22, 23),
    ("loadcell_3", "paha_kiri",   24, 25),
    ("loadcell_4", "kaki_kanan",  12, 16),
    ("loadcell_5", "kaki_kiri",   20, 21),
]


# ============================================================
# PATH CONFIGURATION
# ============================================================

PACKAGE_DIR = (
    Path.home()
    / "blackbox2_ws"
    / "src"
    / "blackbox2_core"
)

CALIBRATION_FILE = (
    PACKAGE_DIR
    / "config"
    / "loadcell_calibration.json"
)

LOG_DIR = (
    Path.home()
    / "blackbox2_ws"
    / "logs"
    / "loadcell"
)

RAW_LOG_FILE = LOG_DIR / "loadcell_raw.csv"
KALMAN_LOG_FILE = LOG_DIR / "loadcell_kalman.csv"


# ============================================================
# KALMAN FILTER
# ============================================================

class KalmanFilter:

    def __init__(self, q=KALMAN_Q, r=KALMAN_R):

        self.q = q
        self.r = r

        self.x = None
        self.p = 1.0

    def update(self, measurement):

        if self.x is None:

            self.x = float(measurement)

            return self.x

        self.p += self.q

        k = self.p / (self.p + self.r)

        self.x += k * (measurement - self.x)

        self.p *= (1.0 - k)

        return self.x

    def reset(self):

        self.x = None
        self.p = 1.0


# ============================================================
# HX711 DRIVER
# ============================================================

class HX711:

    def __init__(self, gpio_handle, dt_pin, sck_pin):

        self.gpio_handle = gpio_handle
        self.dt_pin = dt_pin
        self.sck_pin = sck_pin

        lgpio.gpio_claim_output(
            self.gpio_handle,
            self.sck_pin,
            0
        )

        lgpio.gpio_claim_input(
            self.gpio_handle,
            self.dt_pin
        )

    def is_ready(self):

        return lgpio.gpio_read(
            self.gpio_handle,
            self.dt_pin
        ) == 0

    def read_raw(self, timeout=READ_TIMEOUT):

        start_time = time.monotonic()

        while not self.is_ready():

            if time.monotonic() - start_time > timeout:

                raise TimeoutError(
                    f"HX711 timeout DT GPIO {self.dt_pin}"
                )

            time.sleep(0.0001)

        value = 0

        # 24-bit reading
        for _ in range(24):

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                1
            )

            value <<= 1

            if lgpio.gpio_read(
                self.gpio_handle,
                self.dt_pin
            ):

                value |= 1

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                0
            )

        # Pulse 25: Channel A, gain 128
        lgpio.gpio_write(
            self.gpio_handle,
            self.sck_pin,
            1
        )

        lgpio.gpio_write(
            self.gpio_handle,
            self.sck_pin,
            0
        )

        # Signed 24-bit conversion
        if value & 0x800000:

            value -= 0x1000000

        return value

    def cleanup(self):

        try:

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                0
            )

        except Exception:

            pass


# ============================================================
# CALIBRATION
# ============================================================

def default_calibration():

    return {
        "sensors": {

            "loadcell_1": {
                "offset": 547198,
                "scale": 191.965
            },

            "loadcell_2": {
                "offset": 0,
                "scale": 1.0
            },

            "loadcell_3": {
                "offset": 0,
                "scale": 1.0
            },

            "loadcell_4": {
                "offset": 0,
                "scale": 1.0
            },

            "loadcell_5": {
                "offset": 0,
                "scale": 1.0
            }
        }
    }


def load_calibration():

    CALIBRATION_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if not CALIBRATION_FILE.exists():

        data = default_calibration()

        with open(
            CALIBRATION_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                indent=4
            )

        print(
            f"[INFO] Calibration file created: "
            f"{CALIBRATION_FILE}"
        )

        return data["sensors"]

    with open(
        CALIBRATION_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        data = json.load(f)

    sensors = data.get(
        "sensors",
        {}
    )

    for name, _, _, _ in LOADCELLS:

        if name not in sensors:

            sensors[name] = {
                "offset": 0,
                "scale": 1.0
            }

        if float(sensors[name]["scale"]) == 0:

            raise ValueError(
                f"Scale calibration for {name} is zero"
            )

    return sensors


# ============================================================
# CSV LOGGER
# ============================================================

def create_csv_header():

    header = ["timestamp"]

    for i in range(1, 6):

        header.extend([
            f"raw_{i}",
            f"gram_{i}",
            f"gram_kalman_{i}",
            f"newton_{i}",
            f"newton_kalman_{i}"
        ])

    return header


def initialize_csv(file_path):

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if not file_path.exists():

        with open(
            file_path,
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.writer(f)

            writer.writerow(
                create_csv_header()
            )


def append_csv(file_path, row):

    with open(
        file_path,
        "a",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(f)

        writer.writerow(row)


# ============================================================
# TARE UTILITIES
# ============================================================

def calculate_trimmed_mean(samples, trim_percent=0.10):

    if not samples:

        raise ValueError("No valid tare samples")

    sorted_samples = sorted(samples)

    trim_count = int(
        len(sorted_samples) * trim_percent
    )

    if trim_count > 0:

        sorted_samples = sorted_samples[
            trim_count:-trim_count
        ]

    if not sorted_samples:

        raise ValueError("Tare sample list is empty")

    return sum(sorted_samples) / len(sorted_samples)


# ============================================================
# ROS2 NODE
# ============================================================

class HX711Node(Node):

    def __init__(self):

        super().__init__("hx711_node")

        self.declare_parameter(
            "gpiochip",
            GPIO_CHIP
        )

        self.chip_number = int(
            self.get_parameter("gpiochip").value
        )

        self.gpio_handle = None

        self.loadcells = {}

        self.kalman_filters = {}

        self.calibration = load_calibration()

        # Active offset, initially loaded from calibration JSON.
        # Auto-tare will replace these values in RAM only.
        self.active_offsets = {}

        for name, _, _, _ in LOADCELLS:

            self.active_offsets[name] = float(
                self.calibration[name]["offset"]
            )

        initialize_csv(RAW_LOG_FILE)

        initialize_csv(KALMAN_LOG_FILE)

        # JSON publisher
        self.publisher = self.create_publisher(
            String,
            "/sensors/loadcell",
            10
        )

        self.initialize_gpio()

        # ----------------------------------------------------
        # AUTO TARE ON STARTUP
        # ----------------------------------------------------

        self.get_logger().info(
            "Starting automatic tare..."
        )

        time.sleep(TARE_SETTLE_TIME)

        self.perform_tare(
            samples=TARE_SAMPLES,
            startup=True
        )

        # ----------------------------------------------------
        # MANUAL TARE SERVICE
        # ----------------------------------------------------

        self.tare_service = self.create_service(
            Trigger,
            "/sensors/loadcell/tare",
            self.manual_tare_callback
        )

        # ----------------------------------------------------
        # PUBLISH TIMER
        # ----------------------------------------------------

        self.timer = self.create_timer(
            PUBLISH_INTERVAL,
            self.read_all_sensors
        )

        self.get_logger().info(
            "HX711 JSON node started"
        )

        self.get_logger().info(
            f"GPIO chip: {self.chip_number}"
        )

        self.get_logger().info(
            "Topic: /sensors/loadcell"
        )

        self.get_logger().info(
            "Manual tare service: /sensors/loadcell/tare"
        )


    # ========================================================
    # GPIO INIT
    # ========================================================

    def initialize_gpio(self):

        try:

            self.gpio_handle = lgpio.gpiochip_open(
                self.chip_number
            )

            for name, location, dt_pin, sck_pin in LOADCELLS:

                self.loadcells[name] = HX711(
                    self.gpio_handle,
                    dt_pin,
                    sck_pin
                )

                self.kalman_filters[name] = KalmanFilter()

                self.get_logger().info(
                    f"{name} | {location} | "
                    f"DT={dt_pin} | SCK={sck_pin}"
                )

        except Exception as e:

            self.get_logger().error(
                f"GPIO initialization error: {e}"
            )

            self.cleanup_gpio()

            raise


    # ========================================================
    # TARE
    # ========================================================

    def tare_sensor(self, name, sample_count):

        samples = []

        for i in range(sample_count):

            raw = self.loadcells[name].read_raw()

            samples.append(raw)

            if TARE_SAMPLE_DELAY > 0:

                time.sleep(TARE_SAMPLE_DELAY)

        new_offset = calculate_trimmed_mean(
            samples,
            TARE_TRIM_PERCENT
        )

        return new_offset, samples


    def perform_tare(self, samples=TARE_SAMPLES, startup=False):

        successful = []
        failed = []

        self.get_logger().info(
            f"Tare started: {samples} samples per sensor"
    )

        for name, location, _, _ in LOADCELLS:

            try:

                self.get_logger().info(
                    f"Taring {name} ({location})..."
                )

                new_offset, raw_samples = self.tare_sensor(
                    name,
                    samples
                )

                self.active_offsets[name] = new_offset

                # Reset Kalman so previous values do not affect
                # the newly calibrated zero point.
                self.kalman_filters[name].reset()

                successful.append(name)

                raw_min = min(raw_samples)
                raw_max = max(raw_samples)

                self.get_logger().info(
                    f"{name} tare OK | "
                    f"offset={new_offset:.2f} | "
                    f"RAW range={raw_min}..{raw_max}"
                )

            except Exception as e:

                failed.append(name)

                self.get_logger().error(
                    f"{name} tare failed: {e}"
            )

        if startup:

            self.get_logger().info(
                "Startup auto-tare completed"
            )

        return successful, failed


    # ========================================================
    # MANUAL TARE SERVICE
    # ========================================================

    def manual_tare_callback(self, request, response):

        self.get_logger().warn(
            "Manual tare requested. "
            "Ensure all loadcells are unloaded."
        )

        successful, failed = self.perform_tare(
            samples=TARE_SAMPLES,
            startup=False
        )

        if failed:

            response.success = False

            response.message = (
                "Tare completed partially. Successful: "
                + ", ".join(successful)
                + " | Failed: "
                + ", ".join(failed)
            )

        else:

            response.success = True

            response.message = (
                "Tare successful for all 5 loadcells. "
                "Offsets updated in RAM."
            )

        return response


    # ========================================================
    # RAW TO GRAM
    # ========================================================

    def raw_to_gram(self, raw, name):

        calibration = self.calibration[name]

        offset = self.active_offsets[name]

        scale = float(
            calibration["scale"]
        )

        return (raw - offset) / scale


    # ========================================================
    # GRAM TO NEWTON
    # ========================================================

    def gram_to_newton(self, gram):

        return (
            gram / 1000.0
        ) * GRAVITY


    # ========================================================
    # READ SENSOR
    # ========================================================

    def read_sensor(self, name):

        raw = self.loadcells[name].read_raw()

        gram = self.raw_to_gram(
            raw,
            name
        )

        gram_kalman = self.kalman_filters[name].update(
            gram
        )

        newton = self.gram_to_newton(
            gram
        )

        newton_kalman = self.gram_to_newton(
            gram_kalman
        )

        return [
            int(raw),
            int(round(gram)),
            int(round(gram_kalman)),
            round(newton, 4),
            round(newton_kalman, 4)
        ]


    # ========================================================
    # READ ALL 5 SENSORS
    # ========================================================

    def read_all_sensors(self):

        timestamp = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

        loadcell_data = []

        raw_csv_row = [timestamp]

        kalman_csv_row = [timestamp]

        for name, location, dt_pin, sck_pin in LOADCELLS:

            try:

                sensor_values = self.read_sensor(
                    name
                )

                loadcell_data.append(
                    sensor_values
                )

                raw_csv_row.extend(
                    sensor_values
                )

                kalman_csv_row.extend(
                    sensor_values
                )

            except Exception as e:

                self.get_logger().warning(
                    f"{name} read failed: {e}"
            )

                sensor_values = [
                    0,
                    0,
                    0,
                    0.0,
                    0.0
                ]

                loadcell_data.append(
                    sensor_values
                )

                raw_csv_row.extend(
                    sensor_values
                )

                kalman_csv_row.extend(
                    sensor_values
                )


        # ----------------------------------------------------
        # CREATE JSON
        # ----------------------------------------------------

        payload = {
            "loadcell": loadcell_data
        }

        json_output = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False
        )


        # ----------------------------------------------------
        # ROS PUBLISH
        # ----------------------------------------------------

        msg = String()

        msg.data = json_output

        self.publisher.publish(
            msg
        )


        # ----------------------------------------------------
        # CSV LOG
        # ----------------------------------------------------

        append_csv(
            RAW_LOG_FILE,
            raw_csv_row
        )

        append_csv(
            KALMAN_LOG_FILE,
            kalman_csv_row
        )


    # ========================================================
    # GPIO CLEANUP
    # ========================================================

    def cleanup_gpio(self):

        for sensor in self.loadcells.values():

            try:

                sensor.cleanup()

            except Exception:

                pass

        if self.gpio_handle is not None:

            try:

                lgpio.gpiochip_close(
                    self.gpio_handle
            )

            except Exception:

                pass

            self.gpio_handle = None


    # ========================================================
    # NODE CLEANUP
    # ========================================================

    def destroy_node(self):

        self.cleanup_gpio()

        return super().destroy_node()


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

        print(
            "\n[INFO] HX711 stopped"
        )

    except Exception as e:

        print(
            f"[ERROR] HX711 node failed: {e}"
        )

    finally:

        if node is not None:

            node.destroy_node()

        if rclpy.ok():

            rclpy.shutdown()


if __name__ == "__main__":

    main()
