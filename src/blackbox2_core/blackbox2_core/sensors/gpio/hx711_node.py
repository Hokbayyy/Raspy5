#!/usr/bin/env python3

import csv
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path

import lgpio
import rclpy
from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# KONFIGURASI LOAD CELL
# Format: (sensor_id, lokasi, GPIO DT, GPIO SCK)
# ============================================================

LOADCELLS = [
    ("loadcell_1", "leher",       17, 27),
    ("loadcell_2", "paha_kanan",  22, 23),
    ("loadcell_3", "paha_kiri",   24, 25),
    ("loadcell_4", "kaki_kanan",  12, 16),
    ("loadcell_5", "kaki_kiri",  20, 21),
]


# ============================================================
# KONFIGURASI GPIO
# ============================================================

GPIO_CHIP = 0


# ============================================================
# KONFIGURASI PEMBACAAN
# ============================================================

READ_TIMEOUT = 1.0
PUBLISH_INTERVAL = 0.05

KALMAN_Q = 0.01
KALMAN_R = 0.1

GRAVITY = 9.80665


# ============================================================
# FILE CONFIGURATION
# ============================================================

PACKAGE_CONFIG_DIR = (
    Path.home()
    / "blackbox2_ws"
    / "src"
    / "blackbox2_core"
    / "config"
)

CALIBRATION_FILE = PACKAGE_CONFIG_DIR / "loadcell_calibration.json"

LOG_DIR = Path.home() / "blackbox2_ws" / "logs" / "loadcell"

RAW_CSV = LOG_DIR / "loadcell_raw.csv"
KALMAN_CSV = LOG_DIR / "loadcell_kalman.csv"


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

    def reset(self):

        self.x = None
        self.p = 1.0


# ============================================================
# HX711 DRIVER
# ============================================================

class HX711:

    def __init__(
        self,
        gpio_handle,
        dt_pin,
        sck_pin,
        read_timeout=1.0
    ):

        self.gpio_handle = gpio_handle

        self.dt_pin = dt_pin
        self.sck_pin = sck_pin

        self.read_timeout = read_timeout

        self.initialized = False

        # DT input
        lgpio.gpio_claim_input(
            self.gpio_handle,
            self.dt_pin
        )

        # SCK output
        lgpio.gpio_claim_output(
            self.gpio_handle,
            self.sck_pin,
            0
        )

        self.initialized = True

    # --------------------------------------------------------
    # RAW READ
    # --------------------------------------------------------

    def read_raw(self):

        start_time = time.monotonic()

        # Tunggu HX711 ready (DT LOW)
        while lgpio.gpio_read(
            self.gpio_handle,
            self.dt_pin
        ) == 1:

            if time.monotonic() - start_time > self.read_timeout:

                raise TimeoutError(
                    "HX711 timeout: DT tetap HIGH"
                )

            time.sleep(0.0001)

        value = 0

        # Baca 24 bit
        for _ in range(24):

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                1
            )

            bit = lgpio.gpio_read(
                self.gpio_handle,
                self.dt_pin
            )

            value = (value << 1) | bit

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                0
            )

        # Pulsa ke-25:
        # Channel A, gain 128
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

        # Konversi signed 24-bit
        if value & 0x800000:

            value -= 0x1000000

        return value

    # --------------------------------------------------------
    # GPIO CLEANUP
    # --------------------------------------------------------

    def cleanup(self):

        if not self.initialized:
            return

        try:

            lgpio.gpio_write(
                self.gpio_handle,
                self.sck_pin,
                0
            )

            lgpio.gpio_free(
                self.gpio_handle,
                self.dt_pin
            )

            lgpio.gpio_free(
                self.gpio_handle,
                self.sck_pin
            )

        except Exception:

            pass

        self.initialized = False


# ============================================================
# CALIBRATION MANAGER
# ============================================================

def load_calibration_file():

    if not CALIBRATION_FILE.exists():

        raise FileNotFoundError(
            f"Calibration file tidak ditemukan: {CALIBRATION_FILE}"
        )

    with open(
        CALIBRATION_FILE,
        "r"
    ) as file:

        data = json.load(file)

    if "sensors" not in data:

        raise ValueError(
            "Format calibration harus memiliki key 'sensors'"
        )

    return data["sensors"]


# ============================================================
# CSV LOGGER
# ============================================================

def initialize_csv():

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    sensor_ids = [
        sensor[0]
        for sensor in LOADCELLS
    ]

    raw_header = ["timestamp"]

    for sensor_id in sensor_ids:

        raw_header.append(
            f"{sensor_id}_raw"
        )

    kalman_header = ["timestamp"]

    for sensor_id in sensor_ids:

        kalman_header.extend([
            f"{sensor_id}_weight_gram",
            f"{sensor_id}_weight_kalman_gram",
            f"{sensor_id}_force_newton",
            f"{sensor_id}_force_kalman_newton",
        ])

    # RAW CSV
    if not RAW_CSV.exists():

        with open(
            RAW_CSV,
            "w",
            newline=""
        ) as file:

            writer = csv.writer(file)

            writer.writerow(raw_header)

    # KALMAN CSV
    if not KALMAN_CSV.exists():

        with open(
            KALMAN_CSV,
            "w",
            newline=""
        ) as file:

            writer = csv.writer(file)

            writer.writerow(kalman_header)


def save_csv(filename, data):

    with open(
        filename,
        "a",
        newline=""
    ) as file:

        writer = csv.writer(file)

        writer.writerow(data)


# ============================================================
# HX711 ROS 2 NODE
# ============================================================

class HX711Node(Node):

    def __init__(self):

        super().__init__("hx711_node")

        # ----------------------------------------------------
        # ROS PARAMETERS
        # ----------------------------------------------------

        self.declare_parameter(
            "gpiochip",
            GPIO_CHIP
        )

        self.declare_parameter(
            "publish_interval",
            PUBLISH_INTERVAL
        )

        self.declare_parameter(
            "read_timeout",
            READ_TIMEOUT
        )

        self.declare_parameter(
            "kalman_q",
            KALMAN_Q
        )

        self.declare_parameter(
            "kalman_r",
            KALMAN_R
        )

        self.declare_parameter(
            "topic",
            "/sensors/loadcell"
        )

        # ----------------------------------------------------
        # PARAMETER VALUES
        # ----------------------------------------------------

        self.gpiochip = int(
            self.get_parameter(
                "gpiochip"
            ).value
        )

        self.publish_interval = float(
            self.get_parameter(
                "publish_interval"
            ).value
        )

        self.read_timeout = float(
            self.get_parameter(
                "read_timeout"
            ).value
        )

        self.kalman_q = float(
            self.get_parameter(
                "kalman_q"
            ).value
        )

        self.kalman_r = float(
            self.get_parameter(
                "kalman_r"
            ).value
        )

        self.topic = str(
            self.get_parameter(
                "topic"
            ).value
        )

        # ----------------------------------------------------
        # GPIO INITIALIZATION
        # ----------------------------------------------------

        self.gpio_handle = None

        self.hx711_modules = {}

        self.kalman_filters = {}

        self.calibration = {}

        # ----------------------------------------------------
        # LOAD CALIBRATION
        # ----------------------------------------------------

        try:

            self.calibration = load_calibration_file()

            self.get_logger().info(
                f"Calibration loaded: {CALIBRATION_FILE}"
            )

        except Exception as error:

            self.get_logger().error(
                f"Calibration error: {error}"
            )

            raise

        # ----------------------------------------------------
        # GPIO CHIP OPEN
        # ----------------------------------------------------

        try:

            self.gpio_handle = lgpio.gpiochip_open(
                self.gpiochip
            )

        except Exception as error:

            self.get_logger().error(
                f"Cannot open gpiochip{self.gpiochip}: {error}"
            )

            raise

        # ----------------------------------------------------
        # SENSOR INITIALIZATION
        # ----------------------------------------------------

        for sensor_id, location, dt_pin, sck_pin in LOADCELLS:

            try:

                self.hx711_modules[sensor_id] = HX711(
                    gpio_handle=self.gpio_handle,
                    dt_pin=dt_pin,
                    sck_pin=sck_pin,
                    read_timeout=self.read_timeout
                )

                self.kalman_filters[sensor_id] = KalmanFilter(
                    q=self.kalman_q,
                    r=self.kalman_r
                )

                if sensor_id in self.calibration:

                    offset = self.calibration[sensor_id].get(
                        "offset"
                    )

                    scale = self.calibration[sensor_id].get(
                        "scale"
                    )

                    if scale is None or scale == 0:

                        self.get_logger().error(
                            f"{sensor_id}: scale tidak valid"
                        )

                    else:

                        self.get_logger().info(
                            f"{sensor_id} initialized | "
                                f"location={location} | "
                                f"DT=GPIO{dt_pin} | "
                                f"SCK=GPIO{sck_pin} | "
                                f"offset={offset} | "
                                f"scale={scale}"
                        )

                else:

                    self.get_logger().warning(
                        f"{sensor_id}: calibration belum tersedia"
                    )

            except Exception as error:

                self.get_logger().error(
                    f"{sensor_id} initialization failed: {error}"
                )

        # ----------------------------------------------------
        # ROS PUBLISHER
        # ----------------------------------------------------

        self.publisher = self.create_publisher(
            String,
            self.topic,
            10
        )

        # ----------------------------------------------------
        # CSV
        # ----------------------------------------------------

        initialize_csv()

        # ----------------------------------------------------
        # TIMER
        # ----------------------------------------------------

        self.timer = self.create_timer(
            self.publish_interval,
            self.read_all_sensors
        )

        self.get_logger().info(
            "=================================================="
        )

        self.get_logger().info(
            "HX711 MULTI SENSOR NODE"
        )

        self.get_logger().info(
            f"GPIO CHIP: {self.gpiochip}"
        )

        self.get_logger().info(
            f"SENSORS: {len(LOADCELLS)}"
        )

        self.get_logger().info(
            f"TOPIC: {self.topic}"
        )

        self.get_logger().info(
            "=================================================="
        )

    # ========================================================
    # SENSOR READING
    # ========================================================

    def read_all_sensors(self):

        timestamp = datetime.now().astimezone().isoformat(
            timespec="milliseconds"
        )

        raw_csv_data = [timestamp]

        kalman_csv_data = [timestamp]

        sensor_data = {}

        for sensor_id, location, dt_pin, sck_pin in LOADCELLS:

            sensor_output = {
                "location": location,
                "raw": None,
                "weight_gram": None,
                "weight_kalman_gram": None,
                "force_newton": None,
                "force_kalman_newton": None,
                "status": "error"
            }

            raw_value = None

            # ------------------------------------------------
            # READ RAW
            # ------------------------------------------------

            try:

                if sensor_id not in self.hx711_modules:

                    raise RuntimeError(
                        "HX711 module tidak terinisialisasi"
                    )

                raw_value = self.hx711_modules[
                    sensor_id
                ].read_raw()

                sensor_output["raw"] = raw_value

                sensor_output["status"] = "raw_ok"

            except TimeoutError as error:

                self.get_logger().warning(
                    f"{sensor_id}: {error}"
                )

            except Exception as error:

                self.get_logger().error(
                    f"{sensor_id} read error: {error}"
                )

            raw_csv_data.append(
                raw_value
            )

            # ------------------------------------------------
            # CALIBRATION
            # ------------------------------------------------

            if raw_value is not None:

                if sensor_id not in self.calibration:

                    self.get_logger().warning(
                        f"{sensor_id}: calibration tidak tersedia"
                    )

                else:

                    try:

                        offset = float(
                            self.calibration[sensor_id]["offset"]
                        )

                        scale = float(
                            self.calibration[sensor_id]["scale"]
                        )

                        if scale == 0:

                            raise ValueError(
                                "Scale tidak boleh 0"
                            )

                        weight_gram = (
                            raw_value - offset
                        ) / scale

                        sensor_output[
                            "weight_gram"
                        ] = round(weight_gram, 4)

                        # ----------------------------------------
                        # KALMAN
                        # ----------------------------------------

                        weight_kalman = self.kalman_filters[
                            sensor_id
                        ].update(weight_gram)

                        sensor_output[
                            "weight_kalman_gram"
                        ] = round(weight_kalman, 4)

                        # ----------------------------------------
                        # FORCE CONVERSION
                        # ----------------------------------------

                        force_newton = (
                            weight_gram / 1000.0
                        ) * GRAVITY

                        force_kalman_newton = (
                            weight_kalman / 1000.0
                        ) * GRAVITY

                        sensor_output[
                            "force_newton"
                        ] = round(force_newton, 6)

                        sensor_output[
                            "force_kalman_newton"
                        ] = round(force_kalman_newton, 6)

                        sensor_output[
                            "status"
                        ] = "ok"

                    except Exception as error:

                        self.get_logger().error(
                            f"{sensor_id} calculation error: {error}"
                        )

            # ------------------------------------------------
            # CSV KALMAN DATA
            # ------------------------------------------------

            kalman_csv_data.extend([
                sensor_output["weight_gram"],
                sensor_output["weight_kalman_gram"],
                sensor_output["force_newton"],
                sensor_output["force_kalman_newton"]
            ])

            # ------------------------------------------------
            # SENSOR OUTPUT
            # ------------------------------------------------

            sensor_data[sensor_id] = sensor_output

        # ====================================================
        # TOTAL FORCE
        # ====================================================

        valid_forces = [
            sensor["force_newton"]
            for sensor in sensor_data.values()
            if sensor["force_newton"] is not None
        ]

        valid_filtered_forces = [
            sensor["force_kalman_newton"]
            for sensor in sensor_data.values()
            if sensor["force_kalman_newton"] is not None
        ]

        total_force = sum(
            valid_forces
        ) if valid_forces else None

        total_force_kalman = sum(
            valid_filtered_forces
        ) if valid_filtered_forces else None

        # ====================================================
        # JSON MESSAGE
        # ====================================================

        payload = {
            "source": "hx711",
            "timestamp": timestamp,
            "gravity": GRAVITY,
            "data": sensor_data,
            "total_force_newton": (
                round(total_force, 6)
                if total_force is not None
                else None
            ),
            "total_force_kalman_newton": (
                round(total_force_kalman, 6)
                if total_force_kalman is not None
                else None
            )
        }

        # ====================================================
        # ROS PUBLISH
        # ====================================================

        message = String()

        message.data = json.dumps(
            payload,
            separators=(",", ":"),
            allow_nan=False
        )

        self.publisher.publish(
            message
        )

        # ====================================================
        # CSV LOGGING
        # ====================================================

        try:

            save_csv(
                RAW_CSV,
                raw_csv_data
            )

            save_csv(
                KALMAN_CSV,
                kalman_csv_data
            )

        except Exception as error:

            self.get_logger().error(
                f"CSV logging error: {error}"
            )

        # ====================================================
        # TERMINAL OUTPUT
        # ====================================================

        print("\n" + "=" * 110)

        print(
            f"TIME: {timestamp}"
        )

        for sensor_id, sensor in sensor_data.items():

            print(
                f"{sensor_id:<14} "
                f"({sensor['location']:<12}) | "
                f"RAW: {str(sensor['raw']):>10} | "
                f"GRAM: {str(sensor['weight_gram']):>10} | "
                f"GRAM KF: {str(sensor['weight_kalman_gram']):>10} | "
                f"NEWTON: {str(sensor['force_newton']):>10} | "
                f"NEWTON KF: {str(sensor['force_kalman_newton']):>10} | "
                f"STATUS: {sensor['status']}"
            )

        print("-" * 110)

        print(
            f"TOTAL FORCE: {total_force} N | "
            f"TOTAL FORCE KF: {total_force_kalman} N"
        )

        print("=" * 110)

    # ========================================================
    # CLEANUP
    # ========================================================

    def cleanup(self):

        self.get_logger().info(
            "Cleaning up HX711..."
        )

        # GPIO pin cleanup
        for sensor in self.hx711_modules.values():

            try:

                sensor.cleanup()

            except Exception:

                pass

        # GPIO chip cleanup
        if self.gpio_handle is not None:

            try:

                lgpio.gpiochip_close(
                    self.gpio_handle
                )

                self.gpio_handle = None

            except Exception as error:

                self.get_logger().warning(
                    f"GPIO cleanup error: {error}"
                )


# ============================================================
# MAIN
# ============================================================

def main(args=None):

    rclpy.init(args=args)

    node = None

    try:

        node = HX711Node()

        rclpy.spin(
            node
        )

    except KeyboardInterrupt:

        print("\nHX711 node dihentikan.")

    except Exception as error:

        print(
            f"HX711 node failed: {error}"
        )

    finally:

        if node is not None:

            node.cleanup()

            node.destroy_node()

        if rclpy.ok():

            rclpy.shutdown()


if __name__ == "__main__":

    main()