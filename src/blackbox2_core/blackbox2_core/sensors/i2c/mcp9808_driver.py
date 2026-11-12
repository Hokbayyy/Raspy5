
import time


class MCP9808Driver:
    """
    MCP9808 temperature sensor driver.
    Supports direct I2C and TCA9548A multiplexer paths.
    """

    ADDRESS_DEFAULT = 0x18
    REG_AMBIENT_TEMP = 0x05
    REG_MANUFACTURER_ID = 0x06
    REG_DEVICE_ID = 0x07

    def __init__(
        self,
        i2c_manager,
        sensor_id,
        address=ADDRESS_DEFAULT,
        mux_path=None
    ):
        self.i2c = i2c_manager
        self.sensor_id = sensor_id
        self.address = int(address)
        self.mux_path = mux_path or []

    def read_temperature(self):
        with self.i2c.device(self.mux_path) as bus:
            raw = bus.read_word_data(
                self.address,
                self.REG_AMBIENT_TEMP
            )

        # SMBus word data uses little-endian byte ordering.
        raw = ((raw & 0xFF) << 8) | (raw >> 8)

        temperature = raw & 0x0FFF
        temperature /= 16.0

        if raw & 0x1000:
            temperature -= 256.0

        return {
            "sensor_id": self.sensor_id,
            "parameter": "temperature",
            "value": round(temperature, 4),
            "unit": "degC",
            "timestamp": time.time(),
            "quality": "valid",
            "source": "raspberry_pi"
        }

    def verify(self):
        with self.i2c.device(self.mux_path) as bus:
            manufacturer = bus.read_word_data(
                self.address,
                self.REG_MANUFACTURER_ID
            )

            device = bus.read_word_data(
                self.address,
                self.REG_DEVICE_ID
            )

        manufacturer = (
            ((manufacturer & 0xFF) << 8) |
            (manufacturer >> 8)
        )

        device = (
            ((device & 0xFF) << 8) |
            (device >> 8)
        )

        return {
            "sensor_id": self.sensor_id,
            "manufacturer_id": hex(manufacturer),
            "device_id": hex(device),
            "detected": manufacturer == 0x0054
        }
