class MCP9808Manager:
    """
    Manager untuk membaca 14 sensor MCP9808
    melalui dua TCA9548A paralel.
    """

    DEFAULT_ADDRESS = 0x18
    TEMP_REGISTER = 0x05

    SENSOR_CONFIG = [
        {"id": "mcp_j7",  "mux": 0x70, "channel": 0},
        {"id": "mcp_j8",  "mux": 0x70, "channel": 1},
        {"id": "mcp_j9",  "mux": 0x70, "channel": 2},
        {"id": "mcp_j10", "mux": 0x70, "channel": 3},
        {"id": "mcp_j11", "mux": 0x70, "channel": 4},
        {"id": "mcp_j12", "mux": 0x70, "channel": 5},
        {"id": "mcp_j13", "mux": 0x70, "channel": 6},
        {"id": "mcp_j14", "mux": 0x70, "channel": 7},

        {"id": "mcp_j15", "mux": 0x71, "channel": 0},
        {"id": "mcp_j16", "mux": 0x71, "channel": 1},
        {"id": "mcp_j17", "mux": 0x71, "channel": 2},
        {"id": "mcp_j18", "mux": 0x71, "channel": 3},
        {"id": "mcp_j19", "mux": 0x71, "channel": 4},
        {"id": "mcp_j20", "mux": 0x71, "channel": 5},
    ]

    def __init__(self, i2c_manager, address=DEFAULT_ADDRESS):
        self.i2c = i2c_manager
        self.address = address

    @staticmethod
    def decode_temperature(raw_word):
        """
        Mengubah register temperatur MCP9808
        menjadi derajat Celsius.
        """

        raw = ((raw_word & 0xFF) << 8) | (raw_word >> 8)
        raw &= 0x1FFF

        temperature = (raw & 0x0FFF) / 16.0

        if raw & 0x1000:
            temperature -= 256.0

        return temperature

    def read_sensor(self, sensor):
        """Membaca satu sensor MCP9808."""

        mux_address = sensor["mux"]
        channel = sensor["channel"]

        with self.i2c.device((mux_address, channel)) as bus:
            raw = bus.read_word_data(
                self.address,
                self.TEMP_REGISTER
            )

        return self.decode_temperature(raw)

    def read_all(self):
        """
        Membaca MCP9808 berdasarkan urutan J7 sampai J20.
        Menghasilkan list nilai temperatur saja.
        Sensor gagal dibaca menghasilkan None.
        """

        results = []

        for sensor in self.SENSOR_CONFIG:
            try:
                temperature = self.read_sensor(sensor)
                results.append(round(temperature, 2))

            except OSError:
                results.append(None)

        return results
