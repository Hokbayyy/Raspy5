import time


class ADS1115:
    ADDRESS = 0x48

    REG_CONVERSION = 0x00
    REG_CONFIG = 0x01

    def __init__(
        self,
        i2c_manager,
        address=0x48,
        gain=1,
        data_rate=860
    ):
        self.i2c = i2c_manager
        self.address = address
        self.gain = gain
        self.data_rate = data_rate

        self.pga_config = {
            2/3: 0x0000,
            1: 0x0200,
            2: 0x0400,
            4: 0x0600,
            8: 0x0800,
            16: 0x0A00
        }

        self.pga_voltage = {
            2/3: 6.144,
            1: 4.096,
            2: 2.048,
            4: 1.024,
            8: 0.512,
            16: 0.256
        }

        if gain not in self.pga_config:
            raise ValueError("Gain ADS1115 tidak valid")

        if data_rate not in (8, 16, 32, 64, 128, 250, 475, 860):
            raise ValueError("Data rate ADS1115 tidak valid")

    def read_voltage(self, channel):
        if channel not in (0, 1, 2, 3):
            raise ValueError("Channel harus A0 sampai A3")

        mux_config = 0x4000 + (channel << 12)

        rate_config = {
            8: 0x0000,
            16: 0x0020,
            32: 0x0040,
            64: 0x0060,
            128: 0x0080,
            250: 0x00A0,
            475: 0x00C0,
            860: 0x00E0
        }

        config = (
            0x8000
            | mux_config
            | self.pga_config[self.gain]
            | 0x0100
            | rate_config[self.data_rate]
            | 0x0003
        )

        with self.i2c.lock:
            self.i2c.bus.write_word_data(
                self.address,
                self.REG_CONFIG,
                ((config & 0xFF) << 8) | (config >> 8)
            )

            time.sleep(1.0 / self.data_rate + 0.002)

            raw = self.i2c.bus.read_word_data(
                self.address,
                self.REG_CONVERSION
            )

        raw = ((raw & 0xFF) << 8) | (raw >> 8)

        if raw & 0x8000:
            raw -= 65536

        voltage = (
            raw * self.pga_voltage[self.gain]
        ) / 32768.0

        return voltage
