import time
import lgpio


class AnalogMuxDriver:
    """
    Mengontrol tiga HW-178 (CD74HC4067)
    menggunakan empat GPIO seleksi bersama.
    """

    def __init__(
        self,
        s0=6,
        s1=13,
        s2=19,
        s3=26,
        gpiochip=4,
        settle_time=0.001
    ):
        self.pins = [s0, s1, s2, s3]
        self.settle_time = settle_time

        self.chip = lgpio.gpiochip_open(gpiochip)
        self.claimed = []

        try:
            for pin in self.pins:
                lgpio.gpio_claim_output(
                    self.chip,
                    pin,
                    0
                )
                self.claimed.append(pin)

        except Exception:
            self.close()
            raise

    def select_channel(self, channel):
        if not 0 <= channel <= 15:
            raise ValueError("Channel harus 0 sampai 15")

        # S0 adalah bit paling rendah.
        for bit, pin in enumerate(self.pins):
            value = (channel >> bit) & 1
            lgpio.gpio_write(self.chip, pin, value)

        time.sleep(self.settle_time)

    def close(self):
        if getattr(self, "chip", None) is not None:
            for pin in self.claimed:
                try:
                    lgpio.gpio_write(self.chip, pin, 0)
                    lgpio.gpio_free(self.chip, pin)
                except Exception:
                    pass

            lgpio.gpiochip_close(self.chip)
            self.chip = None
            self.claimed = []
