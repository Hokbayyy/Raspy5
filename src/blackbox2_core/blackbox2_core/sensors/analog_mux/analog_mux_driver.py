
import time
import lgpio


class AnalogMuxDriver:
    """
    Driver tiga HW-178 CD74HC4067.

    GPIO seleksi digunakan bersama:
        S0 = GPIO6
        S1 = GPIO13
        S2 = GPIO19
        S3 = GPIO26

    Nomor GPIO menggunakan BCM.
    """

    def __init__(
        self,
        s0=6,
        s1=13,
        s2=19,
        s3=26,
        gpiochip=4,
        settle_time=0.002
    ):

        self.pins = [s0, s1, s2, s3]

        self.settle_time = settle_time

        self.chip = None
        self.claimed = []

        try:

            self.chip = lgpio.gpiochip_open(
                gpiochip
            )

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
            raise ValueError(
                "Multiplexer channel harus 0-15"
            )

        # S0 = LSB
        for bit, pin in enumerate(self.pins):

            value = (channel >> bit) & 1

            lgpio.gpio_write(
                self.chip,
                pin,
                value
            )

        time.sleep(self.settle_time)

    def close(self):

        if self.chip is None:
            return

        for pin in self.claimed:

            try:

                lgpio.gpio_write(
                    self.chip,
                    pin,
                    0
                )

                lgpio.gpio_free(
                    self.chip,
                    pin
                )

            except Exception:
                pass

        self.claimed.clear()

        try:
            lgpio.gpiochip_close(
                self.chip
            )
        except Exception:
            pass

        self.chip = None
