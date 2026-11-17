
from contextlib import contextmanager
from threading import RLock

from smbus2 import SMBus


class I2CManager:
    """
    Mengelola I2C utama Raspberry Pi 5
    dan beberapa TCA9548A.
    """

    def __init__(self, bus_number=1):

        self.bus_number = bus_number
        self.bus = SMBus(bus_number)
        self.lock = RLock()

        self.multiplexers = {}

    def register_mux(self, mux_id, address):
        """
        Mendaftarkan TCA9548A.

        Registrasi ulang dengan alamat yang sama
        diperbolehkan jika mux_id juga sama.
        """

        if not 0x70 <= address <= 0x77:
            raise ValueError(
                f"Alamat TCA tidak valid: {hex(address)}"
            )

        with self.lock:

            if mux_id in self.multiplexers:

                if self.multiplexers[mux_id] == address:
                    return

                raise ValueError(
                    f"Mux ID {mux_id} sudah digunakan"
                )

            if address in self.multiplexers.values():
                raise ValueError(
                    f"Alamat TCA sudah terdaftar: {hex(address)}"
                )

            self.multiplexers[mux_id] = address

    def select_channel(self, address, channel):

        if not 0 <= channel <= 7:
            raise ValueError(
                "Channel TCA harus 0 sampai 7"
            )

        self.bus.write_byte(
            address,
            1 << channel
        )

    def disable_mux(self, address):

        self.bus.write_byte(
            address,
            0x00
        )

    @contextmanager
    def device(self, mux=None):
        """
        Akses perangkat I2C.

        mux=None:
            Sensor langsung ke bus I2C.

        mux=(0x70, 0):
            TCA 0x70 channel 0.
        """

        with self.lock:

            selected_address = None

            try:

                if mux is not None:

                    address, channel = mux

                    if address not in self.multiplexers.values():
                        raise ValueError(
                            f"TCA belum terdaftar: {hex(address)}"
                        )

                    self.select_channel(
                        address,
                        channel
                    )

                    selected_address = address

                yield self.bus

            finally:

                if selected_address is not None:

                    self.disable_mux(
                        selected_address
                    )

    def close(self):

        with self.lock:

            if self.bus is not None:

                self.bus.close()
                self.bus = None
