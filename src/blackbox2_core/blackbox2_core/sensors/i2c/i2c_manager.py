from contextlib import contextmanager
from threading import RLock

from smbus2 import SMBus


class I2CManager:
    """
    Mengelola I2C utama Raspberry Pi dan beberapa TCA9548A
    yang terhubung secara paralel.
    """

    def __init__(self, bus_number=1):
        self.bus_number = bus_number
        self.bus = SMBus(bus_number)
        self.lock = RLock()
        self.multiplexers = {}

    def register_mux(self, mux_id, address):
        """Mendaftarkan multiplexer dengan alamat I2C."""
        if not 0x70 <= address <= 0x77:
            raise ValueError(
                f"Alamat TCA9548A tidak valid: {hex(address)}"
            )

        if address in self.multiplexers.values():
            raise ValueError(
                f"Alamat TCA9548A sudah digunakan: {hex(address)}"
            )

        self.multiplexers[mux_id] = address

    def select_channel(self, address, channel):
        """Memilih satu kanal pada TCA9548A."""
        if not 0 <= channel <= 7:
            raise ValueError("Channel TCA9548A harus 0 sampai 7")

        self.bus.write_byte(address, 1 << channel)

    def disable_mux(self, address):
        """Mematikan seluruh kanal TCA9548A."""
        self.bus.write_byte(address, 0x00)

    @contextmanager
    def device(self, mux=None):
        """
        Membuka akses ke perangkat I2C.

        mux:
            None             -> sensor langsung ke I2C utama
            (0x70, 0)         -> TCA alamat 0x70, channel 0
            (0x71, 3)         -> TCA alamat 0x71, channel 3
        """
        with self.lock:
            selected_address = None

            try:
                if mux is not None:
                    address, channel = mux

                    if address not in self.multiplexers.values():
                        raise ValueError(
                            f"TCA belum didaftarkan: {hex(address)}"
                        )

                    self.select_channel(address, channel)
                    selected_address = address

                yield self.bus

            finally:
                if selected_address is not None:
                    self.disable_mux(selected_address)

    def close(self):
        """Menutup koneksi I2C."""
        with self.lock:
            self.bus.close()
