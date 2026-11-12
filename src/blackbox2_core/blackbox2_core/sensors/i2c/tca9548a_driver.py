class TCA9548A:
    """Representasi satu multiplexer TCA9548A."""

    def __init__(self, manager, mux_id, address):
        self.manager = manager
        self.mux_id = mux_id
        self.address = address

        self.manager.register_mux(
            mux_id=mux_id,
            address=address
        )

    def channel(self, channel):
        """Menghasilkan jalur akses untuk channel tertentu."""
        if not 0 <= channel <= 7:
            raise ValueError("Channel harus 0 sampai 7")

        return self.manager.device(
            mux=(self.address, channel)
        )

    def disable(self):
        """Mematikan seluruh kanal multiplexer."""
        with self.manager.lock:
            self.manager.disable_mux(self.address)
