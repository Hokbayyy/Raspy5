
import time


class AnalogSensorManager:
    """
    Akuisisi tiga modul analog multiplexer.

    FSR:
        ADS1115 A0, channel 0-13

    Piezo:
        ADS1115 A1, channel 0-13

    Flex:
        ADS1115 A2, channel 0-15

    Ketiga multiplexer berbagi GPIO S0-S3.
    """

    FSR_CHANNELS = 14
    PIEZO_CHANNELS = 14
    FLEX_CHANNELS = 16
    TOTAL_CHANNELS = 16

    def __init__(self, mux, adc):
        self.mux = mux
        self.adc = adc

    @staticmethod
    def safe_read(adc, channel):
        """
        Membaca satu kanal ADC.
        Jika pembacaan gagal, kembalikan None.
        """
        try:
            value = adc.read_voltage(channel)

            if value is None:
                return None

            return round(float(value), 4)

        except (OSError, ValueError, RuntimeError):
            return None

    def read_all(self):
        """
        Membaca seluruh sensor dalam satu batch.

        Output:
        {
            "fsr": [...14 nilai...],
            "piezo": [...14 nilai...],
            "flex": [...16 nilai...]
        }
        """

        fsr = []
        piezo = []
        flex = []

        start_time = time.perf_counter()

        for channel in range(self.TOTAL_CHANNELS):

            # Pilih kanal multiplexer yang sama
            # untuk ketiga modul analog.
            self.mux.select_channel(channel)

            # FSR: A0, channel 0-13
            if channel < self.FSR_CHANNELS:
                fsr.append(
                    self.safe_read(self.adc, 0)
                )

            # Piezo: A1, channel 0-13
            if channel < self.PIEZO_CHANNELS:
                piezo.append(
                    self.safe_read(self.adc, 1)
                )

            # Flex: A2, channel 0-15
            flex.append(
                self.safe_read(self.adc, 2)
            )

        elapsed = time.perf_counter() - start_time

        result = {
            "fsr": fsr,
            "piezo": piezo,
            "flex": flex
        }

        return result, elapsed
