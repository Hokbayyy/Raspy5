import time


class AnalogSensorManager:
    """
    Batch acquisition:
    - FSR: 14 kanal melalui ADS1115 A0
    - Piezo: 14 kanal melalui ADS1115 A1
    - Flex: 16 kanal melalui ADS1115 A2

    Output berupa array nilai sesuai urutan kanal.
    """

    def __init__(self, mux, adc):
        self.mux = mux
        self.adc = adc

    def read_all(self):
        fsr = []
        piezo = []
        flex = []

        start_time = time.perf_counter()

        for channel in range(16):
            self.mux.select_channel(channel)

            # FSR: CH0-CH13
            if channel < 14:
                try:
                    value = self.adc.read_voltage(0)
                    fsr.append(round(value, 4))
                except OSError:
                    fsr.append(None)

                # Piezo: CH0-CH13
                try:
                    value = self.adc.read_voltage(1)
                    piezo.append(round(value, 4))
                except OSError:
                    piezo.append(None)

            # Flex: CH0-CH15
            try:
                value = self.adc.read_voltage(2)
                flex.append(round(value, 4))
            except OSError:
                flex.append(None)

        elapsed = time.perf_counter() - start_time

        return {
            "fsr": fsr,
            "piezo": piezo,
            "flex": flex
        }, elapsed