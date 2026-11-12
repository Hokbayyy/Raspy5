from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.adc.ads1115_driver import ADS1115
from blackbox2_core.sensors.analog_mux.analog_mux_driver import AnalogMuxDriver

i2c = None
mux = None

try:
    i2c = I2CManager(bus_number=1)

    adc = ADS1115(
        i2c_manager=i2c,
        address=0x48,
        gain=1,
        data_rate=128
    )

    mux = AnalogMuxDriver(
        s0=6,
        s1=13,
        s2=19,
        s3=26,
        gpiochip=4
    )

    mux.select_channel(15)

    print("Testing CH15 with GND reference")

    for _ in range(5):
        fsr = adc.read_voltage(0)
        piezo = adc.read_voltage(1)
        flex = adc.read_voltage(2)

        print(
            f"FSR: {fsr:.4f} V | "
            f"Piezo: {piezo:.4f} V | "
            f"Flex: {flex:.4f} V"
        )

finally:
    if mux is not None:
        mux.close()

    if i2c is not None:
        i2c.close()
