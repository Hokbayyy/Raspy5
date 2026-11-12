from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.adc.ads1115_driver import ADS1115

i2c = None

try:
    print("Initializing ADS1115...")

    i2c = I2CManager(bus_number=1)

    adc = ADS1115(
        i2c_manager=i2c,
        address=0x48,
        gain=1,
        data_rate=128
    )

    print("ADS1115 initialization OK")

    for channel in range(3):
        voltage = adc.read_voltage(channel)

        print(
            f"A{channel}: {voltage:.4f} V"
        )

    print("ADS1115 test completed")

except Exception as error:
    print(f"ADS1115 TEST FAILED: {error}")

finally:
    if i2c is not None:
        i2c.close()
