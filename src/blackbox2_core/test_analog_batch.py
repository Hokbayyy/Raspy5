import json

from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.adc.ads1115_driver import ADS1115
from blackbox2_core.sensors.analog_mux.analog_mux_driver import AnalogMuxDriver
from blackbox2_core.sensors.analog_mux.analog_sensor_manager import AnalogSensorManager


i2c = None
mux = None

try:
    print("Initializing analog acquisition...")

    i2c = I2CManager(bus_number=1)

    adc = ADS1115(
        i2c_manager=i2c,
        address=0x48,
        gain=1,
        data_rate=860
    )

    mux = AnalogMuxDriver(
        s0=6,
        s1=13,
        s2=19,
        s3=26,
        gpiochip=4,
        settle_time=0.001
    )

    manager = AnalogSensorManager(mux, adc)

    print("Starting 44-channel batch scan...")

    data, elapsed = manager.read_all()

    payload = json.dumps(
        data,
        separators=(",", ":"),
        allow_nan=False
    )

    print("\nBATCH DATA:")
    print(payload)

    print("\nDATA COUNT:")
    print(f"FSR   : {len(data['fsr'])}")
    print(f"Piezo : {len(data['piezo'])}")
    print(f"Flex  : {len(data['flex'])}")
    print(f"Total : {sum(len(v) for v in data.values())}")

    print(f"\nScan duration: {elapsed * 1000:.2f} ms")
    print(f"Payload size : {len(payload.encode('utf-8'))} bytes")

except Exception as error:
    print(f"ANALOG BATCH TEST FAILED: {error}")

finally:
    if mux is not None:
        mux.close()

    if i2c is not None:
        i2c.close()
