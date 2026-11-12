from blackbox2_core.sensors.analog_mux.analog_mux_driver import AnalogMuxDriver

mux = None

try:
    print("Initializing HW-178 GPIO controller...")

    mux = AnalogMuxDriver(
        s0=6,
        s1=13,
        s2=19,
        s3=26,
        gpiochip=4
    )

    print("GPIO initialization OK")

    for channel in range(16):
        mux.select_channel(channel)
        print(f"Selected channel: CH{channel}")

    print("All 16 channels tested successfully")

except Exception as error:
    print(f"GPIO TEST FAILED: {error}")

finally:
    if mux is not None:
        mux.close()
        print("GPIO released")
