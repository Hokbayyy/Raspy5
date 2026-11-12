from blackbox2_core.sensors.i2c.i2c_manager import I2CManager
from blackbox2_core.sensors.i2c.tca9548a_driver import TCA9548A
from blackbox2_core.sensors.i2c.mcp9808_manager import MCP9808Manager


i2c = I2CManager(bus_number=1)

# Registrasi dua multiplexer
tca_1 = TCA9548A(i2c, "tca_1", 0x70)
tca_2 = TCA9548A(i2c, "tca_2", 0x71)

# Inisialisasi MCP9808 Manager
mcp = MCP9808Manager(i2c)

print("Jumlah sensor:", len(mcp.SENSOR_CONFIG))
print("\nMulai membaca MCP9808...\n")

try:
    data = mcp.read_all()

    for sensor_id, result in data.items():
        print(sensor_id, ":", result)

finally:
    i2c.close()
