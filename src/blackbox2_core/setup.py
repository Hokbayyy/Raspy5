
from glob import glob
from setuptools import find_packages, setup

package_name = 'blackbox2_core'

setup(
    name=package_name,
    version='0.0.0',

    packages=find_packages(exclude=['test']),

    data_files=[
        (
            'share/ament_index/resource_index/packages',
            ['resource/' + package_name],
        ),
        (
            'share/' + package_name,
            ['package.xml'],
        ),
        (
            'share/' + package_name + '/launch',
            glob('launch/*.launch.py'),
        ),
        (
            'share/' + package_name + '/config',
            glob('config/*.yaml'),
        ),
    ],

    install_requires=['setuptools'],
    zip_safe=True,

    maintainer='manekin2',
    maintainer_email='manekin2@todo.todo',
    description='Smart Manekin ROS 2 core package',
    license='TODO: License declaration',

    entry_points={
        'console_scripts': [
            'arduino_uno_q = blackbox2_core.drivers.arduino_uno_q_node:main',
            'cosmic_aurora = blackbox2_core.drivers.cosmic_aurora_node:main',
            'lora_transmitter = blackbox2_core.transport.lora_transmitter_node:main',
            'mcp9808_node = blackbox2_core.sensors.i2c.mcp9808_node:main',
            'analog_sensor_node = blackbox2_core.sensors.analog_mux.analog_sensor_node:main',
            'hx711_node = blackbox2_core.sensors.gpio.hx711_node:main',
            'witmotion_node = blackbox2_core.sensors.imu.witmotion_node:main',
        ],
    },
)
