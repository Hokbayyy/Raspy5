from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description():

    return LaunchDescription([

        # Arduino Uno Q
        Node(
            package='blackbox2_core',
            executable='arduino_uno_q',
            name='arduino_uno_q_node',
            output='screen',
        ),

        # Cosmic LoRa Aurora V3
        Node(
            package='blackbox2_core',
            executable='cosmic_aurora',
            name='cosmic_aurora_node',
            output='screen',
        ),

        # WitMotion WT901SDCL-BT50 IMU
        Node(
            package='blackbox2_core',
            executable='witmotion_node',
            name='witmotion_hwt901b_node',
            output='screen',
            parameters=[{
                'port': '/dev/ttyACM0',
                'baudrate': 115200,
                'polling_rate': 20.0,
                'serial_timeout': 0.1,
            }],
        ),

        # MCP9808 - 14 Temperature Sensors
        Node(
            package='blackbox2_core',
            executable='mcp9808_node',
            name='mcp9808_node',
            output='screen',
        ),

        # Analog Sensors - FSR, Piezo, Flex
        Node(
            package='blackbox2_core',
            executable='analog_sensor_node',
            name='analog_sensor_node',
            output='screen',
        ),

        # HX711 Load Cell
        Node(
            package='blackbox2_core',
            executable='hx711_node',
            name='hx711_node',
            output='screen',
        ),

        # RAK LoRa Transmitter (Manekin 2)
        Node(
            package='blackbox2_core',
            executable='lora_transmitter',
            name='lora_transmitter_node',
            output='screen',
            parameters=[{
                'mannequin_id': 2,
            }],
        ),

    ])
