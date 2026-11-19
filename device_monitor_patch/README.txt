Smart Manekin - Device Monitor Node patch

Copy:
  blackbox2_core/core/device_monitor_node.py
into:
  ~/blackbox2_ws/src/blackbox2_core/blackbox2_core/core/device_monitor_node.py

Then add this entry to setup.py -> entry_points -> console_scripts:
  'device_monitor = blackbox2_core.core.device_monitor_node:main',

Build:
  cd ~/blackbox2_ws
  source /opt/ros/jazzy/setup.bash
  source ~/blackbox2_ws/venv/bin/activate
  colcon build --packages-select blackbox2_core --symlink-install
  source ~/blackbox2_ws/install/setup.bash

Run standalone in one terminal:
  ros2 run blackbox2_core device_monitor

Observe in another terminal:
  ros2 topic echo /system/device_status
  ros2 topic echo /system/device_events

This node monitors the following existing std_msgs/String topics:
  /telemetri/arduino/data
  /telemetri/aurora/data
  /sensors/analog_data
  /sensors/mcp9808
  /sensors/loadcell
  /sensors/witmotion

Timeout parameters are configurable. Defaults:
  Arduino 3s, Aurora 3s, Analog 5s, MCP9808 5s, HX711 5s, WT901 4s.

Do not add it to the production systemd service until it passes the standalone test.
