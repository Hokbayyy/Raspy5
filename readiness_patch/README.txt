Smart Manekin - Readiness Manager patch

File:
  blackbox2_core/core/readiness_manager_node.py

Install into:
  ~/blackbox2_ws/src/blackbox2_core/blackbox2_core/core/

Add to setup.py console_scripts:
  'readiness_manager = blackbox2_core.core.readiness_manager_node:main',

Build:
  cd ~/blackbox2_ws
  source /opt/ros/jazzy/setup.bash
  source ~/blackbox2_ws/venv/bin/activate
  colcon build --packages-select blackbox2_core --symlink-install
  source ~/blackbox2_ws/install/setup.bash

Run in separate terminal after device_monitor is running:
  ros2 run blackbox2_core readiness_manager

Inspect:
  ros2 topic echo /system/status

Input:
  /system/device_status (std_msgs/msg/String), published by device_monitor.
Output:
  /system/status (std_msgs/msg/String)

Default calibration-aware device:
  witmotion_wt901

Status precedence:
  DEGRADED if any monitored device is OFFLINE
  INITIALIZING if any monitored device is STARTING/UNKNOWN
  CALIBRATING if WT901 reports CALIBRATING/WAITING_FOR_SENSOR
  OPERATIONAL otherwise

This node does not start/stop sensor nodes and does not gate LoRa transmission.
