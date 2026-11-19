#!/usr/bin/env python3
"""Aggregate per-device health into a Smart Manekin system readiness state."""

import json
from datetime import datetime, timezone

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


class ReadinessManagerNode(Node):
    """Publish INITIALIZING, CALIBRATING, OPERATIONAL, or DEGRADED."""

    def __init__(self):
        super().__init__('readiness_manager_node')

        self.declare_parameter('publish_rate_hz', 1.0)
        self.declare_parameter('device_status_topic', '/system/device_status')
        self.declare_parameter('system_status_topic', '/system/status')
        self.declare_parameter('calibration_devices', ['witmotion_wt901'])

        rate = float(self.get_parameter('publish_rate_hz').value)
        if rate <= 0.0:
            raise ValueError('publish_rate_hz must be greater than zero')

        self.calibration_devices = list(
            self.get_parameter('calibration_devices').value
        )
        self.latest_device_status = None
        self.last_device_status_rx = None
        self.previous_status = None

        device_topic = str(self.get_parameter('device_status_topic').value)
        system_topic = str(self.get_parameter('system_status_topic').value)

        self.subscription = self.create_subscription(
            String, device_topic, self._device_status_callback, 10
        )
        self.status_pub = self.create_publisher(String, system_topic, 10)
        self.timer = self.create_timer(1.0 / rate, self._publish_system_status)

        self.get_logger().info(
            f'Readiness manager started; input={device_topic}, output={system_topic}'
        )

    def _device_status_callback(self, msg):
        try:
            payload = json.loads(msg.data)
            if not isinstance(payload, dict) or not isinstance(payload.get('devices'), dict):
                raise ValueError('Expected JSON object containing a devices object')
            self.latest_device_status = payload
            self.last_device_status_rx = self.get_clock().now()
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            self.get_logger().warning(f'Invalid device status message: {exc}')

    def _derive_status(self):
        if self.latest_device_status is None:
            return 'INITIALIZING', {}, [], [], []

        devices = self.latest_device_status['devices']
        starting = []
        offline = []
        calibrating = []
        online = []

        for name, details in devices.items():
            if not isinstance(details, dict):
                offline.append(name)
                continue

            state = str(details.get('state', 'UNKNOWN')).upper()
            calibration = str(details.get('calibration_status') or '').upper()

            if state == 'OFFLINE':
                offline.append(name)
            elif state in ('STARTING', 'UNKNOWN'):
                starting.append(name)
            else:
                online.append(name)
                if name in self.calibration_devices and calibration in (
                    'CALIBRATING', 'WAITING_FOR_SENSOR'
                ):
                    calibrating.append(name)

        # A failed device takes precedence: available devices continue operating
        # and the whole gateway reports DEGRADED rather than stopping.
        if offline:
            status = 'DEGRADED'
        elif starting:
            status = 'INITIALIZING'
        elif calibrating:
            status = 'CALIBRATING'
        else:
            status = 'OPERATIONAL'

        return status, devices, online, offline, calibrating

    def _publish_system_status(self):
        status, devices, online, offline, calibrating = self._derive_status()
        timestamp = datetime.now(timezone.utc).isoformat()

        payload = {
            'timestamp': timestamp,
            'status': status,
            'device_count': len(devices),
            'online_count': len(online),
            'offline_count': len(offline),
            'calibrating_count': len(calibrating),
            'online_devices': online,
            'offline_devices': offline,
            'calibrating_devices': calibrating,
            'source': 'readiness_manager',
        }

        msg = String()
        msg.data = json.dumps(payload, separators=(',', ':'))
        self.status_pub.publish(msg)

        if status != self.previous_status:
            self.get_logger().info(
                f'System status changed: {self.previous_status} -> {status}'
            )
            self.previous_status = status


def main(args=None):
    rclpy.init(args=args)
    node = ReadinessManagerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
