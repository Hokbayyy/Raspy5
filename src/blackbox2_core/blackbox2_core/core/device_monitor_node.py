#!/usr/bin/env python3
"""Monitor Smart Manekin sensor topics and publish per-device health."""

import json
import time
from datetime import datetime, timezone

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


class DeviceMonitorNode(Node):
    """Track the freshness and basic state of sensor telemetry topics."""

    def __init__(self):
        super().__init__('device_monitor_node')

        # Timeouts are deliberately configurable because device rates may change.
        self.declare_parameter('publish_rate_hz', 1.0)
        self.declare_parameter('arduino_timeout_sec', 3.0)
        self.declare_parameter('aurora_timeout_sec', 3.0)
        self.declare_parameter('analog_timeout_sec', 5.0)
        self.declare_parameter('mcp9808_timeout_sec', 5.0)
        self.declare_parameter('loadcell_timeout_sec', 5.0)
        self.declare_parameter('witmotion_timeout_sec', 4.0)

        rate = float(self.get_parameter('publish_rate_hz').value)
        if rate <= 0:
            raise ValueError('publish_rate_hz must be greater than zero')

        self.devices = {
            'arduino_uno_q': {
                'topic': '/telemetri/arduino/data',
                'timeout': float(self.get_parameter('arduino_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': None,
                'message_count': 0,
                'last_error': None,
            },
            'cosmic_aurora': {
                'topic': '/telemetri/aurora/data',
                'timeout': float(self.get_parameter('aurora_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': None,
                'message_count': 0,
                'last_error': None,
            },
            'analog_sensors': {
                'topic': '/sensors/analog_data',
                'timeout': float(self.get_parameter('analog_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': None,
                'message_count': 0,
                'last_error': None,
            },
            'mcp9808': {
                'topic': '/sensors/mcp9808',
                'timeout': float(self.get_parameter('mcp9808_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': None,
                'message_count': 0,
                'last_error': None,
            },
            'hx711_loadcells': {
                'topic': '/sensors/loadcell',
                'timeout': float(self.get_parameter('loadcell_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': None,
                'message_count': 0,
                'last_error': None,
            },
            'witmotion_wt901': {
                'topic': '/sensors/witmotion',
                'timeout': float(self.get_parameter('witmotion_timeout_sec').value),
                'last_seen': None,
                'state': 'STARTING',
                'message_status': None,
                'calibration_status': 'UNKNOWN',
                'message_count': 0,
                'last_error': None,
            },
        }

        self.status_pub = self.create_publisher(
            String, '/system/device_status', 10
        )
        self.event_pub = self.create_publisher(
            String, '/system/device_events', 10
        )

        self.started_at = time.monotonic()
        self.subscription_handles = []
        for device_name, device in self.devices.items():
            callback = self._make_callback(device_name)
            self.subscription_handles.append(
                self.create_subscription(String, device['topic'], callback, 10)
            )

        self.timer = self.create_timer(1.0 / rate, self._publish_status)
        self.get_logger().info(
            f'Device monitor started: tracking {len(self.devices)} topics'
        )

    def _make_callback(self, device_name):
        def callback(message):
            device = self.devices[device_name]
            now = time.monotonic()
            previous_state = device['state']

            device['last_seen'] = now
            device['message_count'] += 1
            device['last_error'] = None

            # Parse JSON opportunistically. Receipt itself is enough to establish
            # that the publisher/topic is alive; malformed payload is reported.
            try:
                payload = json.loads(message.data)
                if isinstance(payload, dict):
                    device['message_status'] = payload.get('status')
                    if device_name == 'witmotion_wt901':
                        device['calibration_status'] = self._extract_witmotion_state(payload)
            except (json.JSONDecodeError, TypeError) as exc:
                device['last_error'] = f'Invalid JSON: {exc}'

            device['state'] = 'ONLINE'
            if previous_state != 'ONLINE':
                self._publish_event(
                    device_name,
                    'DEVICE_RECOVERED' if previous_state == 'OFFLINE' else 'DEVICE_ONLINE',
                    'INFO',
                    f'{device_name} telemetry is being received again'
                    if previous_state == 'OFFLINE'
                    else f'{device_name} telemetry detected',
                )

        return callback

    @staticmethod
    def _extract_witmotion_state(payload):
        """Extract WT901 calibration state from current JSON payload shapes."""
        status = payload.get('status')
        if status:
            normalized = str(status).upper()
            if normalized in ('CALIBRATING', 'READY', 'WAITING_FOR_SENSOR', 'ERROR'):
                return normalized

        calibration = payload.get('calibration')
        if isinstance(calibration, dict):
            state = calibration.get('status') or calibration.get('state')
            if state:
                return str(state).upper()
            if calibration.get('complete') is True:
                return 'READY'
            if calibration.get('complete') is False:
                return 'CALIBRATING'

        return 'UNKNOWN'

    def _publish_event(self, device_name, event, severity, message):
        payload = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'device': device_name,
            'event': event,
            'severity': severity,
            'message': message,
        }
        msg = String()
        msg.data = json.dumps(payload, separators=(',', ':'))
        self.event_pub.publish(msg)
        self.get_logger().info(f'{severity}: {device_name}: {event}')

    def _publish_status(self):
        now = time.monotonic()
        timestamp = datetime.now(timezone.utc).isoformat()
        overall = 'OPERATIONAL'
        devices_output = {}

        for name, device in self.devices.items():
            last_seen = device['last_seen']
            age = None if last_seen is None else max(0.0, now - last_seen)

            if last_seen is None:
                state = (
                    'STARTING'
                    if now - self.started_at <= device['timeout']
                    else 'OFFLINE'
                )
            elif age > device['timeout']:
                state = 'OFFLINE'
            else:
                state = 'ONLINE'

            previous_state = device['state']
            if state != previous_state:
                device['state'] = state
                if state == 'OFFLINE':
                    self._publish_event(
                        name,
                        'DEVICE_OFFLINE',
                        'WARNING',
                        f'No telemetry received on {device["topic"]} '
                        f'within {device["timeout"]:.1f} seconds',
                    )

            if state == 'OFFLINE':
                overall = 'DEGRADED'
            elif state == 'STARTING' and overall == 'OPERATIONAL':
                overall = 'INITIALIZING'

            devices_output[name] = {
                'state': state,
                'topic': device['topic'],
                'timeout_sec': device['timeout'],
                'last_seen_age_sec': None if age is None else round(age, 2),
                'message_count': device['message_count'],
                'message_status': device['message_status'],
                'calibration_status': device['calibration_status'],
                'last_error': device['last_error'],
            }

        payload = {
            'timestamp': timestamp,
            'overall_status': overall,
            'online_count': sum(1 for item in devices_output.values() if item['state'] == 'ONLINE'),
            'device_count': len(devices_output),
            'devices': devices_output,
        }

        msg = String()
        msg.data = json.dumps(payload, separators=(',', ':'))
        self.status_pub.publish(msg)



def main(args=None):
    rclpy.init(args=args)
    node = DeviceMonitorNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
