#!/usr/bin/env python3
"""Alert manager for Smart Mannequin ROS 2 gateway.

Consumes device events and readiness state, publishes normalized alert JSON,
and writes a local rotating JSONL log. It does not control acquisition nodes
or transmit directly over LoRa.
"""

import json
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
import logging

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def parse_json_message(raw):
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {"payload": value}
    except (TypeError, json.JSONDecodeError):
        return {"raw": raw, "parse_error": True}


class AlertManagerNode(Node):
    def __init__(self):
        super().__init__('alert_manager')

        self.declare_parameter('device_events_topic', '/system/device_events')
        self.declare_parameter('system_status_topic', '/system/status')
        self.declare_parameter('alerts_topic', '/system/alerts')
        self.declare_parameter('log_directory', os.path.expanduser('~/.ros/smart_mannequin/alerts'))
        self.declare_parameter('max_log_bytes', 2 * 1024 * 1024)
        self.declare_parameter('backup_count', 5)

        events_topic = self.get_parameter('device_events_topic').value
        status_topic = self.get_parameter('system_status_topic').value
        alerts_topic = self.get_parameter('alerts_topic').value
        log_directory = os.path.expanduser(self.get_parameter('log_directory').value)
        max_bytes = int(self.get_parameter('max_log_bytes').value)
        backup_count = int(self.get_parameter('backup_count').value)

        os.makedirs(log_directory, exist_ok=True)
        self.logger = logging.getLogger('smart_mannequin_alerts')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        if not self.logger.handlers:
            handler = RotatingFileHandler(
                os.path.join(log_directory, 'alerts.jsonl'),
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8',
            )
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)

        self.publisher = self.create_publisher(String, alerts_topic, 20)
        self.event_subscription = self.create_subscription(
            String, events_topic, self.on_device_event, 20)
        self.status_subscription = self.create_subscription(
            String, status_topic, self.on_system_status, 20)

        self.last_system_status = None
        self.last_event_fingerprint = None
        self.get_logger().info(f'Alert Manager aktif. Events: {events_topic}; status: {status_topic}; output: {alerts_topic}')
        self.get_logger().info(f'Log lokal: {os.path.join(log_directory, "alerts.jsonl")}')

    def emit_alert(self, severity, code, message, details=None, source='alert_manager'):
        alert = {
            'timestamp': utc_now(),
            'severity': severity,
            'code': code,
            'message': message,
            'source': source,
            'details': details if isinstance(details, dict) else {},
        }
        encoded = json.dumps(alert, ensure_ascii=False, separators=(',', ':'))
        msg = String()
        msg.data = encoded
        self.publisher.publish(msg)
        self.logger.info(encoded)
        self.get_logger().info(f'[{severity}] {code}: {message}')

    def on_device_event(self, msg):
        event = parse_json_message(msg.data)
        fingerprint = json.dumps(event, sort_keys=True, ensure_ascii=False)
        # Device monitor may publish a persistent event payload periodically.
        # Suppress identical consecutive payloads to avoid duplicate alert spam.
        if fingerprint == self.last_event_fingerprint:
            return
        self.last_event_fingerprint = fingerprint

        if event.get('parse_error'):
            self.emit_alert('WARNING', 'DEVICE_EVENT_PARSE_ERROR',
                            'Pesan device_events bukan JSON yang valid.',
                            {'raw': msg.data}, source='device_monitor')
            return

        # Preserve the monitor's event structure rather than assuming a schema.
        self.emit_alert('WARNING', 'DEVICE_EVENT',
                        'Peristiwa perangkat diterima dari Device Monitor.',
                        {'device_event': event}, source='device_monitor')

    def on_system_status(self, msg):
        status_data = parse_json_message(msg.data)
        current = status_data.get('status', 'UNKNOWN')
        if current == self.last_system_status:
            return

        previous = self.last_system_status
        self.last_system_status = current

        if current == 'OPERATIONAL':
            severity = 'INFO'
            message = 'Sistem gateway berstatus OPERATIONAL.'
        elif current == 'DEGRADED':
            severity = 'WARNING'
            message = 'Sistem gateway berstatus DEGRADED; sebagian perangkat tidak tersedia.'
        elif current == 'CALIBRATING':
            severity = 'INFO'
            message = 'Sistem gateway sedang melakukan kalibrasi.'
        elif current == 'INITIALIZING':
            severity = 'INFO'
            message = 'Sistem gateway sedang inisialisasi.'
        else:
            severity = 'WARNING'
            message = f'Sistem gateway memiliki status {current}.'

        self.emit_alert(severity, 'SYSTEM_STATUS_CHANGED', message,
                        {'previous_status': previous, 'current_status': current,
                         'system_status': status_data}, source='readiness_manager')


def main(args=None):
    rclpy.init(args=args)
    node = AlertManagerNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
