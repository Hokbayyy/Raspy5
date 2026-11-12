#!/usr/bin/env python3
"""
LoRa P2P Transmitter Node (ROS2) - RAK3272S di Raspberry Pi 5

Gabungan:
  - Arsitektur node ROS2 (subscribe /telemetri/arduino & /telemetri/aurora,
    envelope JSON + CRC16-CCITT) dari kode lama
  - Inisialisasi modul RAK3272S (AT, NWM=0, PRECV=0, AT+P2P=...) dari kode baru

Tambahan:
  - Semua parameter radio bisa diatur lewat ROS parameter
  - Flag tx_busy: tidak mengirim paket baru sebelum +EVT:TXP2P DONE
    (mencegah AT_BUSY_ERROR saat interval kecil)
  - Cek ukuran payload maksimum P2P (255 byte)
  - Auto-reconnect serial
"""

import json
import time

import serial

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


# ============================================================
# CRC16-CCITT
# ============================================================

def crc16_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


# ============================================================
# LORA TRANSMITTER NODE
# ============================================================

class LoRaTransmitterNode(Node):

    def __init__(self):
        super().__init__("lora_transmitter_node")

        # ---------------- Parameter umum ----------------
        self.declare_parameter("port", "/dev/ttyAMA3")
        self.declare_parameter("baudrate", 115200)
        self.declare_parameter("interval", 0.25)
        self.declare_parameter("stale_timeout", 1.0)

        # ---------------- Parameter radio P2P ----------------
        # HARUS SAMA PERSIS DENGAN RECEIVER
        self.declare_parameter("configure_radio", True)
        self.declare_parameter("freq", 923200000)   # Hz
        self.declare_parameter("sf", 6)             # 5-12
        self.declare_parameter("bw", 2)             # 0:125k 1:250k 2:500k
        self.declare_parameter("cr", 0)             # 0:4/5 .. 3:4/8
        self.declare_parameter("preamble", 8)
        self.declare_parameter("tx_power", 20)      # dBm, maks 22
        self.declare_parameter("max_payload", 255)  # byte, batas P2P
        self.declare_parameter("tx_timeout", 3.0)   # detik tunggu TX DONE

        port = self.get_parameter("port").value
        baud = int(self.get_parameter("baudrate").value)
        self.interval = float(self.get_parameter("interval").value)
        self.stale_timeout = float(self.get_parameter("stale_timeout").value)
        self.max_payload = int(self.get_parameter("max_payload").value)
        self.tx_timeout = float(self.get_parameter("tx_timeout").value)

        self.port = port
        self.baud = baud

        # ---------------- State internal ----------------
        self.packet_seq = 0
        self.start_time = time.monotonic()
        
        self.sources = {
            "arduino": {"raw": None, "received_at": None},
            "aurora": {"raw": None, "received_at": None},
        }

        self.ser = None
        self.rx_buffer = ""
        self.tx_busy = False
        self.tx_started_at = 0.0
        self.last_reconnect_try = 0.0

        # ---------------- Koneksi & konfigurasi RAK ----------------
        if self.connect_rak(port, baud):
            if bool(self.get_parameter("configure_radio").value):
                if not self.init_radio():
                    self.get_logger().error(
                        "Inisialisasi radio gagal, TX akan dicoba tetap "
                        "berjalan dengan konfigurasi modul saat ini"
                    )

        # ---------------- Subscriptions ----------------
        self.create_subscription(
            String, "/telemetri/arduino",
            lambda msg: self.source_callback("arduino", msg), 10)

        self.create_subscription(
            String, "/telemetri/aurora",
            lambda msg: self.source_callback("aurora", msg), 10)

        # ---------------- Timers ----------------
        self.create_timer(0.01, self.read_rak_response)
        self.create_timer(self.interval, self.transmit)

        self.get_logger().info("LoRa transmitter node started")

    # ========================================================
    # SERIAL CONNECTION
    # ========================================================

    def connect_rak(self, port, baud) -> bool:
        try:
            self.ser = serial.Serial(
                port=port,
                baudrate=baud,
                timeout=0,
                write_timeout=0.2,
            )
            self.get_logger().info(f"RAK connected: {port} @ {baud}")
            return True
        except (serial.SerialException, OSError) as exc:
            self.ser = None
            self.get_logger().error(f"RAK connection failed: {exc}")
            return False

    def try_reconnect(self):
        """Coba sambung ulang maksimal sekali per 3 detik."""
        now = time.monotonic()
        if now - self.last_reconnect_try < 3.0:
            return
        self.last_reconnect_try = now

        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

        self.get_logger().warning("Mencoba reconnect ke RAK...")
        if self.connect_rak(self.port, self.baud):
            self.tx_busy = False
            self.rx_buffer = ""
            if bool(self.get_parameter("configure_radio").value):
                self.init_radio()

    # ========================================================
    # AT COMMAND HELPER (blocking, hanya dipakai saat init)
    # ========================================================

    def send_at(self, cmd, timeout=1.5, expected=None):
        self.ser.reset_input_buffer()
        self.ser.write((cmd + "\r\n").encode("ascii"))
        self.ser.flush()

        start = time.monotonic()
        responses = []
        buf = ""

        while time.monotonic() - start < timeout:
            n = self.ser.in_waiting
            if n > 0:
                buf += self.ser.read(n).decode("ascii", errors="ignore")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    responses.append(line)
                    if expected:
                        if expected in line:
                            return responses
                    elif ("OK" in line or "AT_ERROR" in line
                          or "AT_PARAM_ERROR" in line):
                        return responses
            time.sleep(0.02)

        return responses

    @staticmethod
    def has_ok(lines):
        return any("OK" in l for l in lines)

    # ========================================================
    # INISIALISASI RADIO
    # ========================================================

    def init_radio(self) -> bool:
        log = self.get_logger()
        log.info("Inisialisasi RAK3272S...")

        # 1. Tes AT
        res = self.send_at("AT", timeout=1.0)
        if not self.has_ok(res):
            time.sleep(0.5)
            res = self.send_at("AT", timeout=1.0)
            if not self.has_ok(res):
                log.error(
                    "Modul tidak merespon AT. Cek wiring TX/RX dan port.")
                return False
        log.info("AT OK, modul terdeteksi")

        ver = self.send_at("AT+VER=?", timeout=1.0)
        log.info(f"Firmware: {' '.join(ver)}")

        # 2. Pastikan mode P2P (NWM=0)
        res_nwm = self.send_at("AT+NWM=?", timeout=1.0)
        current_mode = None
        for line in res_nwm:
            if "AT+NWM=" in line:
                current_mode = line.split("=")[-1].strip()

        if current_mode != "0":
            log.warning("Modul bukan mode P2P, mengubah ke AT+NWM=0...")
            self.ser.write(b"AT+NWM=0\r\n")
            self.ser.flush()
            time.sleep(2.5)  # modul reboot otomatis
            self.ser.reset_input_buffer()
            if not self.has_ok(self.send_at("AT", timeout=1.0)):
                log.error("Gagal reboot ke mode P2P")
                return False
            log.info("Berhasil pindah ke mode P2P")
        else:
            log.info("Modul sudah di mode P2P")

        # 3. Matikan RX agar siap TX
        self.send_at("AT+PRECV=0", timeout=0.5)

        # 4. Parameter P2P
        freq = int(self.get_parameter("freq").value)
        sf = int(self.get_parameter("sf").value)
        bw = int(self.get_parameter("bw").value)
        cr = int(self.get_parameter("cr").value)
        pre = int(self.get_parameter("preamble").value)
        pwr = int(self.get_parameter("tx_power").value)

        cmd = f"AT+P2P={freq}:{sf}:{bw}:{cr}:{pre}:{pwr}"
        log.info(f"Set parameter: {cmd}")
        res_p2p = self.send_at(cmd, timeout=1.5)
        if not self.has_ok(res_p2p):
            log.error(f"Gagal set parameter P2P: {res_p2p}")
            return False

        status = self.send_at("AT+P2P=?", timeout=1.0)
        log.info(f"Status P2P aktif: {' '.join(status)}")
        log.info("RAK3272S siap mengirim")
        return True

    # ========================================================
    # ROS CALLBACK
    # ========================================================

    def source_callback(self, source_name, msg):
        self.sources[source_name]["raw"] = msg.data
        self.sources[source_name]["received_at"] = time.monotonic()

    # ========================================================
    # BUILD PACKET
    # ========================================================

    def build_packet(self):
        now = time.monotonic()
        sources_payload = {}
        available_count = 0

        for name, source in self.sources.items():
            raw = source["raw"]
            received_at = source["received_at"]

            available = (
                raw is not None
                and received_at is not None
                and now - received_at <= self.stale_timeout
            )

            if available:
                age_ms = int((now - received_at) * 1000)
                available_count += 1
            else:
                age_ms = None

            sources_payload[name] = {
                "available": available,
                "age_ms": age_ms,
                "raw": raw if available else None,
            }

        if available_count == 0:
            return None

        self.packet_seq += 1

        packet = {
            "version": 1,
            "seq": self.packet_seq,
            "uptime_ms": int((now - self.start_time) * 1000),
            "sources": sources_payload,
        }

        return json.dumps(packet, separators=(",", ":"), ensure_ascii=True)

    # ========================================================
    # TRANSMIT
    # ========================================================

    def transmit(self):
        if self.ser is None or not self.ser.is_open:
            self.try_reconnect()
            return

        # Lepas flag busy jika TX DONE tidak pernah datang
        if self.tx_busy:
            if time.monotonic() - self.tx_started_at > self.tx_timeout:
                self.get_logger().warning(
                    "Timeout menunggu +EVT:TXP2P DONE, lanjut kirim")
                self.tx_busy = False
            else:
                return

        payload = self.build_packet()
        if payload is None:
            self.get_logger().warning("Belum ada telemetry yang tersedia")
            return

        body = payload.encode("utf-8")
        crc = crc16_ccitt(body)
        packet = body + f"*{crc:04X}".encode("ascii")

        if len(packet) > self.max_payload:
            self.get_logger().error(
                f"Paket {len(packet)} byte melebihi batas "
                f"{self.max_payload} byte, paket dilewati. "
                "Kurangi isi telemetry atau pakai format biner/kompresi."
            )
            return

        payload_hex = packet.hex().upper()
        command = f"AT+PSEND={payload_hex}\r\n"

        try:
            self.ser.write(command.encode("ascii"))
            self.ser.flush()

            self.tx_busy = True
            self.tx_started_at = time.monotonic()

            self.get_logger().info(
                f"LoRa TX seq={self.packet_seq} | {len(packet)} bytes | "
                f"HEX={len(payload_hex)} chars"
            )

        except (serial.SerialException,
                serial.SerialTimeoutException,
                OSError) as exc:
            self.get_logger().error(f"LoRa TX error: {exc}")
            self.try_reconnect()

    # ========================================================
    # READ RAK RESPONSE
    # ========================================================

    def read_rak_response(self):
        if self.ser is None or not self.ser.is_open:
            return

        try:
            waiting = self.ser.in_waiting
            if waiting <= 0:
                return

            data = self.ser.read(waiting).decode("ascii", errors="replace")
            self.rx_buffer += data

            while "\n" in self.rx_buffer:
                line, self.rx_buffer = self.rx_buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                if "+EVT:TXP2P DONE" in line:
                    self.tx_busy = False
                    elapsed = time.monotonic() - self.tx_started_at
                    self.get_logger().info(
                        f"TX DONE ({elapsed * 1000:.0f} ms)")
                elif "AT_BUSY_ERROR" in line:
                    # Modul masih sibuk; tahan, nanti dilepas oleh timeout
                    self.get_logger().warning(f"RAK RX: {line}")
                elif "AT_PARAM_ERROR" in line or "AT_ERROR" in line:
                    self.tx_busy = False
                    self.get_logger().error(f"RAK RX: {line}")
                else:
                    self.get_logger().info(f"RAK RX: {line}")

            if len(self.rx_buffer) > 4096:
                self.rx_buffer = ""
                self.get_logger().warning("RAK response buffer reset")

        except (serial.SerialException, OSError) as exc:
            self.get_logger().error(f"RAK read error: {exc}")
            self.try_reconnect()

    # ========================================================
    # CLEANUP
    # ========================================================

    def destroy_node(self):
        if self.ser is not None and self.ser.is_open:
            try:
                self.ser.write(b"AT+PRECV=0\r\n")
                time.sleep(0.1)
            except Exception:
                pass
            self.ser.close()
        super().destroy_node()


# ============================================================
# MAIN
# ============================================================

def main(args=None):
    rclpy.init(args=args)
    node = LoRaTransmitterNode()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()