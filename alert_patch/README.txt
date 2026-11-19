SMART MANNEQUIN - ALERT MANAGER PATCH

File:
  blackbox2_core/core/alert_manager_node.py

Fungsi:
- Subscribe /system/device_events
- Subscribe /system/status
- Publish alert JSON ke /system/alerts
- Simpan alert ke ~/.ros/smart_mannequin/alerts/alerts.jsonl
- Rotasi log otomatis (default 2 MB x 5 file)
- Hanya mengirim alert saat status sistem berubah; payload event identik berturut-turut disaring.

Catatan:
- Node ini tidak menghentikan node akuisisi.
- Node ini belum mengirim alert langsung melalui LoRa. Integrasi LoRa dilakukan setelah memeriksa format/topik input lora_transmitter_node yang sedang dipakai.
- Pastikan nama package dan struktur folder sesuai workspace pengguna.
