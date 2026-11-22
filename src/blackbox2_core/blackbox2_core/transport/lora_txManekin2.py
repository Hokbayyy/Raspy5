#!/usr/bin/env python3
"""Entrypoint LoRa Transmitter khusus Manekin 2 (default mannequin_id=2)."""

import os
import sys

try:
    from blackbox2_core.transport.lora_transmitter_node import main as _tx_main
except ModuleNotFoundError:
    pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if pkg_root not in sys.path:
        sys.path.insert(0, pkg_root)
    from blackbox2_core.transport.lora_transmitter_node import main as _tx_main


def main(args=None):
    _tx_main(args=args, default_mannequin_id=2)


if __name__ == "__main__":
    main()
