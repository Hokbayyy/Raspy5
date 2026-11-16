import sys
if sys.prefix == '/usr':
    sys.real_prefix = sys.prefix
    sys.prefix = sys.exec_prefix = '/home/manekin2/blackbox2_ws/src/blackbox2_core/install/blackbox2_core'
