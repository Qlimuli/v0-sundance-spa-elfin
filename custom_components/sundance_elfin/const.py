"""Constants for the Sundance Spa Elfin integration."""
from typing import Final

DOMAIN: Final = "sundance_elfin"

# Configuration
CONF_HOST: Final = "host"
CONF_PORT: Final = "port"

DEFAULT_PORT: Final = 8899
DEFAULT_NAME: Final = "Sundance Spa"

# Connection settings
RECONNECT_INTERVAL: Final = 30  # seconds
CONNECTION_TIMEOUT: Final = 10  # seconds
READ_TIMEOUT: Final = 5  # seconds

# Temperature limits (Celsius)
MIN_TEMP: Final = 26.0
MAX_TEMP: Final = 40.0
TEMP_STEP: Final = 0.5

# Protocol constants based on actual Sundance Cameo 880 data
# Frame always starts with 0x7E 0x7E
FRAME_START: Final = bytes([0x7E, 0x7E])
FRAME_START_BYTE: Final = 0x7E

# Address bytes observed in packets
ADDR_BROADCAST: Final = 0xFF
ADDR_PANEL: Final = 0xAF  # Topside panel address

# Message type bytes (byte after length)
# 0x05 = Heartbeat/ping (short packets like 0510bf065c)
MSG_HEARTBEAT: Final = 0x05

# Status packets have length 0x22 (34) or 0x26 (38)
# These contain the actual spa state
MSG_STATUS_LEN_34: Final = 0x22  # 34 byte status packet
MSG_STATUS_LEN_38: Final = 0x26  # 38 byte status packet

# Command message types (based on Jacuzzi/Balboa protocol research)
# These need verification with your specific spa
MSG_SET_TEMP: Final = 0x20
MSG_TOGGLE_PUMP1: Final = 0x04
MSG_TOGGLE_PUMP2: Final = 0x05
MSG_TOGGLE_LIGHT: Final = 0x11
MSG_BUTTON_PRESS: Final = 0x17  # Generic button press command

# Button codes for command packets (Jacuzzi protocol)
BTN_PUMP1: Final = 0x04
BTN_PUMP2: Final = 0x05
BTN_LIGHT: Final = 0x11
BTN_TEMP_UP: Final = 0x01
BTN_TEMP_DOWN: Final = 0x02

# Status byte positions in 38-byte status packet (0x26 length)
# These are offsets from start of packet (after 7E7E prefix)
# Based on packet: 7e7e26ffafc40f1c0b0a85079dfc070046003d1cbf1b22181b0032142d1611101317736c2f2e29047e
# Position counting from byte after 7E7E:
# [0]=0x26 (len), [1]=0xFF, [2]=0xAF, [3]=0xC4 (msg type?), [4+]=data
POS_LENGTH: Final = 0
POS_ADDR1: Final = 1   # 0xFF
POS_ADDR2: Final = 2   # 0xAF
POS_MSG_TYPE: Final = 3

# Data positions within status packet (relative to packet start after 7E7E)
# These need fine-tuning based on observed values
POS_CURRENT_TEMP: Final = 6   # Current water temperature
POS_TARGET_TEMP: Final = 7    # Target/set temperature
POS_HOUR: Final = 8           # Current hour
POS_MINUTE: Final = 9         # Current minute
POS_HEATING_STATE: Final = 10 # Heating active flag
POS_FLAGS1: Final = 14        # Pump/accessory flags byte 1
POS_FLAGS2: Final = 15        # Pump/accessory flags byte 2
POS_LIGHT_STATE: Final = 18   # Light state
POS_PUMP1_STATE: Final = 14   # Pump 1 in flags byte
POS_PUMP2_STATE: Final = 14   # Pump 2 in flags byte (different bit)

# Bit masks for status flags
MASK_PUMP1_LOW: Final = 0x01
MASK_PUMP1_HIGH: Final = 0x02
MASK_PUMP2_LOW: Final = 0x04
MASK_PUMP2_HIGH: Final = 0x08
MASK_HEATING: Final = 0x30    # Heating active bits
MASK_LIGHT: Final = 0x03

# Entity unique ID prefixes
CLIMATE_UNIQUE_ID: Final = "climate"
PUMP1_UNIQUE_ID: Final = "pump1"
PUMP2_UNIQUE_ID: Final = "pump2"
LIGHT_UNIQUE_ID: Final = "light"
TEMP_SENSOR_UNIQUE_ID: Final = "temperature"
CONNECTION_SENSOR_UNIQUE_ID: Final = "connection"

# Platforms
PLATFORMS: Final = ["climate", "switch", "light", "sensor"]
