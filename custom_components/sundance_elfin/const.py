"""Constants for the Sundance Spa Elfin integration."""
from typing import Final

DOMAIN: Final = "sundance_elfin"

# Default port for Elfin WiFi-to-Serial adapter (TCP transparent mode)
DEFAULT_PORT: Final = 8899

# Connection settings
RECONNECT_INTERVAL: Final = 30
CONNECTION_TIMEOUT: Final = 10

# Temperature limits (Celsius)
MIN_TEMP: Final = 10.0
MAX_TEMP: Final = 40.0

# Temperature limits (Fahrenheit)
MIN_TEMP_F: Final = 50.0
MAX_TEMP_F: Final = 104.0

# Dispatcher signal for updates
UPDATE_SIGNAL: Final = f"{DOMAIN}_update"
