"""Balboa protocol implementation for Sundance Spa via EW11 RS485-to-TCP bridge.

Based on the protocol used by balboa_worldwide_app (https://github.com/ccutrer/balboa_worldwide_app)
which is also used by bwalink (https://github.com/jshank/bwalink).

The Balboa protocol is a simple framed message protocol:
- Start delimiter: 0x7E (~)
- Length byte (includes everything from length to checksum, excluding delimiters)
- Source address (0x0A for WiFi clients)
- Message type (2 bytes)
- Payload (variable)
- CRC-8 checksum
- End delimiter: 0x7E (~)
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable
from enum import IntEnum

_LOGGER = logging.getLogger(__name__)

# Message delimiters
MSG_START = 0x7E
MSG_END = 0x7E

# Message types (2 bytes combined, but we use the second byte as the type ID)
# Format: 0xAF <type>
MSG_TYPE_STATUS = 0x13          # Status update from spa (0xAF 0x13)
MSG_TYPE_FILTER_CYCLES = 0x23   # Filter cycles info
MSG_TYPE_INFO = 0x24            # System info response
MSG_TYPE_SETTINGS = 0x25        # Settings response
MSG_TYPE_SETUP_PARAMS = 0x26    # Setup parameters
MSG_TYPE_CONFIG = 0x2E          # Configuration response (Control Configuration 2)
MSG_TYPE_CONFIG_REQ = 0x04      # Configuration request (what we send)
MSG_TYPE_TOGGLE_ITEM = 0x11     # Toggle item command
MSG_TYPE_SET_TEMP = 0x20        # Set temperature command
MSG_TYPE_SET_TIME = 0x21        # Set time command
MSG_TYPE_SET_TEMP_SCALE = 0x27  # Set temperature scale
MSG_TYPE_READY = 0x14           # Ready to receive (Clear-To-Send)
MSG_TYPE_NOTHING_TO_SEND = 0x06 # Nothing to send

# Default source address for WiFi clients
SRC_WIFI = 0x0A


class HeatMode(IntEnum):
    """Heat mode enumeration."""
    READY = 0
    REST = 1
    READY_IN_REST = 2


class HeatState(IntEnum):
    """Heat state enumeration."""
    OFF = 0
    HEATING = 1
    HEAT_WAITING = 2


class TempRange(IntEnum):
    """Temperature range enumeration."""
    LOW = 0
    HIGH = 1


class PumpState(IntEnum):
    """Pump state enumeration."""
    OFF = 0
    LOW = 1
    HIGH = 2


class ToggleItem(IntEnum):
    """Toggle item codes."""
    PUMP1 = 0x04
    PUMP2 = 0x05
    PUMP3 = 0x06
    PUMP4 = 0x07
    PUMP5 = 0x08
    PUMP6 = 0x09
    LIGHT1 = 0x11
    LIGHT2 = 0x12
    AUX1 = 0x16
    AUX2 = 0x17
    MISTER = 0x0E
    BLOWER = 0x0C
    HOLD = 0x3C
    TEMP_RANGE = 0x50
    HEAT_MODE = 0x51


@dataclass
class SpaStatus:
    """Current spa status data."""
    current_temp: float | None = None
    target_temp: float | None = None
    temp_scale_celsius: bool = False
    temp_range: TempRange = TempRange.HIGH
    heat_mode: HeatMode = HeatMode.READY
    heating: bool = False
    pump1: int = 0
    pump2: int = 0
    pump3: int = 0
    pump4: int = 0
    pump5: int = 0
    pump6: int = 0
    blower: int = 0
    light1: bool = False
    light2: bool = False
    mister: bool = False
    aux1: bool = False
    aux2: bool = False
    circ_pump: bool = False
    filter1_running: bool = False
    filter2_running: bool = False
    hour: int = 0
    minute: int = 0
    clock_24hr: bool = True
    priming: bool = False
    hold_mode: bool = False


@dataclass
class SpaConfig:
    """Spa configuration data."""
    model: str = ""
    software_id: str = ""
    pump_count: int = 2
    pump_speeds: list[int] = field(default_factory=lambda: [2, 2, 0, 0, 0, 0])
    has_blower: bool = False
    blower_speeds: int = 0
    has_mister: bool = False
    has_aux1: bool = False
    has_aux2: bool = False
    has_circ_pump: bool = True
    light_count: int = 1


def crc8_checksum(data: bytes) -> int:
    """Calculate CRC-8 checksum for Balboa protocol.
    
    Algorithm from balboa_worldwide_app Ruby implementation.
    """
    crc = 0x02  # Initial value
    for byte in data:
        for i in range(8):
            bit = crc & 0x80
            crc = ((crc << 1) & 0xFF) | ((byte >> (7 - i)) & 0x01)
            if bit:
                crc ^= 0x07
    # Final 8 iterations with no input
    for _ in range(8):
        bit = crc & 0x80
        crc = (crc << 1) & 0xFF
        if bit:
            crc ^= 0x07
    return crc ^ 0x02


def build_message(src: int, msg_type_bytes: bytes, payload: bytes = b"") -> bytes:
    """Build a Balboa protocol message.
    
    Args:
        src: Source address (0x0A for WiFi)
        msg_type_bytes: 2-byte message type (e.g., b'\\xAF\\x13')
        payload: Message payload
    """
    # Content = src + msg_type (2 bytes) + payload
    content = bytes([src]) + msg_type_bytes + payload
    # Length = content + 2 (for length byte itself and checksum)
    length = len(content) + 2
    # Message without delimiters for CRC calculation
    msg_body = bytes([length]) + content
    crc = crc8_checksum(msg_body)
    return bytes([MSG_START]) + msg_body + bytes([crc, MSG_END])


def parse_message(data: bytes) -> tuple[int, bytes, bytes] | None:
    """Parse a Balboa protocol message.
    
    Returns: (src, msg_type_bytes, payload) or None if invalid.
    """
    if len(data) < 7:
        return None
    if data[0] != MSG_START or data[-1] != MSG_END:
        return None
    
    length = data[1]
    if len(data) != length + 2:
        _LOGGER.debug("Length mismatch: expected %d, got %d", length + 2, len(data))
        return None
    
    # Verify checksum
    expected_crc = crc8_checksum(data[1:-2])
    actual_crc = data[-2]
    if expected_crc != actual_crc:
        _LOGGER.debug("CRC mismatch: expected %02X, got %02X", expected_crc, actual_crc)
        # Accept anyway for compatibility - some firmware has different CRC
    
    src = data[2]
    msg_type = data[3:5]  # 2 bytes
    payload = data[5:-2]
    return src, msg_type, payload


class SpaClient:
    """Client for communicating with Balboa spa via EW11 TCP bridge."""
    
    # Message type constants as bytes
    MT_STATUS = b'\xAF\x13'
    MT_FILTER = b'\xAF\x23'
    MT_INFO = b'\xAF\x24'
    MT_SETTINGS = b'\xAF\x25'
    MT_SETUP = b'\xAF\x26'
    MT_CONFIG = b'\xAF\x2E'
    MT_READY = b'\xAF\x14'
    MT_NTS = b'\xAF\x06'
    MT_CONFIG_REQ = b'\xBF\x04'  # 0xBF for requests
    MT_TOGGLE = b'\xBF\x11'
    MT_SET_TEMP = b'\xBF\x20'
    MT_SET_TIME = b'\xBF\x21'
    MT_SET_SCALE = b'\xBF\x27'
    
    def __init__(self, host: str, port: int = 8899) -> None:
        """Initialize the spa client."""
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._status = SpaStatus()
        self._config = SpaConfig()
        self._config_loaded = False
        self._update_callbacks: list[Callable[[], None]] = []
        self._receive_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._buffer = bytearray()
        self._message_queue: list[bytes] = []
        self._config_event = asyncio.Event()
        self._status_received = asyncio.Event()
    
    @property
    def host(self) -> str:
        return self._host
    
    @property
    def connected(self) -> bool:
        return self._connected
    
    @property
    def status(self) -> SpaStatus:
        return self._status
    
    @property
    def config(self) -> SpaConfig:
        return self._config
    
    @property
    def model(self) -> str:
        return self._config.model
    
    @property
    def temperature(self) -> float | None:
        return self._status.current_temp
    
    @property
    def target_temperature(self) -> float | None:
        return self._status.target_temp
    
    @property
    def temperature_unit_celsius(self) -> bool:
        return self._status.temp_scale_celsius
    
    @property
    def temperature_minimum(self) -> float:
        if self._status.temp_scale_celsius:
            return 10.0 if self._status.temp_range == TempRange.LOW else 26.0
        return 50.0 if self._status.temp_range == TempRange.LOW else 80.0
    
    @property
    def temperature_maximum(self) -> float:
        if self._status.temp_scale_celsius:
            return 37.0 if self._status.temp_range == TempRange.LOW else 40.0
        return 99.0 if self._status.temp_range == TempRange.LOW else 104.0
    
    @property
    def heat_mode(self) -> HeatMode:
        return self._status.heat_mode
    
    @property
    def heat_state(self) -> HeatState:
        if self._status.heating:
            return HeatState.HEATING
        return HeatState.OFF
    
    def add_update_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Add a callback for status updates."""
        self._update_callbacks.append(callback)
        
        def remove():
            if callback in self._update_callbacks:
                self._update_callbacks.remove(callback)
        
        return remove
    
    def _notify_update(self) -> None:
        """Notify all callbacks of status update."""
        for cb in self._update_callbacks:
            try:
                cb()
            except Exception as err:
                _LOGGER.error("Callback error: %s", err)
    
    async def connect(self) -> bool:
        """Connect to spa via EW11 bridge."""
        try:
            _LOGGER.info("Connecting to spa at %s:%d", self._host, self._port)
            
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=10,
            )
            
            self._connected = True
            self._buffer.clear()
            self._message_queue.clear()
            self._config_event.clear()
            self._status_received.clear()
            self._config_loaded = False
            
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            _LOGGER.info("Connected to spa")
            return True
            
        except asyncio.TimeoutError:
            _LOGGER.error("Connection timeout to %s:%d", self._host, self._port)
            return False
        except Exception as err:
            _LOGGER.error("Connection error: %s", err)
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from spa."""
        self._connected = False
        
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None
        
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
        
        _LOGGER.info("Disconnected from spa")
    
    async def async_configuration_loaded(self, timeout: float = 30.0) -> bool:
        """Wait for initial configuration to be loaded."""
        try:
            # First wait for any status message to confirm communication
            await asyncio.wait_for(
                self._status_received.wait(),
                timeout=timeout
            )
            _LOGGER.info("Received first status message from spa")
            
            # Request configuration
            await self._request_config(1)  # Control config
            await asyncio.sleep(0.5)
            await self._request_config(2)  # Panel config
            await asyncio.sleep(0.5)
            await self._request_config(3)  # Filter cycles
            
            # Wait for config response
            try:
                await asyncio.wait_for(
                    self._config_event.wait(),
                    timeout=10.0
                )
                _LOGGER.info("Configuration loaded successfully")
            except asyncio.TimeoutError:
                _LOGGER.warning("Config request timeout - using defaults for Sundance Cameo 880")
                self._use_cameo_880_defaults()
            
            self._config_loaded = True
            return True
            
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout waiting for spa status - check EW11 configuration")
            return False
    
    def _use_cameo_880_defaults(self) -> None:
        """Set default configuration for Sundance Cameo 880."""
        self._config.model = "Sundance Cameo 880"
        self._config.pump_count = 3
        self._config.pump_speeds = [2, 2, 1, 0, 0, 0]  # 3 pumps
        self._config.has_blower = False
        self._config.has_circ_pump = True
        self._config.light_count = 1
        _LOGGER.info("Using Cameo 880 default configuration")
    
    async def _receive_loop(self) -> None:
        """Receive and process messages."""
        while self._connected and self._reader:
            try:
                data = await asyncio.wait_for(
                    self._reader.read(1024),
                    timeout=60,
                )
                
                if not data:
                    _LOGGER.warning("Connection closed by spa")
                    self._connected = False
                    self._notify_update()
                    break
                
                self._buffer.extend(data)
                await self._process_buffer()
                
            except asyncio.TimeoutError:
                _LOGGER.debug("No data for 60s - connection may be idle")
                # Don't disconnect - spa may just be idle
                continue
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Receive error: %s", err)
                self._connected = False
                self._notify_update()
                break
    
    async def _process_buffer(self) -> None:
        """Process received data buffer."""
        while True:
            # Find start of message
            try:
                start = self._buffer.index(MSG_START)
                if start > 0:
                    _LOGGER.debug("Discarding %d bytes before message start", start)
                    self._buffer = self._buffer[start:]
            except ValueError:
                self._buffer.clear()
                return
            
            # Need at least 2 bytes to get length
            if len(self._buffer) < 2:
                return
            
            length = self._buffer[1]
            msg_len = length + 2  # +2 for start delimiter and length byte
            
            # Sanity check
            if length < 5 or length >= 0x7E:
                _LOGGER.debug("Invalid message length %d, skipping byte", length)
                self._buffer = self._buffer[1:]
                continue
            
            # Wait for complete message
            if len(self._buffer) < msg_len:
                return
            
            # Check end delimiter
            if self._buffer[msg_len - 1] != MSG_END:
                _LOGGER.debug("Missing end delimiter at position %d", msg_len - 1)
                self._buffer = self._buffer[1:]
                continue
            
            # Extract and process message
            msg_data = bytes(self._buffer[:msg_len])
            self._buffer = self._buffer[msg_len:]
            
            parsed = parse_message(msg_data)
            if parsed:
                src, msg_type, payload = parsed
                await self._handle_message(src, msg_type, payload)
            else:
                _LOGGER.debug("Failed to parse message: %s", msg_data.hex())
    
    async def _handle_message(self, src: int, msg_type: bytes, payload: bytes) -> None:
        """Handle received message."""
        _LOGGER.debug("Rx: src=%02X type=%s payload=%s", 
                      src, msg_type.hex(), payload.hex() if payload else "")
        
        if msg_type == self.MT_STATUS:
            self._parse_status(payload)
            self._status_received.set()
            self._notify_update()
        
        elif msg_type == self.MT_CONFIG:
            self._parse_config(payload)
            self._config_event.set()
        
        elif msg_type == self.MT_INFO:
            self._parse_info(payload)
        
        elif msg_type == self.MT_FILTER:
            self._parse_filter_cycles(payload)
        
        elif msg_type == self.MT_READY:
            # Spa is ready to receive - send queued message
            if self._message_queue:
                msg = self._message_queue.pop(0)
                await self._send_raw(msg)
        
        elif msg_type == self.MT_NTS:
            # Nothing to send - spa is idle
            pass
        
        else:
            _LOGGER.debug("Unknown message type: %s", msg_type.hex())
    
    def _parse_status(self, payload: bytes) -> None:
        """Parse status message (0xAF 0x13)."""
        if len(payload) < 20:
            _LOGGER.warning("Status message too short: %d bytes", len(payload))
            return
        
        # Byte 0: flags (hold mode)
        self._status.hold_mode = (payload[0] & 0x05) != 0
        
        # Byte 1: priming flag
        self._status.priming = payload[1] == 0x01
        
        # Byte 2: current temperature (0xFF = unknown)
        if payload[2] != 0xFF:
            temp = payload[2]
            if self._status.temp_scale_celsius:
                self._status.current_temp = temp / 2.0
            else:
                self._status.current_temp = float(temp)
        else:
            self._status.current_temp = None
        
        # Byte 3-4: hour and minute
        self._status.hour = payload[3]
        self._status.minute = payload[4]
        
        # Byte 5: heat mode (bits 0-1)
        heat_mode = payload[5] & 0x03
        if heat_mode == 0:
            self._status.heat_mode = HeatMode.READY
        elif heat_mode == 1:
            self._status.heat_mode = HeatMode.REST
        elif heat_mode == 2:
            self._status.heat_mode = HeatMode.READY_IN_REST
        
        # Byte 9: misc flags
        flags9 = payload[9]
        self._status.temp_scale_celsius = (flags9 & 0x01) != 0
        self._status.clock_24hr = (flags9 & 0x02) != 0
        self._status.filter1_running = (flags9 & 0x04) != 0
        self._status.filter2_running = (flags9 & 0x08) != 0
        
        # Byte 10: heat state and temp range
        flags10 = payload[10]
        self._status.heating = (flags10 & 0x30) != 0
        self._status.temp_range = TempRange.HIGH if (flags10 & 0x04) else TempRange.LOW
        
        # Byte 11: pumps 1-4
        flags11 = payload[11]
        self._status.pump1 = flags11 & 0x03
        self._status.pump2 = (flags11 >> 2) & 0x03
        self._status.pump3 = (flags11 >> 4) & 0x03
        self._status.pump4 = (flags11 >> 6) & 0x03
        
        # Byte 12: pumps 5-6
        flags12 = payload[12]
        self._status.pump5 = flags12 & 0x03
        self._status.pump6 = (flags12 >> 2) & 0x03
        
        # Byte 13: circ pump and blower
        flags13 = payload[13]
        self._status.circ_pump = (flags13 & 0x02) != 0
        self._status.blower = (flags13 >> 2) & 0x03
        
        # Byte 14: lights
        flags14 = payload[14]
        self._status.light1 = (flags14 & 0x03) != 0
        self._status.light2 = ((flags14 >> 2) & 0x03) != 0
        
        # Byte 15: mister and aux
        flags15 = payload[15]
        self._status.mister = (flags15 & 0x01) != 0
        self._status.aux1 = (flags15 & 0x08) != 0
        self._status.aux2 = (flags15 & 0x10) != 0
        
        # Byte 20: target temperature
        if len(payload) > 20:
            target = payload[20]
            if self._status.temp_scale_celsius:
                self._status.target_temp = target / 2.0
            else:
                self._status.target_temp = float(target)
        
        _LOGGER.debug(
            "Status: temp=%s target=%s heating=%s pumps=[%d,%d,%d] lights=[%s,%s]",
            self._status.current_temp,
            self._status.target_temp,
            self._status.heating,
            self._status.pump1, self._status.pump2, self._status.pump3,
            self._status.light1, self._status.light2
        )
    
    def _parse_config(self, payload: bytes) -> None:
        """Parse configuration message (0xAF 0x2E)."""
        if len(payload) < 5:
            return
        
        _LOGGER.debug("Parsing config: %s", payload.hex())
        
        # Pump configuration is in various bytes
        # This varies by model - using simplified parsing
        if len(payload) >= 6:
            pump_info = payload[4]
            self._config.pump_speeds[0] = pump_info & 0x03
            self._config.pump_speeds[1] = (pump_info >> 2) & 0x03
            self._config.pump_speeds[2] = (pump_info >> 4) & 0x03
            self._config.pump_speeds[3] = (pump_info >> 6) & 0x03
            
            # Count pumps
            self._config.pump_count = sum(1 for s in self._config.pump_speeds if s > 0)
        
        if len(payload) >= 7:
            misc = payload[5]
            self._config.has_circ_pump = (misc & 0x02) != 0
            self._config.has_blower = (misc & 0x0C) != 0
            self._config.blower_speeds = (misc >> 2) & 0x03
        
        if len(payload) >= 8:
            light_info = payload[6]
            self._config.light_count = 1 if (light_info & 0x03) else 0
            if light_info & 0x0C:
                self._config.light_count = 2
        
        _LOGGER.info("Config: pumps=%d speeds=%s circ=%s blower=%s lights=%d",
                     self._config.pump_count, self._config.pump_speeds,
                     self._config.has_circ_pump, self._config.has_blower,
                     self._config.light_count)
    
    def _parse_info(self, payload: bytes) -> None:
        """Parse system info message (0xAF 0x24)."""
        if len(payload) >= 3:
            # Model info is encoded in these bytes
            model_bytes = payload[:3]
            self._config.model = f"M{model_bytes[0]:d}_V{model_bytes[1]:d}.{model_bytes[2]:d}"
            _LOGGER.info("Spa model: %s", self._config.model)
    
    def _parse_filter_cycles(self, payload: bytes) -> None:
        """Parse filter cycles message (0xAF 0x23)."""
        _LOGGER.debug("Filter cycles: %s", payload.hex())
    
    async def _send_raw(self, data: bytes) -> bool:
        """Send raw data to spa."""
        if not self._connected or not self._writer:
            return False
        
        async with self._lock:
            try:
                _LOGGER.debug("Tx: %s", data.hex())
                self._writer.write(data)
                await self._writer.drain()
                return True
            except Exception as err:
                _LOGGER.error("Send error: %s", err)
                self._connected = False
                return False
    
    async def _send_message(self, msg_type: bytes, payload: bytes = b"") -> bool:
        """Queue a message to send (will be sent on next Ready)."""
        msg = build_message(SRC_WIFI, msg_type, payload)
        
        # For immediate sending (most commands work this way with EW11)
        return await self._send_raw(msg)
    
    async def _request_config(self, config_type: int) -> None:
        """Request configuration from spa."""
        # Config request: type byte as payload
        payload = bytes([config_type, 0x00, 0x00])
        await self._send_message(self.MT_CONFIG_REQ, payload)
        _LOGGER.debug("Requested config type %d", config_type)
    
    async def toggle_pump(self, pump_num: int) -> None:
        """Toggle a pump (1-6)."""
        if pump_num < 1 or pump_num > 6:
            return
        item = ToggleItem.PUMP1 + (pump_num - 1)
        await self._send_message(self.MT_TOGGLE, bytes([item, 0x00]))
        _LOGGER.debug("Toggled pump %d", pump_num)
    
    async def toggle_light(self, light_num: int) -> None:
        """Toggle a light (1-2)."""
        if light_num < 1 or light_num > 2:
            return
        item = ToggleItem.LIGHT1 if light_num == 1 else ToggleItem.LIGHT2
        await self._send_message(self.MT_TOGGLE, bytes([item, 0x00]))
        _LOGGER.debug("Toggled light %d", light_num)
    
    async def toggle_blower(self) -> None:
        """Toggle the blower."""
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.BLOWER, 0x00]))
        _LOGGER.debug("Toggled blower")
    
    async def toggle_mister(self) -> None:
        """Toggle the mister."""
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.MISTER, 0x00]))
        _LOGGER.debug("Toggled mister")
    
    async def toggle_heat_mode(self) -> None:
        """Toggle heat mode (ready/rest)."""
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.HEAT_MODE, 0x00]))
        _LOGGER.debug("Toggled heat mode")
    
    async def toggle_temp_range(self) -> None:
        """Toggle temperature range (high/low)."""
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.TEMP_RANGE, 0x00]))
        _LOGGER.debug("Toggled temp range")
    
    async def set_target_temperature(self, temp: float) -> None:
        """Set target temperature."""
        # Convert to wire format
        if self._status.temp_scale_celsius:
            wire_temp = int(temp * 2)
        else:
            wire_temp = int(temp)
        
        # Clamp to valid range
        wire_temp = max(0, min(255, wire_temp))
        
        await self._send_message(self.MT_SET_TEMP, bytes([wire_temp]))
        _LOGGER.debug("Set target temp to %s (wire: %d)", temp, wire_temp)
    
    async def set_time(self, hour: int, minute: int, is_24h: bool = True) -> None:
        """Set spa time."""
        flags = 0x80 if is_24h else 0x00
        await self._send_message(self.MT_SET_TIME, bytes([flags | hour, minute]))
        _LOGGER.debug("Set time to %02d:%02d", hour, minute)
    
    async def set_pump(self, pump_num: int, speed: int) -> None:
        """Set pump to specific speed (cycles through speeds)."""
        if pump_num < 1 or pump_num > 6:
            return
        
        current = getattr(self._status, f"pump{pump_num}", 0)
        max_speed = self._config.pump_speeds[pump_num - 1]
        
        if max_speed == 0:
            return
        
        # Calculate toggles needed to reach desired speed
        toggles = (speed - current) % (max_speed + 1)
        
        for i in range(toggles):
            await self.toggle_pump(pump_num)
            if i < toggles - 1:
                await asyncio.sleep(0.2)
    
    async def set_light(self, light_num: int, on: bool) -> None:
        """Set light state."""
        current = getattr(self._status, f"light{light_num}", False)
        if current != on:
            await self.toggle_light(light_num)
