"""Async TCP client for Sundance/Balboa Spa communication.

Based on the balboa_worldwide_app Ruby implementation:
https://github.com/ccutrer/balboa_worldwide_app

Protocol details:
- Message format: 7E [LENGTH] [SRC] [MSG_TYPE_1] [MSG_TYPE_2] [DATA...] [CRC] 7E
- LENGTH is the count from LENGTH to CRC (inclusive)
- CRC is CRC-8 with init=0x02 and XOR=0x02
- Status message type: 0xAF 0x13
- Toggle message type: 0xBF 0x11
- Ready message type: 0xBF 0x06 (spa ready to receive command)
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

_LOGGER = logging.getLogger(__name__)

# Protocol constants
M_STARTEND = 0x7E
DEFAULT_PORT = 8899

# Message type identifiers (2 bytes each)
# Status: sent by spa to all listeners (0xAF 0x13)
MSG_TYPE_STATUS = b"\xaf\x13"
# Ready: spa is ready to receive a command (0xBF 0x06)
MSG_TYPE_READY = b"\xbf\x06"
# New client clear to send (0xBF 0x07)
MSG_TYPE_NEW_CLIENT_CTS = b"\xbf\x07"
# Toggle item: sent to spa to toggle pumps/lights/etc (0xBF 0x11)
MSG_TYPE_TOGGLE = b"\xbf\x11"
# Set temperature (0xBF 0x20)
MSG_TYPE_SET_TEMP = b"\xbf\x20"
# Set time (0xBF 0x21)
MSG_TYPE_SET_TIME = b"\xbf\x21"
# Configuration request (0xBF 0x04)
MSG_TYPE_CONFIG_REQ = b"\xbf\x04"
# Configuration response (0xAF 0x24)
MSG_TYPE_CONFIG = b"\xaf\x24"
# Control configuration (0xAF 0x26)
MSG_TYPE_CONTROL_CONFIG = b"\xaf\x26"

# Toggle item codes (from balboa_worldwide_app)
TOGGLE_NORMAL_OPERATION = 0x01
TOGGLE_CLEAR_NOTIFICATION = 0x03
TOGGLE_PUMP1 = 0x04
TOGGLE_PUMP2 = 0x05
TOGGLE_PUMP3 = 0x06
TOGGLE_PUMP4 = 0x07
TOGGLE_PUMP5 = 0x08
TOGGLE_PUMP6 = 0x09
TOGGLE_BLOWER = 0x0C
TOGGLE_MISTER = 0x0E
TOGGLE_LIGHT1 = 0x11
TOGGLE_LIGHT2 = 0x12
TOGGLE_AUX1 = 0x16
TOGGLE_AUX2 = 0x17
TOGGLE_SOAK = 0x1D
TOGGLE_HOLD = 0x3C
TOGGLE_TEMP_RANGE = 0x50
TOGGLE_HEAT_MODE = 0x51

# Source addresses
SRC_WIFI_MODULE = 0x0A  # WiFi module address
SRC_BROADCAST = 0xFF    # Broadcast address


@dataclass
class SpaState:
    """Data class holding the current spa state."""
    
    # Connection state
    connected: bool = False
    
    # Temperature (in Celsius for internal use)
    current_temp: float | None = None
    target_temp: float | None = None
    temp_scale_celsius: bool = False
    
    # Heating
    is_heating: bool = False
    heat_mode: str = "ready"  # ready, rest, ready_in_rest
    temperature_range: str = "high"  # high, low
    
    # Pumps (0=off, 1=low, 2=high)
    pump1_speed: int = 0
    pump2_speed: int = 0
    pump3_speed: int = 0
    pump4_speed: int = 0
    pump5_speed: int = 0
    pump6_speed: int = 0
    
    # Other accessories
    circ_pump_on: bool = False
    blower: int = 0
    mister: bool = False
    
    # Lights
    light1_on: bool = False
    light2_on: bool = False
    
    # Aux
    aux1_on: bool = False
    aux2_on: bool = False
    
    # Time
    time_hour: int = 0
    time_minute: int = 0
    twenty_four_hour_time: bool = False
    
    # Filter cycles
    filter1_running: bool = False
    filter2_running: bool = False
    
    # Status flags
    priming: bool = False
    hold: bool = False
    notification: str | None = None
    
    # Stats
    last_update: float = 0.0
    packets_received: int = 0
    valid_messages: int = 0
    status_updates: int = 0
    ready_messages: int = 0
    commands_sent: int = 0
    last_raw_status: str = ""
    crc_errors: int = 0
    
    @property
    def pump1_on(self) -> bool:
        return self.pump1_speed > 0
    
    @property
    def pump2_on(self) -> bool:
        return self.pump2_speed > 0
    
    @property
    def light_on(self) -> bool:
        return self.light1_on


class SundanceElfinClient:
    """Async TCP client for Sundance Spa via Elfin-EW11A RS485-WiFi adapter.
    
    This client implements the Balboa protocol as documented in balboa_worldwide_app.
    
    Key protocol insight: The spa sends "Ready" messages (0xBF 0x06) to indicate
    it's ready to receive a command. Commands should only be sent after receiving
    a Ready message. This implementation queues commands and sends them when Ready.
    """
    
    def __init__(self, host: str, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        self.state = SpaState()
        
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._listen_task: asyncio.Task | None = None
        self._running = False
        self._callbacks: list[Callable[[], None]] = []
        self._lock = asyncio.Lock()
        self._buffer = b""
        
        # Command queue - commands are queued and sent when spa sends Ready
        self._command_queue: deque[bytes] = deque(maxlen=10)
        self._last_ready_time: float = 0.0
        
    def register_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Register a callback for state changes."""
        self._callbacks.append(callback)
        
        def unregister():
            if callback in self._callbacks:
                self._callbacks.remove(callback)
        return unregister
    
    def _notify(self) -> None:
        """Notify all callbacks."""
        for cb in self._callbacks:
            try:
                cb()
            except Exception:
                _LOGGER.exception("Callback error")
    
    @staticmethod
    def crc8(data: bytes) -> int:
        """Calculate CRC-8 checksum per Balboa spec.
        
        From balboa_worldwide_app/lib/bwa/crc.rb:
        - Uses CRC-8 polynomial 0x07
        - INIT_CRC = 0x02
        - XOR_MASK = 0x02
        """
        crc = 0x02  # Initial value
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x80:
                    crc = ((crc << 1) ^ 0x07) & 0xFF
                else:
                    crc = (crc << 1) & 0xFF
        return crc ^ 0x02  # XOR with mask
    
    async def connect(self) -> bool:
        """Connect to the spa."""
        try:
            _LOGGER.debug("Connecting to %s:%s...", self.host, self.port)
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=10.0
            )
            self.state.connected = True
            self._buffer = b""
            self._command_queue.clear()
            _LOGGER.info("Connected to Sundance Spa at %s:%s", self.host, self.port)
            self._notify()
            return True
        except Exception as e:
            _LOGGER.error("Connection failed: %s", e)
            self.state.connected = False
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from the spa."""
        self._running = False
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None
        self.state.connected = False
        _LOGGER.info("Disconnected from Sundance Spa")
        self._notify()
    
    async def start(self) -> None:
        """Start the client."""
        self._running = True
        if await self.connect():
            self._listen_task = asyncio.create_task(self._listen_loop())
    
    async def stop(self) -> None:
        """Stop the client."""
        await self.disconnect()
    
    async def _listen_loop(self) -> None:
        """Main receive loop."""
        reconnect_delay = 5
        
        while self._running:
            try:
                if not self._reader or not self.state.connected:
                    _LOGGER.debug("Not connected, waiting %ds before reconnect...", reconnect_delay)
                    await asyncio.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, 60)
                    if self._running:
                        if await self.connect():
                            reconnect_delay = 5
                    continue
                
                # Read available data
                try:
                    chunk = await asyncio.wait_for(
                        self._reader.read(1024),
                        timeout=30.0
                    )
                except asyncio.TimeoutError:
                    _LOGGER.debug("Read timeout - connection may be idle")
                    continue
                
                if not chunk:
                    _LOGGER.debug("Connection closed by remote")
                    self.state.connected = False
                    self._notify()
                    continue
                
                self._buffer += chunk
                self.state.packets_received += 1
                
                # Process all complete messages in buffer
                await self._process_buffer()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                _LOGGER.error("Listen error: %s", e)
                self.state.connected = False
                self._notify()
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
                if self._running:
                    await self.connect()
    
    async def _process_buffer(self) -> None:
        """Process complete messages from the buffer.
        
        Message format (from balboa_worldwide_app):
        [0x7E] [LENGTH] [SRC] [TYPE1] [TYPE2] [DATA...] [CRC] [0x7E]
        
        LENGTH = number of bytes from LENGTH to CRC (inclusive)
        So total message length = LENGTH + 2 (for start and end bytes)
        """
        while len(self._buffer) >= 5:  # Minimum message size
            # Find start byte
            start_idx = self._buffer.find(bytes([M_STARTEND]))
            if start_idx == -1:
                self._buffer = b""
                return
            
            # Discard bytes before start
            if start_idx > 0:
                _LOGGER.debug("Discarding %d bytes before message start", start_idx)
                self._buffer = self._buffer[start_idx:]
            
            if len(self._buffer) < 2:
                return
            
            # Get length byte
            length = self._buffer[1]
            
            # Sanity check length (valid range is 5-126 per balboa_worldwide_app)
            if length < 5 or length >= M_STARTEND:
                _LOGGER.debug("Invalid length %d, skipping byte", length)
                self._buffer = self._buffer[1:]
                continue
            
            # Check if we have the full message
            total_len = length + 2  # +2 for start byte and end byte
            if len(self._buffer) < total_len:
                return  # Wait for more data
            
            # Check end byte
            if self._buffer[total_len - 1] != M_STARTEND:
                _LOGGER.debug("Missing end byte at position %d", total_len - 1)
                self._buffer = self._buffer[1:]
                continue
            
            # Extract message
            message = self._buffer[:total_len]
            
            # Verify CRC (calculated over bytes from LENGTH to before CRC)
            crc_data = message[1:total_len - 2]  # LENGTH through DATA
            calculated_crc = self.crc8(crc_data)
            received_crc = message[total_len - 2]
            
            if calculated_crc != received_crc:
                self.state.crc_errors += 1
                _LOGGER.debug("CRC error #%d: calc=0x%02x recv=0x%02x", 
                            self.state.crc_errors, calculated_crc, received_crc)
                self._buffer = self._buffer[1:]
                continue
            
            # Valid message!
            self.state.valid_messages += 1
            
            # Remove from buffer and process
            self._buffer = self._buffer[total_len:]
            await self._process_message(message)
    
    async def _process_message(self, msg: bytes) -> None:
        """Process a validated message.
        
        Format: 7E [LEN] [SRC] [TYPE1] [TYPE2] [DATA...] [CRC] 7E
        """
        if len(msg) < 7:  # Minimum: 7E LEN SRC T1 T2 CRC 7E
            return
        
        src = msg[2]
        msg_type = msg[3:5]  # Two-byte message type
        data = msg[5:-2]     # Data between type and CRC
        
        # Status message (0xAF 0x13)
        if msg_type == MSG_TYPE_STATUS:
            self._parse_status(data)
        
        # Ready message (0xBF 0x06) - spa is ready to receive a command
        elif msg_type == MSG_TYPE_READY:
            self.state.ready_messages += 1
            self._last_ready_time = time.time()
            await self._send_queued_command()
        
        # New client clear to send (0xBF 0x07)
        elif msg_type == MSG_TYPE_NEW_CLIENT_CTS:
            _LOGGER.debug("New client CTS received")
            await self._send_queued_command()
        
        # Configuration response
        elif msg_type == MSG_TYPE_CONFIG:
            _LOGGER.debug("Configuration received: %s", data.hex())
        
        # Control configuration
        elif msg_type == MSG_TYPE_CONTROL_CONFIG:
            _LOGGER.debug("Control configuration received: %s", data.hex())
        
        # Unknown message type - log only occasionally
        else:
            _LOGGER.debug("Unknown msg type: %s", msg_type.hex())
    
    def _parse_status(self, data: bytes) -> None:
        """Parse status message data.
        
        Based on balboa_worldwide_app/lib/bwa/messages/status.rb:
        data[0] = flags (hold: 0x05 mask)
        data[1] = priming flag (0x01 = priming, 0x03 = notification)
        data[2] = current temperature (0xFF = unknown)
        data[3] = hour
        data[4] = minute
        data[5] = heating mode flags (bits 0-1: 0=ready, 1=rest, 2=ready_in_rest)
        data[6] = notification type
        data[7-8] = ?
        data[9] = temp scale (bit 0=celsius), 24h time (bit 1), filter cycles (bits 2-3)
        data[10] = heating (bits 4-5), temp range (bit 2=high)
        data[11] = pumps 1-4 state (2 bits each)
        data[12] = pumps 5-6 state
        data[13] = circ pump (bit 1), blower (bits 2-3)
        data[14] = lights 1-2
        data[15] = mister (bit 0), aux1 (bit 3), aux2 (bit 4)
        ...
        data[20] = target temperature
        """
        # Status messages are 23-32 bytes per balboa_worldwide_app
        if len(data) < 23:
            _LOGGER.debug("Status message too short: %d bytes (need 23+)", len(data))
            return
        
        # Store raw hex for diagnostics
        self.state.last_raw_status = data.hex()
        
        try:
            # Hold mode (flags in data[0])
            self.state.hold = (data[0] & 0x05) != 0
            
            # Priming/notification (data[1])
            self.state.priming = data[1] == 0x01
            
            # Notification handling
            if data[1] == 0x03:
                notifications = {0x0A: "ph", 0x04: "filter", 0x09: "sanitizer"}
                self.state.notification = notifications.get(data[6])
            else:
                self.state.notification = None
            
            # Temperature scale from data[9] - need this first for temp conversion
            self.state.temp_scale_celsius = (data[9] & 0x01) == 0x01
            self.state.twenty_four_hour_time = (data[9] & 0x02) == 0x02
            self.state.filter1_running = (data[9] & 0x04) != 0
            self.state.filter2_running = (data[9] & 0x08) != 0
            
            # Current temperature (data[2])
            raw_temp = data[2]
            if raw_temp != 0xFF:
                if self.state.temp_scale_celsius:
                    self.state.current_temp = raw_temp / 2.0
                else:
                    # Fahrenheit - keep as is (Home Assistant will handle conversion)
                    self.state.current_temp = raw_temp
            else:
                self.state.current_temp = None
            
            # Time (data[3], data[4])
            self.state.time_hour = data[3]
            self.state.time_minute = data[4]
            
            # Heating mode from data[5]
            heat_mode_val = data[5] & 0x03
            self.state.heat_mode = {0: "ready", 1: "rest", 2: "ready_in_rest"}.get(heat_mode_val, "ready")
            
            # Heating state and temp range from data[10]
            self.state.is_heating = (data[10] & 0x30) != 0
            self.state.temperature_range = "high" if (data[10] & 0x04) else "low"
            
            # Pump states from data[11] - 2 bits each for pumps 1-4
            pumps = data[11]
            self.state.pump1_speed = pumps & 0x03
            self.state.pump2_speed = (pumps >> 2) & 0x03
            self.state.pump3_speed = (pumps >> 4) & 0x03
            self.state.pump4_speed = (pumps >> 6) & 0x03
            
            # Pumps 5-6 from data[12]
            pumps56 = data[12]
            self.state.pump5_speed = pumps56 & 0x03
            self.state.pump6_speed = (pumps56 >> 2) & 0x03
            
            # Circ pump and blower from data[13]
            self.state.circ_pump_on = (data[13] & 0x02) == 0x02
            self.state.blower = (data[13] >> 2) & 0x03
            
            # Lights from data[14]
            self.state.light1_on = (data[14] & 0x03) != 0
            self.state.light2_on = ((data[14] >> 2) & 0x03) != 0
            
            # Mister and aux from data[15]
            self.state.mister = (data[15] & 0x01) == 0x01
            self.state.aux1_on = (data[15] & 0x08) != 0
            self.state.aux2_on = (data[15] & 0x10) != 0
            
            # Target temperature from data[20]
            if len(data) > 20:
                raw_target = data[20]
                if self.state.temp_scale_celsius:
                    self.state.target_temp = raw_target / 2.0
                else:
                    self.state.target_temp = raw_target
            
            self.state.status_updates += 1
            self.state.last_update = time.time()
            
            self._notify()
            
        except Exception as e:
            _LOGGER.exception("Status parse error: %s", e)
    
    def _build_message(self, msg_type: bytes, data: bytes = b"") -> bytes:
        """Build a complete message to send to the spa.
        
        Format: 7E [LEN] [SRC] [TYPE1] [TYPE2] [DATA...] [CRC] 7E
        
        Per balboa_worldwide_app:
        - Length includes: LEN byte itself, SRC, TYPE (2 bytes), DATA, CRC
        - So length = 1 + 1 + 2 + len(data) + 1 = 5 + len(data)
        """
        length = 5 + len(data)
        
        # Build message body (LENGTH through DATA, for CRC calculation)
        body = bytes([length, SRC_WIFI_MODULE]) + msg_type + data
        
        # Calculate CRC over body
        crc = self.crc8(body)
        
        # Complete message with start/end markers
        message = bytes([M_STARTEND]) + body + bytes([crc, M_STARTEND])
        
        return message
    
    async def _send_queued_command(self) -> None:
        """Send the next queued command if available.
        
        Called when a Ready message is received from the spa.
        """
        if not self._command_queue:
            return
        
        if not self._writer or not self.state.connected:
            return
        
        message = self._command_queue.popleft()
        
        try:
            self._writer.write(message)
            await self._writer.drain()
            self.state.commands_sent += 1
            _LOGGER.debug("Sent queued command: %s", message.hex())
        except Exception as e:
            _LOGGER.error("Failed to send command: %s", e)
            self.state.connected = False
            self._notify()
    
    async def send_message(self, msg_type: bytes, data: bytes = b"") -> bool:
        """Queue a message to be sent to the spa.
        
        Messages are queued and sent when the spa signals Ready.
        """
        if not self.state.connected:
            _LOGGER.debug("Cannot send: not connected")
            return False
        
        message = self._build_message(msg_type, data)
        
        # Add to queue
        self._command_queue.append(message)
        _LOGGER.debug("Queued command: %s (queue size: %d)", message.hex(), len(self._command_queue))
        
        # If we recently received a Ready message, try to send immediately
        if time.time() - self._last_ready_time < 0.5:
            await self._send_queued_command()
        
        return True
    
    async def toggle_item(self, item: int) -> bool:
        """Toggle a spa item (pump, light, etc).
        
        Message format: 7E [LEN] [SRC] BF 11 [ITEM] 00 [CRC] 7E
        """
        _LOGGER.debug("Toggle item: 0x%02x", item)
        return await self.send_message(MSG_TYPE_TOGGLE, bytes([item, 0x00]))
    
    async def toggle_pump1(self) -> bool:
        """Toggle pump 1."""
        return await self.toggle_item(TOGGLE_PUMP1)
    
    async def toggle_pump2(self) -> bool:
        """Toggle pump 2."""
        return await self.toggle_item(TOGGLE_PUMP2)
    
    async def toggle_pump3(self) -> bool:
        """Toggle pump 3."""
        return await self.toggle_item(TOGGLE_PUMP3)
    
    async def toggle_light(self) -> bool:
        """Toggle light 1."""
        return await self.toggle_item(TOGGLE_LIGHT1)
    
    async def toggle_light2(self) -> bool:
        """Toggle light 2."""
        return await self.toggle_item(TOGGLE_LIGHT2)
    
    async def toggle_blower(self) -> bool:
        """Toggle blower."""
        return await self.toggle_item(TOGGLE_BLOWER)
    
    async def toggle_mister(self) -> bool:
        """Toggle mister."""
        return await self.toggle_item(TOGGLE_MISTER)
    
    async def toggle_aux1(self) -> bool:
        """Toggle aux 1."""
        return await self.toggle_item(TOGGLE_AUX1)
    
    async def toggle_aux2(self) -> bool:
        """Toggle aux 2."""
        return await self.toggle_item(TOGGLE_AUX2)
    
    async def toggle_hold(self) -> bool:
        """Toggle hold mode."""
        return await self.toggle_item(TOGGLE_HOLD)
    
    async def toggle_temperature_range(self) -> bool:
        """Toggle temperature range (high/low)."""
        return await self.toggle_item(TOGGLE_TEMP_RANGE)
    
    async def toggle_heating_mode(self) -> bool:
        """Toggle heating mode (ready/rest)."""
        return await self.toggle_item(TOGGLE_HEAT_MODE)
    
    async def set_temperature(self, temp: float) -> bool:
        """Set the target temperature.
        
        Temperature is in the spa's current scale (Celsius or Fahrenheit).
        For Celsius, multiply by 2 per protocol spec.
        
        Message format: 7E [LEN] [SRC] BF 20 [TEMP] [CRC] 7E
        """
        if self.state.temp_scale_celsius:
            # Celsius: multiply by 2
            raw_temp = int(temp * 2)
        else:
            # Fahrenheit: use directly
            raw_temp = int(temp)
        
        _LOGGER.debug("Set temperature: %.1f -> raw=%d", temp, raw_temp)
        return await self.send_message(MSG_TYPE_SET_TEMP, bytes([raw_temp]))
    
    async def set_time(self, hour: int, minute: int, twenty_four_hour: bool = False) -> bool:
        """Set the spa time.
        
        Message format: 7E [LEN] [SRC] BF 21 [HOUR] [MINUTE] [CRC] 7E
        """
        _LOGGER.debug("Set time: %02d:%02d (24h=%s)", hour, minute, twenty_four_hour)
        flags = 0x80 if twenty_four_hour else 0x00
        return await self.send_message(MSG_TYPE_SET_TIME, bytes([hour | flags, minute]))
