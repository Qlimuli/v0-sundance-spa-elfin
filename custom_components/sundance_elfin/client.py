"""Async TCP client for Sundance Spa Elfin communication."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

from .const import (
    ADDR_BROADCAST,
    ADDR_PANEL,
    BTN_LIGHT,
    BTN_PUMP1,
    BTN_PUMP2,
    BTN_TEMP_DOWN,
    BTN_TEMP_UP,
    CONNECTION_TIMEOUT,
    FRAME_START,
    FRAME_START_BYTE,
    MASK_HEATING,
    MASK_LIGHT,
    MASK_PUMP1_HIGH,
    MASK_PUMP1_LOW,
    MASK_PUMP2_HIGH,
    MASK_PUMP2_LOW,
    MSG_BUTTON_PRESS,
    MSG_HEARTBEAT,
    MSG_STATUS_LEN_34,
    MSG_STATUS_LEN_38,
    POS_CURRENT_TEMP,
    POS_FLAGS1,
    POS_LIGHT_STATE,
    POS_TARGET_TEMP,
    RECONNECT_INTERVAL,
)

_LOGGER = logging.getLogger(__name__)

# Rate limiting for logging to prevent "logging too frequently" warnings
_last_log_time: dict[str, float] = {}
_LOG_INTERVAL = 10.0  # Minimum seconds between identical log messages


def _rate_limited_log(level: int, key: str, msg: str, *args) -> None:
    """Log a message with rate limiting to prevent spam."""
    now = time.time()
    if key in _last_log_time and (now - _last_log_time[key]) < _LOG_INTERVAL:
        return
    _last_log_time[key] = now
    _LOGGER.log(level, msg, *args)


@dataclass
class SpaState:
    """Data class holding the current spa state."""

    current_temp: float | None = None  # None = unknown
    target_temp: float | None = None
    is_heating: bool = False
    pump1_speed: int = 0  # 0=off, 1=low, 2=high
    pump2_speed: int = 0
    light_on: bool = False
    connected: bool = False
    last_update: float = 0.0
    raw_packets_received: int = 0
    status_packets_parsed: int = 0

    @property
    def pump1_on(self) -> bool:
        """Return True if pump1 is running at any speed."""
        return self.pump1_speed > 0

    @property
    def pump2_on(self) -> bool:
        """Return True if pump2 is running at any speed."""
        return self.pump2_speed > 0


@dataclass
class SundanceElfinClient:
    """Async TCP client for Elfin-EW11A RS485 adapter.
    
    Implements the Sundance/Jacuzzi RS485 protocol based on observed packets:
    - Frame start: 0x7E 0x7E
    - Byte 0: Length of packet (including length byte, excluding frame start)
    - Byte 1-2: Addresses (0xFF 0xAF for status broadcasts)
    - Byte 3+: Message data
    """

    host: str
    port: int
    state: SpaState = field(default_factory=SpaState)
    
    _reader: asyncio.StreamReader | None = field(default=None, repr=False)
    _writer: asyncio.StreamWriter | None = field(default=None, repr=False)
    _listen_task: asyncio.Task | None = field(default=None, repr=False)
    _running: bool = field(default=False, repr=False)
    _callbacks: list[Callable[[], None]] = field(default_factory=list, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    def register_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Register a callback to be called when state changes."""
        self._callbacks.append(callback)
        
        def unregister() -> None:
            if callback in self._callbacks:
                self._callbacks.remove(callback)
        
        return unregister

    def _notify_callbacks(self) -> None:
        """Notify all registered callbacks of state change."""
        for callback in self._callbacks:
            try:
                callback()
            except Exception:
                _LOGGER.exception("Error in state callback")

    async def connect(self) -> bool:
        """Establish TCP connection to the Elfin adapter."""
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=CONNECTION_TIMEOUT,
            )
            self.state.connected = True
            _LOGGER.info("Connected to Sundance Spa at %s:%s", self.host, self.port)
            self._notify_callbacks()
            return True
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout connecting to %s:%s", self.host, self.port)
            self.state.connected = False
            return False
        except OSError as err:
            _LOGGER.error("Failed to connect to %s:%s: %s", self.host, self.port, err)
            self.state.connected = False
            return False

    async def disconnect(self) -> None:
        """Close the TCP connection."""
        self._running = False
        
        if self._listen_task and not self._listen_task.done():
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
        self._notify_callbacks()

    async def start(self) -> None:
        """Start the client and begin listening for data."""
        self._running = True
        if await self.connect():
            self._listen_task = asyncio.create_task(self._listen_loop())

    async def stop(self) -> None:
        """Stop the client and close connection."""
        await self.disconnect()

    async def _listen_loop(self) -> None:
        """Main loop that reads data from the TCP stream."""
        buffer = bytearray()
        
        while self._running:
            try:
                if not self._reader or not self.state.connected:
                    await self._reconnect()
                    continue
                
                try:
                    data = await asyncio.wait_for(
                        self._reader.read(512),
                        timeout=30.0,
                    )
                except asyncio.TimeoutError:
                    continue
                
                if not data:
                    _LOGGER.warning("Connection closed by Elfin adapter")
                    self.state.connected = False
                    self._notify_callbacks()
                    await self._reconnect()
                    continue
                
                buffer.extend(data)
                self.state.raw_packets_received += 1
                
                # Process complete packets
                buffer = self._process_buffer(buffer)
                
            except asyncio.CancelledError:
                break
            except Exception as err:
                _rate_limited_log(
                    logging.ERROR, "listen_error",
                    "Error in listen loop: %s", err
                )
                self.state.connected = False
                self._notify_callbacks()
                await self._reconnect()

    async def _reconnect(self) -> None:
        """Handle reconnection with backoff."""
        if not self._running:
            return
        
        _LOGGER.info("Attempting to reconnect in %d seconds...", RECONNECT_INTERVAL)
        await asyncio.sleep(RECONNECT_INTERVAL)
        
        if self._running:
            await self.connect()

    def _process_buffer(self, buffer: bytearray) -> bytearray:
        """Process the receive buffer and extract complete packets.
        
        Packet format observed:
        7E 7E [LEN] [ADDR1] [ADDR2] [DATA...] 
        
        Where LEN is the total packet length after the 7E7E prefix.
        """
        while len(buffer) >= 4:
            # Find frame start (7E 7E)
            start_idx = buffer.find(FRAME_START)
            
            if start_idx == -1:
                # No frame start found, keep last byte in case it's a partial 7E
                if buffer and buffer[-1] == FRAME_START_BYTE:
                    return buffer[-1:]
                return bytearray()
            
            # Remove garbage before frame start
            if start_idx > 0:
                buffer = buffer[start_idx:]
            
            if len(buffer) < 3:
                break
            
            # Get packet length (byte after 7E7E)
            packet_len = buffer[2]
            
            # Validate length (reasonable bounds)
            if packet_len < 3 or packet_len > 100:
                # Invalid length, skip this frame start and look for next
                buffer = buffer[2:]
                continue
            
            # Total bytes needed: 2 (frame start) + packet_len
            total_needed = 2 + packet_len
            
            if len(buffer) < total_needed:
                # Incomplete packet, wait for more data
                break
            
            # Extract complete packet (excluding 7E7E prefix)
            packet = bytes(buffer[2:total_needed])
            buffer = buffer[total_needed:]
            
            # Parse the packet
            self._parse_packet(packet)
        
        return buffer

    def _parse_packet(self, packet: bytes) -> None:
        """Parse a packet and update state if it's a status packet."""
        if len(packet) < 4:
            return
        
        length = packet[0]
        
        # Check for status packets (length 0x22=34 or 0x26=38)
        if length in (MSG_STATUS_LEN_34, MSG_STATUS_LEN_38):
            # This is likely a status packet
            if len(packet) >= 2 and packet[1] == ADDR_BROADCAST and packet[2] == ADDR_PANEL:
                self._parse_status_packet(packet)
        elif length == MSG_HEARTBEAT:
            # Heartbeat packet (0510bf...) - just acknowledge connection is alive
            pass
        else:
            # Log unknown packet types occasionally for debugging
            _rate_limited_log(
                logging.DEBUG, f"unknown_pkt_{length}",
                "Unknown packet type (len=%d): %s", length, packet[:20].hex()
            )

    def _parse_status_packet(self, packet: bytes) -> None:
        """Parse a status packet and update the spa state.
        
        Observed 38-byte status packet structure:
        7e7e 26 ff af c4 0f 1c 0b 0a 85 07 9d fc 07 00 46 00 3d 1c bf 1b 22 18 1b 00 32 14 2d 16 11 10 13 17 73 6c 2f 2e 29 04 7e
        
        Byte positions (after 7E7E, so packet[0] = length byte):
        [0] = 0x26 (38) - length
        [1] = 0xFF - broadcast address
        [2] = 0xAF - panel address  
        [3] = 0xC4 - message type/subtype
        [4] = 0x0F - ? 
        [5] = 0x1C - ? (28)
        [6] = 0x0B - current temp raw? (11 -> could be index or encoded)
        [7] = 0x0A - target temp raw?
        ...
        
        Temperature encoding needs verification - trying multiple approaches.
        """
        if len(packet) < 20:
            return
        
        try:
            state_changed = False
            
            # Try to extract temperatures
            # Common encodings: raw/2 for F, direct C, or lookup table
            # Let's try positions 6 and 7 first as observed
            
            if len(packet) > POS_TARGET_TEMP:
                raw_current = packet[POS_CURRENT_TEMP]
                raw_target = packet[POS_TARGET_TEMP]
                
                # Try direct Fahrenheit / 2 conversion (common in Balboa/Jacuzzi)
                # Values like 0x0B (11) seem too low for temp
                # Try treating as direct value or with offset
                
                # Attempt 1: If values > 50, might be direct F
                # Attempt 2: If values < 50, might need different position
                
                # Look for reasonable temperature bytes (26-40C = 79-104F = 158-208 raw if *2)
                # Or 79-104 raw if direct F
                
                # Scan packet for temperature-like values
                temp_candidates = []
                for i, b in enumerate(packet[4:25]):
                    # Looking for values that could be temps
                    # Direct C: 26-42 range
                    # F/2: 79-104 range (39-52)
                    # Direct F: 79-104
                    if 26 <= b <= 50:  # Could be C or F/2
                        temp_candidates.append((i + 4, b, "C_or_F2"))
                    elif 70 <= b <= 110:  # Could be direct F
                        temp_candidates.append((i + 4, b, "direct_F"))
                
                if temp_candidates:
                    _rate_limited_log(
                        logging.DEBUG, "temp_candidates",
                        "Temperature candidates in packet: %s", temp_candidates
                    )
                
                # For now, try a few common positions
                # Position 6-7 from observations
                if 60 <= raw_current <= 110:  # Direct Fahrenheit
                    self.state.current_temp = round((raw_current - 32) * 5 / 9, 1)
                    state_changed = True
                elif 26 <= raw_current <= 50:  # Direct Celsius or F/2
                    # Try F/2 first (more common)
                    temp_f = raw_current * 2
                    self.state.current_temp = round((temp_f - 32) * 5 / 9, 1)
                    state_changed = True
                
                if 60 <= raw_target <= 110:
                    self.state.target_temp = round((raw_target - 32) * 5 / 9, 1)
                    state_changed = True
                elif 26 <= raw_target <= 50:
                    temp_f = raw_target * 2
                    self.state.target_temp = round((temp_f - 32) * 5 / 9, 1)
                    state_changed = True
            
            # Parse flags for pump/light/heating states
            if len(packet) > POS_FLAGS1:
                flags1 = packet[POS_FLAGS1]
                
                # Pump 1: bits 0-1 for off/low/high
                pump1_bits = flags1 & (MASK_PUMP1_LOW | MASK_PUMP1_HIGH)
                if pump1_bits == 0:
                    self.state.pump1_speed = 0
                elif pump1_bits == MASK_PUMP1_LOW:
                    self.state.pump1_speed = 1
                else:
                    self.state.pump1_speed = 2
                
                # Pump 2: bits 2-3
                pump2_bits = flags1 & (MASK_PUMP2_LOW | MASK_PUMP2_HIGH)
                if pump2_bits == 0:
                    self.state.pump2_speed = 0
                elif pump2_bits == MASK_PUMP2_LOW:
                    self.state.pump2_speed = 1
                else:
                    self.state.pump2_speed = 2
                
                # Heating: bits 4-5
                self.state.is_heating = bool(flags1 & MASK_HEATING)
                
                state_changed = True
            
            # Light state
            if len(packet) > POS_LIGHT_STATE:
                light_byte = packet[POS_LIGHT_STATE]
                self.state.light_on = bool(light_byte & MASK_LIGHT)
                state_changed = True
            
            self.state.last_update = time.time()
            self.state.status_packets_parsed += 1
            
            # Log parsed state periodically
            _rate_limited_log(
                logging.DEBUG, "parsed_state",
                "Status: temp=%.1f/%.1f°C, heat=%s, pump1=%d, pump2=%d, light=%s",
                self.state.current_temp or 0,
                self.state.target_temp or 0,
                self.state.is_heating,
                self.state.pump1_speed,
                self.state.pump2_speed,
                self.state.light_on,
            )
            
            if state_changed:
                self._notify_callbacks()
                
        except Exception as err:
            _rate_limited_log(
                logging.WARNING, "parse_error",
                "Failed to parse status: %s - %s", packet.hex()[:40], err
            )

    def _calculate_checksum(self, data: bytes) -> int:
        """Calculate CRC-8 checksum (common in Balboa/Jacuzzi protocol)."""
        # CRC-8 with polynomial 0x07 (common for spa protocols)
        crc = 0x02  # Initial value
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x80:
                    crc = (crc << 1) ^ 0x07
                else:
                    crc <<= 1
                crc &= 0xFF
        return crc ^ 0x02

    def _build_command(self, button_code: int) -> bytes:
        """Build a button press command packet.
        
        Command format (based on Jacuzzi protocol):
        7E 7E [LEN] [DST] [SRC] [MSG_TYPE] [BUTTON] [CHECKSUM]
        """
        src_addr = 0x0A  # Controller/client address
        dst_addr = ADDR_PANEL
        
        # Build packet data (after 7E7E)
        packet_data = bytes([
            0x06,  # Length (6 bytes after 7E7E)
            dst_addr,
            src_addr,
            MSG_BUTTON_PRESS,
            button_code,
        ])
        
        checksum = self._calculate_checksum(packet_data)
        packet = FRAME_START + packet_data + bytes([checksum])
        
        return packet

    async def _send_command(self, command: bytes) -> bool:
        """Send a command to the spa."""
        async with self._lock:
            if not self._writer or not self.state.connected:
                _LOGGER.error("Cannot send command: not connected")
                return False
            
            try:
                _LOGGER.debug("Sending command: %s", command.hex())
                self._writer.write(command)
                await self._writer.drain()
                return True
            except Exception as err:
                _LOGGER.error("Failed to send command: %s", err)
                self.state.connected = False
                self._notify_callbacks()
                return False

    async def set_target_temperature(self, temperature: float) -> bool:
        """Set the target water temperature by sending temp up/down buttons."""
        if self.state.target_temp is None:
            _LOGGER.warning("Cannot set temperature: current target unknown")
            return False
        
        current = self.state.target_temp
        diff = temperature - current
        
        # Send appropriate number of temp up/down button presses
        # Most spas change by 0.5°C or 1°F per press
        steps = int(abs(diff) / 0.5)
        button = BTN_TEMP_UP if diff > 0 else BTN_TEMP_DOWN
        
        _LOGGER.info(
            "Setting temperature from %.1f to %.1f°C (%d steps)",
            current, temperature, steps
        )
        
        for _ in range(min(steps, 20)):  # Limit to 20 presses max
            command = self._build_command(button)
            if not await self._send_command(command):
                return False
            await asyncio.sleep(0.3)  # Small delay between button presses
        
        # Optimistically update
        self.state.target_temp = temperature
        self._notify_callbacks()
        return True

    async def toggle_pump1(self) -> bool:
        """Toggle pump 1."""
        command = self._build_command(BTN_PUMP1)
        _LOGGER.info("Toggling pump 1")
        
        if await self._send_command(command):
            # Cycle through speeds: 0 -> 1 -> 2 -> 0
            self.state.pump1_speed = (self.state.pump1_speed + 1) % 3
            self._notify_callbacks()
            return True
        return False

    async def toggle_pump2(self) -> bool:
        """Toggle pump 2."""
        command = self._build_command(BTN_PUMP2)
        _LOGGER.info("Toggling pump 2")
        
        if await self._send_command(command):
            self.state.pump2_speed = (self.state.pump2_speed + 1) % 3
            self._notify_callbacks()
            return True
        return False

    async def toggle_light(self) -> bool:
        """Toggle the spa light."""
        command = self._build_command(BTN_LIGHT)
        _LOGGER.info("Toggling light")
        
        if await self._send_command(command):
            self.state.light_on = not self.state.light_on
            self._notify_callbacks()
            return True
        return False

    async def set_pump1(self, on: bool) -> bool:
        """Set pump 1 to on or off."""
        if on and self.state.pump1_speed == 0:
            return await self.toggle_pump1()
        elif not on and self.state.pump1_speed > 0:
            # Toggle until off
            while self.state.pump1_speed > 0:
                if not await self.toggle_pump1():
                    return False
                await asyncio.sleep(0.5)
        return True

    async def set_pump2(self, on: bool) -> bool:
        """Set pump 2 to on or off."""
        if on and self.state.pump2_speed == 0:
            return await self.toggle_pump2()
        elif not on and self.state.pump2_speed > 0:
            while self.state.pump2_speed > 0:
                if not await self.toggle_pump2():
                    return False
                await asyncio.sleep(0.5)
        return True

    async def set_light(self, on: bool) -> bool:
        """Set light to a specific state."""
        if self.state.light_on != on:
            return await self.toggle_light()
        return True
