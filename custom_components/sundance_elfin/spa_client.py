"""Balboa protocol implementation for Sundance Spa via EW11 RS485-to-TCP bridge."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Any
from enum import IntEnum
import struct

from .const import (
    MSG_DELIMITER,
    MSG_TYPE_STATUS_UPDATE,
    MSG_TYPE_TOGGLE_ITEM,
    MSG_TYPE_SET_TEMP,
    MSG_TYPE_SETTINGS_REQ,
    MSG_TYPE_CONFIG_RESP,
    MSG_TYPE_INFO_RESP,
    MSG_TYPE_CTS,
    MSG_TYPE_NTS,
    MSG_TYPE_CHANNEL_ASSIGN_REQ,
    MSG_TYPE_CHANNEL_ASSIGN_RESP,
    MSG_TYPE_CHANNEL_ASSIGN_ACK,
    MSG_TYPE_NEW_CLIENT_CTS,
    CHANNEL_BROADCAST,
    CHANNEL_MULTICAST,
    CHANNEL_WIFI,
    ITEM_PUMP_1,
    ITEM_PUMP_2,
    ITEM_PUMP_3,
    ITEM_LIGHT_1,
    ITEM_LIGHT_2,
    ITEM_BLOWER,
    ITEM_TEMP_RANGE,
    ITEM_HEAT_MODE,
    SETTINGS_CONFIG,
    SETTINGS_INFO,
    HEAT_MODE_READY,
    HEAT_MODE_REST,
    HEAT_MODE_READY_IN_REST,
    HEAT_STATE_OFF,
    HEAT_STATE_HEATING,
    HEAT_STATE_HEAT_WAITING,
    TEMP_RANGE_LOW,
    TEMP_RANGE_HIGH,
)

_LOGGER = logging.getLogger(__name__)


class HeatMode(IntEnum):
    """Heat mode enumeration."""
    READY = HEAT_MODE_READY
    REST = HEAT_MODE_REST
    READY_IN_REST = HEAT_MODE_READY_IN_REST


class HeatState(IntEnum):
    """Heat state enumeration."""
    OFF = HEAT_STATE_OFF
    HEATING = HEAT_STATE_HEATING
    HEAT_WAITING = HEAT_STATE_HEAT_WAITING


class TempRange(IntEnum):
    """Temperature range enumeration."""
    LOW = TEMP_RANGE_LOW
    HIGH = TEMP_RANGE_HIGH


class PumpState(IntEnum):
    """Pump state enumeration."""
    OFF = 0
    LOW = 1
    HIGH = 2


@dataclass
class SpaStatus:
    """Current spa status data."""
    # Temperatures
    current_temp: float | None = None
    target_temp: float | None = None
    temp_scale_celsius: bool = True
    temp_range: TempRange = TempRange.HIGH

    # Heating
    heat_mode: HeatMode = HeatMode.READY
    heat_state: HeatState = HeatState.OFF

    # Pumps (0=off, 1=low, 2=high)
    pump1: PumpState = PumpState.OFF
    pump2: PumpState = PumpState.OFF
    pump3: PumpState = PumpState.OFF
    pump4: PumpState = PumpState.OFF
    pump5: PumpState = PumpState.OFF
    pump6: PumpState = PumpState.OFF

    # Other devices
    blower: int = 0
    light1: bool = False
    light2: bool = False
    mister: bool = False
    circ_pump: bool = False

    # Filter cycles
    filter_mode: int = 0  # 0=off, 1=cycle1, 2=cycle2, 3=both

    # Time
    hour: int = 0
    minute: int = 0
    clock_24hr: bool = True

    # Status flags
    priming: bool = False
    hold_mode: bool = False
    panel_locked: bool = False

    # Model info
    model: str = ""
    software_id: str = ""


@dataclass
class SpaConfig:
    """Spa configuration data."""
    pump_count: int = 2
    pump1_speeds: int = 2
    pump2_speeds: int = 2
    pump3_speeds: int = 0
    pump4_speeds: int = 0
    pump5_speeds: int = 0
    pump6_speeds: int = 0
    has_blower: bool = False
    has_mister: bool = False
    has_aux1: bool = False
    has_aux2: bool = False
    has_circ_pump: bool = False
    light_count: int = 1


def calculate_crc8(data: bytes) -> int:
    """Calculate CRC-8 checksum for Balboa protocol.

    Polynomial: 0x07 | Initial: 0x02 | Final XOR: 0x02
    """
    crc = 0x02
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = ((crc << 1) ^ 0x07) & 0xFF
            else:
                crc = (crc << 1) & 0xFF
    return crc ^ 0x02


def build_message(channel: int, msg_type: int, data: bytes = b"") -> bytes:
    """Build a Balboa protocol message."""
    # Flag byte: 0xAF = broadcast, 0xBF = addressed/unicast
    flag = 0xAF if channel == CHANNEL_BROADCAST else 0xBF

    content = bytes([channel, flag, msg_type]) + data
    length = len(content) + 2  # +2 for length byte and checksum

    checksum_data = bytes([length]) + content
    crc = calculate_crc8(checksum_data)

    return bytes([MSG_DELIMITER, length]) + content + bytes([crc, MSG_DELIMITER])


def parse_message(data: bytes) -> tuple[int, int, bytes] | None:
    """Parse a Balboa protocol message.

    Returns: (channel, msg_type, payload) or None if invalid.
    """
    if len(data) < 7:
        return None

    if data[0] != MSG_DELIMITER or data[-1] != MSG_DELIMITER:
        return None

    length = data[1]
    if len(data) != length + 2:
        return None

    # Verify checksum over data[1:-2] (length byte through end of payload)
    expected_crc = calculate_crc8(data[1:-2])
    if data[-2] != expected_crc:
        _LOGGER.debug("CRC mismatch: expected %02X, got %02X", expected_crc, data[-2])
        return None

    channel  = data[2]
    # data[3] = flag byte (0xAF / 0xBF) – ignored here
    msg_type = data[4]
    payload  = data[5:-2]

    return channel, msg_type, payload


class SpaClient:
    """Client for communicating with Balboa spa via EW11 bridge."""

    def __init__(self, host: str, port: int = 8899) -> None:
        """Initialize the spa client."""
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._channel: int | None = None
        self._status = SpaStatus()
        self._config = SpaConfig()
        self._config_loaded = False
        self._update_callbacks: list[Callable[[], None]] = []
        self._receive_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._buffer = bytearray()
        self._last_cts_time: float = 0
        self._config_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

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
        return self._status.model

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
            return 10.0 if self._status.temp_range == TempRange.LOW else 26.5
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
        return self._status.heat_state

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    def add_update_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Add a callback for status updates. Returns a removal function."""
        self._update_callbacks.append(callback)

        def remove_callback():
            if callback in self._update_callbacks:
                self._update_callbacks.remove(callback)

        return remove_callback

    def _notify_update(self) -> None:
        """Notify all registered callbacks."""
        for callback in self._update_callbacks:
            try:
                callback()
            except Exception as err:
                _LOGGER.error("Error in update callback: %s", err)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to the spa via EW11 bridge."""
        try:
            _LOGGER.info("Connecting to spa at %s:%d", self._host, self._port)

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=10,
            )

            self._connected = True
            self._channel = None  # Reset channel on every new connection
            self._buffer.clear()

            self._receive_task = asyncio.create_task(self._receive_loop())

            _LOGGER.info("Connected to spa at %s:%d", self._host, self._port)
            return True

        except asyncio.TimeoutError:
            _LOGGER.error("Connection timeout to %s:%d", self._host, self._port)
            return False
        except Exception as err:
            _LOGGER.error("Connection error: %s", err)
            return False

    async def disconnect(self) -> None:
        """Disconnect from the spa."""
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

    # ------------------------------------------------------------------
    # Receive loop
    # ------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        """Receive and process messages from the spa."""
        while self._connected and self._reader:
            try:
                data = await asyncio.wait_for(
                    self._reader.read(1024),
                    timeout=30,
                )

                if not data:
                    _LOGGER.warning("Connection closed by spa")
                    self._connected = False
                    self._notify_update()
                    break

                self._buffer.extend(data)
                await self._process_buffer()

            except asyncio.TimeoutError:
                # Status updates arrive every ~1 s; 30 s silence = dead link
                _LOGGER.warning("No data for 30 s – marking spa as disconnected")
                self._connected = False
                self._notify_update()
                break
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Receive error: %s", err)
                self._connected = False
                self._notify_update()
                break

    async def _process_buffer(self) -> None:
        """Extract and dispatch complete messages from the receive buffer."""
        while True:
            try:
                start = self._buffer.index(MSG_DELIMITER)
                if start > 0:
                    self._buffer = self._buffer[start:]
            except ValueError:
                self._buffer.clear()
                return

            if len(self._buffer) < 2:
                return

            msg_len = self._buffer[1] + 2  # +2: start delimiter + length byte

            if len(self._buffer) < msg_len:
                return

            msg_data = bytes(self._buffer[:msg_len])
            self._buffer = self._buffer[msg_len:]

            parsed = parse_message(msg_data)
            if parsed:
                channel, msg_type, payload = parsed
                await self._handle_message(channel, msg_type, payload)
            else:
                _LOGGER.debug("Invalid message discarded: %s", msg_data.hex())

    # ------------------------------------------------------------------
    # Message handler
    # ------------------------------------------------------------------

    async def _handle_message(self, channel: int, msg_type: int, payload: bytes) -> None:
        """Dispatch a received message to the appropriate handler."""
        _LOGGER.debug(
            "Rx: channel=%02X type=%02X payload=%s",
            channel, msg_type, payload.hex() if payload else "",
        )

        if msg_type == MSG_TYPE_STATUS_UPDATE:
            self._parse_status_update(payload)
            self._notify_update()

        elif msg_type == MSG_TYPE_CONFIG_RESP:
            self._parse_config_response(payload)
            self._config_loaded = True
            self._config_event.set()

        elif msg_type == MSG_TYPE_INFO_RESP:
            self._parse_info_response(payload)

        elif msg_type == MSG_TYPE_NEW_CLIENT_CTS:
            # -------------------------------------------------------
            # BUG FIX #1
            # The spa broadcasts NEW_CLIENT_CTS to invite new RS485
            # devices to identify themselves.  We MUST reply with a
            # channel assignment request; without this the spa never
            # assigns us a channel and we receive nothing back.
            # The original code had only `pass` here.
            # -------------------------------------------------------
            _LOGGER.debug("NEW_CLIENT_CTS received – requesting channel assignment")
            await self._request_channel()

        elif msg_type == MSG_TYPE_CTS:
            self._last_cts_time = asyncio.get_event_loop().time()

        elif msg_type == MSG_TYPE_CHANNEL_ASSIGN_RESP:
            # -------------------------------------------------------
            # BUG FIX #2
            # After receiving our assigned channel we must send an ACK.
            # Without the ACK the spa treats the channel as unconfirmed
            # and stops routing messages to us.
            # The original code never sent the ACK.
            # -------------------------------------------------------
            if len(payload) >= 1:
                self._channel = payload[0]
                _LOGGER.info("Assigned channel: %02X", self._channel)
                await self._send_channel_ack()

    # ------------------------------------------------------------------
    # BUG FIX #3 + #4 – Channel assignment helpers (were missing entirely)
    # ------------------------------------------------------------------

    async def _request_channel(self) -> None:
        """Send a Balboa RS485 channel assignment request.

        Must use CHANNEL_MULTICAST (0xFE) as source channel because we
        have no assigned channel yet.  Device type 0x0A = WiFi client.
        """
        if not self._connected or not self._writer:
            return

        # [device_type, client_hash_hi, client_hash_lo]
        data = bytes([0x0A, 0x00, 0x00])
        message = build_message(CHANNEL_MULTICAST, MSG_TYPE_CHANNEL_ASSIGN_REQ, data)

        async with self._lock:
            try:
                self._writer.write(message)
                await self._writer.drain()
                _LOGGER.debug("Sent channel assignment request: %s", message.hex())
            except Exception as err:
                _LOGGER.error("Error sending channel request: %s", err)
                self._connected = False

    async def _send_channel_ack(self) -> None:
        """Acknowledge the channel assigned by the spa."""
        if not self._connected or not self._writer or self._channel is None:
            return

        message = build_message(self._channel, MSG_TYPE_CHANNEL_ASSIGN_ACK, b"")

        async with self._lock:
            try:
                self._writer.write(message)
                await self._writer.drain()
                _LOGGER.debug(
                    "Sent channel ACK for channel %02X", self._channel
                )
            except Exception as err:
                _LOGGER.error("Error sending channel ACK: %s", err)
                self._connected = False

    # ------------------------------------------------------------------
    # Message parsers
    # ------------------------------------------------------------------

    def _parse_status_update(self, payload: bytes) -> None:
        """Parse status update message (type 0x13)."""
        if len(payload) < 20:
            _LOGGER.debug("Status update too short: %d bytes", len(payload))
            return

        # Byte 0: Spa state
        self._status.hold_mode = payload[0] == 0x05

        # Byte 1: Init mode
        self._status.priming = payload[1] == 0x01

        # Byte 2: Current temperature (0xFF = unknown)
        if payload[2] != 0xFF:
            self._status.current_temp = (
                payload[2] / 2.0 if self._status.temp_scale_celsius else float(payload[2])
            )
        else:
            self._status.current_temp = None

        # Bytes 3-4: Time
        self._status.hour   = payload[3]
        self._status.minute = payload[4]

        # -----------------------------------------------------------
        # BUG FIX #5
        # Byte 5 is a flags byte; heat mode lives in bits 0–1 only.
        # The original code used the raw byte value as the enum key,
        # which produced invalid HeatMode values whenever other bits
        # in the same byte happened to be set (e.g. 0x03 instead of
        # 0x01 for REST mode), causing an unhandled exception.
        # -----------------------------------------------------------
        heat_mode_raw = payload[5] & 0x03
        if heat_mode_raw in (0, 1, 3):
            self._status.heat_mode = HeatMode(heat_mode_raw)

        # Byte 9: Misc flags
        flags9 = payload[9]
        self._status.temp_scale_celsius = bool(flags9 & 0x01)
        self._status.clock_24hr         = bool(flags9 & 0x02)
        self._status.filter_mode        = (flags9 >> 3) & 0x03
        self._status.panel_locked       = bool(flags9 & 0x20)

        # Byte 10: Heating flags
        flags10 = payload[10]
        self._status.temp_range   = TempRange.HIGH if (flags10 & 0x04) else TempRange.LOW
        heat_state_val = (flags10 >> 4) & 0x03
        if heat_state_val in (0, 1, 2):
            self._status.heat_state = HeatState(heat_state_val)

        # Byte 11: Pumps 1–4 (2 bits each)
        flags11 = payload[11]
        self._status.pump1 = PumpState(min(flags11 & 0x03,        2))
        self._status.pump2 = PumpState(min((flags11 >> 2) & 0x03, 2))
        self._status.pump3 = PumpState(min((flags11 >> 4) & 0x03, 2))
        self._status.pump4 = PumpState(min((flags11 >> 6) & 0x03, 2))

        # Byte 12: Pumps 5–6
        flags12 = payload[12]
        self._status.pump5 = PumpState(min(flags12 & 0x03,        2))
        self._status.pump6 = PumpState(min((flags12 >> 2) & 0x03, 2))

        # Byte 13: Circ pump + blower
        flags13 = payload[13]
        self._status.circ_pump = bool(flags13 & 0x02)
        self._status.blower    = (flags13 >> 2) & 0x03

        # Byte 14: Lights
        flags14 = payload[14]
        self._status.light1 = bool(flags14 & 0x03)
        self._status.light2 = bool((flags14 >> 2) & 0x03)

        # Byte 15: Mister
        self._status.mister = bool(payload[15])

        # Byte 20: Target temperature
        if len(payload) > 20:
            self._status.target_temp = (
                payload[20] / 2.0 if self._status.temp_scale_celsius else float(payload[20])
            )

        _LOGGER.debug(
            "Status: temp=%.1f target=%.1f heat_mode=%s heat_state=%s "
            "pump1=%s pump2=%s light1=%s",
            self._status.current_temp or 0,
            self._status.target_temp or 0,
            self._status.heat_mode.name,
            self._status.heat_state.name,
            self._status.pump1.name,
            self._status.pump2.name,
            self._status.light1,
        )

    def _parse_config_response(self, payload: bytes) -> None:
        """Parse configuration response message (type 0x2E).

        Balboa byte 0 layout – 2 bits per pump slot:
          bits 0–1 = pump 1  (0=none, 1=1-speed, 2=2-speed)
          bits 2–3 = pump 2
          bits 4–5 = pump 3
          bits 6–7 = pump 4
        Byte 1:
          bits 0–1 = pump 5
          bits 2–3 = pump 6
        Byte 2: feature flags
        Byte 3: light count
        """
        if len(payload) < 4:
            return

        # -----------------------------------------------------------
        # BUG FIX #6
        # The original code always set pump speeds to 2 for any
        # non-zero bit pattern (e.g. a 1-speed pump was reported as
        # 2-speed).  We now read the actual value from each 2-bit slot.
        # For the Cameo 880: pump 1 = 2-speed, pump 2 = 1-speed.
        # -----------------------------------------------------------
        b0 = payload[0]
        self._config.pump1_speeds = b0 & 0x03
        self._config.pump2_speeds = (b0 >> 2) & 0x03
        self._config.pump3_speeds = (b0 >> 4) & 0x03
        self._config.pump4_speeds = (b0 >> 6) & 0x03

        if len(payload) > 1:
            b1 = payload[1]
            self._config.pump5_speeds = b1 & 0x03
            self._config.pump6_speeds = (b1 >> 2) & 0x03

        self._config.pump_count = sum(
            1 for s in (
                self._config.pump1_speeds,
                self._config.pump2_speeds,
                self._config.pump3_speeds,
                self._config.pump4_speeds,
                self._config.pump5_speeds,
                self._config.pump6_speeds,
            ) if s > 0
        )

        if len(payload) > 2:
            features = payload[2]
            self._config.has_circ_pump = bool(features & 0x01)
            self._config.has_blower    = bool(features & 0x02)
            self._config.has_mister    = bool(features & 0x04)

        if len(payload) > 3:
            self._config.light_count = max(1, payload[3] & 0x03)

        _LOGGER.debug(
            "Config: pump1=%d pump2=%d pump3=%d speeds, "
            "circ=%s blower=%s lights=%d",
            self._config.pump1_speeds,
            self._config.pump2_speeds,
            self._config.pump3_speeds,
            self._config.has_circ_pump,
            self._config.has_blower,
            self._config.light_count,
        )

    def _parse_info_response(self, payload: bytes) -> None:
        """Parse information response message (type 0x24)."""
        if len(payload) < 20:
            return

        ssid      = struct.unpack(">I", payload[0:4])[0]
        model_num = (ssid >> 16) & 0xFFFF
        version   = ssid & 0xFFFF
        self._status.software_id = f"M{model_num} V{version // 100}.{version % 100}"

        try:
            self._status.model = payload[4:12].decode("ascii").strip("\x00").strip()
        except Exception:
            self._status.model = "Unknown"

        _LOGGER.info(
            "Spa model: %s, Software: %s",
            self._status.model,
            self._status.software_id,
        )

    # ------------------------------------------------------------------
    # Send helpers
    # ------------------------------------------------------------------

    async def _send_message(self, msg_type: int, data: bytes = b"") -> None:
        """Send a message to the spa using our assigned channel."""
        if not self._connected or not self._writer:
            _LOGGER.warning("Cannot send: not connected")
            return

        channel = self._channel if self._channel is not None else CHANNEL_WIFI
        message = build_message(channel, msg_type, data)

        async with self._lock:
            try:
                self._writer.write(message)
                await self._writer.drain()
                _LOGGER.debug("Tx type=%02X: %s", msg_type, message.hex())
            except Exception as err:
                _LOGGER.error("Send error: %s", err)
                self._connected = False

    async def request_configuration(self) -> None:
        """Request spa configuration."""
        # -----------------------------------------------------------
        # BUG FIX #7
        # Third byte must be 0x00, not 0x01.  Sending 0x01 caused the
        # spa to silently ignore the request so no CONFIG_RESP arrived,
        # leaving _config_event un-set and async_configuration_loaded()
        # always timing out.
        # -----------------------------------------------------------
        await self._send_message(MSG_TYPE_SETTINGS_REQ, bytes([SETTINGS_CONFIG, 0x00, 0x00]))

    async def request_info(self) -> None:
        """Request spa information."""
        await self._send_message(MSG_TYPE_SETTINGS_REQ, bytes([SETTINGS_INFO, 0x00, 0x00]))

    async def async_configuration_loaded(self) -> None:
        """Request configuration and wait until it arrives."""
        await self.request_configuration()
        await asyncio.sleep(0.5)
        await self.request_info()

        try:
            await asyncio.wait_for(self._config_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout waiting for configuration – using defaults")
            self._config_loaded = True

    # ------------------------------------------------------------------
    # Control commands
    # ------------------------------------------------------------------

    async def set_temperature(self, temperature: float) -> None:
        """Set target temperature."""
        temp_byte = int(temperature * 2) if self._status.temp_scale_celsius else int(temperature)
        await self._send_message(MSG_TYPE_SET_TEMP, bytes([temp_byte]))

    async def toggle_pump(self, pump_num: int) -> None:
        """Toggle a pump (cycles off → low → high → off)."""
        pump_items = {1: ITEM_PUMP_1, 2: ITEM_PUMP_2, 3: ITEM_PUMP_3}
        if pump_num in pump_items:
            await self._send_message(
                MSG_TYPE_TOGGLE_ITEM, bytes([pump_items[pump_num], 0x00])
            )

    async def toggle_light(self, light_num: int = 1) -> None:
        """Toggle lights."""
        light_items = {1: ITEM_LIGHT_1, 2: ITEM_LIGHT_2}
        if light_num in light_items:
            await self._send_message(
                MSG_TYPE_TOGGLE_ITEM, bytes([light_items[light_num], 0x00])
            )

    async def toggle_blower(self) -> None:
        """Toggle blower."""
        await self._send_message(MSG_TYPE_TOGGLE_ITEM, bytes([ITEM_BLOWER, 0x00]))

    async def toggle_heat_mode(self) -> None:
        """Toggle heat mode between Ready and Rest."""
        await self._send_message(MSG_TYPE_TOGGLE_ITEM, bytes([ITEM_HEAT_MODE, 0x00]))

    async def toggle_temp_range(self) -> None:
        """Toggle temperature range between Low and High."""
        await self._send_message(MSG_TYPE_TOGGLE_ITEM, bytes([ITEM_TEMP_RANGE, 0x00]))

    async def set_heat_mode(self, mode: HeatMode) -> None:
        """Set heat mode, toggling until the desired mode is reached."""
        if self._status.heat_mode != mode:
            await self.toggle_heat_mode()
            await asyncio.sleep(0.5)
            # Ready-in-Rest may need a second toggle
            if self._status.heat_mode != mode and mode == HeatMode.READY_IN_REST:
                await self.toggle_heat_mode()
