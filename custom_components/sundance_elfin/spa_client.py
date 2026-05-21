"""Balboa protocol implementation for Sundance Spa via EW11 RS485-to-TCP bridge."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable
from enum import IntEnum

_LOGGER = logging.getLogger(__name__)

MSG_START = 0x7E
MSG_END = 0x7E

MSG_TYPE_STATUS        = 0x13
MSG_TYPE_FILTER_CYCLES = 0x23
MSG_TYPE_INFO          = 0x24
MSG_TYPE_SETTINGS      = 0x25
MSG_TYPE_SETUP_PARAMS  = 0x26
MSG_TYPE_CONFIG        = 0x2E
MSG_TYPE_CONFIG_REQ    = 0x04
MSG_TYPE_TOGGLE_ITEM   = 0x11
MSG_TYPE_SET_TEMP      = 0x20
MSG_TYPE_SET_TIME      = 0x21
MSG_TYPE_SET_TEMP_SCALE = 0x27
MSG_TYPE_READY         = 0x14
MSG_TYPE_NOTHING_TO_SEND = 0x06

SRC_WIFI = 0x0A


class HeatMode(IntEnum):
    READY        = 0
    REST         = 1
    READY_IN_REST = 2


class HeatState(IntEnum):
    OFF          = 0
    HEATING      = 1
    HEAT_WAITING = 2


class TempRange(IntEnum):
    LOW  = 0
    HIGH = 1


class PumpState(IntEnum):
    OFF  = 0
    LOW  = 1
    HIGH = 2


class ToggleItem(IntEnum):
    PUMP1     = 0x04
    PUMP2     = 0x05
    PUMP3     = 0x06
    PUMP4     = 0x07
    PUMP5     = 0x08
    PUMP6     = 0x09
    LIGHT1    = 0x11
    LIGHT2    = 0x12
    AUX1      = 0x16
    AUX2      = 0x17
    MISTER    = 0x0E
    BLOWER    = 0x0C
    HOLD      = 0x3C
    TEMP_RANGE = 0x50
    HEAT_MODE  = 0x51


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
    """Calculate CRC-8 checksum for Balboa protocol."""
    crc = 0x02
    for byte in data:
        for i in range(8):
            bit = crc & 0x80
            crc = ((crc << 1) & 0xFF) | ((byte >> (7 - i)) & 0x01)
            if bit:
                crc ^= 0x07
    for _ in range(8):
        bit = crc & 0x80
        crc = (crc << 1) & 0xFF
        if bit:
            crc ^= 0x07
    return crc ^ 0x02


def build_message(src: int, msg_type_bytes: bytes, payload: bytes = b"") -> bytes:
    """Build a Balboa protocol message."""
    content = bytes([src]) + msg_type_bytes + payload
    length = len(content) + 2
    msg_body = bytes([length]) + content
    crc = crc8_checksum(msg_body)
    return bytes([MSG_START]) + msg_body + bytes([crc, MSG_END])


def parse_message(data: bytes) -> tuple[int, bytes, bytes] | None:
    """Parse a Balboa protocol message. Returns (src, msg_type_bytes, payload) or None."""
    if len(data) < 7:
        return None
    if data[0] != MSG_START or data[-1] != MSG_END:
        return None
    length = data[1]
    if len(data) != length + 2:
        _LOGGER.debug("Length mismatch: expected %d, got %d", length + 2, len(data))
        return None
    expected_crc = crc8_checksum(data[1:-2])
    actual_crc = data[-2]
    if expected_crc != actual_crc:
        _LOGGER.debug("CRC mismatch: expected %02X, got %02X", expected_crc, actual_crc)
    src      = data[2]
    msg_type = data[3:5]
    payload  = data[5:-2]
    return src, msg_type, payload


class SpaClient:
    """Client for communicating with Balboa spa via EW11 TCP bridge."""

    MT_STATUS     = b'\xAF\x13'
    MT_FILTER     = b'\xAF\x23'
    MT_INFO       = b'\xAF\x24'
    MT_SETTINGS   = b'\xAF\x25'
    MT_SETUP      = b'\xAF\x26'
    MT_CONFIG     = b'\xAF\x2E'
    MT_READY      = b'\xAF\x14'
    MT_NTS        = b'\xAF\x06'
    MT_CONFIG_REQ = b'\xBF\x04'
    MT_TOGGLE     = b'\xBF\x11'
    MT_SET_TEMP   = b'\xBF\x20'
    MT_SET_TIME   = b'\xBF\x21'
    MT_SET_SCALE  = b'\xBF\x27'

    def __init__(self, host: str, port: int = 8899) -> None:
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
        # FIX: Separates Event das erst nach dem ersten vollständigen Parse gesetzt wird
        self._first_status_parsed = asyncio.Event()
        # FIX: Event für heat_mode-Änderungen damit set_heat_mode() darauf warten kann
        self._heat_mode_changed = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Properties                                                           #
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # Callbacks                                                            #
    # ------------------------------------------------------------------ #

    def add_update_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        """Register a callback, returns unsubscribe function."""
        self._update_callbacks.append(callback)
        def remove():
            if callback in self._update_callbacks:
                self._update_callbacks.remove(callback)
        return remove

    def _notify_update(self) -> None:
        for cb in self._update_callbacks:
            try:
                cb()
            except Exception as err:
                _LOGGER.error("Callback error: %s", err)

    # ------------------------------------------------------------------ #
    # Connection                                                           #
    # ------------------------------------------------------------------ #

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
            # FIX: Events beim (Re-)Connect zurücksetzen
            self._first_status_parsed.clear()
            self._heat_mode_changed.clear()
            self._config_loaded = False
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

    async def async_configuration_loaded(self) -> bool:
        """
        Warte auf den ersten vollständig geparsten Status-Frame und
        versuche dann die Konfiguration zu laden.

        FIX: Kein doppeltes wait_for mehr – der Timeout liegt beim Aufrufer
             (__init__.py). Intern eigene kurze Timeouts pro Schritt.
        """
        # Schritt 1: Warte auf ersten vollständig geparsten Status (max 30s)
        try:
            await asyncio.wait_for(self._first_status_parsed.wait(), timeout=30.0)
            _LOGGER.info("First status frame received and parsed")
        except asyncio.TimeoutError:
            _LOGGER.error(
                "Timeout waiting for first status frame – "
                "check EW11 mode (must be TCP Server, NOT Modbus)"
            )
            return False

        # Schritt 2: Config anfordern (best-effort, Fehler nicht fatal)
        try:
            await self._request_config(1)
            await asyncio.sleep(0.3)
            await self._request_config(2)
            await asyncio.sleep(0.3)
            await self._request_config(3)

            await asyncio.wait_for(self._config_event.wait(), timeout=8.0)
            _LOGGER.info("Configuration loaded from spa")
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Config response timeout – using Sundance Cameo 880 defaults"
            )
            self._use_cameo_880_defaults()

        self._config_loaded = True
        return True

    def _use_cameo_880_defaults(self) -> None:
        """Fallback-Konfiguration für Sundance Cameo 880."""
        self._config.model = "Sundance Cameo 880"
        self._config.pump_count = 3
        self._config.pump_speeds = [2, 2, 1, 0, 0, 0]
        self._config.has_blower = False
        self._config.has_circ_pump = True
        self._config.light_count = 1
        _LOGGER.info("Using Cameo 880 default configuration")

    # ------------------------------------------------------------------ #
    # Receive loop                                                         #
    # ------------------------------------------------------------------ #

    async def _receive_loop(self) -> None:
        """Hauptschleife: Bytes lesen und Nachrichten verarbeiten."""
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
                # 60s kein Byte → Verbindung prüfen
                _LOGGER.debug("No data for 60s – connection may have dropped")
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
        """Vollständige Nachrichten aus dem Byte-Puffer extrahieren."""
        while True:
            # Start-Delimiter suchen
            try:
                start = self._buffer.index(MSG_START)
                if start > 0:
                    _LOGGER.debug("Discarding %d bytes before message start", start)
                    self._buffer = self._buffer[start:]
            except ValueError:
                self._buffer.clear()
                return

            if len(self._buffer) < 2:
                return

            length = self._buffer[1]
            msg_len = length + 2

            if length < 5 or length >= 0x7E:
                _LOGGER.debug("Invalid length byte 0x%02X – skipping", length)
                self._buffer = self._buffer[1:]
                continue

            if len(self._buffer) < msg_len:
                return  # Noch nicht genug Bytes

            if self._buffer[msg_len - 1] != MSG_END:
                _LOGGER.debug("Missing end delimiter – skipping byte")
                self._buffer = self._buffer[1:]
                continue

            msg_data = bytes(self._buffer[:msg_len])
            self._buffer = self._buffer[msg_len:]

            parsed = parse_message(msg_data)
            if parsed:
                src, msg_type, payload = parsed
                await self._handle_message(src, msg_type, payload)
            else:
                _LOGGER.debug("Failed to parse: %s", msg_data.hex())

    async def _handle_message(self, src: int, msg_type: bytes, payload: bytes) -> None:
        """Empfangene Nachricht verarbeiten."""
        _LOGGER.debug(
            "Rx: src=%02X type=%s payload=%s",
            src, msg_type.hex(), payload.hex() if payload else "",
        )

        if msg_type == self.MT_STATUS:
            self._parse_status(payload)
            # FIX: Event NACH dem Parse setzen, nicht davor
            self._first_status_parsed.set()
            self._notify_update()

        elif msg_type == self.MT_CONFIG:
            self._parse_config(payload)
            self._config_event.set()

        elif msg_type == self.MT_INFO:
            self._parse_info(payload)

        elif msg_type == self.MT_FILTER:
            self._parse_filter_cycles(payload)

        elif msg_type == self.MT_READY:
            if self._message_queue:
                msg = self._message_queue.pop(0)
                await self._send_raw(msg)

        elif msg_type == self.MT_NTS:
            pass

        else:
            _LOGGER.debug("Unknown message type: %s", msg_type.hex())

    # ------------------------------------------------------------------ #
    # Status-Parser                                                        #
    # ------------------------------------------------------------------ #

    def _parse_status(self, payload: bytes) -> None:
        """Status-Nachricht (0xAF 0x13) parsen."""
        if len(payload) < 20:
            _LOGGER.warning("Status message too short: %d bytes", len(payload))
            return

        prev_heat_mode = self._status.heat_mode

        self._status.hold_mode = (payload[0] & 0x05) != 0
        self._status.priming   = payload[1] == 0x01

        # Temperatur-Skala muss VOR der Temperatur-Konvertierung gelesen werden
        flags9 = payload[9]
        self._status.temp_scale_celsius = (flags9 & 0x01) != 0
        self._status.clock_24hr         = (flags9 & 0x02) != 0
        self._status.filter1_running    = (flags9 & 0x04) != 0
        self._status.filter2_running    = (flags9 & 0x08) != 0

        # Aktuelle Temperatur
        if payload[2] != 0xFF:
            raw = payload[2]
            self._status.current_temp = (
                raw / 2.0 if self._status.temp_scale_celsius else float(raw)
            )
        else:
            self._status.current_temp = None

        # Uhrzeit
        self._status.hour   = payload[3]
        self._status.minute = payload[4]

        # Heat-Mode (Bits 0-1 in Byte 5)
        heat_mode_raw = payload[5] & 0x03
        self._status.heat_mode = HeatMode(heat_mode_raw) if heat_mode_raw <= 2 else HeatMode.READY

        # Heat-State und Temp-Range
        flags10 = payload[10]
        self._status.heating    = (flags10 & 0x30) != 0
        self._status.temp_range = TempRange.HIGH if (flags10 & 0x04) else TempRange.LOW

        # Pumpen
        flags11 = payload[11]
        self._status.pump1 = flags11 & 0x03
        self._status.pump2 = (flags11 >> 2) & 0x03
        self._status.pump3 = (flags11 >> 4) & 0x03
        self._status.pump4 = (flags11 >> 6) & 0x03

        flags12 = payload[12]
        self._status.pump5 = flags12 & 0x03
        self._status.pump6 = (flags12 >> 2) & 0x03

        # Zirkulationspumpe & Blower
        flags13 = payload[13]
        self._status.circ_pump = (flags13 & 0x02) != 0
        self._status.blower    = (flags13 >> 2) & 0x03

        # Lichter
        flags14 = payload[14]
        self._status.light1 = (flags14 & 0x03) != 0
        self._status.light2 = ((flags14 >> 2) & 0x03) != 0

        # Mister & Aux
        flags15 = payload[15]
        self._status.mister = (flags15 & 0x01) != 0
        self._status.aux1   = (flags15 & 0x08) != 0
        self._status.aux2   = (flags15 & 0x10) != 0

        # Zieltemperatur (Byte 20)
        if len(payload) > 20:
            raw_target = payload[20]
            self._status.target_temp = (
                raw_target / 2.0 if self._status.temp_scale_celsius else float(raw_target)
            )

        # FIX: Heat-mode-Changed-Event auslösen wenn sich der Modus geändert hat
        if self._status.heat_mode != prev_heat_mode:
            self._heat_mode_changed.set()

        _LOGGER.debug(
            "Status: temp=%s target=%s heating=%s mode=%s pumps=[%d,%d,%d] lights=[%s,%s]",
            self._status.current_temp,
            self._status.target_temp,
            self._status.heating,
            self._status.heat_mode.name,
            self._status.pump1, self._status.pump2, self._status.pump3,
            self._status.light1, self._status.light2,
        )

    def _parse_config(self, payload: bytes) -> None:
        if len(payload) < 5:
            return
        _LOGGER.debug("Parsing config: %s", payload.hex())
        if len(payload) >= 6:
            pump_info = payload[4]
            self._config.pump_speeds[0] = pump_info & 0x03
            self._config.pump_speeds[1] = (pump_info >> 2) & 0x03
            self._config.pump_speeds[2] = (pump_info >> 4) & 0x03
            self._config.pump_speeds[3] = (pump_info >> 6) & 0x03
            self._config.pump_count = sum(1 for s in self._config.pump_speeds if s > 0)
        if len(payload) >= 7:
            misc = payload[5]
            self._config.has_circ_pump = (misc & 0x02) != 0
            self._config.has_blower    = (misc & 0x0C) != 0
            self._config.blower_speeds = (misc >> 2) & 0x03
        if len(payload) >= 8:
            light_info = payload[6]
            self._config.light_count = 2 if (light_info & 0x0C) else (1 if (light_info & 0x03) else 0)
        _LOGGER.info(
            "Config: pumps=%d speeds=%s circ=%s blower=%s lights=%d",
            self._config.pump_count, self._config.pump_speeds,
            self._config.has_circ_pump, self._config.has_blower,
            self._config.light_count,
        )

    def _parse_info(self, payload: bytes) -> None:
        if len(payload) >= 3:
            model_bytes = payload[:3]
            self._config.model = f"M{model_bytes[0]:d}_V{model_bytes[1]:d}.{model_bytes[2]:d}"
            _LOGGER.info("Spa model info: %s", self._config.model)

    def _parse_filter_cycles(self, payload: bytes) -> None:
        _LOGGER.debug("Filter cycles: %s", payload.hex())

    # ------------------------------------------------------------------ #
    # Senden                                                               #
    # ------------------------------------------------------------------ #

    async def _send_raw(self, data: bytes) -> bool:
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
        msg = build_message(SRC_WIFI, msg_type, payload)
        return await self._send_raw(msg)

    async def _request_config(self, config_type: int) -> None:
        payload = bytes([config_type, 0x00, 0x00])
        await self._send_message(self.MT_CONFIG_REQ, payload)
        _LOGGER.debug("Requested config type %d", config_type)

    # ------------------------------------------------------------------ #
    # Steuer-Kommandos                                                     #
    # ------------------------------------------------------------------ #

    async def toggle_pump(self, pump_num: int) -> None:
        if pump_num < 1 or pump_num > 6:
            return
        item = ToggleItem.PUMP1 + (pump_num - 1)
        await self._send_message(self.MT_TOGGLE, bytes([item, 0x00]))
        _LOGGER.debug("Toggled pump %d", pump_num)

    async def toggle_light(self, light_num: int) -> None:
        if light_num < 1 or light_num > 2:
            return
        item = ToggleItem.LIGHT1 if light_num == 1 else ToggleItem.LIGHT2
        await self._send_message(self.MT_TOGGLE, bytes([item, 0x00]))
        _LOGGER.debug("Toggled light %d", light_num)

    async def toggle_blower(self) -> None:
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.BLOWER, 0x00]))

    async def toggle_mister(self) -> None:
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.MISTER, 0x00]))

    async def toggle_heat_mode(self) -> None:
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.HEAT_MODE, 0x00]))

    async def toggle_temp_range(self) -> None:
        await self._send_message(self.MT_TOGGLE, bytes([ToggleItem.TEMP_RANGE, 0x00]))

    async def set_target_temperature(self, temp: float) -> None:
        """Zieltemperatur setzen."""
        wire_temp = int(temp * 2) if self._status.temp_scale_celsius else int(temp)
        wire_temp = max(0, min(255, wire_temp))
        await self._send_message(self.MT_SET_TEMP, bytes([wire_temp]))
        _LOGGER.debug("Set target temp → %s (wire: %d)", temp, wire_temp)

    async def set_temperature(self, temp: float) -> None:
        """Alias für set_target_temperature() – verwendet von climate.py und number.py."""
        await self.set_target_temperature(temp)

    async def set_heat_mode(self, mode: HeatMode) -> None:
        """
        Heat-Mode setzen.

        FIX: Wartet nach jedem Toggle auf das _heat_mode_changed-Event
             (gesetzt von _parse_status wenn sich heat_mode ändert),
             statt blind asyncio.sleep(0.3) zu verwenden.
             Timeout pro Toggle: 2s (Spa sendet ~3.3 Hz → spätestens nach 0.3s).
        """
        if self._status.heat_mode == mode:
            return

        # Balboa HeatMode cycling: READY → REST → READY_IN_REST → READY
        for attempt in range(3):
            self._heat_mode_changed.clear()
            await self.toggle_heat_mode()
            try:
                await asyncio.wait_for(self._heat_mode_changed.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "No heat mode change response after toggle %d/3", attempt + 1
                )
                continue

            if self._status.heat_mode == mode:
                _LOGGER.debug("Heat mode successfully set to %s", mode.name)
                return

        _LOGGER.error(
            "Failed to set heat mode to %s after 3 attempts (current: %s)",
            mode.name, self._status.heat_mode.name,
        )

    async def set_time(self, hour: int, minute: int, is_24h: bool = True) -> None:
        flags = 0x80 if is_24h else 0x00
        await self._send_message(self.MT_SET_TIME, bytes([flags | hour, minute]))
        _LOGGER.debug("Set time to %02d:%02d", hour, minute)

    async def set_pump(self, pump_num: int, speed: int) -> None:
        if pump_num < 1 or pump_num > 6:
            return
        current   = getattr(self._status, f"pump{pump_num}", 0)
        max_speed = self._config.pump_speeds[pump_num - 1]
        if max_speed == 0:
            return
        toggles = (speed - current) % (max_speed + 1)
        for i in range(toggles):
            await self.toggle_pump(pump_num)
            if i < toggles - 1:
                await asyncio.sleep(0.2)

    async def set_light(self, light_num: int, on: bool) -> None:
        current = getattr(self._status, f"light{light_num}", False)
        if current != on:
            await self.toggle_light(light_num)
