"""The Sundance Spa Elfin integration."""
from __future__ import annotations

import asyncio
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import DEFAULT_PORT, RECONNECT_INTERVAL
from .spa_client import SpaClient

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.CLIMATE,
    Platform.SWITCH,
    Platform.LIGHT,
    Platform.BINARY_SENSOR,
    Platform.SENSOR,
    Platform.NUMBER,
]

type SundanceConfigEntry = ConfigEntry[SpaClient]


async def async_setup_entry(hass: HomeAssistant, entry: SundanceConfigEntry) -> bool:
    """Set up Sundance Spa from a config entry."""
    host = entry.data[CONF_HOST]
    port = entry.data.get(CONF_PORT, DEFAULT_PORT)

    _LOGGER.info("Connecting to Sundance Spa at %s:%s", host, port)
    spa = SpaClient(host, port)

    try:
        if not await spa.connect():
            raise ConfigEntryNotReady(f"Unable to connect to spa at {host}:{port}")

        # FIX: Kein doppeltes wait_for mehr.
        #      async_configuration_loaded() verwaltet seinen eigenen 30s-Timeout intern.
        #      Wir geben ihm hier 35s als äußere Absicherung.
        config_ok = False
        try:
            config_ok = await asyncio.wait_for(
                spa.async_configuration_loaded(),
                timeout=35,
            )
        except asyncio.TimeoutError:
            _LOGGER.warning(
                "Outer timeout waiting for spa configuration – continuing with defaults"
            )
            # Defaults setzen damit Entities nicht dauerhaft "Unbekannt" bleiben
            spa._use_cameo_880_defaults()  # noqa: SLF001

        if not config_ok:
            # Erster Status-Frame kam gar nicht – EW11 wahrscheinlich falsch konfiguriert
            _LOGGER.error(
                "No status frame received from spa – "
                "check EW11 is in TCP Server mode (not Modbus)"
            )
            await spa.disconnect()
            raise ConfigEntryNotReady(
                f"No data received from spa at {host}:{port}. "
                "Ensure EW11 is configured as TCP Server on port 8899."
            )

    except ConfigEntryNotReady:
        raise
    except Exception as err:
        _LOGGER.error("Error setting up spa: %s", err)
        await spa.disconnect()
        raise ConfigEntryNotReady from err

    entry.runtime_data = spa

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Reconnect-Loop im Hintergrund
    entry.async_create_background_task(
        hass,
        _reconnect_loop(hass, entry, spa),
        name=f"sundance_elfin_reconnect_{entry.entry_id}",
    )

    return True


async def _reconnect_loop(
    hass: HomeAssistant,
    entry: SundanceConfigEntry,
    spa: SpaClient,
) -> None:
    """Verbindung bei Ausfall automatisch wiederherstellen."""
    while True:
        await asyncio.sleep(RECONNECT_INTERVAL)

        if spa.connected:
            continue

        _LOGGER.info("Spa disconnected – reconnecting to %s …", spa.host)
        try:
            if await spa.connect():
                _LOGGER.info("Reconnected to %s", spa.host)
                try:
                    await asyncio.wait_for(
                        spa.async_configuration_loaded(),
                        timeout=35,
                    )
                except asyncio.TimeoutError:
                    _LOGGER.warning("Reconnect: config timeout – using defaults")
                    spa._use_cameo_880_defaults()  # noqa: SLF001
            else:
                _LOGGER.warning(
                    "Reconnect failed – retry in %ds", RECONNECT_INTERVAL
                )
        except Exception as err:
            _LOGGER.error("Reconnect error: %s", err)


async def async_unload_entry(hass: HomeAssistant, entry: SundanceConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.disconnect()
    return unload_ok
