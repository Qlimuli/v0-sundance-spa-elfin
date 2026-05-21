"""Diagnostics support for Sundance Spa integration."""
from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant

from . import SundanceConfigEntry


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: SundanceConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    spa = entry.runtime_data
    
    return {
        "connection": {
            "host": entry.data.get(CONF_HOST),
            "port": entry.data.get(CONF_PORT),
            "connected": spa.connected,
            "channel": f"0x{spa._channel:02X}" if spa._channel is not None else None,
        },
        "spa_info": {
            "model": spa.model or "Unknown",
            "software_id": spa.status.software_id or "Unknown",
        },
        "configuration": {
            "pump_count": spa.config.pump_count,
            "pump1_speeds": spa.config.pump1_speeds,
            "pump2_speeds": spa.config.pump2_speeds,
            "pump3_speeds": spa.config.pump3_speeds,
            "has_blower": spa.config.has_blower,
            "has_mister": spa.config.has_mister,
            "has_circ_pump": spa.config.has_circ_pump,
            "light_count": spa.config.light_count,
            "config_loaded": spa._config_loaded,
        },
        "status": {
            "current_temp": spa.status.current_temp,
            "target_temp": spa.status.target_temp,
            "temp_scale_celsius": spa.status.temp_scale_celsius,
            "temp_range": spa.status.temp_range.name,
            "heat_mode": spa.status.heat_mode.name,
            "heat_state": spa.status.heat_state.name,
            "pump1": spa.status.pump1.name,
            "pump2": spa.status.pump2.name,
            "pump3": spa.status.pump3.name,
            "light1": spa.status.light1,
            "light2": spa.status.light2,
            "blower": spa.status.blower,
            "circ_pump": spa.status.circ_pump,
            "filter_mode": spa.status.filter_mode,
            "priming": spa.status.priming,
            "hold_mode": spa.status.hold_mode,
            "panel_locked": spa.status.panel_locked,
            "hour": spa.status.hour,
            "minute": spa.status.minute,
            "clock_24hr": spa.status.clock_24hr,
        },
    }
