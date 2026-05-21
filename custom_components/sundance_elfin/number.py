"""Number platform for Sundance Spa (temperature setting)."""
from __future__ import annotations

import logging

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import SundanceConfigEntry
from .entity import SundanceEntity
from .spa_client import SpaClient

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: SundanceConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up number entities."""
    spa = entry.runtime_data
    async_add_entities([SundanceTargetTempNumber(spa, entry.entry_id)])


class SundanceTargetTempNumber(SundanceEntity, NumberEntity):
    """Number entity for target temperature setting."""

    _attr_mode = NumberMode.SLIDER
    _attr_icon = "mdi:thermometer"

    def __init__(self, spa: SpaClient, entry_id: str) -> None:
        """Initialize target temperature number."""
        super().__init__(spa, entry_id, "set_temperature")
        self._attr_name = "Set Temperature"

    @property
    def native_unit_of_measurement(self) -> str:
        """Return the unit of measurement."""
        if self._spa.temperature_unit_celsius:
            return UnitOfTemperature.CELSIUS
        return UnitOfTemperature.FAHRENHEIT

    @property
    def native_value(self) -> float | None:
        """Return the current target temperature."""
        return self._spa.target_temperature

    @property
    def native_min_value(self) -> float:
        """Return the minimum value."""
        return self._spa.temperature_minimum

    @property
    def native_max_value(self) -> float:
        """Return the maximum value."""
        return self._spa.temperature_maximum

    @property
    def native_step(self) -> float:
        """Return the step value."""
        return 0.5 if self._spa.temperature_unit_celsius else 1.0

    async def async_set_native_value(self, value: float) -> None:
        """Set the target temperature."""
        # set_temperature() ist ein Alias für set_target_temperature() in spa_client.py
        await self._spa.set_temperature(value)
