"""Sundance Spa – Light Entity (RGB-Licht mit Farbmodi)."""
from __future__ import annotations
import logging

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_EFFECT,
    ATTR_HS_COLOR,
    ColorMode,
    LightEntity,
    LightEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import DOMAIN, SpaCoordinator, BTN_LIGHT, BTN_LIGHT_COLOR, LIGHT_MODE_MAP

_LOGGER = logging.getLogger(__name__)

# Alle Farb-Effekte die das Spa kennt (aus LIGHT_MODE_MAP), ohne "Off"
EFFECT_LIST = [v for k, v in LIGHT_MODE_MAP.items() if v != "Off"]


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    data = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([SpaLight(data["coordinator"], entry)])


class SpaLight(CoordinatorEntity, LightEntity):
    """RGB-Licht-Entität für den Whirlpool."""

    _attr_has_entity_name       = True
    _attr_name                  = "Licht"
    _attr_color_mode            = ColorMode.HS
    _attr_supported_color_modes = {ColorMode.HS}
    _attr_supported_features    = LightEntityFeature.EFFECT
    _attr_effect_list           = EFFECT_LIST

    def __init__(self, coordinator: SpaCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id   = f"{entry.entry_id}_light"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name="Sundance Spa",
            manufacturer="Sundance / Balboa",
            model="RS485-TCP",
        )

    # ── Interner Datenzugriff ─────────────────────────────────────

    @property
    def _ldata(self) -> dict | None:
        """Gibt Licht-Daten zurück oder None wenn nicht verfügbar."""
        if not self.coordinator.data:
            return None
        return self.coordinator.data.get("lights")

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()

    # ── Zustand ──────────────────────────────────────────────────

    @property
    def is_on(self) -> bool:
        """True wenn Licht an."""
        if not self._ldata:
            return False
        return bool(self._ldata.get("on", False))

    @property
    def brightness(self) -> int | None:
        """
        Helligkeit 0–255 (HA-Standard).
        Der Spa liefert brightness_raw als 0–100%.
        Umrechnung: raw * 255 / 100
        """
        if not self._ldata:
            return None
        raw = self._ldata.get("brightness_raw", 0)
        return min(255, int(raw * 255 / 100))

    @property
    def hs_color(self) -> tuple[float, float] | None:
        """Hue/Saturation Farbe."""
        if not self._ldata:
            return None
        return self._ldata.get("hs_color")

    @property
    def effect(self) -> str | None:
        """Aktueller Licht-Effekt."""
        if not self._ldata:
            return None
        mode = self._ldata.get("mode")
        return mode if mode in EFFECT_LIST else None

    @property
    def extra_state_attributes(self) -> dict:
        if not self._ldata:
            return {}
        return {
            "mode":           self._ldata.get("mode"),
            "mode_raw":       self._ldata.get("mode_raw"),
            "rgb_r":          self._ldata.get("r"),
            "rgb_g":          self._ldata.get("g"),
            "rgb_b":          self._ldata.get("b"),
            "brightness_pct": self._ldata.get("brightness"),
        }

    # ── Aktionen ─────────────────────────────────────────────────

    async def async_turn_on(self, **kwargs) -> None:
        """
        Licht einschalten, optional mit Effekt.

        HA ruft diese Methode mit verschiedenen kwargs auf:
          - Ohne kwargs          → einfach einschalten
          - ATTR_EFFECT          → Effekt/Farbe wechseln
          - ATTR_BRIGHTNESS      → wird ignoriert (Spa hat feste Stufen)
          - ATTR_HS_COLOR        → wird ignoriert (nur Effekte unterstützt)
        """
        client = self.coordinator.client

        # Schritt 1: Licht einschalten wenn aus
        if not self.is_on:
            _LOGGER.debug("Spa-Licht: Einschalten via BTN_LIGHT (%s)", BTN_LIGHT)
            await client.send_button(BTN_LIGHT)
            await client.wait_lights(n=3, timeout=5.0)

            # Prüfen ob Licht jetzt wirklich an ist
            if not self.is_on:
                _LOGGER.warning("Spa-Licht: Nach BTN_LIGHT immer noch AUS")

        # Schritt 2: Effekt wechseln wenn gewünscht
        if ATTR_EFFECT in kwargs:
            desired_effect = kwargs[ATTR_EFFECT]
            _LOGGER.debug(
                "Spa-Licht: Effekt wechseln zu '%s' (aktuell: '%s')",
                desired_effect,
                self.effect,
            )

            # Maximal len(EFFECT_LIST) Versuche, dann einmal rum
            max_tries = len(EFFECT_LIST) + 2
            for attempt in range(max_tries):
                current = self._ldata.get("mode") if self._ldata else None
                if current == desired_effect:
                    _LOGGER.debug(
                        "Spa-Licht: Effekt '%s' nach %d Versuchen erreicht",
                        desired_effect, attempt,
                    )
                    break

                _LOGGER.debug(
                    "Spa-Licht: BTN_LIGHT_COLOR senden (Versuch %d/%d, "
                    "aktuell='%s', ziel='%s')",
                    attempt + 1, max_tries, current, desired_effect,
                )
                await client.send_button(BTN_LIGHT_COLOR)
                await client.wait_lights(n=2, timeout=3.0)
            else:
                _LOGGER.warning(
                    "Spa-Licht: Effekt '%s' nach %d Versuchen nicht erreicht",
                    desired_effect, max_tries,
                )

        await self.coordinator.async_request_refresh()

    async def async_turn_off(self, **kwargs) -> None:
        """Licht ausschalten."""
        if not self.is_on:
            return
        _LOGGER.debug("Spa-Licht: Ausschalten via BTN_LIGHT (%s)", BTN_LIGHT)
        await self.coordinator.client.send_button(BTN_LIGHT)
        await self.coordinator.client.wait_lights(n=3, timeout=5.0)
        await self.coordinator.async_request_refresh()
