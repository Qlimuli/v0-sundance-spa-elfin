"""Sensor-Entitäten (Read-Only) für Wassertemperatur und Verbindungsstatus."""
from homeassistant.components.sensor import SensorEntity, SensorDeviceClass, SensorStateClass
from homeassistant.const import UnitOfTemperature
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from .const import DOMAIN, UPDATE_TOPIC

async def async_setup_entry(hass, entry, async_add_entities):
    """Richtet die Sensor Plattform ein."""
    client = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        SundanceTempSensor(client),
        SundanceConnectionSensor(client)
    ])

class SundanceTempSensor(SensorEntity):
    """Gibt die Temperatur als reinen Sensor (z.B. für Diagramme) aus."""
    
    _attr_name = "Sundance Aktuelle Temperatur"
    _attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, client):
        self.client = client
        self._attr_unique_id = f"sundance_sensor_temp_{self.client.host}"

    async def async_added_to_hass(self):
        self.async_on_remove(
            async_dispatcher_connect(self.hass, UPDATE_TOPIC, self.async_write_ha_state)
        )

    @property
    def native_value(self):
        return self.client.data.get("temp")

    @property
    def available(self) -> bool:
        return self.client.connected

class SundanceConnectionSensor(SensorEntity):
    """Ein Sensor, der den Status der TCP Verbindung zum Elfin anzeigt."""
    
    _attr_name = "Sundance Elfin Verbindung"
    _attr_icon = "mdi:network"

    def __init__(self, client):
        self.client = client
        self._attr_unique_id = f"sundance_conn_{self.client.host}"

    async def async_added_to_hass(self):
        self.async_on_remove(
            async_dispatcher_connect(self.hass, UPDATE_TOPIC, self.async_write_ha_state)
        )

    @property
    def native_value(self):
        return "Verbunden" if self.client.connected else "Getrennt"
