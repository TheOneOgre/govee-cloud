"""The Govee integration."""
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_KEY
from homeassistant.core import HomeAssistant

from .const import DOMAIN, CONF_IOT_EMAIL, CONF_IOT_PASSWORD, CONF_IOT_PUSH_ENABLED
from .iot_client import GoveeIoTClient
from .api import GoveeClient
from .learning_storage import GoveeLearningStorage

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = ["light"]


async def async_setup(hass: HomeAssistant, config: dict):
    """Set up the Govee integration (YAML not supported)."""
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Set up Govee from a config entry."""

    api_key = entry.options.get(CONF_API_KEY, entry.data.get(CONF_API_KEY, ""))

    storage = GoveeLearningStorage(hass.config.config_dir, hass)
    hub = await GoveeClient.create(api_key, storage, hass)


    # New style: per entry_id
    hass.data[DOMAIN][entry.entry_id] = {"hub": hub}
    # Legacy style: global "hub" key
    hass.data[DOMAIN]["hub"] = hub

    devices, err = await hub.get_devices()
    if err:
        _LOGGER.warning("Could not connect to Govee API at startup: %s", err)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Optionally start IoT push client (read-only) if enabled and credentials present
    try:
        if entry.options.get(CONF_IOT_PUSH_ENABLED, False) and entry.options.get(CONF_IOT_EMAIL) and entry.options.get(CONF_IOT_PASSWORD):
            iot = GoveeIoTClient(hass, entry, hub)
            await iot.start()
            hass.data[DOMAIN][entry.entry_id]["iot_client"] = iot
    except Exception as ex:
        _LOGGER.warning("Govee IoT push not started: %s", ex)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    if unload_ok:
        # New style cleanup
        hub = hass.data[DOMAIN].get(entry.entry_id, {}).pop("hub", None)
        if hub:
            await hub.close()
        # Stop IoT client if running
        iot = hass.data[DOMAIN].get(entry.entry_id, {}).pop("iot_client", None)
        if iot:
            try:
                await iot.stop()
            except Exception:
                pass
        hass.data[DOMAIN].pop(entry.entry_id, None)

        # Legacy cleanup
        hass.data[DOMAIN].pop("hub", None)

    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Handle reload of a config entry."""
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)
