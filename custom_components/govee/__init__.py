"""The Govee integration."""
import asyncio
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_KEY
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import PlatformNotReady

from .const import DOMAIN
from .api import GoveeClient
from .learning_storage import GoveeLearningStorage

_LOGGER = logging.getLogger(__name__)
PLATFORMS = ["light"]


def setup(hass, config):
    """Legacy sync setup (unused)."""
    return True


async def async_setup(hass: HomeAssistant, config: dict):
    """Set up the Govee integration from YAML (not used, config entries only)."""
    hass.data[DOMAIN] = {}
    return True


def is_online(online: bool):
    """Log when API appears offline/online."""
    msg = "API is offline."
    if online:
        msg = "API is back online."
    _LOGGER.warning(msg)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Set up Govee from a config entry."""

    config = entry.data
    options = entry.options
    api_key = options.get(CONF_API_KEY, config.get(CONF_API_KEY, ""))

    # Create learning storage and API client
    storage = GoveeLearningStorage(hass.config.config_dir, hass)
    hub = await GoveeClient.create(api_key, storage)

    hass.data[DOMAIN] = {"hub": hub}

    # Verify API works by fetching devices
    devices, err = await hub.get_devices()
    if err:
        _LOGGER.warning("Could not connect to Govee API: %s", err)
        await hub.close()
        await async_unload_entry(hass, entry)
        raise PlatformNotReady()

    _LOGGER.info("Loaded %d Govee devices", len(devices))

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Unload a config entry."""
    unload_ok = all(
        await asyncio.gather(
            *[
                _unload_component_entry(hass, entry, component)
                for component in PLATFORMS
            ]
        )
    )

    if unload_ok:
        hub = hass.data[DOMAIN].pop("hub", None)
        if hub:
            await hub.close()

    return unload_ok


def _unload_component_entry(
    hass: HomeAssistant, entry: ConfigEntry, component: str
) -> bool:
    """Unload an entry for a specific platform."""
    try:
        return hass.config_entries.async_forward_entry_unload(entry, component)
    except ValueError:
        # Config entry was never loaded
        return False
    except Exception as ex:
        _LOGGER.warning(
            "Continuing on exception when unloading %s component's entry: %s",
            component,
            ex,
        )
        return False
