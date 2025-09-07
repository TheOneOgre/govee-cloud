"""Govee light platform."""
import logging
from datetime import datetime, timedelta

from homeassistant.util.color import value_to_brightness
from propcache import cached_property

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_HS_COLOR,
    ColorMode,
    LightEntity,
)
from homeassistant.const import CONF_DELAY
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import color

from .const import (
    DOMAIN,
    CONF_OFFLINE_IS_OFF,
    CONF_USE_ASSUMED_STATE,
    COLOR_TEMP_KELVIN_MIN,
    COLOR_TEMP_KELVIN_MAX,
)
from .api import GoveeClient
from .models import GoveeDevice, GoveeSource

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass, entry, async_add_entities):
    """Set up the Govee Light platform."""
    _LOGGER.debug("Setting up Govee lights")
    config = entry.data
    options = entry.options
    entry_data = hass.data[DOMAIN].get(entry.entry_id) or hass.data[DOMAIN]
    hub = entry_data["hub"]


    # polling interval
    update_interval = timedelta(
        seconds=options.get(CONF_DELAY, config.get(CONF_DELAY, 10))
    )
    coordinator = GoveeDataUpdateCoordinator(
        hass, _LOGGER, hub, update_interval=update_interval, config_entry=entry
    )
    await coordinator.async_refresh()

    # Add devices from the hub
    devices, _ = await hub.get_devices()
    entities = [GoveeLightEntity(hub, entry.title, coordinator, dev) for dev in devices]
    async_add_entities(entities, update_before_add=False)


class GoveeDataUpdateCoordinator(DataUpdateCoordinator):
    """Device state update handler."""

    def __init__(self, hass, logger, hub: GoveeClient, update_interval=None, *, config_entry):
        self._config_entry = config_entry
        self._hub = hub
        super().__init__(
            hass,
            logger,
            name=DOMAIN,
            update_interval=update_interval,
            update_method=self._async_update,
        )

    @property
    def use_assumed_state(self):
        return self._config_entry.options.get(CONF_USE_ASSUMED_STATE, True)

    @property
    def config_offline_is_off(self):
        return self._config_entry.options.get(CONF_OFFLINE_IS_OFF, False)

    async def _async_update(self):
        """Fetch data from Govee API."""
        try:
            devices, err = await self._hub.get_devices()
            if err:
                raise UpdateFailed(err)
            return devices
        except Exception as ex:
            raise UpdateFailed(f"Exception on getting states: {ex}") from ex


class GoveeLightEntity(LightEntity):
    """Representation of a Govee light."""

    def __init__(self, hub: GoveeClient, title: str, coordinator: GoveeDataUpdateCoordinator, device: GoveeDevice):
        self._hub = hub
        self._title = title
        self._coordinator = coordinator
        self._device = device

    async def async_added_to_hass(self):
        self._coordinator.async_add_listener(self.async_write_ha_state)

    @property
    def is_on(self):
        return self._device.power_state

    @property
    def brightness(self):
        return self._device.brightness if self._device.support_brightness else None

    @property
    def hs_color(self):
        return color.color_RGB_to_hs(*self._device.color) if self._device.support_color else None

    @property
    def rgb_color(self):
        return list(self._device.color) if self._device.support_color else None

    @property
    def color_temp_kelvin(self):
        return self._device.color_temp if self._device.support_color_temp else None

    @property
    def min_color_temp_kelvin(self):
        return COLOR_TEMP_KELVIN_MIN

    @property
    def max_color_temp_kelvin(self):
        return COLOR_TEMP_KELVIN_MAX

    @property
    def supported_color_modes(self) -> set[ColorMode]:
        modes = set()
        if self._device.support_color:
            modes.add(ColorMode.HS)
        if self._device.support_color_temp:
            modes.add(ColorMode.COLOR_TEMP)
        if self._device.support_brightness:
            modes.add(ColorMode.BRIGHTNESS)
        if not modes:
            modes.add(ColorMode.ONOFF)
        return modes

    @property
    def color_mode(self) -> ColorMode:
        if self._device.color_temp > 0:
            return ColorMode.COLOR_TEMP
        if self._device.support_color and any(self._device.color):
            return ColorMode.HS
        if self._device.support_brightness and self._device.brightness > 0:
            return ColorMode.BRIGHTNESS
        return ColorMode.ONOFF

    async def async_turn_on(self, **kwargs):
        err = None
        if ATTR_HS_COLOR in kwargs:
            hs_color = kwargs[ATTR_HS_COLOR]
            col = color.color_hs_to_RGB(hs_color[0], hs_color[1])
            _, err = await self._hub.set_color(self._device, col)
        elif ATTR_BRIGHTNESS in kwargs:
            bright = kwargs[ATTR_BRIGHTNESS]
            _, err = await self._hub.set_brightness(self._device, bright)
        elif ATTR_COLOR_TEMP_KELVIN in kwargs:
            color_temp = kwargs[ATTR_COLOR_TEMP_KELVIN]
            color_temp = max(COLOR_TEMP_KELVIN_MIN, min(COLOR_TEMP_KELVIN_MAX, color_temp))
            _, err = await self._hub.set_color_temp(self._device, color_temp)
        else:
            _, err = await self._hub.turn_on(self._device)

        if err:
            _LOGGER.warning("async_turn_on failed for %s: %s", self._device.device, err)

    async def async_turn_off(self, **kwargs):
        _, err = await self._hub.turn_off(self._device)
        if err:
            _LOGGER.warning("async_turn_off failed for %s: %s", self._device.device, err)

    @property
    def name(self):
        return self._device.device_name

    @property
    def unique_id(self):
        return f"govee_{self._title}_{self._device.device}"

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": self._device.device_name,
            "manufacturer": "Govee",
            "model": self._device.model,
        }

    @property
    def available(self):
        return self._device.online

    @property
    def assumed_state(self):
        return (
            self._coordinator.use_assumed_state
            and self._device.source == GoveeSource.HISTORY
        )

    @property
    def extra_state_attributes(self):
        return {
            "rate_limit_total": self._hub._limit,
            "rate_limit_remaining": self._hub._remaining,
            "rate_limit_reset": datetime.fromtimestamp(self._hub._reset).isoformat(),
            "rate_limit_on": self._hub._rate_limit_on,
        }
