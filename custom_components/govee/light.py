"""Govee light platform."""
import logging
from datetime import datetime, timedelta

from homeassistant.util import color
from homeassistant.util.color import value_to_brightness
from homeassistant.const import CONF_DELAY
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed


from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ATTR_COLOR_TEMP_KELVIN,
    ATTR_HS_COLOR,
    ColorMode,
    LightEntity,
)

from propcache import cached_property
from .const import (
    DOMAIN,
    CONF_OFFLINE_IS_OFF,
    CONF_USE_ASSUMED_STATE,
    CONF_POLLING_MODE, 
    COLOR_TEMP_KELVIN_MIN,
    COLOR_TEMP_KELVIN_MAX,
)
from .api import GoveeClient
from .models import GoveeDevice, GoveeSource

_LOGGER = logging.getLogger(__name__)


DEFAULT_SCAN_INTERVAL = 60  # safe fallback

async def async_setup_entry(hass, entry, async_add_entities):
    """Set up the Govee Light platform."""
    _LOGGER.debug("Setting up Govee lights")
    config = entry.data
    options = entry.options
    entry_data = hass.data[DOMAIN].get(entry.entry_id) or hass.data[DOMAIN]
    hub = entry_data["hub"]

    # Work out polling mode and delay
    mode = options.get(CONF_POLLING_MODE, "auto")
    delay = options.get(CONF_DELAY, config.get(CONF_DELAY, 0))

    if delay == 0:
        # Auto calculation: use count of devices to pick safe interval
        tmp_devices, _ = await hub.get_devices()
        num_devices = max(1, len(tmp_devices))
        delay = max(30, int(86400 / (10000 / num_devices)))  # safe under 10k/day

    if mode == "auto":
        tmp_devices, _ = await hub.get_devices()
        device_count = max(1, len(tmp_devices))
        safe_delay = max(30, int(86400 * device_count / 10000))  # 10k/day quota
        update_interval = timedelta(seconds=safe_delay)
        _LOGGER.warning(
            "Polling mode AUTO: %s devices → interval set to %ss (safe under 10k/day quota).",
            device_count,
            safe_delay,
        )
    else:  # manual mode
        update_interval = timedelta(seconds=delay)
        _LOGGER.warning(
            "Polling mode MANUAL: interval set to %ss. Ensure this does not exceed 10k/day quota.",
            delay,
        )

    # Coordinator drives updates
    coordinator = GoveeDataUpdateCoordinator(
        hass, _LOGGER, hub, update_interval=update_interval, config_entry=entry
    )

    # Fetch initial devices with full state (calls /devices/state once each)
    devices, _ = await hub.init_devices()

    # Prime the coordinator with that data
    coordinator.data = devices


    # Register light entities with fresh data
    entities = [GoveeLightEntity(hub, entry.title, coordinator, dev) for dev in devices]
    async_add_entities(entities, update_before_add=True)




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
        self._device_id = device.device  # store only ID

    @property
    def _device(self) -> GoveeDevice | None:
        """Always return the current device object from coordinator.data."""
        if not self._coordinator.data:
            return None
        return next((d for d in self._coordinator.data if d.device == self._device_id), None)

    async def async_added_to_hass(self):
        self._coordinator.async_add_listener(self.async_write_ha_state)

    @property
    def is_on(self):
        dev = self._device
        return dev.power_state if dev else False

    @property
    def brightness(self):
        dev = self._device
        return dev.brightness if dev and dev.support_brightness else None

    @property
    def hs_color(self):
        dev = self._device
        return color.color_RGB_to_hs(*dev.color) if dev and dev.support_color else None

    @property
    def rgb_color(self):
        dev = self._device
        return list(dev.color) if dev and dev.support_color else None

    @property
    def color_temp_kelvin(self):
        dev = self._device
        return dev.color_temp if dev and dev.support_color_temp else None

    @property
    def min_color_temp_kelvin(self) -> int | None:
        """Warmest color temperature supported in Kelvin.

        Home Assistant is deprecating mired-based min/max; expose Kelvin instead.
        """
        dev = self._device
        if not dev or not dev.support_color_temp:
            return None
        return COLOR_TEMP_KELVIN_MIN

    @property
    def max_color_temp_kelvin(self) -> int | None:
        """Coldest color temperature supported in Kelvin."""
        dev = self._device
        if not dev or not dev.support_color_temp:
            return None
        return COLOR_TEMP_KELVIN_MAX

    @property
    def supported_color_modes(self) -> set[ColorMode]:
        dev = self._device
        if not dev:
            return {ColorMode.ONOFF}

        modes = set()
        if dev.support_color:
            modes.add(ColorMode.HS)
        if dev.support_color_temp:
            modes.add(ColorMode.COLOR_TEMP)
        if dev.support_brightness and not (dev.support_color or dev.support_color_temp):
            # brightness-only (like dimmers without color)
            modes.add(ColorMode.BRIGHTNESS)
        if not modes:
            modes.add(ColorMode.ONOFF)
        return modes


    @property
    def color_mode(self) -> ColorMode:
        dev = self._device
        if not dev:
            return ColorMode.ONOFF

        # Prioritize based on what’s actually supported
        if dev.support_color and dev.color and any(dev.color):
            return ColorMode.HS
        if dev.support_color_temp and dev.color_temp > 0:
            return ColorMode.COLOR_TEMP
        if dev.support_brightness:
            return ColorMode.BRIGHTNESS
        return ColorMode.ONOFF


    async def async_turn_on(self, **kwargs):
        dev = self._device
        if not dev:
            return

        err = None
        ok = False

        if ATTR_HS_COLOR in kwargs and dev.support_color:
            hs_color = kwargs[ATTR_HS_COLOR]
            col = color.color_hs_to_RGB(hs_color[0], hs_color[1])
            ok, err = await self._hub.set_color(dev, col)
            # Trust local update even if rate limited
            if ok or (err and "Rate limit" in err):
                dev.color = col
                dev.power_state = True

        elif ATTR_BRIGHTNESS in kwargs and dev.support_brightness:
            ha_bright = kwargs[ATTR_BRIGHTNESS]  # 0–255 from HA
            ok, err = await self._hub.set_brightness(dev, ha_bright)
            if ok:
                # store the HA-side brightness so the UI reflects it immediately
                dev.brightness = ha_bright
                dev.power_state = True


        elif ATTR_COLOR_TEMP_KELVIN in kwargs and dev.support_color_temp:
            color_temp = kwargs[ATTR_COLOR_TEMP_KELVIN]
            color_temp = max(COLOR_TEMP_KELVIN_MIN, min(COLOR_TEMP_KELVIN_MAX, color_temp))
            ok, err = await self._hub.set_color_temp(dev, color_temp)
            if ok or (err and "Rate limit" in err):
                dev.color_temp = color_temp
                dev.power_state = True

        else:
            ok, err = await self._hub.turn_on(dev)
            if ok or (err and "Rate limit" in err):
                dev.power_state = True

        # Always push local state if we considered it valid
        if ok or (err and "Rate limit" in err):
            self.async_write_ha_state()
        else:
            _LOGGER.warning("async_turn_on failed for %s: %s", dev.device, err)



    async def async_turn_off(self, **kwargs):
        dev = self._device
        if not dev:
            return
        ok, err = await self._hub.turn_off(dev)
        if ok:
            dev.power_state = False
            self.async_write_ha_state()
        else:
            _LOGGER.warning("async_turn_off failed for %s: %s", dev.device, err)



    @property
    def name(self):
        dev = self._device
        return dev.device_name if dev else "Unknown"

    @property
    def unique_id(self):
        return f"govee_{self._title}_{self._device_id}"

    @property
    def device_info(self):
        dev = self._device
        return {
            "identifiers": {(DOMAIN, self.unique_id)},
            "name": dev.device_name if dev else "Unknown",
            "manufacturer": "Govee",
            "model": dev.model if dev else "Unknown",
        }

    @property
    def available(self):
        dev = self._device
        return dev.online if dev else False

    @property
    def assumed_state(self):
        dev = self._device
        return (
            self._coordinator.use_assumed_state
            and dev
            and dev.source == GoveeSource.HISTORY
        )
