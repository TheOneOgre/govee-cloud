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
    _LOGGER.debug("Registering %s Govee light entities", len(entities))
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
        # Prefer per-device limits if known
        return dev.color_temp_min or COLOR_TEMP_KELVIN_MIN

    @property
    def max_color_temp_kelvin(self) -> int | None:
        """Coldest color temperature supported in Kelvin."""
        dev = self._device
        if not dev or not dev.support_color_temp:
            return None
        return dev.color_temp_max or COLOR_TEMP_KELVIN_MAX

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

        # Decide which operations to perform from kwargs
        want_color = ATTR_HS_COLOR in kwargs and dev.support_color
        want_ct = (ATTR_COLOR_TEMP_KELVIN in kwargs or "color_temp" in kwargs) and dev.support_color_temp
        want_brightness = ATTR_BRIGHTNESS in kwargs and dev.support_brightness

        # Ensure power on if any command other than plain turn on
        if (want_color or want_ct or want_brightness) and not dev.power_state and getattr(dev, "support_turn", True):
            ok_on, err_on = await self._hub.turn_on(dev)
            if ok_on:
                dev.power_state = True
            else:
                _LOGGER.debug("turn_on before command failed for %s: %s", dev.device, err_on)

        last_ok = False
        last_err = None

        # If both color and color temp are present, prefer color and ignore CT
        if want_color:
            hs_color = kwargs[ATTR_HS_COLOR]
            col = color.color_hs_to_RGB(hs_color[0], hs_color[1])
            ok, err = await self._hub.set_color(dev, col)
            last_ok, last_err = ok, err
            if ok:
                dev.color = col
                dev.color_temp = 0
                dev.power_state = True
        elif want_ct:
            # Accept both Kelvin (preferred) and mireds (deprecated) from HA
            if ATTR_COLOR_TEMP_KELVIN in kwargs:
                color_temp = int(kwargs[ATTR_COLOR_TEMP_KELVIN])
            else:
                mireds = max(1, int(kwargs["color_temp"]))
                color_temp = int(round(1_000_000 / mireds))

            # Clamp and round to device capabilities if known
            vmin = dev.color_temp_min or COLOR_TEMP_KELVIN_MIN
            vmax = dev.color_temp_max or COLOR_TEMP_KELVIN_MAX
            step = dev.color_temp_step or 1
            color_temp = max(vmin, min(vmax, color_temp))
            if step and step > 1:
                off = color_temp - vmin
                color_temp = vmin + int(round(off / step)) * step
                color_temp = max(vmin, min(vmax, color_temp))
            _LOGGER.debug(
                "Turn_on CT → %s request=%sK clamped=%sK (range %s-%s step %s)",
                dev.device, kwargs.get(ATTR_COLOR_TEMP_KELVIN) or kwargs.get("color_temp"), color_temp, vmin, vmax, step,
            )
            ok, err = await self._hub.set_color_temp(dev, color_temp)
            last_ok, last_err = ok, err
            if ok:
                dev.color_temp = color_temp
                dev.color = (0, 0, 0)
                dev.power_state = True

        # Apply brightness after mode selection if requested
        if want_brightness:
            ha_bright = kwargs[ATTR_BRIGHTNESS]  # 0-255 from HA
            ok, err = await self._hub.set_brightness(dev, ha_bright)
            # Track the result but don't override an earlier failure if this succeeds
            if ok:
                dev.brightness = ha_bright
                dev.power_state = True
            last_ok = last_ok or ok
            if err:
                last_err = err

        # If no specific attribute was requested, this is a plain turn on
        if not (want_color or want_ct or want_brightness):
            ok, err = await self._hub.turn_on(dev)
            last_ok, last_err = ok, err
            if ok:
                dev.power_state = True

        if last_ok:
            self.async_write_ha_state()
        elif last_err:
            _LOGGER.warning("async_turn_on failed for %s: %s", dev.device, last_err)



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
