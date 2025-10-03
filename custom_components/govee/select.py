"""Scene select platform for Govee devices."""
from __future__ import annotations

import logging
from typing import List

from homeassistant.components.select import SelectEntity
from homeassistant.exceptions import HomeAssistantError

from .const import DOMAIN
from .api import GoveeClient

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass, entry, async_add_entities):
    """Set up scene Select entities."""
    entry_data = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if not entry_data:
        return

    hub: GoveeClient = entry_data.get("hub")
    if not hub:
        return

    devices, _ = await hub.get_devices()
    entities: List[GoveeSceneSelect] = []

    for device in devices:
        options = await hub.async_get_scene_options(device.device)
        if not options:
            continue
        option_names = [opt.name for opt in options]
        entities.append(
            GoveeSceneSelect(
                hub,
                entry.entry_id,
                entry.title,
                device.device,
                device.device_name or device.device,
                device.model or "",
                option_names,
            )
        )

    if entities:
        async_add_entities(entities)


class GoveeSceneSelect(SelectEntity):
    """Select entity exposing Govee scenes."""

    _attr_icon = "mdi:palette"
    _attr_should_poll = False

    def __init__(
        self,
        hub: GoveeClient,
        entry_id: str,
        entry_title: str,
        device_id: str,
        device_name: str,
        model: str,
        initial_options: List[str],
    ):
        self._hub = hub
        self._entry_id = entry_id
        self._entry_title = entry_title
        self._device_id = device_id
        self._device_name = device_name
        self._model = model
        self._options: List[str] = initial_options
        self._attr_name = f"{device_name} Scene"
        self._attr_unique_id = f"govee_scene_{entry_id}_{device_id}"

    async def async_added_to_hass(self):
        await self._refresh_options()

    async def _refresh_options(self):
        options = await self._hub.async_get_scene_options(self._device_id)
        if options:
            option_names = [opt.name for opt in options]
            if option_names != self._options:
                self._options = option_names
                self.async_write_ha_state()

    @property
    def options(self) -> List[str]:
        return self._options

    @property
    def current_option(self) -> str | None:
        device = self._hub._devices.get(self._device_id)
        if not device:
            return None
        return getattr(device, "active_scene", None)

    async def async_select_option(self, option: str) -> None:
        if option not in self._options:
            raise HomeAssistantError(f"Scene '{option}' is not available")

        ok, err = await self._hub.async_set_scene(self._device_id, option)
        if not ok:
            raise HomeAssistantError(f"Unable to set scene: {err or 'unknown error'}")
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        device = self._hub._devices.get(self._device_id)
        return bool(device and device.online)

    @property
    def device_info(self):
        device = self._hub._devices.get(self._device_id)
        if not device:
            return None
        return {
            "identifiers": {(DOMAIN, device.device)},
            "name": device.device_name or device.device,
            "manufacturer": "Govee",
            "model": device.model,
        }
