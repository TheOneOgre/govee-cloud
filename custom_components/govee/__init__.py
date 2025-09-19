"""The Govee integration."""
import logging

import homeassistant.helpers.config_validation as cv  # type: ignore
from homeassistant.config_entries import ConfigEntry  # type: ignore
from homeassistant.core import HomeAssistant  # type: ignore

from .const import DOMAIN, CONF_IOT_EMAIL, CONF_IOT_PASSWORD, CONF_IOT_PUSH_ENABLED
from .iot_client import GoveeIoTClient
from .api import GoveeClient
from .learning_storage import GoveeLearningStorage

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[str] = ["light"]

# This integration is config-entry only (no YAML options)
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)


async def async_setup(hass: HomeAssistant, config: dict):
    """Set up the Govee integration (YAML not supported)."""
    hass.data.setdefault(DOMAIN, {})
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Set up Govee from a config entry."""

    storage = GoveeLearningStorage(hass.config.config_dir, hass)
    # API key no longer required; use IoT discovery
    hub = await GoveeClient.create("", storage, hass, config_entry=entry)


    # New style: per entry_id
    hass.data[DOMAIN][entry.entry_id] = {"hub": hub}
    # Legacy style: global "hub" key
    hass.data[DOMAIN]["hub"] = hub

    # Start IoT client early so control is ready ASAP
    try:
        opts = entry.options
        data = entry.data
        enabled = opts.get(CONF_IOT_PUSH_ENABLED, True)
        email = opts.get(CONF_IOT_EMAIL) or data.get(CONF_IOT_EMAIL)
        password = opts.get(CONF_IOT_PASSWORD) or data.get(CONF_IOT_PASSWORD)
        if enabled and email and password:
            iot = GoveeIoTClient(hass, entry, hub)
            _LOGGER.debug(
                "Starting Govee IoT: enabled=%s email=%s password=%s",
                enabled,
                bool(email),
                bool(password),
            )
            await iot.start()
            hass.data[DOMAIN][entry.entry_id]["iot_client"] = iot
            try:
                _LOGGER.debug(
                    "Govee IoT started: can_control=%s account_topic=%s token_present=%s",
                    iot.can_control,
                    getattr(iot, "_account_topic", None) is not None,
                    bool(getattr(iot, "_token", None)),
                )
            except Exception:
                pass
        else:
            _LOGGER.debug("Govee IoT not started: enabled=%s email=%s password=%s", enabled, bool(email), bool(password))
            # Proactively notify users migrating from API-key versions to IoT credentials
            try:
                # Repairs issue (shows in Settings -> Repairs) with deep link to Integrations
                from homeassistant.helpers import issue_registry as ir  # type: ignore
                issue_registry = ir.async_get(hass)
                issue_registry.async_create_issue(
                    DOMAIN,
                    "missing_iot_credentials",
                    is_fixable=False,
                    severity=ir.IssueSeverity.ERROR,
                    translation_key="missing_iot_credentials",
                    translation_placeholders={},
                    learn_more_url="/config/integrations/integration/govee",
                )
            except Exception:
                pass
            try:
                # Persistent notification as a user-facing prompt
                msg = (
                    "Govee has switched to IoT-based control. Open Integrations -> "
                    "Govee -> Configure and enter your Govee account email and password.\n\n"
                    "Quick link: /config/integrations/integration/govee"
                )
                await hass.services.async_call(
                    "persistent_notification",
                    "create",
                    {
                        "title": "Govee: IoT credentials required",
                        "message": msg,
                        "notification_id": f"{DOMAIN}_missing_iot_credentials",
                    },
                )
            except Exception:
                pass
    except Exception as ex:
        _LOGGER.warning("Govee IoT push not started: %s", ex)

    devices, err = await hub.get_devices()
    if err:
        _LOGGER.warning("Could not load Govee devices at startup: %s", err)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

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


