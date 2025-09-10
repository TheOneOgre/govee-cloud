"""Config flow for Govee integration."""

import logging
import voluptuous as vol

from homeassistant import config_entries, core, exceptions
import homeassistant.helpers.config_validation as cv
from homeassistant.const import CONF_API_KEY, CONF_DELAY, CONF_SCAN_INTERVAL
from homeassistant.core import callback

from .const import (
    CONF_DISABLE_ATTRIBUTE_UPDATES,
    CONF_OFFLINE_IS_OFF,
    CONF_USE_ASSUMED_STATE,
    CONF_IOT_EMAIL,
    CONF_IOT_PASSWORD,
    CONF_IOT_PUSH_ENABLED,
    CONF_PLATFORM_APP_ENABLED,
    DOMAIN,
)

from .api import GoveeClient
from .learning_storage import GoveeLearningStorage



_LOGGER = logging.getLogger(__name__)
CONF_POLLING_MODE = "polling_mode"   # "auto" or "manual"
DEFAULT_SCAN_INTERVAL = 60

async def validate_api_key(hass: core.HomeAssistant, user_input: dict):
    """Validate that the API key works by attempting to fetch devices."""
    api_key = user_input[CONF_API_KEY]
    hub = await GoveeClient.create(api_key, GoveeLearningStorage(hass.config.config_dir, hass))
    devices, error = await hub.get_devices()
    await hub.close()

    if error:
        raise CannotConnect(error)

    return user_input


async def validate_disabled_attribute_updates(hass: core.HomeAssistant, user_input: dict):
    """Validate the ignore_device_attributes string (placeholder for future use)."""
    disable_str = user_input.get(CONF_DISABLE_ATTRIBUTE_UPDATES, "")
    if disable_str and not isinstance(disable_str, str):
        raise CannotConnect("Invalid disabled attributes format")
    return user_input


@config_entries.HANDLERS.register(DOMAIN)
class GoveeFlowHandler(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Govee."""

    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            try:
                user_input = await validate_api_key(self.hass, user_input)
            except CannotConnect as conn_ex:
                _LOGGER.exception("Cannot connect: %s", conn_ex)
                errors[CONF_API_KEY] = "cannot_connect"
            except Exception as ex:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception: %s", ex)
                errors["base"] = "unknown"

            if not errors:
                return self.async_create_entry(title="Govee", data=user_input)

        # Step 1: basic + toggles
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_API_KEY): cv.string,
                    vol.Optional(CONF_DELAY, default=10): cv.positive_int,
                    vol.Optional(CONF_IOT_PUSH_ENABLED, default=True): cv.boolean,
                    vol.Optional(CONF_PLATFORM_APP_ENABLED, default=False): cv.boolean,
                }
            ),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Get the options flow."""
        return GoveeOptionsFlowHandler(config_entry)


class GoveeOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle options."""

    VERSION = 1

    def __init__(self, config_entry):
        self.config_entry = config_entry
        self.options = dict(config_entry.options)

    async def async_step_init(self, user_input=None):
        return await self.async_step_user(user_input)

    async def async_step_user(self, user_input=None):
        old_api_key = self.config_entry.options.get(
            CONF_API_KEY, self.config_entry.data.get(CONF_API_KEY, "")
        )
        errors = {}

        if user_input is not None:
            try:
                api_key = user_input[CONF_API_KEY]
                if old_api_key != api_key:
                    user_input = await validate_api_key(self.hass, user_input)

                user_input = await validate_disabled_attribute_updates(self.hass, user_input)
            except CannotConnect as conn_ex:
                _LOGGER.exception("Cannot connect: %s", conn_ex)
                errors[CONF_API_KEY] = "cannot_connect"
            except Exception as ex:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception: %s", ex)
                errors["base"] = "unknown"

            if not errors:
                self.options.update(user_input)
                return self.async_create_entry(title="Govee", data=self.options)

        # Build schema every time (not just on error)
        options_schema = vol.Schema(
            {
                vol.Required(CONF_API_KEY, default=old_api_key): cv.string,
                vol.Optional(
                    CONF_DELAY,
                    default=0,  # default = auto
                    description={"note": "{polling_note}"}
                ): cv.positive_int,
                vol.Required(
                    CONF_USE_ASSUMED_STATE,
                    default=self.config_entry.options.get(CONF_USE_ASSUMED_STATE, True),
                ): cv.boolean,
                vol.Required(
                    CONF_OFFLINE_IS_OFF,
                    default=self.config_entry.options.get(CONF_OFFLINE_IS_OFF, False),
                ): cv.boolean,
                vol.Optional(
                    CONF_DISABLE_ATTRIBUTE_UPDATES,
                    default=self.config_entry.options.get(CONF_DISABLE_ATTRIBUTE_UPDATES, ""),
                ): cv.string,
                vol.Required(
                    CONF_IOT_PUSH_ENABLED,
                    default=self.config_entry.options.get(CONF_IOT_PUSH_ENABLED, False),
                ): cv.boolean,
                vol.Required(
                    CONF_PLATFORM_APP_ENABLED,
                    default=self.config_entry.options.get(CONF_PLATFORM_APP_ENABLED, False),
                ): cv.boolean,
            }
        )


        return self.async_show_form(
            step_id="user",
            data_schema=options_schema,
            errors=errors,
            description_placeholders={
                "polling_note": (
                    "ℹ️ Set to **0** for automatic interval adjustment (recommended). "
                    "Manual values are allowed, but note: the Govee API allows only "
                    "**10,000 requests per day**. Too small a value may cause rate limiting."
                )
            },
        )

    async def async_step_iot(self, user_input=None):
        errors = {}
        if user_input is not None:
            self.options.update(user_input)
            return self.async_create_entry(title="Govee", data=self.options)
        iot_schema = vol.Schema(
            {
                vol.Required(CONF_IOT_EMAIL, default=self.config_entry.options.get(CONF_IOT_EMAIL, "")): cv.string,
                vol.Required(CONF_IOT_PASSWORD, default=self.config_entry.options.get(CONF_IOT_PASSWORD, "")): cv.string,
            }
        )
        return self.async_show_form(step_id="iot", data_schema=iot_schema, errors=errors)






class CannotConnect(exceptions.HomeAssistantError):
    """Error to indicate we cannot connect."""
