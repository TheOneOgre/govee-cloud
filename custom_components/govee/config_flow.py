"""Config flow for Govee integration."""

import logging
import voluptuous as vol  # pyright: ignore[reportMissingImports]

from homeassistant import config_entries, exceptions  # type: ignore
import homeassistant.helpers.config_validation as cv  # type: ignore
from homeassistant.const import CONF_DELAY  # type: ignore
from homeassistant.core import callback  # type: ignore

from .const import (
    CONF_OFFLINE_IS_OFF,
    CONF_USE_ASSUMED_STATE,
    CONF_IOT_EMAIL,
    CONF_IOT_PASSWORD,
    CONF_IOT_PUSH_ENABLED,
    CONF_IOT_CONTROL_ENABLED,
    DOMAIN,
)

# No direct imports of API/storage needed in flow



_LOGGER = logging.getLogger(__name__)


# Removed disabled-attribute option.


@config_entries.HANDLERS.register(DOMAIN)
class GoveeFlowHandler(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Govee."""

    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    def __init__(self):
        self._pending_config: dict | None = None

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            data = {
                CONF_IOT_EMAIL: user_input.get(CONF_IOT_EMAIL, ""),
                CONF_IOT_PASSWORD: user_input.get(CONF_IOT_PASSWORD, ""),
                CONF_IOT_PUSH_ENABLED: True,
                CONF_IOT_CONTROL_ENABLED: True,
                CONF_DELAY: user_input.get(CONF_DELAY, 0),
            }
            return self.async_create_entry(title="Govee", data=data)

        # Single-step: IoT credentials (and optional delay)
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_IOT_EMAIL, default=""): cv.string,
                    vol.Required(CONF_IOT_PASSWORD, default=""): cv.string,
                    vol.Optional(CONF_DELAY, default=0): cv.positive_int,
                }
            ),
            errors=errors,
        )

    # No separate IoT step in new flow

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Get the options flow."""
        return GoveeOptionsFlowHandler(config_entry)


class GoveeOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle options."""

    VERSION = 1

    def __init__(self, config_entry):
        # Do not assign to self.config_entry (deprecated in HA 2025.12)
        self._entry = config_entry
        self.options = dict(config_entry.options)
        self._pending_options: dict | None = None

    @property
    def entry(self):
        # Prefer framework-provided property if available
        return getattr(self, "config_entry", self._entry)

    async def async_step_init(self, user_input=None):
        return await self.async_step_user(user_input)

    async def async_step_user(self, user_input=None):
        errors = {}

        if user_input is not None:
            try:
                # Ensure IoT flags are set by default
                user_input[CONF_IOT_PUSH_ENABLED] = True
                user_input[CONF_IOT_CONTROL_ENABLED] = True
            except CannotConnect as conn_ex:
                _LOGGER.exception("Cannot connect: %s", conn_ex)
                errors["base"] = "cannot_connect"
            except Exception as ex:  # pylint: disable=broad-except
                _LOGGER.exception("Unexpected exception: %s", ex)
                errors["base"] = "unknown"

            if not errors:
                # Collect credentials if missing
                email = user_input.get(CONF_IOT_EMAIL) or self.entry.options.get(CONF_IOT_EMAIL, "")
                password = user_input.get(CONF_IOT_PASSWORD) or self.entry.options.get(CONF_IOT_PASSWORD, "")
                if not email or not password:
                    self._pending_options = dict(self.options)
                    self._pending_options.update(user_input)
                    return await self.async_step_iot()
                self.options.update(user_input)
                return self.async_create_entry(title="Govee", data=self.options)

        # Build schema every time (not just on error)
        options_schema = vol.Schema(
            {
                vol.Optional(
                    CONF_DELAY,
                    default=0,  # default = auto
                    description={"note": "{polling_note}"}
                ): cv.positive_int,
                vol.Required(
                    CONF_USE_ASSUMED_STATE,
                    default=self.entry.options.get(CONF_USE_ASSUMED_STATE, True),
                ): cv.boolean,
                vol.Required(
                    CONF_OFFLINE_IS_OFF,
                    default=self.entry.options.get(CONF_OFFLINE_IS_OFF, False),
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
            # Merge pending options first, if present
            if self._pending_options is not None:
                self.options.update(self._pending_options)
                self._pending_options = None
            self.options.update(user_input)
            return self.async_create_entry(title="Govee", data=self.options)
        iot_schema = vol.Schema(
            {
                vol.Required(CONF_IOT_EMAIL, default=self.entry.options.get(CONF_IOT_EMAIL, "")): cv.string,
                vol.Required(CONF_IOT_PASSWORD, default=self.entry.options.get(CONF_IOT_PASSWORD, "")): cv.string,
            }
        )
        return self.async_show_form(step_id="iot", data_schema=iot_schema, errors=errors)






class CannotConnect(exceptions.HomeAssistantError):
    """Error to indicate we cannot connect."""
