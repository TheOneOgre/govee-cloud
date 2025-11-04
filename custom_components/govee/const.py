"""Constants for the Govee LED strips integration."""

DOMAIN = "govee"

# Deprecated/removed: CONF_DISABLE_ATTRIBUTE_UPDATES
CONF_OFFLINE_IS_OFF = "offline_is_off"
CONF_USE_ASSUMED_STATE = "use_assumed_state"

# Home Assistant expects color temperature ranges in Kelvin.
# Align with Govee API supported range (see api.set_color_temp clamp).
COLOR_TEMP_KELVIN_MIN = 2700
COLOR_TEMP_KELVIN_MAX = 9000

# Known models with guaranteed color temperature support even if mobile metadata is empty
FORCE_CT_MODELS = {
    "H605B",
    "H615B",
    "H6008",
}

# Models whose scene playback relies solely on ptReal bursts (skip follow-up scene command)
SCENE_PTREAL_ONLY_MODELS = {
    "H6121",
}

# Optional cmdVersion overrides for ptReal scene publishes
SCENE_PTREAL_CMD_VERSION = {
    "H6121": 0,
}
CONF_POLLING_MODE = "polling_mode"

# IoT (AWS MQTT) push updates options
CONF_IOT_EMAIL = "iot_email"
CONF_IOT_PASSWORD = "iot_password"
CONF_IOT_PUSH_ENABLED = "enable_iot_push"
# Deprecated/removed: CONF_PLATFORM_APP_ENABLED
CONF_IOT_CONTROL_ENABLED = "enable_iot_control"
