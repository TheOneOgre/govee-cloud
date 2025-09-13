"""Model-specific quirks for Govee devices.

This is a lightweight port of the ideas from govee2mqtt's quirks,
focused on lights: color temperature ranges and LAN capability flags.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class Quirk:
    sku: str
    # Optional override for color temperature range (Kelvin)
    color_temp_range: Optional[Tuple[int, int]] = None
    # Whether this model is known to support the Govee LAN API
    lan_api_capable: bool = False
    # Whether the platform/HTTP metadata is known to be unreliable
    avoid_platform_api: bool = False
    # Whether this model supports IoT (MQTT) in govee2mqtt context (informational)
    iot_api_supported: Optional[bool] = None


# Minimal subset of known light models and flags derived from govee2mqtt.
_QUIRKS: Dict[str, Quirk] = {
    # Specific lights seen to behave differently in metadata; ignore IoT false flags
    "H6121": Quirk("H6121", color_temp_range=(2000, 9000)),
    "H6154": Quirk("H6154", color_temp_range=(2000, 9000)),
    "H6176": Quirk("H6176", color_temp_range=(2000, 9000)),
    # Devices with unreliable Platform metadata
    "H6141": Quirk("H6141", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H6159": Quirk("H6159", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H6003": Quirk("H6003", color_temp_range=(2000, 9000), avoid_platform_api=True),
    # BLE-only in govee2mqtt context; treat Platform metadata cautiously
    "H6102": Quirk("H6102", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H6053": Quirk("H6053", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H617C": Quirk("H617C", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H617E": Quirk("H617E", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H617F": Quirk("H617F", color_temp_range=(2000, 9000), avoid_platform_api=True),
    "H6119": Quirk("H6119", color_temp_range=(2000, 9000), avoid_platform_api=True),
    # LAN-capable lights (subset from govee2mqtt list)
    "H6072": Quirk("H6072", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6073": Quirk("H6073", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6076": Quirk("H6076", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6078": Quirk("H6078", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619B": Quirk("H619B", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619C": Quirk("H619C", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619Z": Quirk("H619Z", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7060": Quirk("H7060", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6046": Quirk("H6046", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6047": Quirk("H6047", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6051": Quirk("H6051", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6056": Quirk("H6056", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6059": Quirk("H6059", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6061": Quirk("H6061", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6062": Quirk("H6062", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6065": Quirk("H6065", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6066": Quirk("H6066", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6067": Quirk("H6067", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6087": Quirk("H6087", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H610A": Quirk("H610A", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H610B": Quirk("H610B", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6117": Quirk("H6117", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6159": Quirk("H6159", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H615E": Quirk("H615E", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6163": Quirk("H6163", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6168": Quirk("H6168", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6172": Quirk("H6172", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6173": Quirk("H6173", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H618A": Quirk("H618A", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H618C": Quirk("H618C", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H618E": Quirk("H618E", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H618F": Quirk("H618F", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619A": Quirk("H619A", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619D": Quirk("H619D", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619E": Quirk("H619E", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A0": Quirk("H61A0", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A1": Quirk("H61A1", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A2": Quirk("H61A2", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A3": Quirk("H61A3", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A5": Quirk("H61A5", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A8": Quirk("H61A8", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61B2": Quirk("H61B2", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61E1": Quirk("H61E1", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7012": Quirk("H7012", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7013": Quirk("H7013", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7021": Quirk("H7021", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7028": Quirk("H7028", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7041": Quirk("H7041", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7042": Quirk("H7042", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7050": Quirk("H7050", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7051": Quirk("H7051", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7052": Quirk("H7052", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7055": Quirk("H7055", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H705A": Quirk("H705A", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H705B": Quirk("H705B", color_temp_range=(2000, 9000), lan_api_capable=True),
    # Reported missing models by users (add for discovery hints and future LAN)
    "H705D": Quirk("H705D", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H705E": Quirk("H705E", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6169": Quirk("H6169", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H702A": Quirk("H702A", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7061": Quirk("H7061", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7062": Quirk("H7062", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H7065": Quirk("H7065", color_temp_range=(2000, 9000), lan_api_capable=True),
}


def resolve_quirk(model: str) -> Optional[Quirk]:
    """Return a quirk definition for the model if known."""
    return _QUIRKS.get(model)
