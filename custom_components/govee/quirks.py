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
    # Floor lamps/strips that have LAN support (for future use)
    "H6076": Quirk("H6076", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H6121": Quirk("H6121", color_temp_range=(2000, 9000), lan_api_capable=False, iot_api_supported=False),
    "H6154": Quirk("H6154", color_temp_range=(2000, 9000), iot_api_supported=False),
    "H6176": Quirk("H6176", color_temp_range=(2000, 9000), iot_api_supported=False),
    # Common LAN-capable light SKUs (non-exhaustive)
    "H6072": Quirk("H6072", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619B": Quirk("H619B", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H619C": Quirk("H619C", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A0": Quirk("H61A0", color_temp_range=(2000, 9000), lan_api_capable=True),
    "H61A1": Quirk("H61A1", color_temp_range=(2000, 9000), lan_api_capable=True),
}


def resolve_quirk(model: str) -> Optional[Quirk]:
    """Return a quirk definition for the model if known."""
    return _QUIRKS.get(model)

