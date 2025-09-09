"""Models for Govee integration."""
from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple, Optional


class GoveeSource(Enum):
    HISTORY = "history"
    API = "api"


@dataclass
class GoveeDevice:
    device: str
    model: str
    device_name: str
    controllable: bool
    retrievable: bool
    support_cmds: List[str]
    support_turn: bool
    support_brightness: bool
    support_color: bool
    support_color_temp: bool
    online: bool = False
    power_state: bool = False
    brightness: int = 0
    color: Tuple[int, int, int] = (0, 0, 0)
    color_temp: int = 0
    # Per-device color temperature capabilities (Kelvin)
    color_temp_min: Optional[int] = None
    color_temp_max: Optional[int] = None
    color_temp_step: int = 1
    # Some models expect color temperature as 0-100 percent instead of Kelvin
    color_temp_send_percent: Optional[bool] = None
    timestamp: int = 0
    source: GoveeSource = GoveeSource.HISTORY
    error: Optional[str] = None
    learned_set_brightness_max: Optional[int] = None
    learned_get_brightness_max: Optional[int] = None
    before_set_brightness_turn_on: bool = False
    config_offline_is_off: bool = False
    # Lightweight throttle windows (monotonic seconds)
    lock_set_until: float = 0.0
    lock_get_until: float = 0.0


@dataclass
class GoveeLearnedInfo:
    set_brightness_max: Optional[int] = None
    get_brightness_max: Optional[int] = None
    before_set_brightness_turn_on: bool = False
    config_offline_is_off: bool = False
    # Persist whether this device expects CT as percent (0-100)
    color_temp_send_percent: Optional[bool] = None
