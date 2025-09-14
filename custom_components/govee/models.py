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
    # Quirk/apparatus flags (for future LAN/IOT support)
    lan_api_capable: bool = False
    avoid_platform_api: bool = False
    # Active scene/effect name (internal; cleared on color/CT changes)
    active_scene: Optional[str] = None
    # Learned overrides for CT range
    learned_color_temp_min: Optional[int] = None
    learned_color_temp_max: Optional[int] = None
    # Some models expect color temperature as 0-100 percent instead of Kelvin
    color_temp_send_percent: Optional[bool] = None
    # Some models require 'colorwc' IoT command instead of legacy 'color'
    color_cmd_use_colorwc: Optional[bool] = None
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
    # Pending state expectation (optimistic update reconciliation)
    pending_until: float = 0.0
    pending_color: Optional[Tuple[int, int, int]] = None
    pending_brightness: Optional[int] = None
    pending_ct: Optional[int] = None
    # Last time we attempted CT range learning
    last_ct_learn_ts: float = 0.0


@dataclass
class GoveeLearnedInfo:
    set_brightness_max: Optional[int] = None
    get_brightness_max: Optional[int] = None
    before_set_brightness_turn_on: bool = False
    config_offline_is_off: bool = False
    # Persist learned CT range overrides
    learned_color_temp_min: Optional[int] = None
    learned_color_temp_max: Optional[int] = None
    # Persist learned per-device protocol quirks
    # Some models require 'colorwc' instead of legacy 'color'
    color_cmd_use_colorwc: Optional[bool] = None
    # Some models expect color temperature as 0-100 percent instead of Kelvin
    color_temp_send_percent: Optional[bool] = None
