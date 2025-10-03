"""Scene command synthesis without the cloud API."""
from __future__ import annotations

from functools import lru_cache
import base64
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

_ASSET_FILE = Path(__file__).resolve().parent / "data" / "scene_params.json"

# Protocol constants reverse-engineered from MultipleControllerCommV1.makeSendBytesV0.
_PRO_TYPE = 0xAA  # 0b1010_1010
_COMMAND_BYTE = 0x05
_PACKET_LEN = 20
_CHUNK_LEN = 17

_LOGGER = logging.getLogger(__name__)
_MISSING_PARAMS_LOGGED: set[int] = set()


class SceneParamNotFound(KeyError):
    """Raised when no baked scene payload exists for a given param id."""


@lru_cache(maxsize=1)
def _load_scene_param_map() -> Dict[int, str]:
    if not _ASSET_FILE.exists():
        raise FileNotFoundError(
            f"Scene parameter mapping not found at {_ASSET_FILE}; ensure the data folder was packaged."
        )
    raw = json.loads(_ASSET_FILE.read_text())
    mapping = {int(k): v for k, v in raw.items() if isinstance(v, str) and v}
    _LOGGER.debug("Loaded %d scene params from %s", len(mapping), _ASSET_FILE)
    return mapping


def _xor_checksum(packet: bytearray) -> int:
    checksum = packet[0]
    for idx in range(1, _PACKET_LEN - 1):
        checksum ^= packet[idx]
    return checksum & 0xFF


def _pack_effect_bytes(effect: bytes) -> List[bytes]:
    if not effect:
        return []

    payloads: List[bytes] = []
    start = bytearray(_PACKET_LEN)
    start[0] = _PRO_TYPE
    start[1] = 0  # start frame index
    start[2] = 0  # reserved (legacy firmware checks for zero)

    remaining = len(effect)
    full_chunks, remainder = divmod(remaining, _CHUNK_LEN)
    total_chunks = full_chunks + (1 if remainder else 0)
    start[3] = (total_chunks + 2) & 0xFF  # total frame count incl. start/end markers
    start[4] = _COMMAND_BYTE
    start[19] = _xor_checksum(start)
    payloads.append(bytes(start))

    offset = 0
    for index in range(1, total_chunks + 1):
        chunk_size = remainder if (index == total_chunks and remainder) else _CHUNK_LEN
        frame = bytearray(_PACKET_LEN)
        frame[0] = _PRO_TYPE
        frame[1] = index & 0xFF
        frame[2 : 2 + chunk_size] = effect[offset : offset + chunk_size]
        frame[19] = _xor_checksum(frame)
        payloads.append(bytes(frame))
        offset += chunk_size

    end = bytearray(_PACKET_LEN)
    end[0] = _PRO_TYPE
    end[1] = 0xFF
    end[19] = _xor_checksum(end)
    payloads.append(bytes(end))

    return payloads


def get_scene_param(param_id: int) -> str:
    mapping = _load_scene_param_map()
    try:
        return mapping[param_id]
    except KeyError as exc:
        if param_id not in _MISSING_PARAMS_LOGGED:
            _MISSING_PARAMS_LOGGED.add(param_id)
            _LOGGER.debug("Scene param %s not found in baked map", param_id)
        raise SceneParamNotFound(param_id) from exc


def generate_scene_commands_from_param(param_id: int) -> List[str]:
    effect_b64 = get_scene_param(param_id)
    effect_bytes = base64.b64decode(effect_b64)
    frames = _pack_effect_bytes(effect_bytes)
    encoded = [base64.b64encode(frame).decode("ascii") for frame in frames]
    _LOGGER.debug("Synthesized %d frame(s) for param_id=%s", len(encoded), param_id)
    return encoded


def generate_scene_commands(scene_id: int, param_id: Optional[int] = None) -> List[str]:
    """Return the command burst for a scene."""

    if param_id is None:
        _LOGGER.debug("Scene synthesis skipped for scene_id=%s: param_id missing", scene_id)
        raise SceneParamNotFound("param_id missing; cannot locate baked scene payload")
    return generate_scene_commands_from_param(param_id)
