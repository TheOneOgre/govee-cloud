"""Simple persistent storage for learned Govee device info.

Stores a mapping of device_id -> GoveeLearnedInfo as JSON under
Home Assistant's `.storage/` directory.
"""
from __future__ import annotations

import json
import os
from typing import Dict

from .models import GoveeLearnedInfo


class GoveeLearningStorage:
    def __init__(self, config_dir: str, hass=None) -> None:
        self._hass = hass
        self._config_dir = config_dir

    def _path(self) -> str:
        # Prefer hass.config.path if hass is provided
        if self._hass is not None:
            return self._hass.config.path(".storage/govee_learning.json")
        # Fallback to provided config_dir
        storage_dir = os.path.join(self._config_dir, ".storage")
        os.makedirs(storage_dir, exist_ok=True)
        return os.path.join(storage_dir, "govee_learning.json")

    async def read(self) -> Dict[str, GoveeLearnedInfo]:
        def _read() -> Dict[str, GoveeLearnedInfo]:
            path = self._path()
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except Exception:
                return {}
            out: Dict[str, GoveeLearnedInfo] = {}
            if isinstance(raw, dict):
                for k, v in raw.items():
                    try:
                        if isinstance(v, dict):
                            out[k] = GoveeLearnedInfo(**v)
                    except Exception:
                        # ignore malformed entries
                        pass
            return out

        if self._hass is not None:
            return await self._hass.async_add_executor_job(_read)
        return _read()

    async def write(self, infos: Dict[str, GoveeLearnedInfo]) -> None:
        def _write() -> None:
            path = self._path()
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump({k: vars(v) for k, v in infos.items()}, f, ensure_ascii=False)
            except Exception:
                # best-effort persistence; ignore failures
                pass

        if self._hass is not None:
            await self._hass.async_add_executor_job(_write)
        else:
            _write()

