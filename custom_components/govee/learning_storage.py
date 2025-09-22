"""Simple persistent storage for learned Govee device info."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

from .models import GoveeLearnedInfo

LEARNING_SCHEMA_VERSION = 1


class GoveeLearningStorage:
    def __init__(self, config_dir: str, hass=None) -> None:
        self._hass = hass
        self._config_dir = config_dir
        self._integration_version = self._load_manifest_version()

    @staticmethod
    def _load_manifest_version() -> str | None:
        try:
            manifest_path = Path(__file__).with_name("manifest.json")
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            version = manifest.get("version")
            if isinstance(version, str) and version:
                return version
        except Exception:
            pass
        return None

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
                with open(path, "r", encoding="utf-8") as f_handle:
                    raw = json.load(f_handle)
            except Exception:
                return {}

            if not isinstance(raw, dict):
                return {}

            schema = raw.get("__schema_version")
            stored_version = raw.get("__integration_version")
            payload = raw.get("devices") if "devices" in raw else raw

            if schema != LEARNING_SCHEMA_VERSION:
                return {}
            if self._integration_version is not None and stored_version != self._integration_version:
                return {}

            out: Dict[str, GoveeLearnedInfo] = {}
            if isinstance(payload, dict):
                for key, value in payload.items():
                    if not isinstance(value, dict):
                        continue
                    try:
                        out[key] = GoveeLearnedInfo(**value)
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
            payload = {
                "__schema_version": LEARNING_SCHEMA_VERSION,
                "__integration_version": self._integration_version,
                "devices": {key: vars(value) for key, value in infos.items()},
            }
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f_handle:
                    json.dump(payload, f_handle, ensure_ascii=False)
            except Exception:
                # best-effort persistence; ignore failures
                pass

        if self._hass is not None:
            await self._hass.async_add_executor_job(_write)
        else:
            _write()
