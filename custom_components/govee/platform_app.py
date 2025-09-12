"""Platform App API control client (experimental).

This client emulates the mobile app's platform API control endpoints.
It is optional and intended to reduce reliance on the Developer API
quota. It does NOT bypass per-device limits enforced by backend.

Currently a scaffold; extends as we learn more models.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

import requests

APP_VERSION = "5.6.01"
_LOGGER = logging.getLogger(__name__)


def _ua() -> str:
    return f"GoveeHome/{APP_VERSION} (com.ihoment.GoVeeSensor; build:2; iOS 16.5.0) Alamofire/5.6.4"

# Simple in-process token cache per email to avoid repeated logins
_PLATFORM_TOKEN_CACHE: dict[str, tuple[str, str, float]] = {}
_PLATFORM_TTL_SEC = 6 * 60 * 60  # 6 hours


class _AuthError(Exception):
    pass

class PlatformAppClient:
    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._token: str | None = None
        self._client_id: str | None = None

    def _login(self):
        import uuid
        self._client_id = uuid.uuid5(uuid.NAMESPACE_DNS, self._email).hex
        resp = requests.post(
            "https://app2.govee.com/account/rest/account/v1/login",
            json={"email": self._email, "password": self._password, "client": self._client_id},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        client = data.get("client") or data.get("data") or data
        self._token = client.get("token") or client.get("accessToken")
        if not self._token:
            raise RuntimeError("No token returned by login")

    async def ensure_auth(self, *, force: bool = False):
        now = __import__("time").monotonic()
        cached = _PLATFORM_TOKEN_CACHE.get(self._email)
        if not force and cached and (now - cached[2]) < _PLATFORM_TTL_SEC:
            self._token, self._client_id, _ = cached
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._login)
        _PLATFORM_TOKEN_CACHE[self._email] = (self._token or "", self._client_id or "", now)

    async def _post(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        await self.ensure_auth()
        headers = {
            "Authorization": f"Bearer {self._token}",
            "appVersion": APP_VERSION,
            "clientId": self._client_id or "",
            "clientType": "1",
            "iotVersion": "0",
            "User-Agent": _ua(),
        }
        loop = asyncio.get_running_loop()
        def _do():
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            if resp.status_code in (401, 403):
                raise _AuthError()
            resp.raise_for_status()
            return resp.json()
        try:
            return await loop.run_in_executor(None, _do)
        except _AuthError:
            # Token likely expired; force re-login and retry once
            _PLATFORM_TOKEN_CACHE.pop(self._email, None)
            self._token = None
            try:
                await self.ensure_auth(force=True)
            except Exception:
                raise
            return await loop.run_in_executor(None, _do)

    async def control_colorwc(self, sku: str, device: str, *, r: int, g: int, b: int, kelvin: int = 0) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        import uuid as _uuid
        if kelvin == 0:
            rgb_int = (int(r) & 0xFF) << 16 | (int(g) & 0xFF) << 8 | (int(b) & 0xFF)
            instance = "colorRgb"
            value = int(rgb_int)
        else:
            instance = "colorTemperatureK"
            value = int(kelvin)
        body = {
            "requestId": str(_uuid.uuid4()),
            "payload": {
                "sku": sku,
                "device": device,
                "capability": {
                    "type": "devices.capabilities.color_setting",
                    "instance": instance,
                    "value": value,
                },
            },
        }
        try:
            data = await self._post(url, body)
            ok = isinstance(data, dict) and (str(data.get("message", "")).lower() == "success" or int(data.get("code", 0)) in (0, 200))
            if not ok:
                _LOGGER.debug("PlatformApp colorwc non-success response: %s", data)
            return ok
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_colorwc failed: %s", ex)
            return False

    async def control_turn(self, sku: str, device: str, on: bool) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        import uuid as _uuid
        body = {
            "requestId": str(_uuid.uuid4()),
            "payload": {
                "sku": sku,
                "device": device,
                "capability": {
                    "type": "devices.capabilities.toggle",
                    "instance": "powerSwitch",
                    "value": 1 if on else 0,
                },
            },
        }
        try:
            data = await self._post(url, body)
            ok = isinstance(data, dict) and (str(data.get("message", "")).lower() == "success" or int(data.get("code", 0)) in (0, 200))
            if not ok:
                _LOGGER.debug("PlatformApp turn non-success response: %s", data)
            return ok
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_turn failed: %s", ex)
            return False

    async def control_brightness(self, sku: str, device: str, percent: int) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        import uuid as _uuid
        body = {
            "requestId": str(_uuid.uuid4()),
            "payload": {
                "sku": sku,
                "device": device,
                "capability": {
                    "type": "devices.capabilities.range",
                    "instance": "brightness",
                    "value": max(1, min(100, int(percent))),
                },
            },
        }
        try:
            data = await self._post(url, body)
            ok = isinstance(data, dict) and (str(data.get("message", "")).lower() == "success" or int(data.get("code", 0)) in (0, 200))
            if not ok:
                _LOGGER.debug("PlatformApp brightness non-success response: %s", data)
            return ok
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_brightness failed: %s", ex)
            return False

    async def list_devices(self) -> Dict[str, Any]:
        """Fetch devices with capabilities via the Platform (router) API.

        Tries `device/list` first, then `user/devices` as fallback.
        Returns the full JSON payload.
        """
        import uuid as _uuid
        body = {"requestId": str(_uuid.uuid4()), "payload": {}}
        # Primary endpoint
        url1 = "https://openapi.api.govee.com/router/api/v1/device/list"
        # Fallback endpoint some accounts use
        url2 = "https://openapi.api.govee.com/router/api/v1/user/devices"
        try:
            data = await self._post(url1, body)
            return data
        except Exception as ex1:
            _LOGGER.debug("PlatformApp list via device/list failed: %s", ex1)
            # try fallback
            try:
                data = await self._post(url2, body)
                return data
            except Exception as ex2:
                _LOGGER.debug("PlatformApp list via user/devices failed: %s", ex2)
                raise
