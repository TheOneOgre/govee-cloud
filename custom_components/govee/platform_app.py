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

    async def ensure_auth(self):
        if not self._token:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._login)

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
            resp.raise_for_status()
            return resp.json()
        return await loop.run_in_executor(None, _do)

    async def control_colorwc(self, sku: str, device: str, *, r: int, g: int, b: int, kelvin: int = 0) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        body = {
            "requestId": "uuid",
            "payload": {
                "sku": sku,
                "device": device,
                "capability": {
                    "type": "devices.capabilities.color_setting",
                    "instance": "colorRgb" if kelvin == 0 else "colorTemperatureK",
                    "value": {"r": r, "g": g, "b": b} if kelvin == 0 else kelvin,
                },
            },
        }
        try:
            data = await self._post(url, body)
            return True
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_colorwc failed: %s", ex)
            return False

    async def control_turn(self, sku: str, device: str, on: bool) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        body = {
            "requestId": "uuid",
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
            return True
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_turn failed: %s", ex)
            return False

    async def control_brightness(self, sku: str, device: str, percent: int) -> bool:
        url = "https://openapi.api.govee.com/router/api/v1/device/control"
        body = {
            "requestId": "uuid",
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
            return True
        except Exception as ex:
            _LOGGER.debug("PlatformApp control_brightness failed: %s", ex)
            return False
