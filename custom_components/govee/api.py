"""Minimal Govee API client."""
import aiohttp
import asyncio
import certifi
import logging
import ssl
import time
from aiohttp import ClientSession
from typing import Any, Dict, List, Tuple, Union

from .models import GoveeDevice, GoveeSource, GoveeLearnedInfo

_LOGGER = logging.getLogger(__name__)

_API_BASE = "https://developer-api.govee.com/v1"
_API_DEVICES = f"{_API_BASE}/devices"
_API_CONTROL = f"{_API_BASE}/devices/control"


class GoveeClient:
    def __init__(self, api_key: str, storage):
        self._api_key = api_key
        self._storage = storage
        self._devices: Dict[str, GoveeDevice] = {}
        self._session: aiohttp.ClientSession | None = None
        self._ssl_context: ssl.SSLContext | None = None  # define upfront

        # rate limit
        self._limit = 100
        self._remaining = 100
        self._reset = 0
        self._rate_limit_on = 5

    @classmethod
    async def create(cls, api_key: str, storage, hass=None):
        """Async-safe constructor."""
        self = cls(api_key, storage)

        # Async-safe SSL context creation
        if hass is not None:
            def _make_ssl():
                return ssl.create_default_context(cafile=certifi.where())
            self._ssl_context = await hass.async_add_executor_job(_make_ssl)
        else:
            self._ssl_context = ssl.create_default_context(cafile=certifi.where())

        await self._init_session()
        return self

    async def _init_session(self):
        """Initialize aiohttp session with SSL context."""
        # Close existing session if already open (important for reloads)
        if self._session and not self._session.closed:
            await self._session.close()

        connector = aiohttp.TCPConnector(ssl=self._ssl_context)
        self._session = ClientSession(connector=connector)


    async def close(self):
        """Gracefully close aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None


    def _headers(self):
        return {"Govee-API-Key": self._api_key}

    async def _rate_limit_delay(self):
        if self._remaining <= self._rate_limit_on:
            reset_in = max(0, self._reset - int(time.time()))
            _LOGGER.warning("Rate limit reached, skipping updates for %ss", reset_in)
            return []


    def _track_rate_limit(self, response: aiohttp.ClientResponse):
        if "Rate-Limit-Total" in response.headers:
            try:
                self._limit = int(response.headers["Rate-Limit-Total"])
                self._remaining = int(response.headers["Rate-Limit-Remaining"])
                self._reset = int(response.headers["Rate-Limit-Reset"])
            except Exception:
                self._remaining -= 1

    async def get_devices(self) -> Tuple[List[GoveeDevice], str | None]:
        await self._rate_limit_delay()
        async with self._session.get(_API_DEVICES, headers=self._headers()) as resp:
            self._track_rate_limit(resp)
            if resp.status != 200:
                return [], f"API error {resp.status}: {await resp.text()}"
            data = await resp.json()
            if "data" not in data or "devices" not in data["data"]:
                return [], "Malformed API response"

            timestamp = int(time.time())
            learning_infos = await self._storage.read()

            for item in data["data"]["devices"]:
                dev_id = item["device"]
                if dev_id in self._devices:
                    continue
                learned = learning_infos.get(dev_id, GoveeLearnedInfo())
                self._devices[dev_id] = GoveeDevice(
                    device=dev_id,
                    model=item["model"],
                    device_name=item["deviceName"],
                    controllable=item["controllable"],
                    retrievable=item["retrievable"],
                    support_cmds=item["supportCmds"],
                    support_turn="turn" in item["supportCmds"],
                    support_brightness="brightness" in item["supportCmds"],
                    support_color="color" in item["supportCmds"],
                    support_color_temp="colorTem" in item["supportCmds"],
                    online=True,
                    timestamp=timestamp,
                    source=GoveeSource.API,
                    learned_set_brightness_max=learned.set_brightness_max,
                    learned_get_brightness_max=learned.get_brightness_max,
                    before_set_brightness_turn_on=learned.before_set_brightness_turn_on,
                    config_offline_is_off=learned.config_offline_is_off,
                )

            return list(self._devices.values()), None

    async def init_devices(self) -> Tuple[List[GoveeDevice], str | None]:
        """Discover devices and fetch their initial state."""
        devices, err = await self.get_devices()
        if err:
            return devices, err

        # Fetch live state for each device
        for dev in devices:
            ok, state_err = await self.get_device_state(dev.device)
            if not ok and state_err:
                _LOGGER.warning("Failed to fetch initial state for %s: %s", dev.device, state_err)

        return list(self._devices.values()), None


    async def _control(self, device: Union[str, GoveeDevice], command: str, value: Any) -> Tuple[bool, str | None]:
        if isinstance(device, str):
            device = self._devices.get(device)
        if not device:
            return False, f"Unknown device {device}"
        if not device.controllable:
            return False, f"Device {device.device} not controllable"
        if command not in device.support_cmds:
            return False, f"Command {command} not supported"

        payload = {
            "device": device.device,
            "model": device.model,
            "cmd": {"name": command, "value": value},
        }

        await self._rate_limit_delay()
        async with self._session.put(_API_CONTROL, headers=self._headers(), json=payload) as resp:
            self._track_rate_limit(resp)
            if resp.status != 200:
                return False, f"API error {resp.status}: {await resp.text()}"
            result = await resp.json()
            return result.get("message") == "Success", None

    async def turn_on(self, device): return await self._control(device, "turn", "on")
    async def turn_off(self, device): return await self._control(device, "turn", "off")
    async def set_brightness(self, device, value: int): return await self._control(device, "brightness", value)
    async def set_color_temp(self, device, value: int): return await self._control(device, "colorTem", value)
    async def set_color(self, device, rgb: Tuple[int, int, int]): 
        return await self._control(device, "color", {"r": rgb[0], "g": rgb[1], "b": rgb[2]})

    async def get_device_state(self, device_id: str) -> Tuple[bool, str | None]:
        """Fetch current state of a device via API."""
        payload = {"device": device_id, "model": self._devices[device_id].model}
        await self._rate_limit_delay()
        async with self._session.get(f"{_API_BASE}/devices/state", headers=self._headers(), params=payload) as resp:
            self._track_rate_limit(resp)
            if resp.status != 200:
                return False, f"API error {resp.status}: {await resp.text()}"
            data = await resp.json()
            if "data" not in data or "properties" not in data["data"]:
                return False, "Malformed state response"

            props = data["data"]["properties"]
            dev = self._devices[device_id]
            for p in props:
                if p["online"] == False:
                    dev.online = False
                if p.get("powerState") == "on":
                    dev.power_state = True
                if p.get("powerState") == "off":
                    dev.power_state = False
                if "brightness" in p:
                    dev.brightness = p["brightness"]
                if "color" in p:
                    c = p["color"]
                    dev.color = (c["r"], c["g"], c["b"])
                if "colorTemInKelvin" in p:
                    dev.color_temp = p["colorTemInKelvin"]

            return True, None
