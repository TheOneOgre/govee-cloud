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


class _Coalescer:
    """Coalesce rapid updates and emit only the latest after a delay."""

    def __init__(self, delay: float = 0.25):
        self.delay = delay
        self._task: asyncio.Task | None = None
        self._future: asyncio.Future | None = None
        self._value: Any = None

    def schedule(self, value: Any, send_func):
        """Schedule send_func(value) after delay; return a Future of (ok, err)."""
        self._value = value
        loop = asyncio.get_running_loop()
        if self._future is None or self._future.done():
            self._future = loop.create_future()

        if self._task and not self._task.done():
            self._task.cancel()

        async def runner():
            try:
                await asyncio.sleep(self.delay)
                ok, err = await send_func(self._value)
                if not self._future.done():
                    self._future.set_result((ok, err))
            except asyncio.CancelledError:
                return
            except Exception as ex:
                if not self._future.done():
                    self._future.set_result((False, f"Exception: {ex}"))
            finally:
                self._task = None
                self._future = None

        self._task = asyncio.create_task(runner())
        return self._future


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

        # Debounce/coalescing for rapid UI updates (per device, per command)
        self._coalesce: Dict[Tuple[str, str], _Coalescer] = {}


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

    async def _debounced_control(self, device: Union[str, GoveeDevice], command: str, value: Any, *, delay: float = 0.25) -> Tuple[bool, str | None]:
        """Coalesce rapid updates per (device, command) with a small delay."""
        dev_id = device.device if isinstance(device, GoveeDevice) else str(device)
        key = (dev_id, command)
        co = self._coalesce.get(key)
        if co is None:
            co = _Coalescer(delay)
            self._coalesce[key] = co
        fut = co.schedule(value, lambda v: self._control(device, command, v))
        ok, err = await fut
        return ok, err

    async def get_devices(self) -> Tuple[List[GoveeDevice], str | None]:
        # Per-device throttle to avoid overwhelming API during rapid UI changes
        now = time.monotonic()
        if now < device.lock_set_until:
            await asyncio.sleep(device.lock_set_until - now)

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

            _LOGGER.debug("Discovered %s devices from Govee API", len(data["data"]["devices"]))
            for item in data["data"]["devices"]:
                dev_id = item["device"]
                if dev_id in self._devices:
                    continue
                learned = learning_infos.get(dev_id, GoveeLearnedInfo())
                # Try to parse color temperature capability range from device list
                ct_min = None
                ct_max = None
                ct_step = 1
                props = item.get("properties") or item.get("capabilities") or []
                if isinstance(props, list):
                    for p in props:
                        t = (p.get("type") or p.get("name") or "").lower()
                        if "colortem" in t or "color_temp" in t or t == "ct":
                            r = p.get("range") or p.get("value") or p.get("values") or {}
                            try:
                                if r.get("min") is not None:
                                    ct_min = int(r.get("min"))
                                if r.get("max") is not None:
                                    ct_max = int(r.get("max"))
                                inc = r.get("step") or r.get("inc") or r.get("increment")
                                if inc is not None:
                                    ct_step = int(inc)
                            except Exception:
                                pass

                support_cmds = item.get("supportCmds", [])
                self._devices[dev_id] = GoveeDevice(
                    device=dev_id,
                    model=item["model"],
                    device_name=item["deviceName"],
                    controllable=item["controllable"],
                    retrievable=item["retrievable"],
                    support_cmds=support_cmds,
                    support_turn="turn" in support_cmds,
                    support_brightness="brightness" in support_cmds,
                    support_color="color" in support_cmds,
                    support_color_temp="colorTem" in support_cmds,
                    color_temp_min=ct_min,
                    color_temp_max=ct_max,
                    color_temp_step=ct_step or 1,
                    online=True,
                    timestamp=timestamp,
                    source=GoveeSource.API,
                    learned_set_brightness_max=learned.set_brightness_max,
                    learned_get_brightness_max=learned.get_brightness_max,
                    before_set_brightness_turn_on=learned.before_set_brightness_turn_on,
                    config_offline_is_off=learned.config_offline_is_off,
                )

                # Log capabilities to help debug missing devices/models
                _LOGGER.debug(
                    "Device %s (%s) controllable=%s retrievable=%s support=%s ct[min=%s max=%s step=%s]",
                    dev_id,
                    item.get("model"),
                    item.get("controllable"),
                    item.get("retrievable"),
                    ",".join(support_cmds),
                    ct_min,
                    ct_max,
                    ct_step,
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

            if resp.status == 429:
                retry = max(0, self._reset - int(time.time()))
                _LOGGER.warning("Rate limited for %s: retry after %ss", device.device, retry)
                # Soft success → let HA trust local state
                device.lock_set_until = time.monotonic() + max(1.0, float(retry))
                return True, f"Rate limit: assumed success, retry in {retry}s"

            if resp.status != 200:
                return False, f"API error {resp.status}: {await resp.text()}"

            result = await resp.json()
            if result.get("message") == "Success":
                device.lock_set_until = time.monotonic() + 0.8
                return True, None
            return False, None




    async def turn_on(self, device):
        return await self._control(device, "turn", "on")

    async def turn_off(self, device):
        return await self._control(device, "turn", "off")

    async def set_brightness(self, device, value: int):
        # Convert 0–255 (HA) → 0–100 (Govee API)
        percent = max(0, min(100, round(value / 255 * 100)))
        return await self._debounced_control(device, "brightness", percent)

    async def set_color_temp(self, device, value: int):
        # Clamp to device's supported Kelvin range and step if known
        dev = device if isinstance(device, GoveeDevice) else self._devices.get(device)
        vmin = 2700
        vmax = 9000
        step = 1
        if dev:
            if isinstance(dev.color_temp_min, int):
                vmin = dev.color_temp_min
            if isinstance(dev.color_temp_max, int):
                vmax = dev.color_temp_max
            if isinstance(dev.color_temp_step, int) and dev.color_temp_step > 1:
                step = dev.color_temp_step
        kelvin = max(vmin, min(vmax, int(value)))
        if step > 1:
            off = kelvin - vmin
            kelvin = vmin + round(off / step) * step
            kelvin = max(vmin, min(vmax, int(kelvin)))
        return await self._debounced_control(dev or device, "colorTem", kelvin)

    async def set_color(self, device, rgb: Tuple[int, int, int]):
        # Defensive: only send if supported
        if "color" not in device.support_cmds:
            return False, "Device does not support color"
        return await self._debounced_control(device, "color", {
            "r": rgb[0],
            "g": rgb[1],
            "b": rgb[2]
        })

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
                # Some properties objects don’t have "online"
                if "online" in p and p["online"] is False:
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
            dev.online = True

            return True, None

