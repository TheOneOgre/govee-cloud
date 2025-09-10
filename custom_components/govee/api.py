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
from .const import CONF_PLATFORM_APP_ENABLED, CONF_IOT_EMAIL, CONF_IOT_PASSWORD
from .platform_app import PlatformAppClient
from .quirks import resolve_quirk

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
                # Capture current future to avoid race with reschedules
                local_future = self._future
                result = await send_func(self._value)
                if isinstance(result, tuple) and len(result) == 2:
                    ok, err = result
                else:
                    ok, err = False, "Exception: invalid control handler result"
                if local_future is not None and not local_future.done():
                    local_future.set_result((ok, err))
            except asyncio.CancelledError:
                return
            except Exception as ex:
                local_future = getattr(self, "_future", None)
                if local_future is not None and not local_future.done():
                    local_future.set_result((False, f"Exception: {ex}"))
            finally:
                self._task = None

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

        # Debounce/coalesce rapid updates (per device, per command)
        self._coalesce: Dict[Tuple[str, str], _Coalescer] = {}

        # Per-device control token bucket (10/min per device)
        self._ctrl_bucket: Dict[str, Tuple[float, float]] = {}
        self._bucket_capacity = 10.0
        self._bucket_refill_per_sec = self._bucket_capacity / 60.0

        # Lightweight duplicate suppression: last value per (device, command)
        self._last_sent: Dict[Tuple[str, str], Tuple[Any, float]] = {}
        # Per-device state token bucket (10/min per device)
        self._state_bucket: Dict[str, Tuple[float, float]] = {}
        # Serialize per-device control to avoid interleaving color/ct/brightness
        self._ctrl_locks: Dict[str, asyncio.Lock] = {}
        # Post-control reconciliation throttle
        self._last_post_poll: Dict[str, float] = {}
        # Optional Platform App control client (experimental)
        self._platform_app: PlatformAppClient | None = None


    @classmethod
    async def create(cls, api_key: str, storage, hass=None, config_entry=None):
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
        # Initialize Platform App client if configured
        try:
            if config_entry and config_entry.options.get(CONF_PLATFORM_APP_ENABLED, False):
                email = config_entry.options.get(CONF_IOT_EMAIL)
                password = config_entry.options.get(CONF_IOT_PASSWORD)
                if email and password:
                    self._platform_app = PlatformAppClient(email, password)
        except Exception as ex:
            _LOGGER.warning("PlatformApp init failed: %s", ex)
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

    def _bucket_take(self, dev_id: str, tokens: float = 1.0) -> float:
        """Control token bucket: returns wait seconds if not enough tokens."""
        now = time.monotonic()
        tokens_now, last = self._ctrl_bucket.get(dev_id, (self._bucket_capacity, now))
        elapsed = max(0.0, now - last)
        tokens_now = min(self._bucket_capacity, tokens_now + elapsed * self._bucket_refill_per_sec)
        if tokens_now >= tokens:
            tokens_now -= tokens
            self._ctrl_bucket[dev_id] = (tokens_now, now)
            return 0.0
        needed = tokens - tokens_now
        wait = needed / self._bucket_refill_per_sec
        self._ctrl_bucket[dev_id] = (tokens_now, now)
        return max(0.0, wait)

    def _state_bucket_take(self, dev_id: str, tokens: float = 1.0) -> float:
        now = time.monotonic()
        tokens_now, last = self._state_bucket.get(dev_id, (self._bucket_capacity, now))
        elapsed = max(0.0, now - last)
        tokens_now = min(self._bucket_capacity, tokens_now + elapsed * self._bucket_refill_per_sec)
        if tokens_now >= tokens:
            tokens_now -= tokens
            self._state_bucket[dev_id] = (tokens_now, now)
            return 0.0
        needed = tokens - tokens_now
        wait = needed / self._bucket_refill_per_sec
        self._state_bucket[dev_id] = (tokens_now, now)
        return max(0.0, wait)

    async def _debounced_control(self, device: Union[str, GoveeDevice], command: str, value: Any, *, delay: float = 0.25) -> Tuple[bool, str | None]:
        """Coalesce rapid updates per (device, command) with a small delay."""
        dev_id = device.device if isinstance(device, GoveeDevice) else str(device)
        key = (dev_id, command)
        co = self._coalesce.get(key)
        if co is None:
            co = _Coalescer(delay)
            self._coalesce[key] = co

        async def _send_latest(v):
            # Enforce per-device 10/min budget without queuing outdated values.
            # Coalescer will cancel this task if a newer value arrives while waiting.
            while True:
                wait = self._bucket_take(dev_id, 1.0)
                if wait <= 0:
                    break
                try:
                    await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    raise

            # Drop exact duplicates sent within 2 seconds
            now = time.monotonic()
            last = self._last_sent.get(key)
            if last is not None:
                last_val, ts = last
                if last_val == v and (now - ts) < 2.0:
                    return True, None

            # Limited retry loop for 429s
            attempts = 0
            while True:
                # Gate each retry by token bucket as well
                while True:
                    wait = self._bucket_take(dev_id, 1.0)
                    if wait <= 0:
                        break
                    try:
                        await asyncio.sleep(wait)
                    except asyncio.CancelledError:
                        raise
                ok, err = await self._control(device, command, v)
                if ok:
                    self._last_sent[key] = (v, time.monotonic())
                    return ok, err
                if not err or not err.startswith("Rate limit:"):
                    return ok, err
                attempts += 1
                if attempts > 2:
                    return ok, err
                # Fallback to a safe wait (6s) if header-derived reset is 0
                sleep_s = 6.0
                try:
                    part = err.split("in ")[-1].rstrip("s")
                    # If server says 0s, still wait a safe window (6s)
                    parsed = float(part)
                    sleep_s = 6.0 if parsed <= 0.5 else parsed
                except Exception:
                    pass
                try:
                    await asyncio.sleep(min(60.0, sleep_s))
                except asyncio.CancelledError:
                    raise

        fut = co.schedule(value, _send_latest)
        ok, err = await fut
        return ok, err

    async def get_devices(self) -> Tuple[List[GoveeDevice], str | None]:
        await self._rate_limit_delay()
        async with self._session.get(_API_DEVICES, headers=self._headers()) as resp:
            self._track_rate_limit(resp)
            if resp.status != 200:
                return [], f"API error {resp.status}: {await resp.text()}"
            data = await resp.json()
            if "data" not in data:
                return [], "Malformed API response"

            # Support both standard and alternate schemas
            raw = data.get("data")
            if isinstance(raw, dict) and "devices" in raw:
                items = raw["devices"]
            elif isinstance(raw, list):
                items = raw
            else:
                return [], "Malformed API response"

            timestamp = int(time.time())
            learning_infos = await self._storage.read()

            _LOGGER.debug("Discovered %s devices from Govee API", len(items))
            for item in items:
                dev_id = item["device"]
                if dev_id in self._devices:
                    continue
                learned = learning_infos.get(dev_id, GoveeLearnedInfo())
                # Try to parse color temperature capability range from device list
                ct_min = None
                ct_max = None
                ct_step = 1

                def _parse_range_dict(r):
                    nonlocal ct_min, ct_max, ct_step
                    if not isinstance(r, dict):
                        return
                    try:
                        if r.get("min") is not None:
                            ct_min = int(r.get("min"))
                        if r.get("max") is not None:
                            ct_max = int(r.get("max"))
                        inc = (
                            r.get("step")
                            or r.get("inc")
                            or r.get("increment")
                            or r.get("precision")
                        )
                        if inc is not None:
                            ct_step = int(inc)
                    except Exception:
                        # Ignore parsing errors; leave defaults
                        pass

                props = item.get("properties") or item.get("capabilities") or []

                # Case A: list of capability dicts (e.g., instance=colorTemperatureK)
                if isinstance(props, list):
                    for p in props:
                        typ = (p.get("type") or p.get("name") or "").lower()
                        inst = (p.get("instance") or "").lower()
                        # Match common representations of color temperature capability
                        if (
                            "colortem" in typ
                            or "color_temp" in typ
                            or ("color_setting" in typ and "colortemperature" in inst)
                            or inst in {"colortemperaturek", "color_temperature_k", "colortemperatur"}
                        ):
                            if isinstance(p.get("parameters"), dict):
                                _parse_range_dict(p["parameters"].get("range"))
                            # Some variants put range at top-level under different keys
                            _parse_range_dict(p.get("range") or p.get("value") or p.get("values"))

                # Case B: dict of properties, e.g. {"colorTem": {"range": {...}}}
                elif isinstance(props, dict):
                    # Check a few likely keys
                    for key in [
                        "colorTem",
                        "color_temperature_k",
                        "colorTemperatureK",
                        "colorTemperature",
                        "ct",
                    ]:
                        if key in props and isinstance(props[key], dict):
                            _parse_range_dict(props[key].get("range"))

                support_cmds = item.get("supportCmds", [])
                # Apply model-specific quirks if known
                model = item.get("model") or item.get("sku") or item.get("type") or "unknown"
                quirk = resolve_quirk(model) if model else None
                if quirk and quirk.color_temp_range:
                    qmin, qmax = quirk.color_temp_range
                    if ct_min is None:
                        ct_min = int(qmin)
                    if ct_max is None:
                        ct_max = int(qmax)

                # Derive support flags from capabilities if supportCmds is missing/empty
                derived_turn = False
                derived_brightness = False
                derived_color = False
                derived_ct = False
                if isinstance(props, list):
                    for p in props:
                        inst = (p.get("instance") or "").lower()
                        typ = (p.get("type") or p.get("name") or "").lower()
                        if inst in {"powerswitch", "light"} or "power" in inst or "turn" in typ:
                            derived_turn = True
                        if inst == "brightness":
                            derived_brightness = True
                        if inst == "colorrgb" or "color" in inst:
                            derived_color = True
                        if inst in {"colortemperaturek", "color_temperature_k"} or "colortem" in inst:
                            derived_ct = True
                elif isinstance(props, dict):
                    # Some responses present a dict of instances
                    keys = {k.lower() for k in props.keys()}
                    if {"powerswitch", "power", "turn"} & keys:
                        derived_turn = True
                    if "brightness" in keys:
                        derived_brightness = True
                    if {"colorrgb", "color"} & keys:
                        derived_color = True
                    if {"colortemperaturek", "colortem", "color_temperature_k"} & keys:
                        derived_ct = True

                self._devices[dev_id] = GoveeDevice(
                    device=dev_id,
                    model=model,
                    device_name=item.get("deviceName") or item.get("device_name") or dev_id,
                    controllable=bool(item.get("controllable", True)),
                    retrievable=bool(item.get("retrievable", True)),
                    support_cmds=support_cmds,
                    support_turn=("turn" in support_cmds) or derived_turn,
                    support_brightness=("brightness" in support_cmds) or derived_brightness,
                    support_color=("color" in support_cmds) or derived_color,
                    # Consider color temp supported if API lists command OR we detected a CT range
                    support_color_temp=("colorTem" in support_cmds) or derived_ct or (ct_min is not None or ct_max is not None),
                    color_temp_min=ct_min,
                    color_temp_max=ct_max,
                    color_temp_step=ct_step or 1,
                    lan_api_capable=bool(quirk and quirk.lan_api_capable),
                    avoid_platform_api=bool(quirk and quirk.avoid_platform_api),
                    online=True,
                    timestamp=timestamp,
                    source=GoveeSource.API,
                    learned_set_brightness_max=learned.set_brightness_max,
                    learned_get_brightness_max=learned.get_brightness_max,
                    before_set_brightness_turn_on=learned.before_set_brightness_turn_on,
                    config_offline_is_off=learned.config_offline_is_off,
                    learned_color_temp_min=None,
                    learned_color_temp_max=None,
                )

                # Log capabilities to help debug missing devices/models
                _LOGGER.debug(
                    "Device %s (%s) controllable=%s retrievable=%s support=%s ct[min=%s max=%s step=%s] quirk[lan=%s avoid_platform=%s]",
                    dev_id,
                    model,
                    item.get("controllable"),
                    item.get("retrievable"),
                    ",".join(support_cmds),
                    ct_min,
                    ct_max,
                    ct_step,
                    bool(quirk and quirk.lan_api_capable),
                    bool(quirk and quirk.avoid_platform_api),
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
            # Fall back to derived support flags when supportCmds is missing/empty
            if command == "turn" and not device.support_turn:
                return False, f"Command {command} not supported"
            if command == "brightness" and not device.support_brightness:
                return False, f"Command {command} not supported"
            if command == "color" and not device.support_color:
                return False, f"Command {command} not supported"
            if command == "colorTem" and not device.support_color_temp:
                return False, f"Command {command} not supported"

        payload = {
            "device": device.device,
            "model": device.model,
            "cmd": {"name": command, "value": value},
        }

        # Ensure single in-flight control per device (all commands)
        lock = self._ctrl_locks.get(device.device)
        if lock is None:
            lock = asyncio.Lock()
            self._ctrl_locks[device.device] = lock

        async with lock:
            _LOGGER.debug("Sending control → %s %s: %s", device.device, command, value)

            # Short post-success cooldown (set below). Avoid long sleeps here to
            # keep coalescer cancellations effective.
            now = time.monotonic()
            if now < device.lock_set_until and device.lock_set_until - now < 2.0:
                await asyncio.sleep(device.lock_set_until - now)

            # Experimental: Try Platform App control first if enabled
            if self._platform_app:
                try:
                    if command == "turn":
                        ok = await self._platform_app.control_turn(device.model, device.device, value == "on")
                        if ok:
                            # Set pending expectation
                            now = time.monotonic()
                            device.pending_until = now + 2.0
                            # Power only; no value field
                            return True, None
                        return False, "PlatformApp turn failed"
                    if command == "brightness":
                        ok = await self._platform_app.control_brightness(device.model, device.device, int(value))
                        if ok:
                            now = time.monotonic()
                            device.pending_until = now + 2.0
                            try:
                                ha_val = int(round(float(value) / 100 * 255))
                            except Exception:
                                ha_val = None
                            device.pending_brightness = ha_val
                            return True, None
                        return False, "PlatformApp brightness failed"
                    if command == "color":
                        ok = await self._platform_app.control_colorwc(device.model, device.device, r=value.get("r",0), g=value.get("g",0), b=value.get("b",0), kelvin=0)
                        if ok:
                            now = time.monotonic()
                            device.pending_until = now + 2.0
                            device.pending_color = (int(value.get("r",0)), int(value.get("g",0)), int(value.get("b",0)))
                            device.pending_ct = 0
                            return True, None
                        return False, "PlatformApp color failed"
                    if command == "colorTem":
                        ok = await self._platform_app.control_colorwc(device.model, device.device, r=0,g=0,b=0, kelvin=int(value))
                        if ok:
                            now = time.monotonic()
                            device.pending_until = now + 2.0
                            device.pending_ct = int(value)
                            device.pending_color = (0,0,0)
                            return True, None
                        return False, "PlatformApp colorTem failed"
                except Exception as ex:
                    _LOGGER.debug("PlatformApp control error: %s", ex)

            await self._rate_limit_delay()
        async with self._session.put(_API_CONTROL, headers=self._headers(), json=payload) as resp:
            self._track_rate_limit(resp)

            if resp.status == 429:
                retry = max(0, self._reset - int(time.time()))
                _LOGGER.warning("Rate limited for %s: retry after %ss", device.device, retry)
                # Do not set a long device lock; let token bucket gate retries.
                device.lock_set_until = time.monotonic() + 0.5
                return False, f"Rate limit: retry in {retry}s"

            if resp.status != 200:
                text = await resp.text()
                return False, f"API error {resp.status}: {text}"

            result = await resp.json()
            if result.get("message") == "Success":
                _LOGGER.debug("Control success ← %s %s", device.device, command)
                device.lock_set_until = time.monotonic() + 0.8
                # Record pending expectation to smooth UI against stale reads/pushes
                now = time.monotonic()
                device.pending_until = now + 2.0
                try:
                    if command == "brightness":
                        # value is 0–100; store HA 0–255 for comparison
                        device.pending_brightness = max(0, min(255, int(round(int(value) / 100 * 255))))
                    elif command == "color":
                        device.pending_color = (int(value.get("r",0)), int(value.get("g",0)), int(value.get("b",0)))
                        device.pending_ct = 0
                    elif command == "colorTem":
                        device.pending_ct = int(value)
                        device.pending_color = (0,0,0)
                    elif command == "turn":
                        # no specific value to record here
                        pass
                except Exception:
                    pass
                # Schedule a reconciliatory state fetch shortly after
                self._schedule_post_control_poll(device.device)
                return True, None
            _LOGGER.debug("Control failure ← %s %s: %s", device.device, command, result)
            return False, result.get("message")




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
        _LOGGER.debug(
            "set_color_temp(%s) request=%sK → send=%sK (range %s-%s step %s)",
            getattr(dev, "device", device), value, kelvin, vmin, vmax, step,
        )

        ok, err = await self._debounced_control(dev or device, "colorTem", kelvin)

        return ok, err

    async def _persist_learning(self):
        """Write learned info for devices to storage."""
        try:
            infos = {}
            for dev_id, dev in self._devices.items():
                infos[dev_id] = GoveeLearnedInfo(
                    set_brightness_max=dev.learned_set_brightness_max,
                    get_brightness_max=dev.learned_get_brightness_max,
                    before_set_brightness_turn_on=dev.before_set_brightness_turn_on,
                    config_offline_is_off=dev.config_offline_is_off,
                    learned_color_temp_min=dev.learned_color_temp_min,
                    learned_color_temp_max=dev.learned_color_temp_max,
                )
            await self._storage.write(infos)
        except Exception as ex:
            _LOGGER.debug("Persist learning failed: %s", ex)

    async def set_color(self, device, rgb: Tuple[int, int, int]):
        # Defensive: only send if supported
        if not getattr(device, "support_color", False):
            return False, "Device does not support color"
        return await self._debounced_control(device, "color", {
            "r": rgb[0],
            "g": rgb[1],
            "b": rgb[2]
        })

    async def get_device_state(self, device_id: str) -> Tuple[bool, str | None]:
        """Fetch current state of a device via API."""
        payload = {"device": device_id, "model": self._devices[device_id].model}
        # Respect 10/min per device
        wait = self._state_bucket_take(device_id, 1.0)
        if wait > 0:
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                return False, "Cancelled"
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
            # Collect values first, then enforce exclusivity
            new_color = None
            new_ct = None
            new_brightness = None
            for p in props:
                # Some properties objects don’t have "online"
                if "online" in p and p["online"] is False:
                    dev.online = False
                if p.get("powerState") == "on":
                    dev.power_state = True
                if p.get("powerState") == "off":
                    dev.power_state = False
                if "brightness" in p:
                    # Govee API reports 0–100; HA expects 0–255
                    try:
                        gv = int(p["brightness"])
                    except Exception:
                        gv = 0
                    new_brightness = max(0, min(255, int(round(gv / 100 * 255))))
                if "color" in p:
                    c = p["color"]
                    new_color = (c.get("r", 0), c.get("g", 0), c.get("b", 0))
                if "colorTemInKelvin" in p:
                    try:
                        new_ct = int(p["colorTemInKelvin"]) or 0
                    except Exception:
                        pass
                elif "colorTemperatureK" in p:
                    try:
                        new_ct = int(p["colorTemperatureK"]) or 0
                    except Exception:
                        pass
                elif "colorTem" in p:
                    # Some devices report CT as 0–100 percent; map to Kelvin
                    try:
                        pct = int(p["colorTem"])  # 0–100
                        vmin = dev.color_temp_min or 2700
                        vmax = dev.color_temp_max or 9000
                        width = max(1, (vmax - vmin))
                        pct = max(0, min(100, pct))
                        new_ct = int(round(vmin + (pct * width / 100)))
                    except Exception:
                        pass

            # Apply brightness with pending reconciliation
            if new_brightness is not None:
                now_mono = time.monotonic()
                if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_brightness is not None:
                    if int(new_brightness) == int(dev.pending_brightness):
                        dev.brightness = int(new_brightness)
                        dev.pending_brightness = None
                    else:
                        # ignore stale brightness
                        pass
                else:
                    dev.brightness = int(new_brightness)

            # Enforce mutual exclusivity with preference for RGB if present
            now_mono = time.monotonic()
            # Brightness pending reconciliation
            # (brightness arrives as separate field in some payloads; handled above)

            # Color reconciliation with pending expectation
            if new_color and any(new_color):
                if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_color is not None:
                    if tuple(new_color) != tuple(dev.pending_color):
                        # Ignore stale color
                        pass
                    else:
                        dev.color = (new_color[0], new_color[1], new_color[2])
                        dev.color_temp = 0
                        dev.pending_color = None
                else:
                    dev.color = (new_color[0], new_color[1], new_color[2])
                    dev.color_temp = 0
            elif new_ct and new_ct > 0:
                if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_ct is not None:
                    if int(new_ct) != int(dev.pending_ct):
                        # Ignore stale CT
                        pass
                    else:
                        dev.color_temp = int(new_ct)
                        dev.color = (0, 0, 0)
                        dev.pending_ct = None
                else:
                    dev.color_temp = int(new_ct)
                    dev.color = (0, 0, 0)
            dev.online = True

            return True, None

    def _schedule_post_control_poll(self, device_id: str, delay: float = 5.0):
        now = time.monotonic()
        last = self._last_post_poll.get(device_id, 0.0)
        # Throttle to at most one scheduled poll per 20 seconds
        if (now - last) < 20.0:
            return
        self._last_post_poll[device_id] = now

        async def runner():
            try:
                await asyncio.sleep(delay)
                await self.get_device_state(device_id)
            except Exception:
                return

        asyncio.create_task(runner())
