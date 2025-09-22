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
from .const import (
    CONF_IOT_EMAIL,
    CONF_IOT_PASSWORD,
    DOMAIN,
    FORCE_CT_MODELS,
    COLOR_TEMP_KELVIN_MIN,
    COLOR_TEMP_KELVIN_MAX,
)
from .iot_client import APP_VERSION, _ua, _login, _extract_token, GoveeLoginError
from .quirks import resolve_quirk

_LOGGER = logging.getLogger(__name__)

_API_BASE = "https://developer-api.govee.com/v1"
_API_DEVICES = f"{_API_BASE}/devices"
_API_CONTROL = f"{_API_BASE}/devices/control"


def _normalize_cmd_name(name: str) -> str:
    """Normalize command names for loose matching."""

    return name.replace("-", "_").strip().lower()


def _is_color_temp_command_name(name: str) -> bool:
    """Return True if the command name refers to color temperature control."""

    norm = _normalize_cmd_name(name)
    if not norm:
        return False
    if "colortem" in norm:
        return True
    if "color_temp" in norm:
        return True
    return False


class _Coalescer:
    """Coalesce rapid updates and emit only the latest after a delay."""

    def __init__(self, delay: float = 0.25):
        self.delay = delay
        self._task: asyncio.Task | None = None
        self._future: asyncio.Future | None = None
        self._value: Any = None
        # Sequence number to invalidate older runners that weren't canceled in time
        self._seq: int = 0

    def schedule(self, value: Any, send_func):
        """Schedule send_func(value) after delay; return a Future of (ok, err)."""
        self._value = value
        loop = asyncio.get_running_loop()
        if self._future is None or self._future.done():
            self._future = loop.create_future()

        # Bump sequence so older runners (that may not cancel in time) become no-ops
        self._seq += 1
        my_seq = self._seq
        if self._task and not self._task.done():
            try:
                self._task.cancel()
            except Exception:
                pass

        async def runner():
            try:
                await asyncio.sleep(self.delay)
                # If a newer schedule has occurred, skip sending
                if my_seq != self._seq:
                    return
                # Capture current future to avoid race with reschedules
                local_future = self._future
                result = await send_func(self._value)
                if isinstance(result, tuple) and len(result) == 2:
                    ok, err = result
                else:
                    ok, err = False, "Exception: invalid control handler result"
                # Only the latest scheduled runner should resolve the future
                if my_seq == self._seq and local_future is not None and not local_future.done():
                    local_future.set_result((ok, err))
            except asyncio.CancelledError:
                return
            except Exception as ex:
                local_future = getattr(self, "_future", None)
                if my_seq == self._seq and local_future is not None and not local_future.done():
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
        self._hass = None
        self._config_entry = None

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
        # Platform App control removed; IoT is preferred


    @classmethod
    async def create(cls, api_key: str, storage, hass=None, config_entry=None):
        """Async-safe constructor."""
        self = cls(api_key, storage)
        self._hass = hass
        self._config_entry = config_entry

        # Async-safe SSL context creation
        if hass is not None:
            def _make_ssl():
                return ssl.create_default_context(cafile=certifi.where())
            self._ssl_context = await hass.async_add_executor_job(_make_ssl)
        else:
            self._ssl_context = ssl.create_default_context(cafile=certifi.where())

        await self._init_session()
        # No Platform App init
        return self

    async def _init_session(self):
        """Initialize aiohttp session with SSL context."""
        # Close existing session if already open (important for reloads)
        if self._session and not self._session.closed:
            await self._session.close()

        connector = aiohttp.TCPConnector(ssl=self._ssl_context)
        self._session = ClientSession(connector=connector)

    async def _ensure_iot_started(self):
        """Ensure IoT client exists and is started; best-effort."""
        try:
            if not (self._hass and self._config_entry):
                return
            from .const import DOMAIN, CONF_IOT_EMAIL, CONF_IOT_PASSWORD, CONF_IOT_PUSH_ENABLED  # type: ignore
            from .iot_client import GoveeIoTClient  # type: ignore
            entry_id = self._config_entry.entry_id
            dom = self._hass.data.setdefault(DOMAIN, {})
            entry_bucket = dom.setdefault(entry_id, {})
            iot = entry_bucket.get("iot_client")
            if iot:
                try:
                    _LOGGER.debug("IoT client already present; can_control=%s", getattr(iot, 'can_control', False))
                except Exception:
                    pass
                return
            opts = self._config_entry.options
            data = self._config_entry.data
            enabled = opts.get(CONF_IOT_PUSH_ENABLED, True)
            email = opts.get(CONF_IOT_EMAIL) or data.get(CONF_IOT_EMAIL)
            password = opts.get(CONF_IOT_PASSWORD) or data.get(CONF_IOT_PASSWORD)
            _LOGGER.debug(
                "Ensure IoT: enabled=%s has_email=%s has_password=%s",
                enabled, bool(email), bool(password)
            )
            if enabled and email and password:
                iot = GoveeIoTClient(self._hass, self._config_entry, self)
                try:
                    await iot.start()
                    entry_bucket["iot_client"] = iot
                    _LOGGER.debug("IoT client started on-demand")
                except Exception as ex:
                    _LOGGER.debug("On-demand IoT start failed: %s", ex)
        except Exception:
            return


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
        else:
            # Ensure the current call's desired delay is honored
            try:
                co.delay = float(delay)
            except Exception:
                pass

        async def _send_latest(v):
            # Drop exact duplicates sent within 2 seconds
            now = time.monotonic()
            last = self._last_sent.get(key)
            if last is not None:
                last_val, ts = last
                if last_val == v and (now - ts) < 2.0:
                    return True, None

            # If IoT control is active, skip per-device REST token bucket gating
            skip_bucket = False
            try:
                if self._hass and self._config_entry:
                    from .const import DOMAIN  # type: ignore
                    entry = self._hass.data.get(DOMAIN, {}).get(self._config_entry.entry_id)
                    iot = entry and entry.get("iot_client")
                    if iot and getattr(iot, "can_control", False):
                        skip_bucket = True
            except Exception:
                skip_bucket = False

            # Limited retry loop for 429s
            attempts = 0
            while True:
                # Gate each attempt by per-device token bucket (10/min default) unless IoT is active
                if not skip_bucket:
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
        """Discover devices via mobile (app) API using cached IoT token (avoid re-login)."""
        # Short-circuit: if we already populated devices in the last 60s, reuse
        try:
            cache_window = 300  # seconds
            now = time.time()
            last = getattr(self, "_last_devices_fetch_ts", 0)
            if self._devices and (now - last) < cache_window:
                return list(self._devices.values()), None
        except Exception:
            pass

        if not self._config_entry:
            return [], "Missing config entry"
        # Prefer options, fall back to initial data for first-run flows
        email = self._config_entry.options.get(CONF_IOT_EMAIL) or self._config_entry.data.get(CONF_IOT_EMAIL)
        password = self._config_entry.options.get(CONF_IOT_PASSWORD) or self._config_entry.data.get(CONF_IOT_PASSWORD)
        if not email or not password:
            return [], "IoT credentials not configured"

        # Ensure IoT client is started so we can reuse its token
        await self._ensure_iot_started()

        # MQTT-only discovery path: try to discover via IoT account topic without HTTP
        try:
            from .const import DOMAIN  # type: ignore
            entry_id = self._config_entry.entry_id if self._config_entry else None
            iot = self._hass.data.get(DOMAIN, {}).get(entry_id or "", {}).get("iot_client") if (self._hass and entry_id) else None
            if iot:
                topics = iot.get_topics() if hasattr(iot, "get_topics") else {}
                seen = iot.get_known_devices() if hasattr(iot, "get_known_devices") else {}
                device_ids = list(seen.keys()) or list(topics.keys())
                if device_ids:
                    try:
                        learning_infos = await self._storage.read()
                    except Exception:
                        learning_infos = {}
                    timestamp = int(time.time())
                    _LOGGER.debug("MQTT discovery found %s devices", len(device_ids))
                    for dev_id in device_ids:
                        if dev_id in self._devices:
                            continue
                        learned = learning_infos.get(dev_id, GoveeLearnedInfo())
                        self._devices[dev_id] = GoveeDevice(
                            device=dev_id,
                            model="unknown",
                            device_name=dev_id,
                            controllable=True,
                            retrievable=True,
                            support_cmds=[],
                            support_turn=True,
                            support_brightness=True,
                            support_color=True,
                            support_color_temp=False,
                            color_temp_min=None,
                            color_temp_max=None,
                            color_temp_step=1,
                            lan_api_capable=False,
                            avoid_platform_api=False,
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
                    try:
                        self._last_devices_fetch_ts = time.time()
                    except Exception:
                        pass
                    # Do not return early; continue to mobile API to enrich names/capabilities
        except Exception:
            pass

        async def _list_with_token(token: str):
            import requests
            import uuid
            import time as _t

            def _client_id(em: str) -> str:
                return uuid.uuid5(uuid.NAMESPACE_DNS, em).hex

            headers = {
                "Authorization": f"Bearer {token}",
                "appVersion": APP_VERSION,
                "clientId": _client_id(email),
                "clientType": "1",
                "iotVersion": "0",
                "timestamp": str(int(_t.time() * 1000)),
                "User-Agent": _ua(),
            }
            resp = requests.post("https://app2.govee.com/device/rest/devices/v1/list", headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()

        # Try token from IoT client first
        token = None
        try:
            from .const import DOMAIN  # type: ignore
            entry_id = self._config_entry.entry_id
            iot = self._hass.data.get(DOMAIN, {}).get(entry_id, {}).get("iot_client") if self._hass else None
            token = getattr(iot, "_token", None)
            if token:
                _LOGGER.debug("login cache found, using cache")
        except Exception:
            token = None

        # If no token in memory, try on-disk token cache
        if not token and self._hass:
            def _read_token(path: str):
                try:
                    import json as _json, time as _t
                    data = _json.load(open(path, "r", encoding="utf-8"))
                    ts = float(data.get("ts", 0))
                    # Use only if cached within 15 days
                    if (_t.time() - ts) < (15 * 24 * 60 * 60):
                        return data.get("token")
                    return None
                except Exception:
                    return None

            try:
                cache_dir = self._hass.config.path(".storage/govee_iot")
                token_path = __import__("os").path.join(cache_dir, "token.json")
                token = await asyncio.get_running_loop().run_in_executor(None, _read_token, token_path)
                if token:
                    _LOGGER.debug("login cache found, using cache")
            except Exception:
                token = None

        # As a last resort, do one login (rare) and persist it for 15 days
        if not token:
            _LOGGER.debug("no login cache founds, logging in")
            try:
                acct = await asyncio.get_running_loop().run_in_executor(None, _login, email, password)
                token = _extract_token(acct)
                # Persist token to the same IoT cache location for reuse across restarts
                if token and self._hass:
                    try:
                        import os
                        import json as _json
                        import uuid as _uuid
                        now_wall = int(time.time())
                        cache_dir = self._hass.config.path(".storage/govee_iot")
                        client_id = _uuid.uuid5(_uuid.NAMESPACE_DNS, email).hex
                        # Normalize topic/account fields if present
                        tval = acct.get("topic")
                        if isinstance(tval, dict) and "value" in tval:
                            tval = tval["value"]
                        account_topic = tval if isinstance(tval, str) else None
                        account_id = acct.get("accountId") or acct.get("account_id")
                        payload = {
                            "token": token,
                            "accountTopic": account_topic,
                            "accountId": account_id,
                            "clientId": client_id,
                            "ts": now_wall,
                        }
                        loop = asyncio.get_running_loop()
                        def _persist_token(dir_path: str, data: dict) -> bool:
                            try:
                                os.makedirs(dir_path, exist_ok=True)
                                token_path = os.path.join(dir_path, "token.json")
                                with open(token_path, "w", encoding="utf-8") as f:
                                    _json.dump(data, f)
                                return True
                            except Exception:
                                return False
                        await loop.run_in_executor(None, _persist_token, cache_dir, payload)
                    except Exception:
                        pass
            except GoveeLoginError as ex:
                _LOGGER.warning("Govee login failed: %s", ex)
                token = None
            except Exception:
                token = None
        if not token:
            return [], "Mobile list failed: no token available"

        # Make the device list call in executor
        def _list_call_sync(tok: str):
            import requests
            import uuid
            import time as _t
            def _client_id(em: str) -> str:
                return uuid.uuid5(uuid.NAMESPACE_DNS, em).hex
            headers = {
                "Authorization": f"Bearer {tok}",
                "appVersion": APP_VERSION,
                "clientId": _client_id(email),
                "clientType": "1",
                "iotVersion": "0",
                "timestamp": str(int(_t.time() * 1000)),
                "User-Agent": _ua(),
            }
            resp = requests.post("https://app2.govee.com/device/rest/devices/v1/list", headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        try:
            loop = asyncio.get_running_loop()
            data = await loop.run_in_executor(None, _list_call_sync, token)
        except Exception as ex2:
            return [], f"Mobile list failed: {ex2}"

        # Extract items from either {devices:[...]} or {data:{devices:[...]}}
        items = []
        if isinstance(data, dict):
            if isinstance(data.get("devices"), list):
                items = data["devices"]
            elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("devices"), list):
                items = data["data"]["devices"]
        if not items:
            return [], "Malformed device list"

        timestamp = int(time.time())
        learning_infos = await self._storage.read()
        platform_enriched_done = False

        _LOGGER.debug("Discovered %s devices from Govee Mobile API", len(items))
        try:
            _LOGGER.debug("Mobile API raw devices payload: %s", items)
        except Exception:
            pass
        for item in items:
            dev_id = item["device"]
            if dev_id in self._devices:
                # Update existing placeholder (e.g., created via MQTT discovery) with richer info
                try:
                    existing = self._devices[dev_id]
                    name = item.get("deviceName") or item.get("device_name")
                    if isinstance(name, str) and name and name != existing.device_name:
                        existing.device_name = name
                    model_val = item.get("model") or item.get("sku") or item.get("type")
                    if isinstance(model_val, str) and model_val:
                        existing.model = model_val
                    # We will also refresh capability flags below when constructing the device
                except Exception:
                    pass
                # Do not skip; allow capability parsing and flag updates using the same logic
                # but we will avoid replacing the object entirely; instead apply updates.
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

            raw_support_cmds = [cmd for cmd in item.get("supportCmds", []) if isinstance(cmd, str)]
            support_cmds = [cmd.lower() for cmd in raw_support_cmds]
            has_ct_cmd = any(_is_color_temp_command_name(cmd) for cmd in raw_support_cmds)
            # Apply model-specific quirks if known
            model = item.get("model") or item.get("sku") or item.get("type") or "unknown"
            quirk = resolve_quirk(model) if model else None
            model_key = model.upper() if isinstance(model, str) else str(model)
            forced_ct_model = model_key in FORCE_CT_MODELS
            if quirk and quirk.color_temp_range:
                qmin, qmax = quirk.color_temp_range
                if ct_min is None:
                    ct_min = int(qmin)
                if ct_max is None:
                    ct_max = int(qmax)
            if forced_ct_model:
                if ct_min is None:
                    ct_min = COLOR_TEMP_KELVIN_MIN
                if ct_max is None:
                    ct_max = COLOR_TEMP_KELVIN_MAX

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

            # If we still have no capability information at all (mobile list often lacks it),
            # assume basic light capabilities to allow control attempts. API will reject if invalid.
            if not support_cmds and not any([derived_turn, derived_brightness, derived_color, derived_ct]):
                derived_turn = True
                derived_brightness = True

            # Treat None as unknown/true for controllable/retrievable when mobile API omits flags
            controllable_flag = item.get("controllable")
            retrievable_flag = item.get("retrievable")
            name_val = item.get("deviceName") or item.get("device_name") or dev_id
            if dev_id in self._devices:
                # Update existing device object in place
                dev_obj = self._devices[dev_id]
                dev_obj.device_name = name_val or dev_obj.device_name
                dev_obj.model = model
                dev_obj.controllable = True if controllable_flag is None else bool(controllable_flag)
                dev_obj.retrievable = True if retrievable_flag is None else bool(retrievable_flag)
                dev_obj.support_cmds = support_cmds
                dev_obj.support_turn = ("turn" in support_cmds) or derived_turn
                dev_obj.support_brightness = ("brightness" in support_cmds) or derived_brightness
                if support_cmds or derived_color:
                    dev_obj.support_color = (
                        "color" in support_cmds or "colorwc" in support_cmds or derived_color
                    )
                if support_cmds or derived_ct or (ct_min is not None or ct_max is not None) or forced_ct_model:
                    dev_obj.support_color_temp = (
                        has_ct_cmd or derived_ct or (ct_min is not None or ct_max is not None) or forced_ct_model
                    )
                dev_obj.color_temp_min = ct_min
                dev_obj.color_temp_max = ct_max
                dev_obj.color_temp_step = ct_step or 1
                dev_obj.lan_api_capable = bool(quirk and quirk.lan_api_capable)
                dev_obj.avoid_platform_api = bool(quirk and quirk.avoid_platform_api)
                dev_obj.online = True
                dev_obj.timestamp = timestamp
                dev_obj.source = GoveeSource.API
                dev_obj.learned_set_brightness_max = learned.set_brightness_max
                dev_obj.learned_get_brightness_max = learned.get_brightness_max
                dev_obj.before_set_brightness_turn_on = learned.before_set_brightness_turn_on
                dev_obj.config_offline_is_off = learned.config_offline_is_off
                # Do not restore protocol quirks from storage; re-learn each run (memory-only)
                # Keep learned CT overrides untouched here; they are managed elsewhere
                try:
                    _LOGGER.debug(
                        "Capabilities ← %s (%s): cmds=%s support_ct=%s range=%s-%s step=%s send_percent=%s forced_ct=%s",
                        dev_id,
                        model,
                        support_cmds,
                        dev_obj.support_color_temp,
                        dev_obj.color_temp_min,
                        dev_obj.color_temp_max,
                        dev_obj.color_temp_step,
                        getattr(dev_obj, "color_temp_send_percent", None),
                        forced_ct_model,
                    )
                except Exception:
                    pass
            else:
                self._devices[dev_id] = GoveeDevice(
                    device=dev_id,
                    model=model,
                    device_name=name_val,
                    controllable=True if controllable_flag is None else bool(controllable_flag),
                    retrievable=True if retrievable_flag is None else bool(retrievable_flag),
                    support_cmds=support_cmds,
                    support_turn=("turn" in support_cmds) or derived_turn,
                    support_brightness=("brightness" in support_cmds) or derived_brightness,
                    support_color=(
                        "color" in support_cmds or "colorwc" in support_cmds or derived_color
                    ),
                    # Consider color temp supported if API lists command OR we detected a CT range
                    support_color_temp=(
                        has_ct_cmd or derived_ct or (ct_min is not None or ct_max is not None) or forced_ct_model
                    ),
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
                    # Do not seed protocol quirk preferences from storage (memory-only learning)
                )
                try:
                    created = self._devices[dev_id]
                    _LOGGER.debug(
                        "Capabilities ← %s (%s): cmds=%s support_ct=%s range=%s-%s step=%s forced_ct=%s",
                        dev_id,
                        model,
                        support_cmds,
                        created.support_color_temp,
                        created.color_temp_min,
                        created.color_temp_max,
                        created.color_temp_step,
                        forced_ct_model,
                    )
                except Exception:
                    pass

        # If any devices still have MAC/ID as name, try the newer Platform API device list to enrich names
        if not platform_enriched_done:
            try:
                missing_names = [d.device for d in self._devices.values() if not d.device_name or d.device_name == d.device]
                if missing_names:
                    from .platform_app import PlatformAppClient  # type: ignore
                    pac = PlatformAppClient(email, password)
                    try:
                        pdata = await pac.list_devices()
                    except Exception as ex_pa:
                        pdata = None
                        _LOGGER.debug("Platform API list failed: %s", ex_pa)
                    mapping: Dict[str, Dict[str, Any]] = {}
                    # Robustly walk payload to find list of devices
                    def _collect_devices(obj):
                        if isinstance(obj, list):
                            for e in obj:
                                _collect_devices(e)
                        elif isinstance(obj, dict):
                            # A plausible device entry has a 'device' id and some name field
                            if ("device" in obj or "deviceId" in obj) and any(k in obj for k in ("deviceName", "device_name", "name")):
                                did = obj.get("device") or obj.get("deviceId")
                                if isinstance(did, str):
                                    mapping[did] = obj
                            for v in obj.values():
                                _collect_devices(v)
                    if isinstance(pdata, dict):
                        _collect_devices(pdata)
                    if mapping:
                        updated = 0
                        for did, dev in mapping.items():
                            if did in self._devices and (self._devices[did].device_name == self._devices[did].device or not self._devices[did].device_name):
                                nm = dev.get("deviceName") or dev.get("device_name") or dev.get("name")
                                if isinstance(nm, str) and nm:
                                    self._devices[did].device_name = nm
                                    updated += 1
                                mv = dev.get("model") or dev.get("sku") or dev.get("type")
                                if isinstance(mv, str) and mv:
                                    self._devices[did].model = mv
                        if updated:
                            _LOGGER.debug("Platform API enrichment updated %s device names", updated)
                platform_enriched_done = True
            except Exception as ex_e:
                _LOGGER.debug("Platform API enrichment error: %s", ex_e)

        try:
            self._last_devices_fetch_ts = time.time()
        except Exception:
            pass
        return list(self._devices.values()), None

    async def init_devices(self) -> Tuple[List[GoveeDevice], str | None]:
        """Discover devices and request IoT status for initial state."""
        devices, err = await self.get_devices()
        if err:
            return devices, err
        try:
            from .const import DOMAIN
            entry_id = self._config_entry.entry_id if self._config_entry else None
            iot = self._hass.data.get(DOMAIN, {}).get(entry_id or "", {}).get("iot_client") if (self._hass and entry_id) else None
            if iot and hasattr(iot, "async_request_status"):
                try:
                    if getattr(iot, "can_control", False):
                        await asyncio.sleep(0.3)
                except Exception:
                    pass
                for dev in devices:
                    try:
                        await iot.async_request_status(dev.device)
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
            # Debug: log device topics if available
            if iot and hasattr(iot, "get_topics"):
                try:
                    topics = iot.get_topics()
                    for dev in devices:
                        _LOGGER.debug("Device topic %s -> %s", dev.device, topics.get(dev.device))
                except Exception:
                    pass
        except Exception:
            pass
        return list(self._devices.values()), None


    async def _control(self, device: Union[str, GoveeDevice], command: str, value: Any) -> Tuple[bool, str | None]:
        if isinstance(device, str):
            device = self._devices.get(device)
        if not device:
            return False, f"Unknown device {device}"
        if not device.controllable:
            return False, f"Device {device.device} not controllable"
        # Gate by known capabilities only. If capabilities are unknown (common with mobile list),
        # optimistically allow commands and let the API return an error if unsupported.
        command_l = command.lower()
        if command_l not in (device.support_cmds or []):
            support_known = bool(device.support_cmds) or any(
                [
                    device.support_turn,
                    device.support_brightness,
                    device.support_color,
                    device.support_color_temp,
                ]
            )
            if support_known:
                if command_l == "turn" and not device.support_turn:
                    return False, f"Command {command} not supported"
                if command_l == "brightness" and not device.support_brightness:
                    return False, f"Command {command} not supported"
                if command_l == "color" and not device.support_color:
                    return False, f"Command {command} not supported"
                if command_l in {"colortem", "colortemperature", "colortemperaturek", "colorteminkelvin"} and not device.support_color_temp:
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
            err_iot = None
            _LOGGER.debug("Sending control → %s %s: %s", device.device, command, value)

            # Short post-success cooldown (set below). Avoid long sleeps here to
            # keep coalescer cancellations effective.
            now = time.monotonic()
            if now < device.lock_set_until and device.lock_set_until - now < 2.0:
                await asyncio.sleep(device.lock_set_until - now)

            # Preferred: IoT (AWS MQTT) control if available
            try:
                from .const import DOMAIN  # type: ignore
                if self._hass and self._config_entry:
                    # Ensure IoT client is running (best-effort)
                    await self._ensure_iot_started()
                    entry_id = self._config_entry.entry_id
                    entry_data = self._hass.data.get(DOMAIN, {}).get(entry_id)
                    iot = entry_data and entry_data.get("iot_client")
                    if iot:
                        ok, err_iot = await iot.async_publish_control(device.device, command, value)
                        if ok:
                            # Record pending expectation for UI smoothing
                            now2 = time.monotonic()
                            device.lock_set_until = now2 + 0.8
                            device.pending_until = now2 + 5.0
                            try:
                                if command == "brightness":
                                    device.pending_brightness = max(0, min(255, int(round(int(value) / 100 * 255))))
                                elif command == "color":
                                    device.pending_color = (
                                        int(value.get("r", 0)),
                                        int(value.get("g", 0)),
                                        int(value.get("b", 0)),
                                    )
                                    device.pending_ct = 0
                                elif command.lower() in {"colortem", "colortemperature", "colortemperaturek", "colorteminkelvin"}:
                                    try:
                                        device.pending_ct = int(value)
                                    except Exception:
                                        device.pending_ct = 0
                                    device.pending_color = (0, 0, 0)
                            except Exception:
                                pass
                            # Schedule a reconcile poll via REST less frequently to ensure UI sync
                            self._schedule_post_control_poll(device.device)
                            return True, None
                        if err_iot and err_iot != "publish_failed":
                            # Surface IoT specific error and avoid REST fallback unless publish truly failed
                            if err_iot == "iot_no_confirm":
                                if device is not None:
                                    device.online = False
                                return False, "Device did not respond to IoT command"
                            if err_iot in {"iot_not_ready", "no_topic", "throttled"}:
                                return False, "IoT control temporarily unavailable"
                            if err_iot == "unsupported_command":
                                _LOGGER.debug("IoT command %s unsupported; falling back to REST", command)
                                err_iot = None
                            # Otherwise proceed to REST fallback if available
            except Exception as ex:
                _LOGGER.debug("IoT control path error: %s", ex)

            # If no API key configured, avoid REST fallback to prevent 401s
            if not self._api_key:
                if err_iot == "publish_failed":
                    return False, "IoT publish failed"
                return False, "IoT control unavailable and no API key configured"

            await self._rate_limit_delay()
        async with self._session.put(_API_CONTROL, headers=self._headers(), json=payload) as resp:
            try:
                _LOGGER.debug("REST control PUT %s payload=%s", _API_CONTROL, payload)
            except Exception:
                pass
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
                device.pending_until = now + 5.0
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

        send_val = kelvin
        if dev and dev.color_temp_send_percent:
            vmin = dev.color_temp_min or 2700
            vmax = dev.color_temp_max or 9000
            rng = max(1, vmax - vmin)
            pct = int(round((kelvin - vmin) / rng * 100))
            send_val = max(0, min(100, pct))
        ok, err = await self._debounced_control(dev or device, "colorTem", send_val)
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
                    # Do not persist protocol quirk preferences; memory-only
                )
            await self._storage.write(infos)
        except Exception as ex:
            _LOGGER.debug("Persist learning failed: %s", ex)

    async def set_color(self, device, rgb: Tuple[int, int, int]):
        # Defensive: only send if supported
        if not getattr(device, "support_color", False):
            return False, "Device does not support color"
        # Longer debounce for color to wait for end-of-drag before sending
        return await self._debounced_control(device, "color", {
            "r": rgb[0],
            "g": rgb[1],
            "b": rgb[2]
        }, delay=1.0)

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
            online_flag = None
            for p in props:
                if "online" in p or "deviceOnline" in p or "isOnline" in p:
                    try:
                        val = p.get("online")
                        if val is None:
                            val = p.get("deviceOnline")
                        if val is None:
                            val = p.get("isOnline")
                        if isinstance(val, bool):
                            online_flag = val
                        elif isinstance(val, str):
                            online_flag = val.lower() == "true"
                        else:
                            online_flag = bool(val)
                    except Exception:
                        pass
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
                    if not device.support_color_temp:
                        device.support_color_temp = True
                    if device.color_temp_min is None:
                        device.color_temp_min = COLOR_TEMP_KELVIN_MIN
                    if device.color_temp_max is None:
                        device.color_temp_max = COLOR_TEMP_KELVIN_MAX
                    try:
                        new_ct = int(p["colorTemInKelvin"]) or 0
                    except Exception:
                        pass
                elif "colorTemperatureK" in p:
                    if not device.support_color_temp:
                        device.support_color_temp = True
                    if device.color_temp_min is None:
                        device.color_temp_min = COLOR_TEMP_KELVIN_MIN
                    if device.color_temp_max is None:
                        device.color_temp_max = COLOR_TEMP_KELVIN_MAX
                    try:
                        new_ct = int(p["colorTemperatureK"]) or 0
                    except Exception:
                        pass
                elif "colorTem" in p:
                    if not device.support_color_temp:
                        device.support_color_temp = True
                    if device.color_temp_min is None:
                        device.color_temp_min = COLOR_TEMP_KELVIN_MIN
                    if device.color_temp_max is None:
                        device.color_temp_max = COLOR_TEMP_KELVIN_MAX
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
            if online_flag is not None:
                dev.online = online_flag
            elif not props:
                dev.online = False

            try:
                dev.timestamp = int(time.time())
            except Exception:
                pass
            try:
                if self._hass and self._config_entry:
                    entry_data = self._hass.data.get(DOMAIN, {}).get(self._config_entry.entry_id)
                    iot_client = entry_data.get("iot_client") if entry_data else None
                    if iot_client and hasattr(iot_client, "mark_seen"):
                        iot_client.mark_seen(device_id, online=online_flag if online_flag is not None else None)
            except Exception:
                pass

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
