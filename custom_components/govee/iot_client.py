"""Govee IoT (AWS MQTT) read-only client for push state updates."""
from __future__ import annotations

import asyncio
import contextlib
import os
import base64
import json
import logging
import ssl
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import certifi
import requests
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from paho.mqtt.client import Client as MqttClient, MQTTMessageInfo
from .const import (
    DOMAIN,
    CONF_IOT_EMAIL,
    CONF_IOT_PASSWORD,
    CONF_IOT_PUSH_ENABLED,
    CONF_IOT_CONTROL_ENABLED,
    COLOR_TEMP_KELVIN_MIN,
    COLOR_TEMP_KELVIN_MAX,
)


class GoveeLoginError(Exception):
    """Raised when Govee login fails."""

    def __init__(self, message: str, code: int | None = None):
        self.code = code
        text = f"{message} (code={code})" if code is not None else message
        super().__init__(text)

_LOGGER = logging.getLogger(__name__)

APP_VERSION = "5.6.01"

# Lightweight in-process caches to avoid excessive logins
_APP_LOGIN_CACHE: dict[str, tuple[dict, float]] = {}
_IOT_KEY_CACHE: dict[str, tuple[dict, float]] = {}
_CACHE_TTL_SEC = 6 * 60 * 60  # 6 hours


def _ua() -> str:
    return (
        f"GoveeHome/{APP_VERSION} (com.ihoment.GoVeeSensor; build:2; iOS 16.5.0) Alamofire/5.6.4"
    )


def _ms_ts() -> str:
    return str(int(time.time() * 1000))


def _client_id(email: str) -> str:
    return uuid.uuid5(uuid.NAMESPACE_DNS, email).hex


def _extract_token(payload: Dict[str, Any]) -> str | None:
    """Extract a token value from various possible fields."""

    if not isinstance(payload, dict):
        return None
    token_keys = ["token", "accessToken", "authToken", "tokenValue"]
    for key in token_keys:
        token = payload.get(key)
        if isinstance(token, str) and token:
            _LOGGER.debug("Login token found under key '%s'", key)
            return token

    # Common containers for tokens observed in various API responses
    for container_key in ("data", "client"):
        nested = payload.get(container_key)
        if isinstance(nested, dict):
            token = _extract_token(nested)
            if token:
                return token

    nested = payload.get("data")
    if isinstance(nested, dict):
        return _extract_token(nested)
    return None


def _login(email: str, password: str) -> Dict[str, Any]:
    resp = requests.post(
        "https://app2.govee.com/account/rest/account/v1/login",
        json={"email": email, "password": password, "client": _client_id(email)},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    token = _extract_token(data)
    client = data.get("client") or data.get("data") or data
    t = client.get("topic")
    if isinstance(t, dict) and "value" in t:
        client["topic"] = t["value"]
    if token:
        client.setdefault("token", token)
        return client
    err_msg = data.get("message") or data.get("msg") or "no token in response"
    code = data.get("code")
    raise GoveeLoginError(err_msg, code)


def _get_iot_key(token: str, email: str) -> Dict[str, Any]:
    resp = requests.get(
        "https://app2.govee.com/app/v1/account/iot/key",
        headers={
            "Authorization": f"Bearer {token}",
            "appVersion": APP_VERSION,
            "clientId": _client_id(email),
            "clientType": "1",
            "iotVersion": "0",
            "timestamp": _ms_ts(),
            "User-Agent": _ua(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data") or data


def _extract_pfx(p12_b64: str, password: str) -> tuple[bytes, bytes]:
    pfx = base64.b64decode(p12_b64)
    key, cert, _ = load_key_and_certificates(pfx, password.encode("utf-8"))
    key_pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    cert_pem = cert.public_bytes(Encoding.PEM)
    return key_pem, cert_pem


@dataclass
class IoTState:
    hass: Any
    entry_id: str
    hub: Any
    mqtt: MqttClient | None = None
    stop_event: asyncio.Event | None = None
    connected_event: asyncio.Event | None = None
    certfile_path: str | None = None
    keyfile_path: str | None = None


class GoveeIoTClient:
    def __init__(self, hass, entry, hub):
        self._hass = hass
        self._entry = entry
        self._hub = hub
        self._iot: IoTState | None = None
        self._last_reconcile: dict[str, float] = {}
        self._device_topics: dict[str, str] = {}
        self._account_topic: str | None = None
        self._token: str | None = None
        # Seed email from entry immediately for on-demand refreshes
        try:
            from .const import CONF_IOT_EMAIL  # type: ignore
            self._email: str | None = (
                entry.options.get(CONF_IOT_EMAIL)
                or entry.data.get(CONF_IOT_EMAIL)
            )
        except Exception:
            self._email = None
        # Passive MQTT discovery cache (device_id -> last state payload)
        self._seen_devices: dict[str, dict] = {}
        self._last_seen_wall: dict[str, float] = {}
        self._pending_status: dict[str, float] = {}
        self._status_grace: float = 5.0
        self._miss_counts: dict[str, int] = {}
        self._status_miss_threshold: int = 2
        self._ping_interval: float = 5 * 60.0
        self._ping_task: asyncio.Task | None = None
        self._status_timers: dict[str, asyncio.Task] = {}
        self._alt_status_models: set[str] = {"H6121"}

        # Global publish token bucket (max publishes per minute across all devices)
        # Default: 20/min
        self._pub_bucket_capacity: float = 20.0
        self._pub_bucket_refill_per_sec: float = self._pub_bucket_capacity / 60.0
        self._pub_bucket_tokens: float = self._pub_bucket_capacity
        self._pub_bucket_last: float = time.monotonic()
        self._loop: asyncio.AbstractEventLoop | None = None
        # Simple duplicate suppression for IoT publishes
        self._last_color_sent: dict[str, tuple[tuple[int,int,int], float]] = {}
        self._last_ct_sent: dict[str, tuple[int, float]] = {}
        self._last_fallback_poll = 0.0
        try:
            opts = dict(entry.options) if isinstance(entry.options, dict) else dict(entry.options or {})
        except Exception:
            opts = {}
        try:
            data = dict(entry.data) if isinstance(entry.data, dict) else dict(entry.data or {})
        except Exception:
            data = {}
        publish_opt = opts.get(CONF_IOT_CONTROL_ENABLED) if isinstance(opts, dict) else None
        if publish_opt is None and isinstance(data, dict):
            publish_opt = data.get(CONF_IOT_CONTROL_ENABLED)
        # Default to allow publishing when option is unset (matches legacy behaviour)
        self._allow_publish = True if publish_opt is None else bool(publish_opt)
        if not self._allow_publish:
            _LOGGER.debug("IoT control disabled via options or credentials")
        self._publish_warned = False
        self._last_publish_monotonic = 0.0


    def _pub_bucket_take(self, tokens: float = 1.0) -> float:
        """Global IoT publish token bucket: return wait seconds if not enough tokens."""
        now = time.monotonic()
        elapsed = max(0.0, now - self._pub_bucket_last)
        self._pub_bucket_tokens = min(
            self._pub_bucket_capacity,
            self._pub_bucket_tokens + elapsed * self._pub_bucket_refill_per_sec,
        )
        self._pub_bucket_last = now
        if self._pub_bucket_tokens >= tokens:
            self._pub_bucket_tokens -= tokens
            return 0.0
        needed = tokens - self._pub_bucket_tokens
        wait = needed / self._pub_bucket_refill_per_sec if self._pub_bucket_refill_per_sec > 0 else 0.0
        return max(0.0, wait)

    def _publish(self, topic: str, payload: dict, *, log_payload: bool = True) -> MQTTMessageInfo | None:
        iot = self._iot
        if not iot or not iot.mqtt:
            return None
        if not self._allow_publish:
            if not self._publish_warned:
                _LOGGER.debug("IoT publish skipped; MQTT credentials appear read-only")
                self._publish_warned = True
            return None
        try:
            js = json.dumps(payload, separators=(",", ":"))
            info = iot.mqtt.publish(topic, js, qos=0, retain=False)
            self._last_publish_monotonic = time.monotonic()
            try:
                if log_payload:
                    _LOGGER.debug("IoT publish topic=%s mid=%s rc=%s payload=%s", topic, getattr(info, "mid", None), getattr(info, "rc", None), js)
                else:
                    _LOGGER.debug("IoT publish topic=%s mid=%s rc=%s", topic, getattr(info, "mid", None), getattr(info, "rc", None))
            except Exception:
                pass
            if getattr(info, "rc", 0) != 0:
                return None
            return info
        except Exception as ex:
            _LOGGER.debug("IoT publish failed: %s", ex)
            return None

    async def _poll_state_fallback(self, device_id: str | None = None) -> bool:
        if not self._hub or not self._hass:
            return False
        import time as _t
        now = _t.monotonic()
        if device_id is None:
            if (now - self._last_fallback_poll) < 20.0:
                return False
            self._last_fallback_poll = now
        try:
            if device_id:
                ok, _ = await self._hub.get_device_state(device_id)
                dev = self._hub._devices.get(device_id)
                if ok:
                    online = getattr(dev, "online", None) if dev else None
                    self.mark_seen(device_id, online=online)
                    self._clear_pending_status(device_id)
                else:
                    # REST fallback failed; treat as offline until IoT reports back.
                    self.mark_seen(device_id, online=False)
                    if dev and getattr(dev, "online", True):
                        dev.online = False
            else:
                for dev_id in list(getattr(self._hub, "_devices", {}).keys()):
                    ok, _ = await self._hub.get_device_state(dev_id)
                    dev = self._hub._devices.get(dev_id)
                    if ok:
                        online = getattr(dev, "online", None) if dev else None
                        self.mark_seen(dev_id, online=online)
                        self._clear_pending_status(dev_id)
                    else:
                        # REST fallback failed; treat as offline until IoT reports back.
                        self.mark_seen(dev_id, online=False)
                        if dev and getattr(dev, "online", True):
                            dev.online = False
        except Exception as ex:
            _LOGGER.debug("IoT fallback poll error: %s", ex)
            return False
        entry_data = self._hass.data.get(DOMAIN, {}).get(self._entry.entry_id)
        if entry_data and "coordinator" in entry_data:
            try:
                entry_data["coordinator"].async_set_updated_data(list(self._hub._devices.values()))
            except Exception:
                pass
        return True

    async def start(self):
        opts = self._entry.options
        data = self._entry.data
        enabled = opts.get(CONF_IOT_PUSH_ENABLED, True)
        # Fall back to config entry data if options are empty (common on first run)
        email = opts.get(CONF_IOT_EMAIL) or data.get(CONF_IOT_EMAIL)
        password = opts.get(CONF_IOT_PASSWORD) or data.get(CONF_IOT_PASSWORD)
        _LOGGER.debug(
            "IoT start: enabled=%s has_email=%s has_password=%s",
            enabled,
            bool(email),
            bool(password),
        )
        if not enabled:
            _LOGGER.debug("IoT disabled via options; skipping start")
            return
        if not email or not password:
            _LOGGER.debug("IoT missing credentials; skipping start")
            return
        loop = asyncio.get_running_loop()
        self._loop = loop
        # Login and fetch IoT key in executor, with 15-day on-disk cache
        try:
            import json as _json
            cache_dir = self._hass.config.path('.storage/govee_iot')
            os.makedirs(cache_dir, exist_ok=True)
            token_path = os.path.join(cache_dir, 'token.json')
            cert_path = os.path.join(cache_dir, 'cert.pem')
            key_path = os.path.join(cache_dir, 'key.pem')
            endpoint_path = os.path.join(cache_dir, 'endpoint.txt')
            now_wall = time.time()
            ttl = 15 * 24 * 60 * 60  # 15 days

            token: str | None = None
            account_id: str | None = None
            # Small blocking helpers moved to executor
            def _read_json_file(path: str):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        return _json.load(f)
                except Exception:
                    return None
            def _write_json_file(path: str, data: dict):
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        _json.dump(data, f)
                    return True
                except Exception:
                    return False
            def _read_text_file(path: str) -> str | None:
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        return f.read()
                except Exception:
                    return None
            def _write_text_file(path: str, text: str) -> bool:
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        f.write(text)
                    return True
                except Exception:
                    return False
            def _write_bytes_file(path: str, data: bytes) -> bool:
                try:
                    with open(path, 'wb') as f:
                        f.write(data)
                    return True
                except Exception:
                    return False
            # Try cached token from disk first
            try:
                tok = await loop.run_in_executor(None, _read_json_file, token_path)
                if (now_wall - float(tok.get('ts', 0))) < ttl:
                    token = tok.get('token')
                    self._account_topic = tok.get('accountTopic')
                    account_id = tok.get('accountId')
                    _LOGGER.debug("login cache found, using cache")
            except Exception:
                token = None

            # If no valid disk token, try in-memory cache, else login
            if not token:
                _LOGGER.debug("no login cache founds, logging in")
                now_mono = time.monotonic()
                cached = _APP_LOGIN_CACHE.get(email)
                acct: dict[str, Any] | None = None
                if cached and (now_mono - cached[1]) < _CACHE_TTL_SEC:
                    acct = cached[0]
                else:
                    try:
                        acct = await loop.run_in_executor(None, _login, email, password)
                        _APP_LOGIN_CACHE[email] = (acct, now_mono)
                    except GoveeLoginError as ex:
                        _LOGGER.warning("Govee IoT login failed: %s", ex)
                        return
                token = _extract_token(acct)
                if not token:
                    _LOGGER.warning("Govee IoT: no token from login response")
                    return
                _LOGGER.debug("IoT login successful; token acquired")
                tval = acct.get("topic")
                if isinstance(tval, dict) and 'value' in tval:
                    tval = tval['value']
                self._account_topic = tval if isinstance(tval, str) else None
                account_id = acct.get('accountId') or acct.get('account_id')
                await loop.run_in_executor(
                    None,
                    _write_json_file,
                    token_path,
                    {
                        'token': token,
                        'accountTopic': self._account_topic,
                        'accountId': account_id,
                        'clientId': _client_id(email),
                        'ts': now_wall,
                    },
                )
            # Save for on-demand refreshes
            self._token = token
            self._email = email

            # Load endpoint and cert/key from cache if fresh; otherwise fetch
            endpoint: str | None = None
            try:
                if (now_wall - os.stat(cert_path).st_mtime) < ttl and os.path.exists(key_path) and os.path.exists(endpoint_path):
                    txt = await loop.run_in_executor(None, _read_text_file, endpoint_path)
                    endpoint = (txt or '').strip() if txt is not None else None
                    if endpoint:
                        _LOGGER.debug("IoT endpoint/certs loaded from cache: %s", endpoint)
            except Exception:
                endpoint = None

            if not endpoint:
                _LOGGER.debug("Fetching IoT key and endpoint from API")
                iot = await loop.run_in_executor(None, _get_iot_key, token, email)
                endpoint = iot.get("endpoint")
                p12_pass = iot.get("p12Pass") or iot.get("p12_pass")
                key_pem, cert_pem = await loop.run_in_executor(None, _extract_pfx, iot["p12"], p12_pass)
                await loop.run_in_executor(None, _write_bytes_file, cert_path, cert_pem)
                await loop.run_in_executor(None, _write_bytes_file, key_path, key_pem)
                await loop.run_in_executor(None, _write_text_file, endpoint_path, endpoint or '')
                _LOGGER.debug("IoT endpoint/certs saved to cache: %s", endpoint)
        except Exception as ex:
            _LOGGER.warning("Govee IoT login failed: %s", ex)
            return

        # Prepare SSL context from cached files (in executor to avoid blocking loop)
        try:
            loop = asyncio.get_running_loop()
            def _build_ssl_context(ca_path: str, cert_path_in: str, key_path_in: str):
                ctx_local = ssl.create_default_context()
                ctx_local.load_verify_locations(cafile=ca_path)
                ctx_local.load_cert_chain(certfile=cert_path_in, keyfile=key_path_in)
                return ctx_local
            ctx = await loop.run_in_executor(None, _build_ssl_context, certifi.where(), cert_path, key_path)
            _LOGGER.debug("IoT SSL context ready")
        except Exception as ex:
            _LOGGER.warning("Govee IoT SSL context failed: %s", ex)
            return

        client = MqttClient(client_id=f"AP/{account_id}/{uuid.uuid4().hex}")
        client.tls_set_context(ctx)

        conn_event = asyncio.Event()
        iot_state = IoTState(
            hass=self._hass,
            entry_id=self._entry.entry_id,
            hub=self._hub,
            mqtt=client,
            connected_event=conn_event,
            certfile_path=cert_path,
            keyfile_path=key_path,
        )
        self._iot = iot_state

        def on_message(_client, _userdata, msg):
            _LOGGER.debug("RAW MQTT message on %s: %s", msg.topic, msg.payload)
            try:
                payload = msg.payload
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", errors="ignore")

                data = json.loads(payload)
                # Expect GA account messages containing device updates
                if isinstance(data, dict) and data.get("device"):
                    device_id = data.get("device")
                    state = data.get("state") or {}
                    self._schedule_state_update(device_id, state)

                    # Track discovery
                    if isinstance(device_id, str):
                        self._seen_devices.setdefault(device_id, {})
                        if isinstance(state, dict):
                            self._seen_devices[device_id].update(state)

            except Exception as ex:
                _LOGGER.debug("IoT message parse failed: %s", ex)

        def on_connect(_client, _userdata, _flags, rc):
            _LOGGER.info("Govee IoT connected rc=%s", rc)
            if rc != 0:
                _LOGGER.error("Govee IoT connect failed rc=%s", rc)
                return
            try:
                conn_event.set()
            except Exception:
                pass
            topic = self._account_topic
            if topic:
                client.subscribe(topic, qos=0)
                _LOGGER.info("Subscribed to account topic: %s", topic)

        client.on_connect = on_connect
        client.on_message = on_message

        try:
            # Async connect and background loop for lower-latency callbacks
            _LOGGER.debug("Connecting MQTT to %s:8883", endpoint)
            client.connect_async(endpoint, 8883, keepalive=120)
            client.loop_start()
            # Wait briefly for connection (non-fatal timeout)
            try:
                await asyncio.wait_for(conn_event.wait(), timeout=5.0)
                _LOGGER.debug("Govee IoT connected (wait event signaled)")
            except Exception:
                _LOGGER.debug("Govee IoT connect wait timed out; continuing")
        except Exception as ex:
            _LOGGER.warning("Govee IoT connect failed: %s", ex)

        # Build device->topic map using the mobile device list API
        try:
            await loop.run_in_executor(None, self._refresh_device_topics, token, email)
            _LOGGER.info("Loaded %s IoT device topics", len(self._device_topics))
            try:
                # Detailed debug of topics mapping
                _LOGGER.debug("IoT device topics map: %s", self._device_topics)
            except Exception:
                pass
        except Exception as ex:
            _LOGGER.debug("Refresh device topics failed: %s", ex)
        # Final readiness summary
        try:
            _LOGGER.debug(
                "IoT start summary: iot_state=%s mqtt_present=%s account_topic=%s topics=%d",
                bool(self._iot),
                bool(self._iot and self._iot.mqtt),
                bool(self._account_topic),
                len(self._device_topics),
            )
        except Exception:
            pass
        try:
            # Seed state promptly so entities don't rely solely on MQTT pushes
            self._hass.loop.call_soon_threadsafe(self._hass.async_create_task, self._poll_state_fallback())
        except Exception:
            self._hass.async_create_task(self._poll_state_fallback())
        def on_disconnect(_client, _userdata, rc, *, _self=self):
            _LOGGER.warning("Govee IoT disconnected rc=%s", rc)
            if rc != 0:
                loop = _self._loop
                if loop is not None:
                    try:
                        loop.call_soon_threadsafe(
                            lambda: _self._hass.async_create_task(_self._poll_state_fallback())
                        )
                    except Exception:
                        pass

        client.on_disconnect = on_disconnect

        self._ensure_ping_loop()


    def _schedule_state_update(self, device_id: str, state: Dict[str, Any]):
        async def _apply():
            import time as _time
            dev = self._hub._devices.get(device_id)
            if not dev:
                return
            now_wall = _time.time()
            try:
                self._last_seen_wall[device_id] = now_wall
            except Exception:
                pass
            self._clear_pending_status(device_id)
            try:
                dev.timestamp = int(now_wall)
            except Exception:
                pass
            online_flag = None
            if isinstance(state, dict):
                if "deviceOnline" in state:
                    online_flag = state.get("deviceOnline")
                elif "online" in state:
                    online_flag = state.get("online")
                elif "connected" in state:
                    online_flag = state.get("connected")
                elif isinstance(state.get("deviceStatus"), dict):
                    ds = state["deviceStatus"]
                    online_flag = ds.get("online")
                    if online_flag is None:
                        online_flag = ds.get("isOnline")
                    if online_flag is None:
                        online_flag = ds.get("connected")
            if online_flag is not None:
                if isinstance(online_flag, str):
                    dev.online = online_flag.lower() == "true"
                else:
                    dev.online = bool(online_flag)
            else:
                try:
                    dev.online = True
                except Exception:
                    pass
            # onOff
            if "onOff" in state:
                try:
                    dev.power_state = bool(int(state.get("onOff") or 0))
                except Exception:
                    pass
            # brightness 0-100 -> 0-255 (respect pending expectation)
            if "brightness" in state:
                try:
                    gv = int(state.get("brightness") or 0)
                    hb = max(0, min(255, int(round(gv / 100 * 255))))
                    now_mono = __import__("time").monotonic()
                    if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_brightness is not None:
                        if int(hb) == int(dev.pending_brightness):
                            dev.brightness = hb
                            dev.pending_brightness = None
                        else:
                            # ignore stale brightness
                            pass
                    else:
                        dev.brightness = hb
                except Exception:
                    pass
            # Collect potential color/ct values first
            new_color = None
            new_ct = None
            if "color" in state and isinstance(state["color"], dict):
                c = state["color"]
                new_color = (int(c.get("r", 0)), int(c.get("g", 0)), int(c.get("b", 0)))
            if "colorTemInKelvin" in state:
                try:
                    if not getattr(dev, "support_color_temp", False):
                        dev.support_color_temp = True
                    if getattr(dev, "color_temp_min", None) is None:
                        dev.color_temp_min = COLOR_TEMP_KELVIN_MIN
                    if getattr(dev, "color_temp_max", None) is None:
                        dev.color_temp_max = COLOR_TEMP_KELVIN_MAX
                except Exception:
                    pass
                try:
                    new_ct = int(state.get("colorTemInKelvin") or 0)
                except Exception:
                    new_ct = None

            # Apply with mutual exclusivity, preferring RGB when present
            import time as _t
            now_mono = _t.monotonic()
            if new_color and any(new_color):
                # Respect pending color expectation if present
                if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_color is not None:
                    if tuple(new_color) == tuple(dev.pending_color):
                        dev.color = new_color
                        dev.color_temp = 0
                        dev.pending_color = None
                    else:
                        # ignore stale color
                        pass
                else:
                    dev.color = new_color
                    dev.color_temp = 0
            elif new_ct and new_ct > 0:
                # Respect pending CT expectation if present
                if now_mono < getattr(dev, "pending_until", 0.0) and dev.pending_ct is not None:
                    if int(new_ct) == int(dev.pending_ct):
                        dev.color_temp = int(new_ct)
                        dev.color = (0, 0, 0)
                        dev.pending_ct = None
                    else:
                        # ignore stale CT
                        pass
                else:
                    dev.color_temp = int(new_ct)
                    dev.color = (0, 0, 0)
            # Push into coordinator if present
            entry_data = self._hass.data.get(DOMAIN, {}).get(self._entry.entry_id)
            if entry_data and "coordinator" in entry_data:
                coord = entry_data["coordinator"]
                coord.async_set_updated_data(list(self._hub._devices.values()))

            # Optional fast reconcile: fetch full state shortly after GA update
            # to ensure UI reflects any backend-only fields. Throttle per device.
            import time as _t
            now = _t.time()
            last = self._last_reconcile.get(device_id, 0.0)
            if now - last > 5.0:
                self._last_reconcile[device_id] = now
                async def _reconcile():
                    try:
                        await asyncio.sleep(0.6)
                        await self._hub.get_device_state(device_id)
                        # propagate again
                        if entry_data and "coordinator" in entry_data:
                            entry_data["coordinator"].async_set_updated_data(list(self._hub._devices.values()))
                    except Exception:
                        pass
                self._hass.async_create_task(_reconcile())

        # Schedule coroutine on HA's event loop thread-safely
        try:
            self._hass.loop.call_soon_threadsafe(self._hass.async_create_task, _apply())
        except Exception:
            # Fallback in case loop call fails (shouldn't on HA)
            asyncio.run_coroutine_threadsafe(_apply(), self._hass.loop)

    def _ensure_ping_loop(self) -> None:
        if self._ping_task and not self._ping_task.done():
            return
        try:
            creator = getattr(self._hass, "async_create_background_task", None)
            if creator:
                self._ping_task = creator(self._status_ping_loop(), name="govee_status_ping")
            else:
                self._ping_task = self._hass.async_create_task(self._status_ping_loop())
        except Exception:
            loop = asyncio.get_running_loop()
            self._ping_task = loop.create_task(self._status_ping_loop())

    async def _status_ping_loop(self):
        try:
            await asyncio.sleep(10.0)
            while True:
                try:
                    targets = list(self._device_topics.keys())
                except Exception:
                    targets = []
                if targets:
                    devices_map = getattr(self._hub, "_devices", {}) if self._hub else {}
                    now_wall = time.time()
                    for dev_id in targets:
                        if dev_id in self._pending_status:
                            continue
                        miss_count = self._miss_counts.get(dev_id, 0)
                        dev_obj = devices_map.get(dev_id) if isinstance(devices_map, dict) else None
                        is_online = bool(getattr(dev_obj, "online", False)) if dev_obj else False
                        last_seen = self._last_seen_wall.get(dev_id)
                        stale = False
                        try:
                            if last_seen is None:
                                stale = True
                            else:
                                stale = (now_wall - float(last_seen)) >= self._ping_interval
                        except Exception:
                            stale = True
                        should_request = (
                            miss_count > 0
                            or not is_online
                            or stale
                        )
                        if not should_request:
                            continue
                        try:
                            await self.async_request_status(dev_id)
                        except Exception as ex:
                            _LOGGER.debug("Periodic status request failed for %s: %s", dev_id, ex)
                        await asyncio.sleep(0.1)
                await asyncio.sleep(self._ping_interval)
        except asyncio.CancelledError:
            return

    async def _status_timeout(self, device_id: str):
        try:
            await asyncio.sleep(self._status_grace)
        except asyncio.CancelledError:
            return
        if device_id not in self._pending_status:
            return
        self._pending_status.pop(device_id, None)
        self._status_timers.pop(device_id, None)
        misses = self._miss_counts.get(device_id, 0) + 1
        self._miss_counts[device_id] = misses
        if misses >= self._status_miss_threshold:
            self._mark_device_offline(device_id)
            return
        try_alt = self._should_try_alt_status(device_id, misses)
        self._hass.async_create_task(self.async_request_status(device_id, use_alt=try_alt))

    def _should_try_alt_status(self, device_id: str, misses: int) -> bool:
        if misses <= 0:
            return False
        dev = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
        model = (getattr(dev, "model", "") or "").upper()
        if model in self._alt_status_models:
            return True
        return misses >= 2

    def _build_status_payload(self, device_id: str, *, use_alt: bool = False) -> dict[str, Any]:
        msg = {
            "cmd": "status",
            "cmdVersion": 1 if use_alt else 2,
            "transaction": f"v_{_ms_ts()}000",
            "type": 0,
        }
        if self._account_topic:
            msg["accountTopic"] = self._account_topic
        return {"msg": msg}

    async def stop(self):
        if self._iot and self._iot.mqtt:
            try:
                self._iot.mqtt.disconnect()
            except Exception:
                pass
        # Clean up temporary cert/key files
        if self._iot:
            for p in (self._iot.certfile_path, self._iot.keyfile_path):
                if p:
                    try:
                        os.unlink(p)
                    except Exception:
                        pass

        if self._ping_task:
            task = self._ping_task
            self._ping_task = None
            try:
                task.cancel()
            except Exception:
                pass
            with contextlib.suppress(asyncio.CancelledError):
                await task

        for dev_id, timer in list(self._status_timers.items()):
            try:
                if timer and not timer.done():
                    timer.cancel()
            except Exception:
                pass
        self._status_timers.clear()

        try:
            self._pending_status.clear()
        except Exception:
            pass
        try:
            self._miss_counts.clear()
        except Exception:
            pass

        self._loop = None

    def _refresh_device_topics(self, token: str, email: str):
        # Prefer cached devices.json if fresh (15-day TTL)
        try:
            now_wall = time.time()
            ttl = 15 * 24 * 60 * 60  # 15 days
            cache_dir = self._hass.config.path('.storage/govee_iot')
            os.makedirs(cache_dir, exist_ok=True)
            path = os.path.join(cache_dir, 'devices.json')
            if os.path.exists(path) and (now_wall - os.stat(path).st_mtime) < ttl:
                try:
                    mapping = __import__('json').load(open(path, 'r', encoding='utf-8'))
                    if isinstance(mapping, dict) and mapping:
                        self._device_topics = {k: v for k, v in mapping.items() if isinstance(v, str)}
                        _LOGGER.debug("Using cached IoT device topics (%s entries)", len(self._device_topics))
                        return
                except Exception:
                    pass
        except Exception:
            pass

        resp = requests.post(
            "https://app2.govee.com/device/rest/devices/v1/list",
            headers={
                "Authorization": f"Bearer {token}",
                "appVersion": APP_VERSION,
                "clientId": _client_id(email),
                "clientType": "1",
                "iotVersion": "0",
                "timestamp": _ms_ts(),
                "User-Agent": _ua(),
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        devices = []
        if isinstance(data, dict):
            if isinstance(data.get("devices"), list):
                devices = data["devices"]
            elif isinstance(data.get("data"), dict) and isinstance(data["data"].get("devices"), list):
                devices = data["data"]["devices"]
        mapping: dict[str, str] = {}
        for d in devices:
            dev_id = d.get("device") or d.get("deviceId") or ""
            ext = d.get("deviceExt") or d.get("device_ext")
            # Some payloads embed JSON strings
            try:
                if isinstance(ext, str) and ext.strip().startswith("{"):
                    ext = __import__("json").loads(ext)
            except Exception:
                pass
            topic = None
            if isinstance(ext, dict):
                ds = ext.get("deviceSettings") or ext.get("device_settings")
                try:
                    if isinstance(ds, str) and ds.strip().startswith("{"):
                        ds = __import__("json").loads(ds)
                except Exception:
                    pass
                if isinstance(ds, dict):
                    t = ds.get("topic")
                    if isinstance(t, dict) and "value" in t:
                        topic = t["value"]
                    elif isinstance(t, str):
                        topic = t
            if dev_id and topic:
                mapping[dev_id] = topic
        self._device_topics = mapping
        # Persist to disk for reuse (15 days)
        try:
            cache_dir = self._hass.config.path('.storage/govee_iot')
            os.makedirs(cache_dir, exist_ok=True)
            path = os.path.join(cache_dir, 'devices.json')
            __import__('json').dump(mapping, open(path, 'w', encoding='utf-8'))
        except Exception:
            pass
        self._ensure_ping_loop()

    @property
    def can_control(self) -> bool:
        # Only allow IoT control when MQTT is ready and publish permissions are enabled.
        return bool(self._allow_publish and self._iot and self._iot.mqtt)

    def get_topics(self) -> dict[str, str]:
        """Return a copy of the device->topic mapping for diagnostics."""
        try:
            return dict(self._device_topics)
        except Exception:
            return {}

    def get_known_devices(self) -> dict[str, dict]:
        """Return mapping of discovered device_id -> last seen state (if any)."""
        try:
            return dict(self._seen_devices)
        except Exception:
            return {}

    def mark_seen(self, device_id: str, when: float | None = None, *, online: bool | None = None) -> None:
        try:
            if online is False:
                self._last_seen_wall.pop(device_id, None)
            else:
                self._last_seen_wall[device_id] = float(when) if when is not None else time.time()
        except Exception:
            pass

    def _cancel_status_timer(self, device_id: str) -> None:
        task = self._status_timers.pop(device_id, None)
        if task and not task.done():
            task.cancel()

    def _clear_pending_status(self, device_id: str, *, reset_miss: bool = True) -> None:
        self._cancel_status_timer(device_id)
        try:
            self._pending_status.pop(device_id, None)
        except Exception:
            pass
        if reset_miss:
            self._miss_counts[device_id] = 0

    def _mark_device_offline(self, device_id: str) -> bool:
        dev_local = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
        updated = False
        if dev_local is not None and getattr(dev_local, "online", True):
            dev_local.online = False
            updated = True
        try:
            self._last_seen_wall.pop(device_id, None)
        except Exception:
            pass
        self._clear_pending_status(device_id, reset_miss=False)
        if not updated:
            return False
        self._miss_counts[device_id] = self._status_miss_threshold
        try:
            entry_bucket = self._hass.data.get(DOMAIN, {}).get(self._entry.entry_id)
            coord = entry_bucket.get("coordinator") if entry_bucket else None
            if coord and self._hub:
                coord.async_set_updated_data(list(self._hub._devices.values()))
        except Exception:
            pass
        return True

    async def async_broadcast_status_request(self) -> bool:
        """Seed state using REST fallback when a broadcast is requested."""

        try:
            if await self._poll_state_fallback():
                _LOGGER.debug("IoT broadcast status satisfied via REST fallback")
                return True
        except Exception as ex:
            _LOGGER.debug("IoT broadcast status fallback failed: %s", ex)
        return False


    async def async_publish_control(self, device_id: str, command: str, value: Any) -> tuple[bool, str | None]:
        if not self.can_control:
            # Be explicit about what's missing
            if not self._iot:
                _LOGGER.debug("IoT publish blocked: IoT state not initialized")
            elif not self._iot.mqtt:
                _LOGGER.debug("IoT publish blocked: MQTT client missing")
            elif not self._allow_publish:
                _LOGGER.debug("IoT publish blocked: MQTT credentials are read-only")
            else:
                _LOGGER.debug("IoT publish blocked: MQTT not ready")
            return False, "iot_not_ready"
        topic = self._device_topics.get(device_id)
        if not topic:
            # Try reading from cached devices.json only; do not hit mobile API
            try:
                loop = asyncio.get_running_loop()
                def _read_cached_map(path: str) -> dict[str, str] | None:
                    try:
                        txt = open(path, 'r', encoding='utf-8').read()
                        return __import__('json').loads(txt)
                    except Exception:
                        return None
                cache_dir = self._hass.config.path('.storage/govee_iot')
                path = __import__('os').path.join(cache_dir, 'devices.json')
                mapping = await loop.run_in_executor(None, _read_cached_map, path)
                if isinstance(mapping, dict):
                    self._device_topics.update({k: v for k, v in mapping.items() if isinstance(v, str)})
                    topic = self._device_topics.get(device_id)
            except Exception as ex:
                _LOGGER.debug("IoT cached topic load failed: %s", ex)
            if not topic:
                _LOGGER.debug("IoT publish blocked: No topic for %s", device_id)
                return False, "no_topic"
        # Enforce global publish rate limit (20/min default)
        wait = self._pub_bucket_take(1.0)
        if wait > 0:
            try:
                _LOGGER.debug(
                    "IoT publish throttled (control %s): waiting %.2fs (tokens=%.2f cap=%s)",
                    command,
                    wait,
                    float(self._pub_bucket_tokens),
                    int(self._pub_bucket_capacity),
                )
            except Exception:
                pass
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                return False, "throttled"

        # Build app-like envelope
        msg: dict[str, Any] = {
            "cmd": None,
            "data": None,
            "cmdVersion": 1,
            "transaction": f"v_{_ms_ts()}000",
            "type": 1,
        }
        if self._account_topic:
            msg["accountTopic"] = self._account_topic

        def _current_onoff() -> int | None:
            state = self._seen_devices.get(device_id, {})
            val = state.get("onOff")
            if val is None:
                val = state.get("powerState")
            if isinstance(val, str):
                normalized = val.strip().lower()
                if normalized in ("on", "true", "1"):
                    return 1
                if normalized in ("off", "false", "0"):
                    return 0
                try:
                    return 1 if int(normalized) else 0
                except Exception:
                    return None
            if isinstance(val, (int, float)):
                try:
                    return 1 if int(val) else 0
                except Exception:
                    return None
            return None

        async def _confirm_power(expected: int, timeout: float = 5.0) -> bool:
            import time as _t
            deadline = _t.monotonic() + max(0.2, float(timeout))
            while _t.monotonic() < deadline:
                current = _current_onoff()
                if current is not None and current == expected:
                    return True
                try:
                    await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    return False
            return False

        def _current_brightness() -> int | None:
            state = self._seen_devices.get(device_id, {})
            val = state.get("brightness")
            if val is None:
                val = state.get("bright")
            if isinstance(val, str):
                try:
                    return int(float(val))
                except Exception:
                    return None
            if isinstance(val, (int, float)):
                try:
                    return int(val)
                except Exception:
                    return None
            return None

        async def _confirm_brightness(expected: int, timeout: float = 5.0) -> bool:
            import time as _t
            deadline = _t.monotonic() + max(0.2, float(timeout))
            while _t.monotonic() < deadline:
                current = _current_brightness()
                if current is not None and int(current) == int(expected):
                    return True
                try:
                    await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    return False
            return False

        if command == "turn":
            target = 1 if str(value).lower() == "on" else 0
            msg["cmd"] = "turn"
            msg["data"] = {"val": target}
            payload = {"msg": msg}
            if not self._publish(topic, payload):
                return False, "publish_failed"
            if await _confirm_power(target):
                return True, None
            try:
                _LOGGER.debug("IoT turn confirm failed for %s target=%s; falling back", device_id, target)
            except Exception:
                pass
            has_api = bool(getattr(self._hub, "_api_key", None))
            last_seen_before_fallback = self._last_seen_wall.get(device_id)
            fallback_ok: bool | None = None
            if has_api:
                try:
                    fallback_ok = await self._poll_state_fallback(device_id)
                except Exception as ex:
                    _LOGGER.debug("IoT turn fallback poll failed for %s: %s", device_id, ex)
                    fallback_ok = False
            dev_local = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            recent_seen = False
            if last_seen_before_fallback is not None:
                try:
                    recent_seen = (time.time() - float(last_seen_before_fallback)) < 60.0
                except Exception:
                    pass
            if recent_seen:
                try:
                    _LOGGER.debug("Skipping offline mark for %s; recent IoT activity", device_id)
                except Exception:
                    pass
            elif (
                fallback_ok is False
                or (fallback_ok and dev_local and getattr(dev_local, "online", True) is False)
                or (fallback_ok is None and not has_api)
            ):
                self._mark_device_offline(device_id)
            return False, "iot_no_confirm"
        if command == "brightness":
            try:
                target = int(value)
            except Exception:
                try:
                    target = int(float(value))
                except Exception:
                    target = 0
            target = max(0, min(100, target))
            msg["cmd"] = "brightness"
            msg["data"] = {"val": target}
            payload = {"msg": msg}
            if not self._publish(topic, payload):
                return False, "publish_failed"
            if await _confirm_brightness(target):
                return True, None
            try:
                _LOGGER.debug("IoT brightness confirm failed for %s → %s; falling back", device_id, target)
            except Exception:
                pass
            has_api = bool(getattr(self._hub, "_api_key", None))
            last_seen_before_fallback = self._last_seen_wall.get(device_id)
            fallback_ok: bool | None = None
            if has_api:
                try:
                    fallback_ok = await self._poll_state_fallback(device_id)
                except Exception as ex:
                    _LOGGER.debug("IoT brightness fallback poll failed for %s: %s", device_id, ex)
                    fallback_ok = False
            dev_local = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            recent_seen = False
            if last_seen_before_fallback is not None:
                try:
                    recent_seen = (time.time() - float(last_seen_before_fallback)) < 60.0
                except Exception:
                    pass
            if recent_seen:
                try:
                    _LOGGER.debug("Skipping offline mark for %s; recent IoT activity", device_id)
                except Exception:
                    pass
            elif (
                fallback_ok is False
                or (fallback_ok and dev_local and getattr(dev_local, "online", True) is False)
                or (fallback_ok is None and not has_api)
            ):
                self._mark_device_offline(device_id)
            return False, "iot_no_confirm"
        if command == "color":
            r = int(value.get("r", 0))
            g = int(value.get("g", 0))
            b = int(value.get("b", 0))
            dev = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            use_wc = getattr(dev, "color_cmd_use_colorwc", None)

            # Drop exact duplicate within 1s to avoid churn while dragging the color wheel
            try:
                now_mono = time.monotonic()
                last = self._last_color_sent.get(device_id)
                if last and last[0] == (r, g, b) and (now_mono - last[1]) < 1.0:
                    _LOGGER.debug("IoT suppress duplicate color publish for %s rgb=%s", device_id, (r, g, b))
                    return True, None
            except Exception:
                pass

            # Helper to read last seen RGB
            def _last_rgb():
                state = self._seen_devices.get(device_id, {}).get("color")
                if isinstance(state, dict):
                    return (
                        int(state.get("r", 0)),
                        int(state.get("g", 0)),
                        int(state.get("b", 0)),
                    )
                if isinstance(state, list) and len(state) >= 3:
                    return tuple(int(c) for c in state[:3])
                return None

            async def _confirm_and_persist(using_wc: bool, timeout: float = 5.0) -> bool:
                import time as _t
                deadline = _t.monotonic() + max(0.2, float(timeout))
                try:
                    _LOGGER.debug(
                        "IoT confirm window start (%s) %.1fs for %s rgb=%s",
                        "colorwc" if using_wc else "color",
                        float(timeout),
                        device_id,
                        (r, g, b),
                    )
                except Exception:
                    pass
                while _t.monotonic() < deadline:
                    try:
                        if dev is not None and dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT confirm aborted (superseded) for %s old=%s new=%s", device_id, (r, g, b), dev.pending_color)
                            return False
                    except Exception:
                        pass
                    rgb_state = _last_rgb()
                    if rgb_state == (r, g, b):
                        if dev is not None:
                            dev.color_cmd_use_colorwc = using_wc
                        try:
                            _LOGGER.debug(
                                "IoT confirmed via %s for %s rgb=%s",
                                "colorwc" if using_wc else "color",
                                device_id,
                                (r, g, b),
                            )
                        except Exception:
                            pass
                        try:
                            self._last_color_sent[device_id] = ((r, g, b), _t.monotonic())
                        except Exception:
                            pass
                        return True
                    try:
                        await asyncio.sleep(0.2)
                    except asyncio.CancelledError:
                        return True
                try:
                    _LOGGER.debug(
                        "IoT confirm timed out via %s for %s after %.1fs; last=%s",
                        "colorwc" if using_wc else "color",
                        device_id,
                        float(timeout),
                        _last_rgb(),
                    )
                except Exception:
                    pass
                return False

            prefer_wc = (use_wc is not False)
            confirmed = False
            if prefer_wc:
                import time as _t
                wc_deadline = _t.monotonic() + 5.0
                msg["cmd"] = "colorwc"
                msg["data"] = {"color": {"r": r, "g": g, "b": b}, "colorTemInKelvin": 0}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False, "publish_failed"
                confirmed = await _confirm_and_persist(True, 5.0)
                if not confirmed and dev is not None:
                    try:
                        nowm = _t.monotonic()
                        if nowm < wc_deadline:
                            _LOGGER.debug(
                                "IoT skip legacy fallback (still within 5s window) for %s rgb=%s",
                                device_id,
                                (r, g, b),
                            )
                            return True, None
                    except Exception:
                        pass
                    try:
                        if dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT skip fallback legacy color; superseded %s -> %s", (r, g, b), dev.pending_color)
                            return True, None
                    except Exception:
                        pass
                    msg2 = {
                        "cmd": "color",
                        "data": {"r": r, "g": g, "b": b},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    if self._publish(topic, payload2):
                        confirmed = await _confirm_and_persist(False, 5.0)
                    else:
                        return False, "publish_failed"
            else:
                import time as _t
                legacy_deadline = _t.monotonic() + 5.0
                msg["cmd"] = "color"
                msg["data"] = {"r": r, "g": g, "b": b}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False, "publish_failed"
                confirmed = await _confirm_and_persist(False, 5.0)
                if not confirmed and dev is not None:
                    try:
                        nowm = _t.monotonic()
                        if nowm < legacy_deadline:
                            _LOGGER.debug(
                                "IoT skip colorwc fallback (still within 5s window) for %s rgb=%s",
                                device_id,
                                (r, g, b),
                            )
                            return True, None
                    except Exception:
                        pass
                    try:
                        if dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT skip fallback colorwc; superseded %s -> %s", (r, g, b), dev.pending_color)
                            return True, None
                    except Exception:
                        pass
                    msg2 = {
                        "cmd": "colorwc",
                        "data": {"color": {"r": r, "g": g, "b": b}, "colorTemInKelvin": 0},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    if self._publish(topic, payload2):
                        confirmed = await _confirm_and_persist(True, 5.0)
                    else:
                        return False, "publish_failed"
            if confirmed:
                return True, None
            try:
                _LOGGER.debug("IoT color confirm failed for %s rgb=%s; scheduling REST fallback", device_id, (r, g, b))
            except Exception:
                pass
            has_api = bool(getattr(self._hub, "_api_key", None))
            last_seen_before_fallback = self._last_seen_wall.get(device_id)
            fallback_ok: bool | None = None
            if has_api:
                try:
                    fallback_ok = await self._poll_state_fallback(device_id)
                except Exception as ex:
                    _LOGGER.debug("IoT color fallback poll failed for %s: %s", device_id, ex)
                    fallback_ok = False
            dev_local = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            recent_seen = False
            if last_seen_before_fallback is not None:
                try:
                    recent_seen = (time.time() - float(last_seen_before_fallback)) < 60.0
                except Exception:
                    pass
            if recent_seen:
                try:
                    _LOGGER.debug("Skipping offline mark for %s; recent IoT activity", device_id)
                except Exception:
                    pass
            elif (
                fallback_ok is False
                or (fallback_ok and dev_local and getattr(dev_local, "online", True) is False)
                or (fallback_ok is None and not has_api)
            ):
                self._mark_device_offline(device_id)
            return False, "iot_no_confirm"
        command_l = command.lower()
        if command_l in {"colortem", "colortemperature", "colortemperaturek", "colorteminkelvin"}:
            dev = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            send_percent = getattr(dev, "color_temp_send_percent", None)
            kelvin = int(value)

            # Non-legacy command names imply Kelvin payloads; prefer colorwc path.
            force_kelvin = command_l != "colortem"

            async def _confirm_kelvin_and_persist(using_percent: bool, timeout: float = 5.0) -> bool:
                import time as _t
                deadline = _t.monotonic() + max(0.2, float(timeout))
                while _t.monotonic() < deadline:
                    try:
                        if dev is not None and dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT confirm CT aborted (superseded) for %s old=%s new=%s", device_id, kelvin, dev.pending_ct)
                            return False
                    except Exception:
                        pass
                    v = self._seen_devices.get(device_id, {}).get("colorTemInKelvin")
                    if isinstance(v, (int, float)) and int(v) == kelvin:
                        if dev is not None:
                            dev.color_temp_send_percent = using_percent
                        try:
                            self._last_ct_sent[device_id] = (int(kelvin), _t.monotonic())
                        except Exception:
                            pass
                        return True
                    try:
                        await asyncio.sleep(0.2)
                    except asyncio.CancelledError:
                        return True
                return False

            prefer_kelvin_via_wc = force_kelvin or (send_percent is not True)
            confirmed = False
            if prefer_kelvin_via_wc:
                msg["cmd"] = "colorwc"
                msg["data"] = {"colorTemInKelvin": kelvin}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False, "publish_failed"
                confirmed = await _confirm_kelvin_and_persist(False, 5.0)
                if not confirmed and dev is not None:
                    try:
                        if dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT skip percent fallback; CT superseded %s -> %s", kelvin, dev.pending_ct)
                            return True, None
                    except Exception:
                        pass
                    vmin = dev.color_temp_min or 2700
                    vmax = dev.color_temp_max or 9000
                    rng = max(1, vmax - vmin)
                    percent = int(round((kelvin - vmin) / rng * 100))
                    percent = max(0, min(100, percent))
                    msg2 = {
                        "cmd": "colorTem",
                        "data": {"colorTem": percent},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    if self._publish(topic, payload2):
                        confirmed = await _confirm_kelvin_and_persist(True, 5.0)
                    else:
                        return False, "publish_failed"
            else:
                vmin = 2700
                vmax = 9000
                if dev is not None:
                    try:
                        vmin = int(getattr(dev, "color_temp_min", None) or vmin)
                        vmax = int(getattr(dev, "color_temp_max", None) or vmax)
                    except Exception:
                        pass
                rng = max(1, vmax - vmin)
                percent = int(round((kelvin - vmin) / rng * 100))
                percent = max(0, min(100, percent))
                msg["cmd"] = "colorTem"
                msg["data"] = {"colorTem": percent}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False, "publish_failed"
                confirmed = await _confirm_kelvin_and_persist(True, 5.0)
                if not confirmed and dev is not None:
                    try:
                        if dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT skip Kelvin fallback; CT superseded %s -> %s", kelvin, dev.pending_ct)
                            return True, None
                    except Exception:
                        pass
                    msg2 = {
                        "cmd": "colorwc",
                        "data": {"colorTemInKelvin": kelvin},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    if self._publish(topic, payload2):
                        confirmed = await _confirm_kelvin_and_persist(False, 5.0)
                    else:
                        return False, "publish_failed"
            if confirmed:
                return True, None
            try:
                _LOGGER.debug("IoT color temperature confirm failed for %s → %sK; scheduling REST fallback", device_id, kelvin)
            except Exception:
                pass
            has_api = bool(getattr(self._hub, "_api_key", None))
            last_seen_before_fallback = self._last_seen_wall.get(device_id)
            fallback_ok: bool | None = None
            if has_api:
                try:
                    fallback_ok = await self._poll_state_fallback(device_id)
                except Exception as ex:
                    _LOGGER.debug("IoT color temperature fallback poll failed for %s: %s", device_id, ex)
                    fallback_ok = False
            dev_local = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            recent_seen = False
            if last_seen_before_fallback is not None:
                try:
                    recent_seen = (time.time() - float(last_seen_before_fallback)) < 60.0
                except Exception:
                    pass
            if recent_seen:
                try:
                    _LOGGER.debug("Skipping offline mark for %s; recent IoT activity", device_id)
                except Exception:
                    pass
            elif (
                fallback_ok is False
                or (fallback_ok and dev_local and getattr(dev_local, "online", True) is False)
                or (fallback_ok is None and not has_api)
            ):
                self._mark_device_offline(device_id)
            return False, "iot_no_confirm"
        return False, "unsupported_command"

    async def async_request_status(self, device_id: str, *, use_alt: bool | None = None) -> bool:
        """Request device status over MQTT, falling back to REST if needed."""

        self._ensure_ping_loop()
        if not self.can_control:
            return await self._poll_state_fallback(device_id)
        topic = self._device_topics.get(device_id)
        if not topic:
            _LOGGER.debug("IoT status request skipped: no topic for %s", device_id)
            return await self._poll_state_fallback(device_id)
        wait = self._pub_bucket_take(1.0)
        if wait > 0:
            _LOGGER.debug("IoT publish throttled (status %s): waiting %.2fs", device_id, wait)
            try:
                await asyncio.sleep(wait)
            except asyncio.CancelledError:
                return False

        misses_before = self._miss_counts.get(device_id, 0)
        alt_flag = bool(use_alt) if use_alt is not None else self._should_try_alt_status(device_id, misses_before)
        payload = self._build_status_payload(device_id, use_alt=alt_flag)
        if alt_flag:
            _LOGGER.debug("IoT status request (alt) → %s", device_id)
        try:
            if not self._publish(topic, payload, log_payload=False):
                raise RuntimeError("publish returned rc!=0")
        except Exception as ex:
            _LOGGER.debug("IoT status publish failed: %s", ex)
            return await self._poll_state_fallback(device_id)

        self._clear_pending_status(device_id, reset_miss=False)
        self._pending_status[device_id] = time.monotonic() + self._status_grace
        self._miss_counts.setdefault(device_id, 0)
        try:
            creator = getattr(self._hass, "async_create_background_task", None)
            if creator:
                self._status_timers[device_id] = creator(self._status_timeout(device_id), name=f"govee_status_timeout_{device_id}")
            else:
                self._status_timers[device_id] = self._hass.async_create_task(self._status_timeout(device_id))
        except Exception:
            loop = asyncio.get_running_loop()
            self._status_timers[device_id] = loop.create_task(self._status_timeout(device_id))
        return True
