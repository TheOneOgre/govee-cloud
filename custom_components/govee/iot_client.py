"""Govee IoT (AWS MQTT) read-only client for push state updates."""
from __future__ import annotations

import asyncio
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
from paho.mqtt.client import Client as MqttClient

from .const import DOMAIN, CONF_IOT_EMAIL, CONF_IOT_PASSWORD, CONF_IOT_PUSH_ENABLED


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

        # Global publish token bucket (max publishes per minute across all devices)
        # Default: 20/min
        self._pub_bucket_capacity: float = 20.0
        self._pub_bucket_refill_per_sec: float = self._pub_bucket_capacity / 60.0
        self._pub_bucket_tokens: float = self._pub_bucket_capacity
        self._pub_bucket_last: float = time.monotonic()
        # Simple duplicate suppression for IoT publishes
        self._last_color_sent: dict[str, tuple[tuple[int,int,int], float]] = {}
        self._last_ct_sent: dict[str, tuple[int, float]] = {}

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

    def _publish(self, topic: str, payload: dict) -> bool:
        try:
            js = __import__('json').dumps(payload, separators=(',', ':'))
            try:
                _LOGGER.debug("IoT publish topic=%s payload=%s", topic, js)
            except Exception:
                pass
            self._iot.mqtt.publish(topic, js, qos=0, retain=False)
            return True
        except Exception as ex:
            _LOGGER.debug("IoT publish failed: %s", ex)
            return False

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
            try:
                payload = msg.payload.decode("utf-8", errors="ignore")
                data = json.loads(payload)
                # Expect GA account messages containing device updates
                if isinstance(data, dict) and data.get("device"):
                    device_id = data.get("device")
                    state = data.get("state") or {}
                    self._schedule_state_update(device_id, state)
                    # Track discovery
                    try:
                        if isinstance(device_id, str):
                            self._seen_devices.setdefault(device_id, {})
                            if isinstance(state, dict):
                                self._seen_devices[device_id].update(state)
                    except Exception:
                        pass
            except Exception as ex:
                _LOGGER.debug("IoT message parse failed: %s", ex)

        def on_connect(_client, _userdata, _flags, rc):
            _LOGGER.info("Govee IoT connected rc=%s", rc)
            try:
                conn_event.set()
            except Exception:
                pass
            topic = self._account_topic
            if topic:
                client.subscribe(topic, qos=0)
                _LOGGER.info("Subscribed to account topic: %s", topic)
            # Also subscribe to known device topics to catch direct device state messages
            try:
                for dev_topic in list(self._device_topics.values()):
                    try:
                        client.subscribe(dev_topic, qos=0)
                    except Exception:
                        pass
                if self._device_topics:
                    _LOGGER.debug("Subscribed to %d device topics", len(self._device_topics))
            except Exception:
                pass

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

        # Build device→topic map using the mobile device list API
        try:
            await loop.run_in_executor(None, self._refresh_device_topics, token, email)
            _LOGGER.info("Loaded %s IoT device topics", len(self._device_topics))
            try:
                # Detailed debug of topics mapping
                _LOGGER.debug("IoT device topics map: %s", self._device_topics)
            except Exception:
                pass
            # Subscribe to device topics after mapping refresh
            try:
                for dev_topic in list(self._device_topics.values()):
                    try:
                        client.subscribe(dev_topic, qos=0)
                    except Exception:
                        pass
                if self._device_topics:
                    _LOGGER.debug("Subscribed to %d device topics (post-refresh)", len(self._device_topics))
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

    def _schedule_state_update(self, device_id: str, state: Dict[str, Any]):
        async def _apply():
            dev = self._hub._devices.get(device_id)
            if not dev:
                return
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

    @property
    def can_control(self) -> bool:
        # Consider control possible as soon as MQTT client exists; device topic and
        # account topic are handled on-demand in publish.
        return bool(self._iot and self._iot.mqtt)

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

    async def async_broadcast_status_request(self) -> bool:
        """Request statuses by publishing a status message to the account topic.

        Not all firmwares listen on account topic for status requests; best-effort.
        """
        try:
            if not self._iot or not self._iot.mqtt:
                return False
            topic = self._account_topic
            if not topic:
                return False
            # Do not consume control bucket for status broadcast
            msg = {
                "cmd": "status",
                "cmdVersion": 2,
                "transaction": f"v_{_ms_ts()}000",
                "type": 0,
            }
            payload = {"msg": msg}
            js = __import__('json').dumps(payload, separators=(',', ':'))
            try:
                _LOGGER.debug("IoT broadcast status on account topic=%s payload=%s", topic, js)
            except Exception:
                pass
            self._iot.mqtt.publish(topic, js, qos=0, retain=False)
            return True
        except Exception as ex:
            _LOGGER.debug("IoT broadcast status failed: %s", ex)
            return False

    async def async_publish_control(self, device_id: str, command: str, value: Any) -> bool:
        if not self.can_control:
            # Be explicit about what's missing
            if not self._iot:
                _LOGGER.debug("IoT publish blocked: IoT state not initialized")
            elif not self._iot.mqtt:
                _LOGGER.debug("IoT publish blocked: MQTT client missing")
            else:
                _LOGGER.debug("IoT publish blocked: MQTT not ready")
            return False
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
                return False
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
                return False

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
        if command == "turn":
            msg["cmd"] = "turn"
            msg["data"] = {"val": 1 if str(value).lower() == "on" else 0}
            payload = {"msg": msg}
            return self._publish(topic, payload)
        if command == "brightness":
            msg["cmd"] = "brightness"
            msg["data"] = {"val": int(value)}
            payload = {"msg": msg}
            return self._publish(topic, payload)
        if command == "color":
            r = int(value.get("r", 0))
            g = int(value.get("g", 0))
            b = int(value.get("b", 0))
            dev = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            use_wc = getattr(dev, "color_cmd_use_colorwc", None)

            # Drop exact duplicate within 1s to avoid churn while dragging the color wheel
            try:
                now_mono = __import__('time').monotonic()
                last = self._last_color_sent.get(device_id)
                if last and last[0] == (r, g, b) and (now_mono - last[1]) < 1.0:
                    _LOGGER.debug("IoT suppress duplicate color publish for %s rgb=%s", device_id, (r, g, b))
                    return True
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
                # Poll quickly for confirmation; break early when matched
                while _t.monotonic() < deadline:
                    # Abort if a newer pending color supersedes this target
                    try:
                        if dev is not None and dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT confirm aborted (superseded) for %s old=%s new=%s", device_id, (r,g,b), dev.pending_color)
                            return False
                    except Exception:
                        pass
                    rgb_state = _last_rgb()
                    if rgb_state == (r, g, b):
                        if dev is not None:
                            # Memory-only: set in-device flag but do not persist to disk
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

            # Choose preferred based on learned flag (default to colorwc)
            prefer_wc = (use_wc is not False)
            # First attempt
            if prefer_wc:
                import time as _t
                wc_deadline = _t.monotonic() + 5.0
                msg["cmd"] = "colorwc"
                # Include colorTemInKelvin=0 in color mode
                msg["data"] = {"color": {"r": r, "g": g, "b": b}, "colorTemInKelvin": 0}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False
                if dev is not None:
                    if await _confirm_and_persist(True, 5.0):
                        return True
                    # Fallback attempt to legacy color
                    # Hard guard: never fallback before full 5s window elapses
                    try:
                        nowm = _t.monotonic()
                        if nowm < wc_deadline:
                            _LOGGER.debug(
                                "IoT skip legacy fallback (still within 5s window) for %s rgb=%s",
                                device_id,
                                (r, g, b),
                            )
                            return True
                    except Exception:
                        pass
                    # Only fallback if still aiming for the same pending color
                    try:
                        if dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT skip fallback legacy color; superseded %s -> %s", (r,g,b), dev.pending_color)
                            return True
                    except Exception:
                        pass
                    try:
                        _LOGGER.debug(
                            "IoT fallback: trying legacy color after unconfirmed colorwc for %s rgb=%s",
                            device_id,
                            (r, g, b),
                        )
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
                    self._publish(topic, payload2)
                    await _confirm_and_persist(False, 5.0)
                return True
            else:
                # Prefer legacy color but confirm and switch back if wrong
                import time as _t
                legacy_deadline = _t.monotonic() + 5.0
                msg["cmd"] = "color"
                msg["data"] = {"r": r, "g": g, "b": b}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False
                if dev is not None:
                    if await _confirm_and_persist(False, 5.0):
                        return True
                    # Try colorwc if legacy didn't reflect correctly
                    # Hard guard: never fallback before full 5s window elapses
                    try:
                        nowm = _t.monotonic()
                        if nowm < legacy_deadline:
                            _LOGGER.debug(
                                "IoT skip colorwc fallback (still within 5s window) for %s rgb=%s",
                                device_id,
                                (r, g, b),
                            )
                            return True
                    except Exception:
                        pass
                    # Only fallback if still aiming for the same pending color
                    try:
                        if dev.pending_color is not None and tuple(dev.pending_color) != (r, g, b):
                            _LOGGER.debug("IoT skip fallback colorwc; superseded %s -> %s", (r,g,b), dev.pending_color)
                            return True
                    except Exception:
                        pass
                    try:
                        _LOGGER.debug(
                            "IoT fallback: trying colorwc after unconfirmed legacy color for %s rgb=%s",
                            device_id,
                            (r, g, b),
                        )
                    except Exception:
                        pass
                    msg2 = {
                        "cmd": "colorwc",
                        # Include colorTemInKelvin=0 in color mode
                        "data": {"color": {"r": r, "g": g, "b": b}, "colorTemInKelvin": 0},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    self._publish(topic, payload2)
                    await _confirm_and_persist(True, 5.0)
                return True
        if command == "colorTem":
            dev = getattr(self._hub, "_devices", {}).get(device_id) if self._hub else None
            send_percent = getattr(dev, "color_temp_send_percent", None)
            kelvin = int(value)

            async def _confirm_kelvin_and_persist(using_percent: bool, timeout: float = 5.0) -> bool:
                import time as _t
                deadline = _t.monotonic() + max(0.2, float(timeout))
                while _t.monotonic() < deadline:
                    # Abort if a newer pending CT supersedes this target
                    try:
                        if dev is not None and dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT confirm CT aborted (superseded) for %s old=%s new=%s", device_id, kelvin, dev.pending_ct)
                            return False
                    except Exception:
                        pass
                    v = self._seen_devices.get(device_id, {}).get("colorTemInKelvin")
                    if isinstance(v, (int, float)) and int(v) == kelvin:
                        if dev is not None:
                            # Memory-only: set in-device flag but do not persist to disk
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

            prefer_kelvin_via_wc = (send_percent is not True)
            if prefer_kelvin_via_wc:
                msg["cmd"] = "colorwc"
                # Send only the color temperature field; omit the color field
                msg["data"] = {"colorTemInKelvin": kelvin}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False
                if dev is not None:
                    if await _confirm_kelvin_and_persist(False, 5.0):
                        return True
                    # Try percent
                    try:
                        if dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT skip percent fallback; CT superseded %s -> %s", kelvin, dev.pending_ct)
                            return True
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
                    self._publish(topic, payload2)
                    await _confirm_kelvin_and_persist(True, 5.0)
                return True
            else:
                # Prefer percent first, but correct if wrong
                msg["cmd"] = "colorTem"
                msg["data"] = {"colorTem": kelvin}
                payload = {"msg": msg}
                if not self._publish(topic, payload):
                    return False
                if dev is not None:
                    if await _confirm_kelvin_and_persist(True, 5.0):
                        return True
                    # Try Kelvin via colorwc if percent didn't reflect correctly
                    try:
                        if dev.pending_ct is not None and int(dev.pending_ct) != int(kelvin):
                            _LOGGER.debug("IoT skip Kelvin fallback; CT superseded %s -> %s", kelvin, dev.pending_ct)
                            return True
                    except Exception:
                        pass
                    msg2 = {
                        "cmd": "colorwc",
                        # Send only the color temperature field; omit the color field
                        "data": {"colorTemInKelvin": kelvin},
                        "cmdVersion": 1,
                        "transaction": f"v_{_ms_ts()}000",
                        "type": 1,
                    }
                    if self._account_topic:
                        msg2["accountTopic"] = self._account_topic
                    payload2 = {"msg": msg2}
                    self._publish(topic, payload2)
                    await _confirm_kelvin_and_persist(False, 5.0)
                return True
        return False

    async def async_request_status(self, device_id: str) -> bool:
        """Request status via device topic to seed state at startup."""
        if not self.can_control:
            return False
        topic = self._device_topics.get(device_id)
        if not topic:
            return False
        # Do not consume control bucket for device status requests
        msg = {
            "cmd": "status",
            "cmdVersion": 2,
            "transaction": f"v_{_ms_ts()}000",
            "type": 0,
            # Some firmwares accept accountTopic inside msg, but for status it is optional
        }
        payload = {"msg": msg}
        try:
            js = __import__('json').dumps(payload, separators=(',', ':'))
            self._iot.mqtt.publish(topic, js, qos=0, retain=False)
            return True
        except Exception as ex:
            _LOGGER.debug("IoT status publish failed: %s", ex)
            return False
