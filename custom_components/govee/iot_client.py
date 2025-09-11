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


def _login(email: str, password: str) -> Dict[str, Any]:
    resp = requests.post(
        "https://app2.govee.com/account/rest/account/v1/login",
        json={"email": email, "password": password, "client": _client_id(email)},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    client = data.get("client") or data.get("data") or data
    t = client.get("topic")
    if isinstance(t, dict) and "value" in t:
        client["topic"] = t["value"]
    return client


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

    async def start(self):
        opts = self._entry.options
        enabled = opts.get(CONF_IOT_PUSH_ENABLED, False)
        email = opts.get(CONF_IOT_EMAIL)
        password = opts.get(CONF_IOT_PASSWORD)
        if not enabled or not email or not password:
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
            except Exception:
                token = None

            # If no valid disk token, try in-memory cache, else login
            if not token:
                now_mono = time.monotonic()
                cached = _APP_LOGIN_CACHE.get(email)
                acct: dict[str, Any] | None = None
                if cached and (now_mono - cached[1]) < _CACHE_TTL_SEC:
                    acct = cached[0]
                else:
                    acct = await loop.run_in_executor(None, _login, email, password)
                    _APP_LOGIN_CACHE[email] = (acct, now_mono)
                token = acct.get("token") or acct.get("accessToken")
                if not token:
                    _LOGGER.warning("Govee IoT: no token from login response")
                    return
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

            # Load endpoint and cert/key from cache if fresh; otherwise fetch
            endpoint: str | None = None
            try:
                if (now_wall - os.stat(cert_path).st_mtime) < ttl and os.path.exists(key_path) and os.path.exists(endpoint_path):
                    txt = await loop.run_in_executor(None, _read_text_file, endpoint_path)
                    endpoint = (txt or '').strip() if txt is not None else None
            except Exception:
                endpoint = None

            if not endpoint:
                iot = await loop.run_in_executor(None, _get_iot_key, token, email)
                endpoint = iot.get("endpoint")
                p12_pass = iot.get("p12Pass") or iot.get("p12_pass")
                key_pem, cert_pem = await loop.run_in_executor(None, _extract_pfx, iot["p12"], p12_pass)
                await loop.run_in_executor(None, _write_bytes_file, cert_path, cert_pem)
                await loop.run_in_executor(None, _write_bytes_file, key_path, key_pem)
                await loop.run_in_executor(None, _write_text_file, endpoint_path, endpoint or '')
        except Exception as ex:
            _LOGGER.warning("Govee IoT login failed: %s", ex)
            return

        # Prepare SSL context from cached files
        try:
            ctx = ssl.create_default_context()
            ctx.load_verify_locations(cafile=certifi.where())
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        except Exception as ex:
            _LOGGER.warning("Govee IoT SSL context failed: %s", ex)
            return

        client = MqttClient(client_id=f"AP/{account_id}/{uuid.uuid4().hex}")
        client.tls_set_context(ctx)

        iot_state = IoTState(
            hass=self._hass,
            entry_id=self._entry.entry_id,
            hub=self._hub,
            mqtt=client,
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
            except Exception as ex:
                _LOGGER.debug("IoT message parse failed: %s", ex)

        def on_connect(_client, _userdata, _flags, rc):
            _LOGGER.info("Govee IoT connected rc=%s", rc)
            topic = self._account_topic
            if topic:
                client.subscribe(topic, qos=0)
                _LOGGER.info("Subscribed to account topic: %s", topic)

        client.on_connect = on_connect
        client.on_message = on_message

        try:
            # Async connect and background loop for lower-latency callbacks
            client.connect_async(endpoint, 8883, keepalive=120)
            client.loop_start()
        except Exception as ex:
            _LOGGER.warning("Govee IoT connect failed: %s", ex)

        # Build device→topic map using the mobile device list API
        try:
            await loop.run_in_executor(None, self._refresh_device_topics, token, email)
            _LOGGER.info("Loaded %s IoT device topics", len(self._device_topics))
        except Exception as ex:
            _LOGGER.debug("Refresh device topics failed: %s", ex)

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
        # Persist to disk for reuse
        try:
            cache_dir = self._hass.config.path('.storage/govee_iot')
            os.makedirs(cache_dir, exist_ok=True)
            path = os.path.join(cache_dir, 'devices.json')
            __import__('json').dump(mapping, open(path, 'w', encoding='utf-8'))
        except Exception:
            pass

    @property
    def can_control(self) -> bool:
        return bool(self._iot and self._iot.mqtt and self._device_topics and self._account_topic)

    async def async_publish_control(self, device_id: str, command: str, value: Any) -> bool:
        if not self.can_control:
            return False
        topic = self._device_topics.get(device_id)
        if not topic:
            return False
        # Build app-like envelope
        msg: dict[str, Any] = {
            "accountTopic": self._account_topic,
            "cmd": None,
            "data": None,
            "cmdVersion": 1,
            "transaction": f"v_{_ms_ts()}000",
            "type": 1,
        }
        if command == "turn":
            msg["cmd"] = "turn"
            msg["data"] = {"val": 1 if str(value).lower() == "on" else 0}
        elif command == "brightness":
            msg["cmd"] = "brightness"
            msg["data"] = {"val": int(value)}
        elif command == "color":
            msg["cmd"] = "color"
            msg["data"] = {
                "r": int(value.get("r", 0)),
                "g": int(value.get("g", 0)),
                "b": int(value.get("b", 0)),
            }
        elif command == "colorTem":
            msg["cmd"] = "colorwc"
            msg["data"] = {"color": {"r": 0, "g": 0, "b": 0}, "colorTemInKelvin": int(value)}
        else:
            return False
        payload = {"msg": msg}
        try:
            js = __import__('json').dumps(payload, separators=(',', ':'))
            # QoS 0 like the app
            self._iot.mqtt.publish(topic, js, qos=0, retain=False)
            return True
        except Exception as ex:
            _LOGGER.debug("IoT publish failed: %s", ex)
            return False

    async def async_request_status(self, device_id: str) -> bool:
        """Request status via device topic to seed state at startup."""
        if not self.can_control:
            return False
        topic = self._device_topics.get(device_id)
        if not topic:
            return False
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
