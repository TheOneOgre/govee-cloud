"""Govee IoT (AWS MQTT) read-only client for push state updates."""
from __future__ import annotations

import asyncio
import os
import base64
import json
import logging
import ssl
import tempfile
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

    async def start(self):
        opts = self._entry.options
        enabled = opts.get(CONF_IOT_PUSH_ENABLED, False)
        email = opts.get(CONF_IOT_EMAIL)
        password = opts.get(CONF_IOT_PASSWORD)
        if not enabled or not email or not password:
            return
        loop = asyncio.get_running_loop()
        # Login and fetch IoT key in executor
        try:
            acct = await loop.run_in_executor(None, _login, email, password)
            token = acct.get("token") or acct.get("accessToken")
            if not token:
                _LOGGER.warning("Govee IoT: no token from login response")
                return
            iot = await loop.run_in_executor(None, _get_iot_key, token, email)
            endpoint = iot.get("endpoint")
            p12_pass = iot.get("p12Pass") or iot.get("p12_pass")
            key_pem, cert_pem = await loop.run_in_executor(
                None, _extract_pfx, iot["p12"], p12_pass
            )
        except Exception as ex:
            _LOGGER.warning("Govee IoT login failed: %s", ex)
            return

        # Prepare SSL context with extracted certs in executor (avoid blocking loop)
        loop = asyncio.get_running_loop()

        def _build_ctx() -> tuple[ssl.SSLContext, str, str]:
            ctx = ssl.create_default_context()
            ctx.load_verify_locations(cafile=certifi.where())
            kf = tempfile.NamedTemporaryFile("wb", delete=False)
            cf = tempfile.NamedTemporaryFile("wb", delete=False)
            try:
                kf.write(key_pem)
                cf.write(cert_pem)
                kf.flush()
                cf.flush()
                ctx.load_cert_chain(certfile=cf.name, keyfile=kf.name)
            except Exception:
                # Clean up on failure
                try:
                    kf.close()
                    os.unlink(kf.name)
                except Exception:
                    pass
                try:
                    cf.close()
                    os.unlink(cf.name)
                except Exception:
                    pass
                raise
            finally:
                # Close handles but keep files for the life of the client
                try:
                    kf.close()
                except Exception:
                    pass
                try:
                    cf.close()
                except Exception:
                    pass
            return ctx, cf.name, kf.name

        try:
            ctx, certfile_path, keyfile_path = await loop.run_in_executor(None, _build_ctx)
        except Exception as ex:
            _LOGGER.warning("Govee IoT SSL context failed: %s", ex)
            return

        client = MqttClient(client_id=f"AP/{acct.get('accountId')}/{uuid.uuid4().hex}")
        client.tls_set_context(ctx)

        iot_state = IoTState(
            hass=self._hass,
            entry_id=self._entry.entry_id,
            hub=self._hub,
            mqtt=client,
            certfile_path=certfile_path,
            keyfile_path=keyfile_path,
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
            topic = acct.get("topic")
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
            # brightness 0-100 -> 0-255
            if "brightness" in state:
                try:
                    gv = int(state.get("brightness") or 0)
                    dev.brightness = max(0, min(255, int(round(gv / 100 * 255))))
                except Exception:
                    pass
            # color / CT mutual exclusivity
            if "color" in state and isinstance(state["color"], dict):
                c = state["color"]
                dev.color = (int(c.get("r", 0)), int(c.get("g", 0)), int(c.get("b", 0)))
                dev.color_temp = 0
            if "colorTemInKelvin" in state:
                try:
                    dev.color_temp = int(state.get("colorTemInKelvin") or 0)
                    dev.color = (0, 0, 0)
                except Exception:
                    pass
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
