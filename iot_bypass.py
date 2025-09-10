#!/usr/bin/env python3

"""
Govee IoT (AWS MQTT) tester: publish commands and subscribe to updates.

This script logs into the Govee mobile APIs with your account
credentials to obtain an AWS IoT certificate/key bundle, then connects
to the device MQTT endpoint. It can list your devices, subscribe to the
account topic for updates, and send basic light commands to a device
topic (power/brightness/color/color temperature).

Requirements (install locally):
  pip install requests cryptography paho-mqtt certifi

Usage examples:
  - List devices and their MQTT topics:
      python iot_bypass.py --email you@example.com --password 'secret' --list-devices

  - Turn a device on by device id:
      python iot_bypass.py --email you@example.com --password 'secret' \
         --device 29:91:9C:04:A0:01:9A:82 --power on --subscribe

  - Set brightness and color temperature:
      python iot_bypass.py --email you@example.com --password 'secret' \
         --device <id> --brightness 60 --ct 3000 --subscribe

Notes:
 - Your credentials are only used to request the IoT key/cert from Govee.
 - The script subscribes to the account topic for updates (like the app).
 - If the broker closes the connection when subscribing to device topics,
   stick to the account-level subscription.
"""

import argparse
import base64
import json
import ssl
import sys
import tempfile
import time
import uuid
from typing import Dict, List, Tuple

import certifi
import requests
from cryptography import x509
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from paho.mqtt.client import Client, MQTTMessage


APP_VERSION = "5.6.01"


def ua() -> str:
    return f"GoveeHome/{APP_VERSION} (com.ihoment.GoVeeSensor; build:2; iOS 16.5.0) Alamofire/5.6.4"


def ms_timestamp_str() -> str:
    return str(int(time.time() * 1000))


def login_account(email: str, password: str) -> Dict:
    client_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, email).hex)
    resp = requests.post(
        "https://app2.govee.com/account/rest/account/v1/login",
        json={"email": email, "password": password, "client": client_id},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") not in (0, 200):
        raise RuntimeError(f"Login failed: {data}")
    # Expected shape: { client: { token, accountId, topic, ... }, ... }
    client = data.get("client") or data.get("data") or data
    # Normalize redacted topic wrapper if present
    t = client.get("topic")
    if isinstance(t, dict) and "value" in t:
        client["topic"] = t["value"]
    return client


def get_iot_key(token: str, client_id: str) -> Dict:
    resp = requests.get(
        "https://app2.govee.com/app/v1/account/iot/key",
        headers={
            "Authorization": f"Bearer {token}",
            "appVersion": APP_VERSION,
            "clientId": client_id,
            "clientType": "1",
            "iotVersion": "0",
            "timestamp": ms_timestamp_str(),
            "User-Agent": ua(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") not in (0, 200):
        raise RuntimeError(f"iot/key failed: {data}")
    return data["data"] if "data" in data else data


def get_device_list(token: str, client_id: str) -> Dict:
    resp = requests.post(
        "https://app2.govee.com/device/rest/devices/v1/list",
        headers={
            "Authorization": f"Bearer {token}",
            "appVersion": APP_VERSION,
            "clientId": client_id,
            "clientType": "1",
            "iotVersion": "0",
            "timestamp": ms_timestamp_str(),
            "User-Agent": ua(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") not in (0, 200):
        raise RuntimeError(f"devices/list failed: {data}")
    return data


def _parse_maybe_embedded_json(val):
    if isinstance(val, str):
        s = val.strip()
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return val
    return val


def _extract_topic_from_entry(entry: Dict) -> str | None:
    # deviceExt may be a JSON string; normalize to dict
    ext = entry.get("deviceExt") or entry.get("device_ext")
    ext = _parse_maybe_embedded_json(ext)
    if isinstance(ext, dict):
        ds = ext.get("deviceSettings") or ext.get("device_settings")
        ds = _parse_maybe_embedded_json(ds)
        if isinstance(ds, dict):
            topic = ds.get("topic")
            if isinstance(topic, dict) and "value" in topic:
                return topic["value"]
            if isinstance(topic, str):
                return topic
    return None


def extract_pfx(p12_b64: str, password: str) -> Tuple[bytes, bytes]:
    pfx = base64.b64decode(p12_b64)
    key, cert, _ = load_key_and_certificates(pfx, password.encode("utf-8"))
    if key is None or cert is None:
        raise RuntimeError("Failed to extract key/cert from PFX")
    # Serialize private key in PKCS8 PEM
    key_pem = key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    cert_pem = cert.public_bytes(Encoding.PEM)
    return key_pem, cert_pem


def build_ssl_context(key_pem: bytes, cert_pem: bytes) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=certifi.where())
    # paho-mqtt tls_set_context requires a context with cert/key loaded via files or set later
    # Workaround: write to temp files and load into context via load_cert_chain
    with tempfile.NamedTemporaryFile("wb", delete=False) as kf, tempfile.NamedTemporaryFile(
        "wb", delete=False
    ) as cf:
        kf.write(key_pem)
        cf.write(cert_pem)
        kf.flush()
        cf.flush()
        ctx.load_cert_chain(certfile=cf.name, keyfile=kf.name)
    ctx.check_hostname = False  # AWS IoT uses SNI-valid hostnames; disable if needed
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def on_connect(client: Client, userdata, flags, rc):
    print(f"Connected: rc={rc}")


def on_message(client: Client, userdata, msg: MQTTMessage):
    try:
        payload = msg.payload.decode("utf-8", errors="ignore")
        print(f"[MQTT] {msg.topic}: {payload}")
    except Exception:
        print(f"[MQTT] {msg.topic}: <{len(msg.payload)} bytes>")


def publish_turn(client: Client, device_topic: str, on: bool):
    val = 1 if on else 0
    payload = {
        "msg": {
            "cmd": "turn",
            "data": {"val": val},
            "cmdVersion": 0,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 1,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def publish_status_request(client: Client, device_topic: str):
    payload = {
        "msg": {
            "cmd": "status",
            "cmdVersion": 2,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 0,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def _xor_checksum(data: bytes) -> int:
    c = 0
    for b in data:
        c ^= b
    return c & 0xFF


def build_ptreal_set_power(on: bool) -> List[str]:
    body = bytearray([0x33, 0x01, 0x01 if on else 0x00])
    if len(body) < 19:
        body.extend(b"\x00" * (19 - len(body)))
    body.append(_xor_checksum(body))
    return [base64.b64encode(bytes(body)).decode("ascii")]


def publish_ptreal_power(client: Client, device_topic: str, on: bool):
    payload = {
        "msg": {
            "cmd": "ptReal",
            "data": {"command": build_ptreal_set_power(on)},
            "cmdVersion": 0,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 1,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def publish_brightness(client: Client, device_topic: str, percent: int):
    percent = max(0, min(100, int(percent)))
    payload = {
        "msg": {
            "cmd": "brightness",
            "data": {"val": percent},
            "cmdVersion": 0,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 1,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def publish_ct(client: Client, device_topic: str, kelvin: int):
    payload = {
        "msg": {
            "cmd": "colorwc",
            "data": {"color": {"r": 0, "g": 0, "b": 0}, "colorTemInKelvin": int(kelvin)},
            "cmdVersion": 0,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 1,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def publish_color(client: Client, device_topic: str, r: int, g: int, b: int):
    payload = {
        "msg": {
            "cmd": "colorwc",
            "data": {"color": {"r": int(r), "g": int(g), "b": int(b)}, "colorTemInKelvin": 0},
            "cmdVersion": 0,
            "transaction": f"v_{ms_timestamp_str()}000",
            "type": 1,
        }
    }
    client.publish(device_topic, json.dumps(payload), qos=0, retain=False)


def main():
    ap = argparse.ArgumentParser(description="Govee AWS IoT (MQTT) tester")
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--list-devices", action="store_true")
    ap.add_argument("--device", help="Device id to control (from list)")
    ap.add_argument("--power", choices=["on", "off"])
    ap.add_argument("--brightness", type=int)
    ap.add_argument("--ct", type=int, help="Color temperature in Kelvin")
    ap.add_argument("--color", help="RGB as r,g,b (0-255)")
    ap.add_argument("--subscribe", action="store_true", help="Subscribe to account topic for updates")
    ap.add_argument("--status-request", action="store_true", help="Publish a device status request")
    ap.add_argument("--ptpower", choices=["on", "off"], help="Send ptReal SetDevicePower to device topic")
    args = ap.parse_args()

    # 1) Login and obtain token + account info
    acct = login_account(args.email, args.password)
    token = acct.get("token") or acct.get("accessToken")
    if not token:
        raise RuntimeError(f"No token in login response: {acct}")
    account_id = acct.get("accountId") or acct.get("account_id")
    account_topic = acct.get("topic")
    client_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, args.email).hex)

    # 2) Fetch devices and topics
    devices_resp = get_device_list(token, client_id)
    devices = devices_resp.get("devices") or []
    dev_by_id = {}
    for d in devices:
        topic = _extract_topic_from_entry(d)
        dev_by_id[d.get("device")] = {
            "sku": d.get("sku"),
            "name": d.get("deviceName") or d.get("device_name"),
            "topic": topic,
        }

    if args.list_devices:
        print("Devices:")
        for dev_id, info in dev_by_id.items():
            print(f"- {dev_id} sku={info['sku']} name={info['name']} topic={info['topic']}")
        return

    if not args.device:
        print("--device is required unless --list-devices", file=sys.stderr)
        sys.exit(2)
    if args.device not in dev_by_id:
        raise RuntimeError(f"Device {args.device} not found in your account")

    device_topic = dev_by_id[args.device]["topic"]
    if not device_topic:
        raise RuntimeError("Device topic not available; device may be BLE-only or not IoT-capable")

    # 3) Obtain IoT PFX and endpoint, extract cert/key
    iot = get_iot_key(token, client_id)
    endpoint = iot.get("endpoint")
    key_pem, cert_pem = extract_pfx(iot["p12"], iot["p12Pass"]) if "p12Pass" in iot else extract_pfx(iot["p12"], iot["p12_pass"])  # handle both variants

    # 4) Connect MQTT (mutual TLS)
    ctx = build_ssl_context(key_pem, cert_pem)
    mqtt_client = Client(client_id=f"AP/{account_id}/{uuid.uuid4().hex}", clean_session=True)
    mqtt_client.tls_set_context(ctx)
    mqtt_client.on_connect = on_connect
    if args.subscribe and account_topic:
        mqtt_client.on_message = on_message

    print(f"Connecting MQTT to {endpoint}:8883; device_topic={device_topic}")
    mqtt_client.connect(endpoint, 8883, keepalive=120)
    mqtt_client.loop_start()
    time.sleep(1.0)

    if args.subscribe and account_topic:
        mqtt_client.subscribe(account_topic, qos=0)
        print(f"Subscribed to account topic: {account_topic}")

    # 5) Publish command(s)
    if args.power:
        publish_turn(mqtt_client, device_topic, args.power == "on")
    if args.brightness is not None:
        publish_brightness(mqtt_client, device_topic, args.brightness)
    if args.ct is not None:
        publish_ct(mqtt_client, device_topic, args.ct)
    if args.color:
        try:
            r, g, b = [int(x.strip()) for x in args.color.split(",")]
        except Exception:
            raise SystemExit("--color must be r,g,b with 0-255 values")
        publish_color(mqtt_client, device_topic, r, g, b)
    if args.status_request:
        publish_status_request(mqtt_client, device_topic)
    if args.ptpower:
        publish_ptreal_power(mqtt_client, device_topic, args.ptpower == "on")

    # Keep process alive briefly to receive responses
    if args.subscribe:
        print("Listening for updates. Ctrl+C to exit.")
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
    else:
        time.sleep(1.5)
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


if __name__ == "__main__":
    main()
