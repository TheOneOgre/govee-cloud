#!/usr/bin/env python3

"""
Lightweight Govee LAN scan and status tester.

Usage examples:
  - Multicast scan (default) for ~5s and print discovered devices:
      python lancontrolbypass.py

  - Also broadcast to 255.255.255.255 (for networks without multicast):
      python lancontrolbypass.py --global-broadcast

  - Target a specific IP for scan/status:
      python lancontrolbypass.py --scan 10.0.0.50 --status

This script runs outside Home Assistant and is intended to validate
that devices respond to the LAN protocol on your network. It sends
the standard "scan" request and optionally the "devStatus" query.

It can also send a basic "bypass" ptReal power command (on/off) used
by some non-LAN devices that still listen for BLE-encoded commands
over UDP. This does NOT require AES keys.
"""

import argparse
import json
import socket
import struct
import sys
import time
from typing import Dict, Tuple, List


MCAST_GRP = "239.255.255.250"
SCAN_PORT = 4001
LISTEN_PORT = 4002
CMD_PORT = 4003


def make_listen_socket() -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("", LISTEN_PORT))
    except OSError as ex:
        print(f"Bind failed on UDP {LISTEN_PORT}: {ex}", file=sys.stderr)
        raise

    # Join multicast group to receive responses when multicast is used
    try:
        mreq = struct.pack("4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    except OSError:
        # Not fatal; still able to receive unicast/broadcast replies
        pass
    return s


def send_scan(sock: socket.socket, addr: Tuple[str, int]):
    # Some firmwares expect camelCase (accountTopic), others expect snake_case (account_topic).
    payloads = [
        {"msg": {"cmd": "scan", "data": {"accountTopic": "reserve"}}},
        {"msg": {"cmd": "scan", "data": {"account_topic": "reserve"}}},
    ]
    for p in payloads:
        data = json.dumps(p).encode("utf-8")
        try:
            sock.sendto(data, addr)
        except OSError as ex:
            print(f"sendto {addr} failed: {ex}", file=sys.stderr)


def send_dev_status(sock: socket.socket, ip: str):
    payload = {"msg": {"cmd": "devStatus", "data": {}}}
    data = json.dumps(payload).encode("utf-8")
    try:
        sock.sendto(data, (ip, CMD_PORT))
    except OSError as ex:
        print(f"send devStatus to {ip} failed: {ex}", file=sys.stderr)


def _xor_checksum(data: bytes) -> int:
    c = 0
    for b in data:
        c ^= b
    return c & 0xFF


def build_ptreal_set_power(on: bool) -> List[str]:
    # Matches govee2mqtt BLE packet encoding for SetDevicePower on "Generic:Light":
    # Body: [0x33, 0x01, on] then padded with zeros up to 19 bytes, with final byte = XOR checksum
    body = bytearray([0x33, 0x01, 0x01 if on else 0x00])
    # Pad to 19 bytes (checksum appended as 20th)
    if len(body) < 19:
        body.extend(b"\x00" * (19 - len(body)))
    checksum = _xor_checksum(body)
    body.append(checksum)
    # ptReal takes base64 chunks of up to 20 bytes; our packet is 20 bytes
    import base64
    return [base64.b64encode(bytes(body)).decode("ascii")]


def send_ptreal_power(sock: socket.socket, ip: str, on: bool):
    chunks = build_ptreal_set_power(on)
    payload = {"msg": {"cmd": "ptReal", "data": {"command": chunks}}}
    data = json.dumps(payload).encode("utf-8")
    try:
        sock.sendto(data, (ip, CMD_PORT))
    except OSError as ex:
        print(f"send ptReal to {ip} failed: {ex}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description="Govee LAN scan/status tester")
    ap.add_argument("--no-multicast", action="store_true", help="Disable multicast scan send")
    ap.add_argument("--global-broadcast", action="store_true", help="Also broadcast to 255.255.255.255:4001")
    ap.add_argument("--scan", action="append", default=[], help="Specific IPs to scan (can repeat)")
    ap.add_argument("--timeout", type=float, default=5.0, help="Listen timeout seconds (default 5)")
    ap.add_argument("--status", action="store_true", help="After discovery, query devStatus for each device")
    ap.add_argument("--pt-power", choices=["on", "off"], help="Send ptReal power on/off to each discovered or --scan IP")
    args = ap.parse_args()

    listen = make_listen_socket()
    listen.settimeout(0.5)

    # Sender sockets
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    # Kick off scans
    if not args.no_multicast:
        send_scan(send_sock, (MCAST_GRP, SCAN_PORT))
    if args.global_broadcast:
        send_scan(send_sock, ("255.255.255.255", SCAN_PORT))
    for ip in args.scan:
        send_scan(send_sock, (ip, SCAN_PORT))

    deadline = time.time() + args.timeout
    devices: Dict[str, Dict] = {}

    print("Listening for LAN responses...")
    while time.time() < deadline:
        try:
            data, addr = listen.recvfrom(4096)
        except socket.timeout:
            continue
        except OSError as ex:
            print(f"recv error: {ex}", file=sys.stderr)
            break
        try:
            txt = data.decode("utf-8", errors="ignore")
            js = json.loads(txt)
        except Exception:
            # Not JSON or bad
            continue

        # Expect wrapper: {"msg": {"cmd": "scan"|"devStatus", "data": {...}}}
        msg = js.get("msg") or {}
        cmd = msg.get("cmd")
        dat = msg.get("data") or {}

        if cmd == "scan":
            dev = {
                "ip": addr[0],
                "device": dat.get("device") or dat.get("deviceId") or "",
                "sku": dat.get("sku") or dat.get("model") or "",
                "bleVersionHard": dat.get("bleVersionHard"),
                "bleVersionSoft": dat.get("bleVersionSoft"),
                "wifiVersionHard": dat.get("wifiVersionHard"),
                "wifiVersionSoft": dat.get("wifiVersionSoft"),
            }
            key = dev["device"] or dev["ip"]
            devices[key] = dev
        elif cmd == "devStatus":
            # status packets include onOff/brightness/color/colorTemInKelvin
            print(f"Status from {addr[0]}: {dat}")

    if not devices:
        print("No LAN devices discovered.")
    else:
        print("\nDiscovered LAN devices:")
        for k, d in devices.items():
            print(f"- {d['ip']} dev={d['device']} sku={d['sku']} ble={d['bleVersionHard']}/{d['bleVersionSoft']} wifi={d['wifiVersionHard']}/{d['wifiVersionSoft']}")

    if (args.status or args.pt_power):
        targets = list(devices.values())
        # If nothing discovered but explicit IPs were provided, probe them directly
        if not targets and args.scan:
            targets = [{"ip": ip} for ip in args.scan]
        if targets and args.status:
            print("\nQuerying status...")
            for d in targets:
                send_dev_status(send_sock, d["ip"])
        # Collect for a short window
        if args.status:
            end2 = time.time() + 3.0
            while time.time() < end2:
                try:
                    data, addr = listen.recvfrom(4096)
                except socket.timeout:
                    continue
                try:
                    js = json.loads(data.decode("utf-8", errors="ignore"))
                    msg = js.get("msg") or {}
                    if msg.get("cmd") == "devStatus":
                        print(f"Status from {addr[0]}: {msg.get('data')}")
                except Exception:
                    pass

    # Send ptReal power if requested
    if args.pt_power:
        desired = args.pt_power == "on"
        targets = devices.values()
        # If no discovered devices but scan IPs were provided, target those
        if not devices and args.scan:
            targets = [{"ip": ip} for ip in args.scan]
        if not targets:
            print("No targets for ptReal power. Use --scan or perform discovery first.")
        else:
            print(f"\nSending ptReal power {args.pt_power}...")
            for d in targets:
                ip = d["ip"]
                print(f"- {ip}")
                send_ptreal_power(send_sock, ip, desired)
            # Optionally query status again
            time.sleep(0.5)
            for d in targets:
                send_dev_status(send_sock, d["ip"])
            end3 = time.time() + 2.0
            while time.time() < end3:
                try:
                    data, addr = listen.recvfrom(4096)
                except socket.timeout:
                    continue
                try:
                    js = json.loads(data.decode("utf-8", errors="ignore"))
                    msg = js.get("msg") or {}
                    if msg.get("cmd") == "devStatus":
                        print(f"Status from {addr[0]}: {msg.get('data')}")
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
