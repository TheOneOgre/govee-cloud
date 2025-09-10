#!/usr/bin/env python3

"""
Govee Platform (mobile app) API tester.

Allows you to authenticate with your Govee app credentials, list devices
with capabilities, and send control commands (power/brightness/color/CT)
through the "router" API used by the mobile app.

Usage examples:
  # List devices with capabilities
  python platform_api.py --email you@example.com --password 'secret' --list

  # Turn on by device id (sku auto-resolved from list)
  python platform_api.py --email you@example.com --password 'secret' \
      --device 29:91:9C:04:A0:01:9A:82 --on

  # Set brightness (0-100)
  python platform_api.py --email you@example.com --password 'secret' \
      --device <id> --brightness 60

  # Set color by RGB or hex
  python platform_api.py --email you@example.com --password 'secret' \
      --device <id> --color 255,0,64
  python platform_api.py --email you@example.com --password 'secret' \
      --device <id> --hex ff0040

  # Set color temperature in Kelvin
  python platform_api.py --email you@example.com --password 'secret' \
      --device <id> --ct 3000

Notes:
 - If you pass --sku, it will be used; otherwise the script will list devices
   and try to resolve sku from the provided device id.
 - Prints JSON results and basic success/failure output for quick validation.
"""

import argparse
import asyncio
import json
import sys
from typing import Any, Dict, Optional, Tuple

# Import PlatformAppClient without importing the Home Assistant package.
# Avoid `import custom_components.govee` because that runs __init__.py which
# depends on Home Assistant. Instead, load the module directly by file path.
from pathlib import Path
import importlib.util as _import_util

def _load_platform_app_client():
    here = Path(__file__).resolve().parent
    mod_path = here / "custom_components" / "govee" / "platform_app.py"
    if not mod_path.exists():
        raise RuntimeError(f"platform_app.py not found at {mod_path}")
    spec = _import_util.spec_from_file_location("_govee_platform_app", str(mod_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to create import spec for platform_app.py")
    mod = _import_util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod.PlatformAppClient

PlatformAppClient = _load_platform_app_client()


def _parse_color_arg(arg: str) -> Tuple[int, int, int]:
    s = arg.strip().lower()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) != 3:
            raise ValueError("color must be R,G,B or hex")
        r, g, b = (max(0, min(255, int(x))) for x in parts)
        return int(r), int(g), int(b)
    # hex like ff00aa or #ff00aa
    if s.startswith("#"):
        s = s[1:]
    if len(s) != 6:
        raise ValueError("hex color must be 6 chars (e.g., ff00aa)")
    v = int(s, 16)
    r = (v >> 16) & 0xFF
    g = (v >> 8) & 0xFF
    b = v & 0xFF
    return r, g, b


async def main_async():
    ap = argparse.ArgumentParser(description="Govee Platform API tester")
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--list", action="store_true", help="List devices and exit")
    ap.add_argument("--device", help="Device id for control")
    ap.add_argument("--sku", help="SKU/model (optional; auto-resolve from list if omitted)")
    ap.add_argument("--on", action="store_true")
    ap.add_argument("--off", action="store_true")
    ap.add_argument("--brightness", type=int, help="Brightness 0-100")
    ap.add_argument("--color", help="Color as R,G,B (0-255 each)")
    ap.add_argument("--hex", help="Color as 6-char hex (e.g., ff00aa)")
    ap.add_argument("--ct", type=int, help="Color temperature in Kelvin")
    args = ap.parse_args()

    client = PlatformAppClient(args.email, args.password)

    # List devices if requested or needed to resolve sku
    device_map: Dict[str, Dict[str, Any]] = {}
    if args.list or (args.device and not args.sku):
        data = await client.list_devices()
        print(json.dumps(data, indent=2))
        # Normalize to list of items
        items = []
        if isinstance(data, dict):
            raw = data.get("data")
            if isinstance(raw, list):
                items = raw
        for it in items:
            dev = it.get("device") or it.get("deviceId") or it.get("id")
            if dev:
                device_map[str(dev)] = it
        if args.list and not args.device:
            return

    sku = args.sku
    if args.device and not sku:
        info = device_map.get(args.device)
        if not info:
            print("Could not resolve SKU from list. Provide --sku explicitly.", file=sys.stderr)
            sys.exit(2)
        sku = info.get("sku") or info.get("model") or info.get("type")
        if not sku:
            print("Device is missing SKU/model in list payload; provide --sku.", file=sys.stderr)
            sys.exit(2)

    # Controls
    if args.device and sku:
        if args.on or args.off:
            ok = await client.control_turn(sku, args.device, on=bool(args.on))
            print(f"TURN {'on' if args.on else 'off'} → {ok}")
        if args.brightness is not None:
            pct = max(0, min(100, int(args.brightness)))
            ok = await client.control_brightness(sku, args.device, pct)
            print(f"BRIGHTNESS {pct}% → {ok}")
        if args.color or args.hex:
            r, g, b = _parse_color_arg(args.color or args.hex)
            ok = await client.control_colorwc(sku, args.device, r=r, g=g, b=b, kelvin=0)
            print(f"COLOR {r},{g},{b} → {ok}")
        if args.ct is not None:
            ok = await client.control_colorwc(sku, args.device, r=0, g=0, b=0, kelvin=int(args.ct))
            print(f"CT {int(args.ct)}K → {ok}")


def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
