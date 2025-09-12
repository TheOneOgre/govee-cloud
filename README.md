# Govee Home Assistant Integration

Control Govee lights in Home Assistant with fast IoT push updates and reliable cloud fallback.

## Highlights

- IoT push: near‑instant updates and control (no polling needed most of the time).
- 15‑day login cache: avoids repeated logins, reduces rate‑limit risk.
- Smart discovery: names and capabilities are enriched from Govee’s app APIs.

## Requirements

- Home Assistant with [HACS](https://hacs.xyz/) installed.
- Your Govee account email and password (used to obtain an IoT token).

## Install

1) Add as custom repository:
   - HACS → Integrations → ••• → Custom repositories → URL: `https://github.com/TheOneOgre/govee-cloud` → Category: Integration
   - Or click this button from a browser logged into Home Assistant:

     [![Open your Home Assistant instance and show a repository in the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=TheOneOgre&repository=govee-cloud&category=integration)

2) Restart Home Assistant.
3) Settings → Devices & Services → Add Integration → Govee IoT.
4) Enter your Govee account email and password.

Notes:
- A Developer API key is not required. If present, it may be used as a fallback for certain REST calls.
- Credentials are cached under `.storage/govee_iot` for 15 days.

## Migration (from older versions)

- This release switches to IoT by default. After updating, open the Govee integration and add your email/password.
- The integration will show a Repairs issue and a persistent notification with a link to the Govee config page if credentials are missing.
- Entity unique IDs remain the same; existing automations and dashboards continue to work.

## How it works

- IoT client logs into the Govee, fetches credentials, and connects directly for IoT control.
- Device names/models are enriched from Govee’s app device list; a secondary platform list may fill gaps when needed.
- REST polling is minimized when IoT is active; a coordinated poll runs at a safe interval for reconciliation.

## Supported controls

- Power on/off
- Brightness
- Color (HS/RGB)
- Color temperature (Kelvin)

Actual features depend on your device model; the integration infers capabilities from the app APIs.

## Rate limits and caching

- Per‑device control/state token buckets reduce burstiness.
- Credentials are cached for 15 days to avoid re‑logins.

## Troubleshooting

- Logs: set `custom_components.govee` to debug in Settings → System → Logs.
- If devices show as MAC addresses briefly, names will be filled once the app device list is fetched.
- If IoT fails to start, check that credentials are set; the Repairs issue includes a link to the config page.

## Sponsor

If this integration helps you, consider supporting development:

<a href="https://www.buymeacoffee.com/theogre" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" style="height: 60px !important;width: 217px !important;" ></a>

## Disclaimer

This project is provided for personal use only and without any warranty. Use at your own risk. The authors and maintainers are not responsible for any issues, damages, account restrictions/bans, or device problems arising from the use of this integration.
