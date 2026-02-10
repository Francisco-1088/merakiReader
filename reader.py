#!/usr/bin/env python3
"""
Meraki Configuration Export Tool
================================
Exports VLAN, SSID, MX Firewall, Site-to-Site VPN, and Switch Port
configurations from all networks in an organization to CSV files.

Uses asyncio + aiohttp for high-throughput concurrent API calls with
automatic rate-limit (429) retry and per-second request throttling.

Requirements:
    pip install aiohttp aiofiles

Usage:
    export MERAKI_API_KEY="your_key_here"
    python meraki_export.py                          # interactive org picker
    python meraki_export.py --org-id 123456          # explicit org
    python meraki_export.py --org-id 123456 --out ./exports
"""

import argparse, asyncio, csv, io, json, logging, os, sys, time
from dataclasses import dataclass, field
from typing import Any

import aiohttp

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE = "https://api.meraki.com/api/v1"
MAX_CONCURRENCY = 8          # parallel in-flight requests (stay under 10 rps)
RATE_LIMIT_DELAY = 1.0       # base wait on 429 before using Retry-After
MAX_RETRIES = 5              # retries per request on 429 / transient errors
PAGE_SIZE = 1000             # perPage for paginated endpoints

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("meraki_export")


# ---------------------------------------------------------------------------
# Async HTTP helpers with rate-limit handling
# ---------------------------------------------------------------------------
@dataclass
class ApiClient:
    api_key: str
    sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(MAX_CONCURRENCY))
    _session: aiohttp.ClientSession | None = field(default=None, repr=False)

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=60),
        )
        return self

    async def __aexit__(self, *exc):
        if self._session:
            await self._session.close()

    async def get(self, path: str, params: dict | None = None) -> Any:
        """GET with semaphore throttle, 429 back-off, and retry."""
        url = f"{BASE}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            async with self.sem:
                try:
                    async with self._session.get(url, params=params) as r:
                        if r.status == 200:
                            return await r.json()
                        if r.status == 429:
                            wait = float(r.headers.get("Retry-After", RATE_LIMIT_DELAY))
                            log.warning("429 on %s – retry in %.1fs (attempt %d)", path, wait, attempt)
                            await asyncio.sleep(wait)
                            continue
                        if r.status in (404, 400):
                            # Endpoint not applicable to this network type
                            return None
                        body = await r.text()
                        log.error("HTTP %d on %s: %s", r.status, path, body[:300])
                        return None
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    log.warning("Transient error on %s: %s (attempt %d)", path, e, attempt)
                    await asyncio.sleep(RATE_LIMIT_DELAY * attempt)
        log.error("Max retries exceeded for %s", path)
        return None

    async def get_paginated(self, path: str) -> list:
        """Auto-paginate link-header based Meraki endpoints."""
        results = []
        url = f"{BASE}{path}"
        params = {"perPage": PAGE_SIZE}
        for _ in range(200):  # safety cap
            for attempt in range(1, MAX_RETRIES + 1):
                async with self.sem:
                    try:
                        async with self._session.get(url, params=params) as r:
                            if r.status == 429:
                                wait = float(r.headers.get("Retry-After", RATE_LIMIT_DELAY))
                                await asyncio.sleep(wait)
                                continue
                            if r.status != 200:
                                return results
                            page = await r.json()
                            if isinstance(page, list):
                                results.extend(page)
                            else:
                                return results
                            # Follow pagination via Link header
                            link = r.headers.get("Link", "")
                            if 'rel=next' in link:
                                next_url = link.split("<")[1].split(">")[0]
                                url = next_url
                                params = None  # params are baked into the next URL
                            else:
                                return results
                            break  # success – move to next page
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        log.warning("Paginate error %s: %s (attempt %d)", path, e, attempt)
                        await asyncio.sleep(RATE_LIMIT_DELAY * attempt)
            else:
                return results  # retries exhausted for this page
        return results


# ---------------------------------------------------------------------------
# Data fetchers – one per config type
# ---------------------------------------------------------------------------
async def fetch_vlans(api: ApiClient, net: dict) -> list[dict]:
    """GET /networks/{id}/appliance/vlans"""
    nid, name = net["id"], net["name"]
    # VLANs must be enabled; check first
    status = await api.get(f"/networks/{nid}/appliance/vlans/settings")
    if not status or not status.get("vlansEnabled"):
        return []
    vlans = await api.get(f"/networks/{nid}/appliance/vlans")
    if not vlans or not isinstance(vlans, list):
        return []
    for v in vlans:
        v["network_name"] = name
    return vlans


async def fetch_ssids(api: ApiClient, net: dict) -> list[dict]:
    """GET /networks/{id}/wireless/ssids"""
    nid, name = net["id"], net["name"]
    ssids = await api.get(f"/networks/{nid}/wireless/ssids")
    if not ssids or not isinstance(ssids, list):
        return []
    for s in ssids:
        s["network_name"] = name
    return ssids


async def fetch_mx_firewall(api: ApiClient, net: dict) -> list[dict]:
    """GET /networks/{id}/appliance/firewall/l3FirewallRules"""
    nid, name = net["id"], net["name"]
    data = await api.get(f"/networks/{nid}/appliance/firewall/l3FirewallRules")
    if not data:
        return []
    rules = data.get("rules", data) if isinstance(data, dict) else data
    if not isinstance(rules, list):
        return []
    for r in rules:
        r["network_name"] = name
    return rules


async def fetch_site_vpn(api: ApiClient, net: dict) -> list[dict]:
    """GET /networks/{id}/appliance/vpn/siteToSiteVpn"""
    nid, name = net["id"], net["name"]
    data = await api.get(f"/networks/{nid}/appliance/vpn/siteToSiteVpn")
    if not data or not isinstance(data, dict):
        return []
    # Flatten subnets into individual rows while keeping top-level fields
    mode = data.get("mode", "")
    hubs = json.dumps(data.get("hubs", []))
    subnets = data.get("subnets", [])
    if not subnets:
        return [{"network_name": name, "mode": mode, "hubs": hubs,
                 "localSubnet": "", "useVpn": ""}]
    rows = []
    for s in subnets:
        rows.append({
            "network_name": name,
            "mode": mode,
            "hubs": hubs,
            "localSubnet": s.get("localSubnet", ""),
            "useVpn": s.get("useVpn", ""),
        })
    return rows


async def fetch_switch_ports(api: ApiClient, switches: list[dict]) -> list[dict]:
    """GET /devices/{serial}/switch/ports for every switch."""
    rows = []
    tasks = []
    for sw in switches:
        serial = sw["serial"]
        sw_name = sw.get("name") or sw.get("serial")
        net_name = sw.get("_network_name", "")
        tasks.append((serial, sw_name, net_name))

    async def _get_ports(serial, sw_name, net_name):
        ports = await api.get(f"/devices/{serial}/switch/ports")
        if not ports or not isinstance(ports, list):
            return []
        for p in ports:
            p["network_name"] = net_name
            p["switch_name"] = sw_name
        return ports

    results = await asyncio.gather(*[_get_ports(*t) for t in tasks])
    for r in results:
        rows.extend(r)
    return rows


# ---------------------------------------------------------------------------
# CSV writer helper
# ---------------------------------------------------------------------------
def write_csv(filename: str, rows: list[dict], key_columns: list[str] | None = None):
    """Write rows to CSV. key_columns appear first if specified."""
    if not rows:
        log.info("  ↳ %s – no data, skipping", filename)
        return
    # Determine column order: key_columns first, then alphabetical remainder
    all_keys: set[str] = set()
    for r in rows:
        all_keys.update(r.keys())
    key_columns = key_columns or []
    ordered = [k for k in key_columns if k in all_keys]
    ordered += sorted(all_keys - set(ordered))

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            # Stringify any nested dicts / lists for clean CSV output
            flat = {}
            for k in ordered:
                v = r.get(k, "")
                flat[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
            writer.writerow(flat)
    log.info("  ↳ %s – %d rows written", filename, len(rows))


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
async def run(api_key: str, org_id: str, out_dir: str):
    async with ApiClient(api_key) as api:
        # --- 1. Fetch all networks (paginated) --------------------------------
        log.info("Fetching networks for org %s …", org_id)
        networks = await api.get_paginated(f"/organizations/{org_id}/networks")
        if not networks:
            log.error("No networks found – check org ID and API key permissions.")
            return
        log.info("Found %d networks.", len(networks))

        # Classify networks by product types to avoid pointless API calls
        appliance_nets = [n for n in networks if "appliance" in (n.get("productTypes") or [])]
        wireless_nets  = [n for n in networks if "wireless"  in (n.get("productTypes") or [])]
        switch_nets    = [n for n in networks if "switch"    in (n.get("productTypes") or [])]

        log.info("  appliance: %d | wireless: %d | switch: %d",
                 len(appliance_nets), len(wireless_nets), len(switch_nets))

        # --- 2. Fetch all devices org-wide (single paginated call) to find switches
        log.info("Fetching org device inventory …")
        devices = await api.get_paginated(f"/organizations/{org_id}/devices")
        switches = [d for d in (devices or []) if (d.get("model") or "").startswith("MS")]

        # Attach network name to each switch for later CSV output
        net_map = {n["id"]: n["name"] for n in networks}
        for sw in switches:
            sw["_network_name"] = net_map.get(sw.get("networkId"), "")
        log.info("Found %d switches across org.", len(switches))

        # --- 3. Fire off all config fetches concurrently -----------------------
        log.info("Fetching configurations (this may take a while) …")

        vlan_tasks    = [fetch_vlans(api, n)       for n in appliance_nets]
        ssid_tasks    = [fetch_ssids(api, n)       for n in wireless_nets]
        fw_tasks      = [fetch_mx_firewall(api, n) for n in appliance_nets]
        vpn_tasks     = [fetch_site_vpn(api, n)    for n in appliance_nets]

        # Gather all network-level tasks together
        all_tasks = vlan_tasks + ssid_tasks + fw_tasks + vpn_tasks
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Slice results back apart
        idx = 0
        def _collect(length):
            nonlocal idx
            out = []
            for r in results[idx:idx + length]:
                if isinstance(r, list):
                    out.extend(r)
                elif isinstance(r, Exception):
                    log.error("Task exception: %s", r)
            idx += length
            return out

        vlan_rows = _collect(len(vlan_tasks))
        ssid_rows = _collect(len(ssid_tasks))
        fw_rows   = _collect(len(fw_tasks))
        vpn_rows  = _collect(len(vpn_tasks))

        # Switch ports are fetched per-device (can be many)
        port_rows = await fetch_switch_ports(api, switches)

        # --- 4. Write CSVs -----------------------------------------------------
        os.makedirs(out_dir, exist_ok=True)
        log.info("Writing CSV files to %s …", out_dir)

        write_csv(
            os.path.join(out_dir, "vlans.csv"), vlan_rows,
            key_columns=["network_name", "id", "name", "subnet", "applianceIp",
                         "fixedIpAssignments", "reservedIpRanges", "dnsNameservers",
                         "dhcpHandling", "dhcpLeaseTime", "dhcpBootOptionsEnabled"],
        )
        write_csv(
            os.path.join(out_dir, "ssids.csv"), ssid_rows,
            key_columns=["network_name", "number", "name", "enabled", "authMode",
                         "encryptionMode", "splashPage", "ipAssignmentMode",
                         "bandSelection", "minBitrate", "vlanId"],
        )
        write_csv(
            os.path.join(out_dir, "mx_firewall_rules.csv"), fw_rows,
            key_columns=["network_name", "comment", "policy", "protocol",
                         "srcPort", "srcCidr", "destPort", "destCidr",
                         "syslogEnabled"],
        )
        write_csv(
            os.path.join(out_dir, "site_to_site_vpn.csv"), vpn_rows,
            key_columns=["network_name", "mode", "hubs", "localSubnet", "useVpn"],
        )
        write_csv(
            os.path.join(out_dir, "switch_ports.csv"), port_rows,
            key_columns=["network_name", "switch_name", "portId", "name",
                         "enabled", "type", "vlan", "voiceVlan", "allowedVlans",
                         "poeEnabled", "rstpEnabled", "stpGuard",
                         "accessPolicyType", "linkNegotiation"],
        )

    log.info("Done.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Export Meraki configs to CSV")
    parser.add_argument("--api-key", default=os.getenv("MERAKI_API_KEY"),
                        help="Dashboard API key (or set MERAKI_API_KEY env var)")
    parser.add_argument("--org-id", default=None,
                        help="Organization ID (omit for interactive picker)")
    parser.add_argument("--out", default="meraki_export",
                        help="Output directory for CSVs (default: ./meraki_export)")
    args = parser.parse_args()

    if not args.api_key:
        sys.exit("Error: provide --api-key or set MERAKI_API_KEY environment variable.")

    # If no org-id supplied, list orgs and let the user pick
    if not args.org_id:
        async def pick_org():
            async with ApiClient(args.api_key) as api:
                orgs = await api.get("/organizations")
                if not orgs:
                    sys.exit("Could not retrieve organizations.")
                if len(orgs) == 1:
                    return orgs[0]["id"]
                print("\nAvailable organizations:")
                for i, o in enumerate(orgs, 1):
                    print(f"  {i}. {o['name']}  (ID: {o['id']})")
                choice = int(input("\nSelect org number: ")) - 1
                return orgs[choice]["id"]

        args.org_id = asyncio.run(pick_org())

    asyncio.run(run(args.api_key, args.org_id, args.out))


if __name__ == "__main__":
    main()