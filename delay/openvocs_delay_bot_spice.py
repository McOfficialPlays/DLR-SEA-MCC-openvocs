#!/usr/bin/env python3
"""
openvocs_delay_bot_spice.py

Delay bot for OpenVOCS:

- HAB/IN  -> delayed by Earth–Mars light-time -> MCC/OUT
- MCC/IN  -> delayed by Earth–Mars light-time -> HAB/OUT

Uses SPICE (via spiceypy) to compute current one-way light time
between Earth and Mars and updates it periodically.

In development, you can override the delay with a fixed value
via the DEV_DELAY_SECONDS environment variable, e.g.:

    DEV_DELAY_SECONDS=3 python openvocs_delay_bot_spice.py
"""

import asyncio
import dataclasses
import os
import socket
import struct
import time
from typing import Tuple, List

import numpy as np
import spiceypy as spice

# -------------------- SPICE CONFIG -------------------- #

# Update these paths to where you put your SPICE kernels
KERNEL_FILES = [
    "/opt/openvocs-delay-bot/kernels/naif0012.tls",  # leapseconds
    "/opt/openvocs-delay-bot/kernels/de440.bsp",     # planetary ephemeris
]

EARTH_ID = 3
MARS_ID = 4

# Speed of light in km/s
C_KM_PER_S = 299_792.458

# Environment variable override for development:
# If set (e.g. DEV_DELAY_SECONDS=3), we ignore SPICE completely.
DEV_DELAY_SECONDS = float(os.environ.get("DEV_DELAY_SECONDS", "0.0"))


def load_spice_kernels():
    """Load the SPICE kernels (only once)."""
    for k in KERNEL_FILES:
        if not os.path.isfile(k):
            raise FileNotFoundError(
                f"SPICE kernel not found: {k} (update KERNEL_FILES paths)"
            )
        spice.furnsh(k)
    print("[SPICE] Kernels loaded.")


def get_current_light_time_seconds_spice() -> float:
    """
    Compute Earth->Mars one-way light time (seconds) using SPICE ephemeris.
    Uses numeric barycenter IDs (3, 4) to match de440.bsp coverage.
    """
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    et = spice.str2et(now_utc)

    # Debug: show what IDs we're actually using
    print(f"[SPICE] Computing lt for target={MARS_ID}, observer={EARTH_ID} at {now_utc}")

    # Use spkgeo with numeric IDs
    state, _ = spice.spkgeo(MARS_ID, et, "J2000", EARTH_ID)
    pos_km = np.array(state[:3])
    dist_km = np.linalg.norm(pos_km)

    lt = dist_km / C_KM_PER_S
    return float(lt)


# -------------------- DELAY BOT CONFIG -------------------- #

@dataclasses.dataclass
class LoopMapping:
    name: str
    src_group: str
    src_port: int
    dst_group: str
    dst_port: int
    iface_ip: str = "0.0.0.0"  # local interface IP for joining/sending


# These values are taken from your OpenVOCS project JSON:
# HAB/IN  -> 224.0.0.1:20000
# HAB/OUT -> 224.0.0.1:20011
# MCC/IN  -> 224.0.0.1:20012
# MCC/OUT -> 224.0.0.1:20013
LOOP_MAPPINGS: List[LoopMapping] = [
    # HAB/IN  -> MCC/OUT
    LoopMapping(
        name="HAB_IN_to_MCC_OUT",
        src_group="224.0.0.1",
        src_port=20000,
        dst_group="224.0.0.1",
        dst_port=20013,
        iface_ip="0.0.0.0",   # change to specific NIC IP if needed
    ),
    # MCC/IN -> HAB/OUT
    LoopMapping(
        name="MCC_IN_to_HAB_OUT",
        src_group="224.0.0.1",
        src_port=20012,
        dst_group="224.0.0.1",
        dst_port=20011,
        iface_ip="0.0.0.0",
    ),
]

# Max UDP packet size for buffering
MAX_PACKET_SIZE = 2048

# How often (seconds) to recompute light time from SPICE
LIGHT_TIME_REFRESH_SEC = 60.0


class LightTimeCache:
    """
    Cache the Earth–Mars light time OR use a fixed override for development.
    """

    def __init__(self, refresh_interval: float):
        self._refresh_interval = refresh_interval
        self._last_update = 0.0
        self._cached_value = 600.0  # fallback default if SPICE fails

        if DEV_DELAY_SECONDS > 0:
            print(
                f"[LightTime] DEV OVERRIDE ACTIVE — "
                f"fixed delay = {DEV_DELAY_SECONDS} seconds"
            )

    def current(self) -> float:
        # If development override is active, skip SPICE entirely
        if DEV_DELAY_SECONDS > 0:
            return DEV_DELAY_SECONDS

        # Production mode: use SPICE
        now = time.time()
        if now - self._last_update > self._refresh_interval:
            try:
                lt = get_current_light_time_seconds_spice()
                self._cached_value = lt
                self._last_update = now
                print(f"[LightTime] SPICE updated — Earth→Mars lt = {lt:.1f} s")
            except Exception as e:
                # Don’t crash the bot if SPICE fails; keep last good value
                print(f"[LightTime] SPICE update failed: {e}")
        return self._cached_value


light_time_cache = LightTimeCache(LIGHT_TIME_REFRESH_SEC)


# -------------------- MULTICAST SOCKET HELPERS -------------------- #

def create_multicast_rx_socket(group: str, port: int, iface_ip: str) -> socket.socket:
    """
    Create a UDP socket bound to `group:port` and joined to the multicast group.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind to the port on all interfaces
    sock.bind(("", port))

    # Join multicast group on a specific interface
    mreq = struct.pack("=4s4s", socket.inet_aton(group), socket.inet_aton(iface_ip))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Optional: receive our own sent packets if needed
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

    sock.setblocking(False)
    return sock


def create_multicast_tx_socket(iface_ip: str) -> socket.socket:
    """
    Create a UDP socket for sending to multicast groups.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # Bind outgoing interface for multicast (if specified)
    if iface_ip != "0.0.0.0":
        sock.setsockopt(
            socket.SOL_IP,
            socket.IP_MULTICAST_IF,
            socket.inet_aton(iface_ip),
        )

    # Limit TTL so multicast stays inside your lab network
    ttl_bin = struct.pack("b", 16)  # or 1 for very local
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl_bin)

    sock.setblocking(False)
    return sock


# -------------------- DELAY PIPELINE -------------------- #

async def relay_direction(mapping: LoopMapping):
    """
    One-direction relay:
        src_group:src_port  --(delay by light-time)-->  dst_group:dst_port
    """

    rx_sock = create_multicast_rx_socket(
        mapping.src_group, mapping.src_port, mapping.iface_ip
    )
    tx_sock = create_multicast_tx_socket(mapping.iface_ip)

    print(
        f"[{mapping.name}] Listening on {mapping.src_group}:{mapping.src_port}, "
        f"sending to {mapping.dst_group}:{mapping.dst_port}"
    )

    loop = asyncio.get_running_loop()
    queue: asyncio.PriorityQueue[Tuple[float, bytes]] = asyncio.PriorityQueue()

    async def rx_task():
        while True:
            try:
                data, addr = await loop.run_in_executor(
                    None, rx_sock.recvfrom, MAX_PACKET_SIZE
                )
            except OSError as e:
                print(f"[{mapping.name}] RX socket error: {e}")
                await asyncio.sleep(1.0)
                continue

            delay = light_time_cache.current()
            send_time = time.monotonic() + delay
            await queue.put((send_time, data))

    async def tx_task():
        while True:
            send_time, data = await queue.get()
            now = time.monotonic()
            sleep_time = send_time - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            try:
                await loop.run_in_executor(
                    None,
                    tx_sock.sendto,
                    data,
                    (mapping.dst_group, mapping.dst_port),
                )
            except OSError as e:
                print(f"[{mapping.name}] TX socket error: {e}")
                await asyncio.sleep(0.1)

    await asyncio.gather(rx_task(), tx_task())


async def main():
    # Only load SPICE kernels in real mode (no DEV override)
    if DEV_DELAY_SECONDS <= 0:
        load_spice_kernels()

    tasks = [asyncio.create_task(relay_direction(m)) for m in LOOP_MAPPINGS]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down delay bot...")
    finally:
        try:
            # Be nice and unload kernels
            if DEV_DELAY_SECONDS <= 0:
                spice.kclear()
        except Exception:
            pass
