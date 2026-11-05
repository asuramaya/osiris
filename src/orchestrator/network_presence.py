"""AT HOME OR AWAY, FOR A NETWORK OFFLOAD TARGET: the operator's rule is that the NAS is an
offload target only while the laptop is on the home network. Away from home the NAS still
answers, but over the VPN, and a full vault upload (hundreds of GB) over a tunnel is exactly
what must not happen. So presence for an sftp target is not "does it answer" but "does the
route to it leave through a local interface".

The check asks the kernel, never the network: `ip route get <address>` names the interface
(`dev`), the routing table and any gateway (`via`) the next packet would use. A target on a
private address is at home only when that route is ON-LINK: no `via` gateway, on a local
interface whose own subnet holds the address. Away is everything else the kernel can tell us:
a tunnel interface (tailscale*, wg*, tun*, tap*, utun*, ppp*, zt*) or the table Tailscale
installs its routes in (52); a route through a gateway (public wifi, or a tunnel that dropped
and left the default route to carry the private address into someone else's network); an
interface whose subnet does not hold the address; no route at all. The incident this closes:
on public wifi the Tailscale route briefly vanished, the address fell to the default gateway,
and "not a tunnel" was read as "home". A target that opts in with `allow_tunnel: true` skips
the check. Not being able to ask at all (no `ip` binary, an address that will not resolve)
leaves the old behaviour in place: try, and let the real transfer report what it finds.
A public address keeps the old rule (only a tunnel is away): "home" has no meaning for it.

The only lookups are the ones that give the address a name: `ssh -G` (reads ssh config, no
connection) and the resolver."""
from __future__ import annotations

import ipaddress
import re
import socket
import subprocess
from typing import Any

TUNNEL_DEVICE_PREFIXES = ("tailscale", "wg", "tun", "tap", "utun", "ppp", "zt")
TUNNEL_ROUTE_TABLES = ("52",)  # the table Tailscale installs its routes in
_LOOKUP_TIMEOUT_SECS = 5

_SFTP_URL = re.compile(r"^sftp://(?:[^@/]+@)?(?P<host>\[[^\]]+\]|[^:/]+)", re.IGNORECASE)
_SFTP_SCP = re.compile(r"^sftp:(?:[^@:/]+@)?(?P<host>[^:/]+):", re.IGNORECASE)


def sftp_host(url: str) -> str | None:
    """The host (or ssh alias) of an sftp restic repository: `sftp:truenas:/mnt/x`,
    `sftp:user@host:/path` or `sftp://user@host:22/path`. None for any other backend."""
    if not isinstance(url, str) or not url.lower().startswith("sftp:"):
        return None
    m = _SFTP_URL.match(url) or _SFTP_SCP.match(url)
    return m.group("host").strip("[]") if m else None


def _ssh_hostname(alias: str) -> str:
    """What ssh would connect to for `alias` (its config's HostName), without connecting."""
    try:
        out = subprocess.run(
            ["ssh", "-G", alias], capture_output=True, text=True, timeout=_LOOKUP_TIMEOUT_SECS,
            check=False).stdout
    except (OSError, subprocess.SubprocessError):
        return alias
    for line in out.splitlines():
        if line.startswith("hostname "):
            return line.split(None, 1)[1].strip() or alias
    return alias


def resolve_address(host: str) -> str | None:
    """An IP for `host` (an ssh alias, a name or a literal address), or None."""
    name = _ssh_hostname(host)
    try:
        return str(ipaddress.ip_address(name))
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(name, None, type=socket.SOCK_STREAM)
    except OSError:
        return None
    return str(infos[0][4][0]) if infos else None


def _run_ip_route_get(address: str) -> str | None:
    """The text of `ip route get <address>`, or None when it cannot be asked."""
    try:
        proc = subprocess.run(
            ["ip", "route", "get", address], capture_output=True, text=True,
            timeout=_LOOKUP_TIMEOUT_SECS, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return proc.stdout if proc.returncode == 0 else ""


def route_interface(route_text: str) -> dict[str, str | None]:
    """{'dev': ..., 'table': ..., 'via': ...} out of an `ip route get` line (each may be
    None). `via` is the gateway the packet is handed to; None means the destination is
    directly connected."""
    dev = re.search(r"\bdev\s+(\S+)", route_text)
    table = re.search(r"\btable\s+(\S+)", route_text)
    via = re.search(r"\bvia\s+(\S+)", route_text)
    return {"dev": dev.group(1) if dev else None, "table": table.group(1) if table else None,
            "via": via.group(1) if via else None}


def _dev_networks(dev: str) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network] | None:
    """The subnets interface `dev` is directly attached to, or None when that cannot be asked."""
    try:
        proc = subprocess.run(
            ["ip", "-o", "addr", "show", "dev", dev], capture_output=True, text=True,
            timeout=_LOOKUP_TIMEOUT_SECS, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for m in re.finditer(r"\binet6?\s+(\S+)", proc.stdout):
        try:
            nets.append(ipaddress.ip_interface(m.group(1)).network)
        except ValueError:
            continue
    return nets or None


def is_tunnel(dev: str | None, table: str | None) -> bool:
    if table in TUNNEL_ROUTE_TABLES:
        return True
    return bool(dev) and str(dev).lower().startswith(TUNNEL_DEVICE_PREFIXES)


def lan_presence(target: dict[str, Any]) -> dict[str, Any]:
    """{'present': bool, 'reason': str | None} for a restic target. An sftp target on a private
    address is present only on an on-link route (see the module note); a public address is
    absent only over a tunnel. Not being able to find out is present, and left to the
    transfer."""
    if target.get("allow_tunnel") is True:
        return {"present": True, "reason": None}
    host = sftp_host(str(target.get("path_or_url") or ""))
    if host is None:
        return {"present": True, "reason": None}
    address = resolve_address(host)
    if address is None:
        return {"present": True, "reason": None}
    text = _run_ip_route_get(address)
    if text is None:
        return {"present": True, "reason": None}
    local_only = not ipaddress.ip_address(address).is_global
    if not text.strip():
        # `ip` answered and there is no route (network unreachable): nothing to upload over.
        if local_only:
            return {"present": False, "reason": f"no route to {address}", "address": address}
        return {"present": True, "reason": None}
    where = route_interface(text)
    if is_tunnel(where["dev"], where["table"]):
        via = where["dev"] or f"routing table {where['table']}"
        return {"present": False, "reason": f"reachable only over {via}",
                "dev": where["dev"], "table": where["table"], "address": address}
    if local_only:
        if where["via"]:
            return {"present": False, "address": address, "dev": where["dev"],
                    "via": where["via"],
                    "reason": f"not on the local network (the route to {address} goes via "
                              f"{where['via']} on {where['dev'] or 'an unnamed interface'})"}
        nets = _dev_networks(where["dev"]) if where["dev"] else None
        if nets and not any(ipaddress.ip_address(address) in n for n in nets
                            if n.version == ipaddress.ip_address(address).version):
            return {"present": False, "address": address, "dev": where["dev"],
                    "reason": f"not on the local network ({where['dev']} is not on the "
                              f"subnet of {address})"}
    return {"present": True, "reason": None, "dev": where["dev"]}


_UNREACHABLE_MARKERS = ("no route to host", "network is unreachable", "network unreachable")


def is_unreachable_error(text: str | None) -> bool:
    """Whether a transfer or check failed because the network had no way to the target (as
    opposed to the repository being bad). That is "away", not a failed backup or drill."""
    low = (text or "").lower()
    return any(marker in low for marker in _UNREACHABLE_MARKERS)
