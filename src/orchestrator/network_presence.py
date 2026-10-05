"""AT HOME OR AWAY, FOR A NETWORK OFFLOAD TARGET: the operator's rule is that the NAS is an
offload target only while the laptop is on the home network. Away from home the NAS still
answers, but over the VPN, and a full vault upload (hundreds of GB) over a tunnel is exactly
what must not happen. So presence for an sftp target is not "does it answer" but "does the
route to it leave through a local interface".

The check asks the kernel, never the network: `ip route get <address>` names the interface
(`dev`) and routing table the next packet would use. A tunnel interface (tailscale*, wg*,
tun*, tap*, utun*, ppp*, zt*) or the table Tailscale installs its routes in (52) means "away".
A target that opts in with `allow_tunnel: true` skips the check. Any failure to find out (no
`ip` binary, an address that will not resolve) leaves the old behaviour in place: try, and let
the real transfer report what it finds; only a route that is positively a tunnel stops the
upload.

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
    """{'dev': ..., 'table': ...} out of an `ip route get` line (either may be None)."""
    dev = re.search(r"\bdev\s+(\S+)", route_text)
    table = re.search(r"\btable\s+(\S+)", route_text)
    return {"dev": dev.group(1) if dev else None, "table": table.group(1) if table else None}


def is_tunnel(dev: str | None, table: str | None) -> bool:
    if table in TUNNEL_ROUTE_TABLES:
        return True
    return bool(dev) and str(dev).lower().startswith(TUNNEL_DEVICE_PREFIXES)


def lan_presence(target: dict[str, Any]) -> dict[str, Any]:
    """{'present': bool, 'reason': str | None} for a restic target. Only an sftp target whose
    route positively leaves through a tunnel is absent ('reachable only over tailscale0');
    everything else, including not being able to tell, is present and left to the transfer."""
    if target.get("allow_tunnel") is True:
        return {"present": True, "reason": None}
    host = sftp_host(str(target.get("path_or_url") or ""))
    if host is None:
        return {"present": True, "reason": None}
    address = resolve_address(host)
    if address is None:
        return {"present": True, "reason": None}
    text = _run_ip_route_get(address)
    if not text:
        return {"present": True, "reason": None}
    where = route_interface(text)
    if is_tunnel(where["dev"], where["table"]):
        via = where["dev"] or f"routing table {where['table']}"
        return {"present": False, "reason": f"reachable only over {via}",
                "dev": where["dev"], "table": where["table"], "address": address}
    return {"present": True, "reason": None, "dev": where["dev"]}
