"""At home or away, for a network offload target: an sftp target is present only while the route
to it leaves through a local interface. Away from home the NAS still answers, but over the VPN,
and a full upload over a tunnel is what must not happen."""
from __future__ import annotations

import ipaddress
from pathlib import Path
from typing import Any

import pytest
from src.actions.core import Actions
from src.orchestrator import network_presence, offload_runner, recovery_copies
from src.orchestrator.backup_settings import write_backup_settings
from src.orchestrator.network_presence import is_tunnel, lan_presence, route_interface, sftp_host

NAS = "192.168.4.198"
AT_HOME = f"{NAS} dev wlp0s20f3 src 192.168.4.57 uid 1000\n    cache\n"
AWAY = f"{NAS} dev tailscale0 table 52 src 100.101.102.103 uid 1000\n    cache\n"
# public wifi, the Tailscale route gone: the private address falls to the default gateway
VIA_GATEWAY = f"{NAS} via 172.20.20.1 dev wlp0s20f3 src 172.20.20.5 uid 1000\n    cache\n"


@pytest.fixture(autouse=True)
def _receipts_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(offload_runner._RECEIPTS_ENV, str(tmp_path / "receipts.json"))
    monkeypatch.setenv("OSIRIS_RESTIC_PASSWORD", "a-test-password")


def _route(monkeypatch: pytest.MonkeyPatch, text: str | None,
           nets: Any = None) -> list[str]:
    """Fake `ip route get`: records the address it was asked about."""
    asked: list[str] = []

    def _fake(address: str) -> str | None:
        asked.append(address)
        return text

    monkeypatch.setattr(network_presence, "resolve_address", lambda host: NAS)
    monkeypatch.setattr(network_presence, "_run_ip_route_get", _fake)
    # the interface's own subnets: unknown unless a test says (never ask the real `ip`)
    monkeypatch.setattr(network_presence, "_dev_networks", lambda dev: nets)
    return asked


def _target(**extra: Any) -> dict[str, Any]:
    return {"name": "nas", "kind": "restic", "path_or_url": "sftp:truenas:/mnt/Bunker/restic",
            "schedule": "*-*-* 03:00:00", "enabled": True, **extra}


def test_the_host_of_an_sftp_repository_is_found_in_each_spelling() -> None:
    assert sftp_host("sftp:truenas:/mnt/Bunker/restic") == "truenas"
    assert sftp_host("sftp:backup@192.168.4.198:/srv/r") == "192.168.4.198"
    assert sftp_host("sftp://backup@nas.lan:2222/srv/r") == "nas.lan"
    assert sftp_host("sftp://[fe80::1]/srv/r") == "fe80::1"
    assert sftp_host("s3:s3.amazonaws.com/bucket") is None
    assert sftp_host("/mnt/local/repo") is None


def test_a_tunnel_is_told_by_its_interface_name_or_its_routing_table() -> None:
    assert route_interface(AWAY) == {"dev": "tailscale0", "table": "52", "via": None}
    assert route_interface(AT_HOME) == {"dev": "wlp0s20f3", "table": None, "via": None}
    assert route_interface(VIA_GATEWAY)["via"] == "172.20.20.1"
    for dev in ("tailscale0", "wg0", "tun0", "tap1", "utun3", "ppp0", "zt1"):
        assert is_tunnel(dev, None) is True
    assert is_tunnel("wlp0s20f3", None) is False
    assert is_tunnel("eth0", None) is False
    assert is_tunnel("eth0", "52") is True  # Tailscale's table, whatever the device is called


def test_at_home_the_nas_is_present(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _route(monkeypatch, AT_HOME)
    assert lan_presence(_target()) == {"present": True, "reason": None, "dev": "wlp0s20f3"}
    assert asked == [NAS]


def test_away_the_nas_is_absent_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    _route(monkeypatch, AWAY)
    out = lan_presence(_target())
    assert out["present"] is False and out["reason"] == "reachable only over tailscale0"
    assert out["table"] == "52" and out["address"] == NAS


def test_a_target_can_opt_in_to_the_tunnel(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _route(monkeypatch, AWAY)
    assert lan_presence(_target(allow_tunnel=True))["present"] is True
    assert asked == []  # the check is skipped, not just overruled


def test_allow_tunnel_false_still_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    _route(monkeypatch, AWAY)
    assert lan_presence(_target(allow_tunnel=False))["present"] is False


def test_not_being_able_to_tell_leaves_the_old_behaviour(monkeypatch: pytest.MonkeyPatch) -> None:
    _route(monkeypatch, None)  # no `ip` binary
    assert lan_presence(_target())["present"] is True
    monkeypatch.setattr(network_presence, "resolve_address", lambda host: None)
    assert lan_presence(_target())["present"] is True  # the name did not resolve


def test_an_on_link_route_in_the_same_subnet_is_home(monkeypatch: pytest.MonkeyPatch) -> None:
    _route(monkeypatch, AT_HOME, nets=[ipaddress.ip_network("192.168.4.0/24")])
    assert lan_presence(_target())["present"] is True


def test_a_route_through_a_gateway_is_away_even_though_it_is_not_a_tunnel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE INCIDENT (2026-11-05 10:00-10:17, public wifi): the Tailscale route dropped and the
    private address went to the default gateway, which the old check read as home."""
    _route(monkeypatch, VIA_GATEWAY)
    out = lan_presence(_target())
    assert out["present"] is False
    assert out["reason"] == ("not on the local network (the route to 192.168.4.198 goes via "
                             "172.20.20.1 on wlp0s20f3)")
    assert out["via"] == "172.20.20.1" and out["address"] == NAS


def test_an_interface_whose_subnet_does_not_hold_the_address_is_away(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _route(monkeypatch, AT_HOME, nets=[ipaddress.ip_network("172.20.20.0/22")])
    out = lan_presence(_target())
    assert out["present"] is False and "not on the local network" in str(out["reason"])


def test_no_route_at_all_is_away(monkeypatch: pytest.MonkeyPatch) -> None:
    _route(monkeypatch, "")  # `ip` ran and found no route (network unreachable)
    out = lan_presence(_target())
    assert out["present"] is False and out["reason"] == f"no route to {NAS}"


def test_allow_tunnel_still_overrides_a_gateway_route(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _route(monkeypatch, VIA_GATEWAY)
    assert lan_presence(_target(allow_tunnel=True))["present"] is True
    assert asked == []


def test_a_public_address_through_the_default_gateway_is_still_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Home means nothing for a public address: only a tunnel makes it absent."""
    _route(monkeypatch, "8.8.8.8 via 172.20.20.1 dev wlp0s20f3 src 172.20.20.5\n")
    monkeypatch.setattr(network_presence, "resolve_address", lambda host: "8.8.8.8")
    assert lan_presence(_target())["present"] is True


def test_unreachable_failures_are_told_apart_from_a_bad_repository() -> None:
    from src.orchestrator.network_presence import is_unreachable_error

    assert is_unreachable_error("ssh: connect to host 192.168.4.198 port 22: No route to host")
    assert is_unreachable_error("connect: Network is unreachable")
    assert not is_unreachable_error("Fatal: repository contains errors")
    assert not is_unreachable_error(None)


async def test_a_drill_that_hits_no_route_to_host_records_a_skip_not_a_failed_drill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.orchestrator import scheduled_drill, soul_key

    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(soul_key, "record_restore_drill",
                        lambda url, **kw: recorded.append({"url": url, **kw}))
    monkeypatch.setattr(soul_key, "restore_drill_receipts", lambda: {})
    monkeypatch.setattr(scheduled_drill, "drill_due", lambda *a, **k: True)

    def _drill(url: str) -> str | None:
        return "ssh: connect to host 192.168.4.198 port 22: No route to host"

    out = await scheduled_drill.run_due_drills([_target()], {"nas": {}}, run_drill=_drill)
    assert out == [{"name": "nas", "skipped": "target not reachable from this network"}]
    assert recorded == []  # no failed-attempt receipt, so no six-hour retry window either

    def _bad(url: str) -> str | None:
        return "repository contains errors"

    out = await scheduled_drill.run_due_drills([_target()], {"nas": {}}, run_drill=_bad)
    assert out[0]["ok"] is False and recorded and recorded[0]["ok"] is False


def test_only_sftp_targets_are_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    asked = _route(monkeypatch, AWAY)
    assert lan_presence(_target(path_or_url="s3:s3.amazonaws.com/bucket"))["present"] is True
    assert asked == []


# --- the real tick ------------------------------------------------------------------------

async def _nas(actions: Actions, **extra: Any) -> None:
    await write_backup_settings(
        actions.pool, actor="operator", because="x", offload_targets=[_target(**extra)])


async def test_the_tick_skips_a_tunnel_target_quietly_and_keeps_its_last_success(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _nas(actions)
    offload_runner._write_receipt(
        "nas", {"last_successful_offload": "T1", "last_attempt_at": "T1", "last_error": None})
    calls: list[str] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: calls.append(kw["repository"]))
    _route(monkeypatch, AWAY)

    out = await offload_runner.run_offload_tick(actions.pool)

    assert out["targets"] == [{"name": "nas", "skipped": "reachable only over tailscale0"}]
    assert calls == []
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_skip_reason"] == "reachable only over tailscale0"
    assert receipt["last_successful_offload"] == "T1"  # a skip never clobbers a success
    assert receipt["last_error"] is None  # and is not reported as a failure


async def test_the_tick_treats_no_route_to_host_as_a_quiet_skip_and_keeps_the_last_success(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE INCIDENT'S OTHER HALF: the presence check said home, restic could not reach the
    NAS. That is away, not a failed backup: no last_error, the last success untouched."""
    await _nas(actions)
    offload_runner._write_receipt(
        "nas", {"last_successful_offload": "T1", "last_attempt_at": "T1", "last_error": None})
    monkeypatch.setattr(
        offload_runner, "_run_restic_backup",
        lambda **kw: "unable to open repository: ssh: connect to host 192.168.4.198 port 22: "
                     "No route to host")
    _route(monkeypatch, AT_HOME)

    out = await offload_runner.run_offload_tick(actions.pool)

    assert out["targets"] == [{"name": "nas",
                               "skipped": "target not reachable from this network"}]
    receipt = offload_runner.offload_receipts()["nas"]
    assert receipt["last_skip_reason"] == "target not reachable from this network"
    assert receipt["last_successful_offload"] == "T1"
    assert receipt["last_error"] is None and receipt["last_attempt_at"] == "T1"


async def test_the_tick_still_records_a_real_backup_failure(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _nas(actions)
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: "Fatal: repository contains errors")
    _route(monkeypatch, AT_HOME)

    out = await offload_runner.run_offload_tick(actions.pool)

    assert out["targets"][0]["ok"] is False
    assert offload_runner.offload_receipts()["nas"]["last_error"] == (
        "Fatal: repository contains errors")


async def test_the_tick_uploads_at_home_and_clears_the_skip_reason(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _nas(actions)
    offload_runner._write_receipt("nas", {"last_skip_reason": "reachable only over tailscale0"})
    calls: list[str] = []

    def _backup(**kw: Any) -> None:
        calls.append(kw["repository"])

    monkeypatch.setattr(offload_runner, "_run_restic_backup", _backup)
    _route(monkeypatch, AT_HOME)

    out = await offload_runner.run_offload_tick(actions.pool)

    assert out["targets"] == [{"name": "nas", "ok": True}]
    assert calls == ["sftp:truenas:/mnt/Bunker/restic"]
    assert offload_runner.offload_receipts()["nas"]["last_skip_reason"] is None


async def test_the_tick_uploads_over_a_tunnel_when_the_target_allows_it(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _nas(actions, allow_tunnel=True)
    calls: list[str] = []
    monkeypatch.setattr(offload_runner, "_run_restic_backup",
                        lambda **kw: calls.append(kw["repository"]))
    _route(monkeypatch, AWAY)
    out = await offload_runner.run_offload_tick(actions.pool)
    assert out["targets"] == [{"name": "nas", "ok": True}] and len(calls) == 1


async def test_recovery_copies_and_drills_also_skip_a_tunnel_target(
    actions: Actions, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _nas(actions)
    _route(monkeypatch, AWAY)
    assert await recovery_copies.present_targets(actions.pool) == []
    _route(monkeypatch, AT_HOME)
    assert [t["name"] for t in await recovery_copies.present_targets(actions.pool)] == ["nas"]


async def test_the_setting_accepts_only_a_boolean_allow_tunnel(actions: Actions) -> None:
    ok = await write_backup_settings(
        actions.pool, actor="operator", because="x", offload_targets=[_target(allow_tunnel=True)])
    assert "error" not in ok
    bad = await write_backup_settings(
        actions.pool, actor="operator", because="x",
        offload_targets=[_target(allow_tunnel="yes")])
    assert "error" in bad
