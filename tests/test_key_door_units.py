"""THE FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail 13065) — the shipped
systemd unit files themselves actually parse, and carry `ImportCredential=`
(never the old `LoadCredentialEncrypted=<name>:<hard path>` shape, which fails a
unit's own start outright until the credential exists — the exact bootstrap
deadlock this ruling fixes). Verified with the REAL `systemd-analyze --user verify`
on this box, not a hand-rolled INI parser — the same tool the operator's own
`systemctl --user` would use to reject a broken unit."""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_DEPLOY_DIR = Path(__file__).resolve().parent.parent / "deploy"

_KEY_DOOR_UNITS = (
    _DEPLOY_DIR / "user" / "osiris-mcp.service",
    _DEPLOY_DIR / "user" / "osiris-worker.service",
    _DEPLOY_DIR / "osiris-offload.service",
    _DEPLOY_DIR / "osiris-offload.timer",
)


@pytest.mark.skipif(shutil.which("systemd-analyze") is None,
                    reason="systemd-analyze not installed on this box")
@pytest.mark.parametrize("unit_path", _KEY_DOOR_UNITS, ids=lambda p: p.name)
def test_unit_file_verifies_clean(unit_path: Path) -> None:
    result = subprocess.run(
        ["systemd-analyze", "--user", "verify", str(unit_path)],
        capture_output=True, text=True, timeout=30, check=False)
    # systemd-analyze verify also loads every OTHER unit already installed on this
    # box (a real dev machine) and reports issues with THOSE too — only lines
    # naming THIS file are this test's own concern.
    own_lines = [line for line in result.stderr.splitlines() if unit_path.name in line]
    assert own_lines == [], f"{unit_path.name} did not verify clean:\n" + "\n".join(own_lines)


@pytest.mark.parametrize("unit_path,cred_name", [
    (_DEPLOY_DIR / "user" / "osiris-mcp.service", "soul.key"),
    (_DEPLOY_DIR / "user" / "osiris-worker.service", "soul.key"),
    (_DEPLOY_DIR / "osiris-offload.service", "restic.password"),
], ids=["osiris-mcp.service", "osiris-worker.service", "osiris-offload.service"])
def test_unit_uses_import_credential_never_the_old_hard_path_shape(
    unit_path: Path, cred_name: str,
) -> None:
    lines = unit_path.read_text().splitlines()
    directive_lines = [ln for ln in lines if not ln.lstrip().startswith("#")]
    assert f"ImportCredential={cred_name}" in directive_lines
    load_cred = [ln for ln in directive_lines if ln.startswith("LoadCredentialEncrypted=")]
    assert load_cred == [], (
        f"{unit_path.name} still carries a real LoadCredentialEncrypted= directive "
        f"(not just a comment mentioning it) — the exact bootstrap deadlock THE "
        f"FIRST KEY MUST COME FROM THE NORMAL CLI (Thoth mail 13065) fixed: a hard "
        f"path fails the unit's own start outright until the credential already "
        f"exists: {load_cred}")


# --- THE SOUL-KEY BOOT GATE IS A GENUINE REFUSAL AGAIN, not the earlier temporary
# degrade-and-warn tolerance, now that the operator has actually minted the key
# through the normal CLI. ------------------------------------------------------------

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
_BOOT_GATE_FILES = (
    _SRC_DIR / "workers" / "arq_worker.py",
    _SRC_DIR / "mcp_server.py",
)


@pytest.mark.parametrize("src_path", _BOOT_GATE_FILES, ids=lambda p: p.name)
def test_soul_key_boot_gate_never_catches_soul_key_missing(src_path: Path) -> None:
    """`get_soul_fernet()` must be left UNCAUGHT at the primary worker's/MCP server's
    own boot gate: a `try/except SoulKeyMissing` there is exactly the earlier temporary
    bootstrap-tolerance shape, now retired, since the operator has actually minted the
    key. A real catch elsewhere in either of these two large files (neither currently
    has one for any OTHER reason) would also be a real, if accidental, regression back
    to the degraded behavior: caught the same way whether it is this gate's own code or
    something new appended near it."""
    lines = src_path.read_text().splitlines()
    directive_lines = [ln for ln in lines if not ln.lstrip().startswith("#")]
    catches = [ln for ln in directive_lines if "except SoulKeyMissing" in ln]
    assert catches == [], (
        f"{src_path.name} still catches SoulKeyMissing somewhere (not just a comment "
        f"mentioning it): the boot gate must refuse outright again, never "
        f"degrade-and-warn: {catches}")
    imports = [ln for ln in directive_lines if "SoulKeyMissing" in ln and "import" in ln]
    assert imports == [], (
        f"{src_path.name} still imports SoulKeyMissing at all: nothing in this file "
        f"should need to name the exception class once it is never caught: {imports}")
