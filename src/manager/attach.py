"""`osiris attach <name>` — the debug door, NEVER the deliverable (doctrine 4, `5cd5b7b6`: the
unified face replacing Warp is the milestone; this is plumbing so the PTY broker is testable and
usable before that face exists). §4.5 names it explicitly for exactly this reason.

WIRE PROTOCOL — server is the manager daemon's control socket (`src.manager.daemon.Manager`,
`default_socket_path()`), which hands the framed half off to `src.manager.pty_broker.serve_
attached` the instant a `pty_attach` request resolves; this file is the client:
  1. Connect to the daemon's control socket (never a dedicated per-session socket — the daemon
     is the one thing holding every named PTY, per §4.5).
  2. Send ONE newline-terminated JSON control request, in the exact same style as every other
     daemon op (`status`/`bodies`/`dissolve`):
       `{"op": "pty_attach", "name": <str>, "rows": <int>, "cols": <int>}`
  3. The daemon answers with ONE JSON line: `{"attached": <name>}` on success — the connection
     now switches into the framed pty stream described below — or `{"error": <str>}` (missing
     or unknown name), in which case this client prints it and exits; nothing further arrives.
  4. From the `{"attached": ...}` line on, everything is a sequence of length-prefixed frames
     (`pty_broker.pack_frame`/`read_frame`): 1 byte type tag + 4-byte big-endian length + that
     many bytes of payload.
       'O' (server -> client): raw pty output — the replay first, then live, byte for byte.
       'X' (server -> client): sent once, JSON `{"returncode": int|null}` — the exited marker.
       'I' (client -> server): raw bytes to write to the pty (this client's stdin).
       'R' (client -> server): an out-of-band resize, JSON `{"rows": int, "cols": int}` —
           sent whenever THIS client's own terminal resizes (SIGWINCH), so the remote pty
           tracks it.
  An 'O'/'I' payload is never JSON; an 'X'/'R' payload always is — the type tag makes the two
  structurally impossible to confuse, which is the whole reason the stream is framed at all
  instead of staying fully raw after the `{"attached": ...}` ack.

This client is a plain byte bridge: stdin (raw mode — see `_run`) becomes 'I' frames out, 'O'
frames become stdout, an 'X' frame prints a marker and exits. It does not parse the pty's
output as a terminal — that job belongs to whatever real terminal the operator is running THIS
client inside of, per the PTY lane's own rule (SPEC §2: "NEVER a chat widget parsing harness
output").
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import signal
import sys
import termios
import tty

from src.manager.daemon import default_socket_path
from src.manager.pty_broker import (
    FRAME_TYPE_EXITED,
    FRAME_TYPE_INPUT,
    FRAME_TYPE_OUTPUT,
    FRAME_TYPE_RESIZE,
    pack_frame,
    read_frame,
)

_STDIN_READ_CHUNK = 65536


async def _run(socket_path: str, name: str) -> int:
    reader, writer = await asyncio.open_unix_connection(socket_path)
    size = shutil.get_terminal_size()
    req = {"op": "pty_attach", "name": name, "rows": size.lines, "cols": size.columns}
    writer.write((json.dumps(req) + "\n").encode())
    await writer.drain()

    ack_line = await reader.readline()
    if not ack_line:
        sys.stderr.write("osiris attach: daemon closed the connection before replying\n")
        writer.close()
        return 1
    ack = json.loads(ack_line.decode())
    if "error" in ack:
        sys.stderr.write(f"osiris attach: {ack['error']}\n")
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
        return 1

    loop = asyncio.get_running_loop()
    stdin_fd = sys.stdin.fileno()
    stdout_fd = sys.stdout.fileno()
    old_settings = termios.tcgetattr(stdin_fd)
    exit_code = 0

    def _on_stdin() -> None:
        try:
            data = os.read(stdin_fd, _STDIN_READ_CHUNK)
        except OSError:
            return
        if not data:
            return
        writer.write(pack_frame(FRAME_TYPE_INPUT, data))

    def _on_winch(*_: object) -> None:
        new_size = shutil.get_terminal_size()
        payload = json.dumps({"rows": new_size.lines, "cols": new_size.columns}).encode()
        writer.write(pack_frame(FRAME_TYPE_RESIZE, payload))

    try:
        tty.setraw(stdin_fd)
        loop.add_reader(stdin_fd, _on_stdin)
        with contextlib.suppress(NotImplementedError, RuntimeError):
            # NotImplementedError on platforms without signal-in-event-loop support (not this
            # module's target, but a debug CLI degrading to "no live resize" beats a crash);
            # RuntimeError if SIGWINCH isn't available at all (non-Unix — same reasoning).
            loop.add_signal_handler(signal.SIGWINCH, _on_winch)

        while True:
            frame = await read_frame(reader)
            if frame is None:
                break
            frame_type, payload = frame
            if frame_type == FRAME_TYPE_OUTPUT:
                os.write(stdout_fd, payload)
            elif frame_type == FRAME_TYPE_EXITED:
                info = json.loads(payload.decode())
                rc = info.get("returncode")
                sys.stderr.write(f"\r\n[osiris attach: session exited, rc={rc}]\r\n")
                break
    finally:
        loop.remove_reader(stdin_fd)
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
            loop.remove_signal_handler(signal.SIGWINCH)
        termios.tcsetattr(stdin_fd, termios.TCSADRAIN, old_settings)
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
    return exit_code


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: python -m src.manager.attach <name>", file=sys.stderr)
        return 2
    name = args[0]
    socket_path = str(default_socket_path())
    try:
        return asyncio.run(_run(socket_path, name))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
