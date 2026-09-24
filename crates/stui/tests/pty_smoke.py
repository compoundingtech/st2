#!/usr/bin/env python3
"""Exercise the built TUI against a running local st3 daemon in a real PTY.

Run with ST3_PERSON=person/<you> python3 crates/stui/tests/pty_smoke.py.
The script reports only checks and byte counts; it never prints terminal contents.
"""

import fcntl
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import time


def run_case(binary: str, ending: str) -> None:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"
    if ending == "panic":
        env["STUI_TEST_PANIC_AFTER_ENTER"] = "1"
    proc = subprocess.Popen([binary], stdin=slave, stdout=slave, stderr=slave, env=env)
    os.close(slave)

    def collect(seconds: float) -> bytes:
        output = bytearray()
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    output.extend(os.read(master, 65536))
                except OSError:
                    break
        return bytes(output)

    initial = bytearray()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        initial.extend(collect(1))
        if ending == "panic" or all(label in initial for label in (b"Now", b"Chat", b"Control", b"Fleet")):
            break
        if proc.poll() is not None:
            break
    if ending != "panic":
        assert all(label in initial for label in (b"Now", b"Chat", b"Control", b"Fleet")), "missing live views"
        os.write(master, b"2341")
        changed = collect(4)
        assert changed, "keyboard navigation did not redraw"
        idle = collect(3)
        assert not idle, f"idle redraw emitted {len(idle)} bytes"
        if ending == "normal":
            os.write(master, b"q")
        else:
            proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise AssertionError(f"{ending}: TUI did not exit")
    final = collect(1)
    os.close(master)
    assert b"\x1b[?1049l" in initial + final, f"{ending}: alternate screen was not restored"
    assert (proc.returncode == 0) == (ending != "panic"), f"{ending}: unexpected exit {proc.returncode}"
    print(f"{ending}: restoration OK" if ending == "panic" else f"{ending}: views, keyboard, idle, restoration OK")


if __name__ == "__main__":
    binary = sys.argv[1] if len(sys.argv) > 1 else "target/debug/stui"
    for case in ("normal", "signal", "panic"):
        run_case(binary, case)
