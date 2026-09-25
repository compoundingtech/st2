#!/usr/bin/env python3
"""Exercise the built TUI against a running local st3 daemon in a real PTY.

Run with ST3_PERSON=person/<you> python3 crates/stui/tests/pty_smoke.py.
The script reports only checks and byte counts; it never prints terminal contents.
"""

import fcntl
import os
import pty
import re
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import termios
import threading
import time


def plain(output: bytes) -> bytes:
    return re.sub(rb"\x1b\[[0-9;?]*[ -/]*[@-~]", b"", output)


def run_case(binary: str, ending: str, endpoint: str | None = None) -> None:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"
    if endpoint:
        env["ST3_ENDPOINT"] = endpoint
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

    def wait_for(marker: bytes, timeout: float) -> tuple[bytes, float]:
        started = time.monotonic()
        output = bytearray()
        while time.monotonic() - started < timeout:
            ready, _, _ = select.select([master], [], [], 0.02)
            if ready:
                try:
                    output.extend(os.read(master, 65536))
                except OSError:
                    break
                if marker in (output if marker.startswith(b"\x1b") else plain(output)):
                    break
        return bytes(output), time.monotonic() - started

    initial, first_frame = wait_for(b"\x1b[?1049h" if ending == "panic" else b"Now", 2)
    assert first_frame < 1, f"first frame took {first_frame:.3f}s"
    if ending != "panic":
        for key in (b"2", b"3", b"4", b"1"):
            os.write(master, key)
            changed, latency = wait_for(b"", 1)
            assert changed, f"{key!r} did not redraw"
            assert latency < 0.5, f"{key!r} navigation took {latency:.3f}s"
        os.write(master, b"v")
        selection, _ = wait_for(b"\x1b[?1000l", 1)
        assert b"\x1b[?1000l" in selection, "selection mode did not release mouse capture"
        os.write(master, b"v")
        mouse, _ = wait_for(b"\x1b[?1000h", 1)
        assert b"\x1b[?1000h" in mouse, "selection mode did not restore mouse capture"
        collect(1)  # A live background snapshot may redraw after navigation.
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
    print(f"{ending}: restoration OK" if ending == "panic" else f"{ending}: first frame {first_frame:.3f}s, keys <0.5s, restoration OK")


def delayed_getter_case(binary: str) -> None:
    with tempfile.TemporaryDirectory(prefix="stui-delay-") as directory:
        endpoint = os.path.join(directory, "slow.sock")
        server = socket.socket(socket.AF_UNIX)
        server.bind(endpoint)
        server.listen(5)
        server.settimeout(0.2)
        stopping = threading.Event()

        def serve() -> None:
            while not stopping.is_set():
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                # Keep the shared getter outstanding while the TUI handles keys.
                threading.Thread(target=lambda connection=connection: (time.sleep(5), connection.close()), daemon=True).start()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        try:
            run_case(binary, "normal", endpoint)
        finally:
            stopping.set()
            server.close()
            thread.join(timeout=1)


def hangup_case(binary: str) -> None:
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    proc = subprocess.Popen([binary], stdin=slave, stdout=slave, stderr=slave,
                            env={**os.environ, "TERM": "xterm-256color"})
    os.close(slave)
    deadline = time.monotonic() + 3
    output = bytearray()
    while time.monotonic() < deadline:
        ready, _, _ = select.select([master], [], [], 0.1)
        if ready:
            output.extend(os.read(master, 65536))
            if b"Now" in plain(output):
                break
    else:
        proc.kill()
        raise AssertionError("hangup: no first frame")
    time.sleep(10)  # Exercise hangup after the live snapshot and event poll are running.
    os.close(master)
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise AssertionError("hangup: TUI survived terminal close")
    print("hangup: exited after PTY close")


if __name__ == "__main__":
    binary = sys.argv[1] if len(sys.argv) > 1 else "target/debug/stui"
    skip_panic = "--no-panic" in sys.argv[2:]
    for case in ("normal", "signal", "panic"):
        if case == "panic" and skip_panic:
            print("panic: skipped (release binary has no debug panic hook)")
            continue
        run_case(binary, case)
    delayed_getter_case(binary)
    hangup_case(binary)
