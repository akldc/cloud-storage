#!/usr/bin/env python3
"""Stage D: multiple fixed chunks, sidecar locking, interruption and resume."""

import filecmp
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
from storage_node_coordinator_test import NODE_ID, stop, wait_listening, wait_node, wait_node_port
from storage_node_test import free_port, head


def wait_for_confirmed(db, process):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        assert process.poll() is None, (process.returncode, process.stderr.read())
        try:
            with sqlite3.connect(db) as connection:
                count = connection.execute("SELECT COUNT(*) FROM chunk_locations").fetchone()[0]
                if count >= 1:
                    row = connection.execute(
                        "SELECT object_id, request_id, upload_id FROM objects WHERE total_size>0"
                    ).fetchone()
                    return row
        except sqlite3.OperationalError:
            pass
        time.sleep(0.03)
    raise AssertionError("first Chunk was not confirmed")


def memory_kib(pid):
    try:
        lines = Path(f"/proc/{pid}/status").read_text().splitlines()
    except FileNotFoundError:
        return 0
    for line in lines:
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


def run_monitored(command, timeout):
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    peak = 0
    try:
        deadline = time.monotonic() + timeout
        while process.poll() is None and time.monotonic() < deadline:
            peak = max(peak, memory_kib(process.pid))
            time.sleep(0.02)
        if process.poll() is None:
            process.kill()
            raise AssertionError("client timed out")
        stdout, stderr = process.communicate(timeout=3)
        return process.returncode, stdout, stderr, peak
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)


def idle_client_rss(client, address, root):
    missing = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    process = subprocess.Popen(
        [client, "download", address, missing, str(root / "never-created")],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    _, status, usage = os.wait4(process.pid, 0)
    process.returncode = os.waitstatus_to_exitcode(status)
    process.communicate(timeout=3)
    assert process.returncode == 1
    return usage.ru_maxrss


def main(coordinator, storage_node, probe, client):
    with tempfile.TemporaryDirectory(prefix="object-resume-") as directory:
        root = Path(directory)
        address = f"127.0.0.1:{free_port()}"
        db = root / "objects.sqlite"
        source = root / "large.bin"
        block = bytes(range(256)) * 256
        with source.open("wb") as file:
            for _ in range(2400):
                file.write(block)
            file.write(b"tail for fixed-size final chunk")
        assert source.stat().st_size > 2 * 64 * 1024 * 1024
        coord = None
        node = None
        first = None
        try:
            coord = subprocess.Popen(
                [coordinator, address, "100", "500", "50", "--db", str(db)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            wait_listening(coord, int(address.rsplit(":", 1)[1]))
            environment = os.environ.copy()
            environment["STORAGE_NODE_DISK_DELAY_MS"] = "2"
            node = subprocess.Popen(
                [storage_node, str(root / "node"), "0", address, NODE_ID, "127.0.0.1",
                 "50", "1000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=environment)
            node_port = wait_node_port(node)
            wait_node(probe, address, "ONLINE")
            baseline = idle_client_rss(client, address, root)

            first = subprocess.Popen([client, "upload", address, str(source)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            state = Path(str(source) + ".object-upload")
            deadline = time.monotonic() + 20
            while not state.exists():
                assert first.poll() is None, first.stderr.read()
                assert time.monotonic() < deadline
                time.sleep(0.01)
            competing = subprocess.run([client, "upload", address, str(source)],
                                       capture_output=True, text=True, timeout=5)
            assert competing.returncode == 1 and "lock" in competing.stderr, competing.stderr
            object_id, request_id, upload_id = wait_for_confirmed(db, first)
            with sqlite3.connect(db) as connection:
                second_chunk = connection.execute(
                    "SELECT chunk_id FROM chunks WHERE object_id=? AND chunk_index=1",
                    (object_id,)).fetchone()[0]
            staging_data = (root / "node" / "staging" /
                            f"{second_chunk}.{upload_id}.1.tmp" / "data.part")
            deadline = time.monotonic() + 20
            while not staging_data.exists() or staging_data.stat().st_size == 0:
                assert first.poll() is None, first.stderr.read()
                assert time.monotonic() < deadline, "second Chunk never started"
                time.sleep(0.01)
            first.kill()
            first.wait(timeout=5)
            deadline = time.monotonic() + 8
            while True:
                code, fields, _ = head(node_port, second_chunk, upload_id)
                if code == 308 and int(fields["x-durable-offset"]) > 0:
                    break
                assert time.monotonic() < deadline, (code, fields)
                time.sleep(0.05)
            assert state.exists()
            assert request_id in state.read_text()
            with sqlite3.connect(db) as connection:
                assert connection.execute("SELECT COUNT(*) FROM chunks").fetchone()[0] == 3
                assert connection.execute("SELECT state FROM objects WHERE upload_id=?",
                                          (upload_id,)).fetchone()[0] == "PREPARING"

            with source.open("r+b") as file:
                original = file.read(1)
                file.seek(0)
                file.write(bytes([original[0] ^ 0xFF]))
            changed = subprocess.run([client, "upload", address, str(source)],
                                     capture_output=True, text=True, timeout=25)
            assert changed.returncode == 1 and "differs" in changed.stderr, changed.stderr
            assert state.exists()
            with source.open("r+b") as file:
                file.write(original)

            code, stdout, stderr, peak = run_monitored(
                [client, "upload", address, str(source)], 120)
            assert code == 0, stderr
            assert stdout.strip() == object_id
            assert not state.exists()
            growth = peak - baseline
            print(f"object_client RSS: idle={baseline} KiB, peak={peak} KiB, "
                  f"growth={growth} KiB", flush=True)
            assert growth <= 32 * 1024, growth

            output = root / "large.download"
            result = subprocess.run([client, "download", address, object_id, str(output)],
                                    capture_output=True, text=True, timeout=90)
            assert result.returncode == 0, result.stderr
            assert filecmp.cmp(source, output, shallow=False)

            empty = root / "empty.bin"
            empty.touch()
            result = subprocess.run([client, "upload", address, str(empty)],
                                    capture_output=True, text=True, timeout=10)
            assert result.returncode == 0, result.stderr
            empty_id = result.stdout.strip()
            empty_output = root / "empty.download"
            result = subprocess.run([client, "download", address, empty_id, str(empty_output)],
                                    capture_output=True, text=True, timeout=10)
            assert result.returncode == 0 and empty_output.read_bytes() == b"", result.stderr
            again = subprocess.run([client, "download", address, empty_id, str(empty_output)],
                                   capture_output=True, text=True, timeout=10)
            assert again.returncode == 1 and "File exists" in again.stderr
            assert empty_output.read_bytes() == b""
        finally:
            if first is not None and first.poll() is None:
                first.kill()
                first.wait(timeout=3)
            for process in (node, coord):
                if process is not None:
                    if process.poll() is None:
                        stop(process)
                    if process.stdout:
                        process.stdout.close()
                    if process.stderr:
                        process.stderr.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
