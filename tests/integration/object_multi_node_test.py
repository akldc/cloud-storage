#!/usr/bin/env python3
"""Stage E: real multi-node placement, Coordinator/Node restart and confirmed reads."""

import filecmp
import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
from coordinator_multi_process_test import wait_state
from storage_node_coordinator_test import stop, wait_listening, wait_node_port
from storage_node_test import free_port


def placement(probe, address, chunk_id):
    result = subprocess.run([probe, address, "placement", chunk_id, "1"],
                            capture_output=True, text=True, timeout=3)
    assert result.returncode == 0, result.stderr
    return result.stdout.split()[0]


def score(chunk_id, node_id):
    value = b"coord-v1-placement\0" + chunk_id.encode() + b"\0" + node_id.encode()
    return hashlib.sha256(value).digest()


def chunk_path(root, chunk_id):
    compact = chunk_id.replace("-", "")
    return root / "chunks" / compact[:2] / compact[2:4] / chunk_id / "data"


def main(coordinator, storage_node, probe, client):
    with tempfile.TemporaryDirectory(prefix="object-multi-node-") as directory:
        root = Path(directory)
        address = f"127.0.0.1:{free_port()}"
        db = root / "objects.sqlite"
        source = root / "source.bin"
        block = bytes(range(256)) * 256
        with source.open("wb") as file:
            for _ in range(2400):
                file.write(block)
            file.write(b"multi-node tail")
        ids = ["11111111-1111-4111-8111-111111111111",
               "22222222-2222-4222-8222-222222222222"]
        roots = {node_id: root / f"node-{index}" for index, node_id in enumerate(ids)}
        nodes = {}
        coordinator_process = None
        upload = None

        def start_coordinator():
            process = subprocess.Popen(
                [coordinator, address, "100", "500", "50", "--db", str(db)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            wait_listening(process, int(address.rsplit(":", 1)[1]))
            return process

        def start_node(node_id):
            previous = nodes.get(node_id)
            if previous is not None:
                previous.stdout.close()
                previous.stderr.close()
            environment = os.environ.copy()
            environment["STORAGE_NODE_DISK_DELAY_MS"] = "1"
            process = subprocess.Popen(
                [storage_node, str(roots[node_id]), "0", address, node_id,
                 "127.0.0.1", "50", "1000"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
            wait_node_port(process)
            wait_state(probe, address, node_id, "ONLINE")
            nodes[node_id] = process

        try:
            coordinator_process = start_coordinator()
            for node_id in ids:
                start_node(node_id)
            upload = subprocess.Popen([client, "upload", address, str(source)],
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 25
            first_chunk = None
            upload_id = None
            while first_chunk is None:
                assert upload.poll() is None, upload.stderr.read()
                assert time.monotonic() < deadline, "upload plan did not appear"
                try:
                    with sqlite3.connect(db) as connection:
                        row = connection.execute(
                            "SELECT c.chunk_id, o.upload_id FROM chunks c JOIN objects o "
                            "ON c.object_id=o.object_id WHERE c.chunk_index=0").fetchone()
                        if row:
                            first_chunk, upload_id = row
                except sqlite3.OperationalError:
                    pass
                time.sleep(0.02)
            deadline = time.monotonic() + 25
            while True:
                data = roots[ids[0]] / "staging" / f"{first_chunk}.{upload_id}.1.tmp" / "data.part"
                other = roots[ids[1]] / "staging" / f"{first_chunk}.{upload_id}.1.tmp" / "data.part"
                if (data.exists() and data.stat().st_size > 0) or (
                    other.exists() and other.stat().st_size > 0):
                    break
                assert upload.poll() is None, upload.stderr.read()
                assert time.monotonic() < deadline, "first Chunk did not start"
                time.sleep(0.02)
            upload.kill()
            upload.wait(timeout=5)
            assert Path(str(source) + ".object-upload").exists()

            for process in nodes.values():
                stop(process)
            stop(coordinator_process)
            coordinator_process.stderr.close()
            coordinator_process = start_coordinator()
            for node_id in ids:
                start_node(node_id)
            recovered = subprocess.run([client, "upload", address, str(source)],
                                       capture_output=True, text=True, timeout=120)
            assert recovered.returncode == 0, recovered.stderr
            object_id = recovered.stdout.strip()
            with sqlite3.connect(db) as connection:
                rows = connection.execute(
                    "SELECT c.chunk_id,c.target_node_id,l.node_id FROM chunks c "
                    "JOIN chunk_locations l ON l.chunk_id=c.chunk_id "
                    "WHERE c.object_id=? ORDER BY c.chunk_index", (object_id,)).fetchall()
                assert len(rows) == 3, rows
                assert connection.execute("SELECT state FROM objects WHERE object_id=?",
                                          (object_id,)).fetchone()[0] == "COMMITTED"
            for chunk_id, target, confirmed in rows:
                assert target == confirmed and target in roots, rows
                assert chunk_path(roots[target], chunk_id).is_file()
                other = ids[1] if target == ids[0] else ids[0]
                assert not chunk_path(roots[other], chunk_id).exists()

            first_id, holder, _ = rows[0]
            decoy = str(uuid.uuid4())
            while score(first_id, decoy) <= max(score(first_id, value) for value in ids):
                decoy = str(uuid.uuid4())
            roots[decoy] = root / "node-decoy"
            start_node(decoy)
            assert placement(probe, address, first_id) == decoy
            output = root / "download.bin"
            result = subprocess.run([client, "download", address, object_id, str(output)],
                                    capture_output=True, text=True, timeout=90)
            assert result.returncode == 0, result.stderr
            assert filecmp.cmp(source, output, shallow=False)

            stop(nodes[holder])
            wait_state(probe, address, holder, "OFFLINE")
            missing = root / "node-offline-download.bin"
            result = subprocess.run([client, "download", address, object_id, str(missing)],
                                    capture_output=True, text=True, timeout=10)
            assert result.returncode == 1 and not missing.exists(), result.stderr
            start_node(holder)
            restored = root / "node-restored-download.bin"
            result = subprocess.run([client, "download", address, object_id, str(restored)],
                                    capture_output=True, text=True, timeout=90)
            assert result.returncode == 0 and filecmp.cmp(source, restored, shallow=False), result.stderr

            stop(coordinator_process)
            coordinator_process.stderr.close()
            coordinator_process = start_coordinator()
            for node_id in list(nodes):
                wait_state(probe, address, node_id, "ONLINE")
            after_restart = root / "coordinator-restored-download.bin"
            result = subprocess.run([client, "download", address, object_id,
                                     str(after_restart)], capture_output=True,
                                    text=True, timeout=90)
            assert result.returncode == 0 and filecmp.cmp(source, after_restart, shallow=False), result.stderr
        finally:
            if upload is not None and upload.poll() is None:
                upload.kill()
                upload.wait(timeout=3)
            for process in list(nodes.values()) + [coordinator_process]:
                if process is not None:
                    if process.poll() is None:
                        stop(process)
                    if process.stdout:
                        process.stdout.close()
                    if process.stderr:
                        process.stderr.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
