#!/usr/bin/env python3
"""Stage C: one Coordinator, one Storage Node and the real C++ object CLI."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
from storage_node_coordinator_test import NODE_ID, stop, wait_listening, wait_node, wait_node_port
from storage_node_test import free_port


def main(coordinator, storage_node, probe, client):
    with tempfile.TemporaryDirectory(prefix="object-small-file-") as root:
        root = Path(root)
        address = f"127.0.0.1:{free_port()}"
        database = root / "objects.sqlite"
        coord = None
        node = None
        try:
            coord = subprocess.Popen(
                [coordinator, address, "100", "500", "50", "--db", str(database)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            wait_listening(coord, int(address.rsplit(":", 1)[1]))
            node = subprocess.Popen(
                [storage_node, str(root / "node"), "0", address, NODE_ID, "127.0.0.1",
                 "50", "1000"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            wait_node_port(node)
            assert wait_node(probe, address, "ONLINE")[0] == "ONLINE"

            source = root / "source.bin"
            payload = os.urandom(768 * 1024 + 37)
            source.write_bytes(payload)
            state_path = root / "session.state"
            uploaded = subprocess.run(
                [client, "upload", address, str(source), str(state_path)],
                capture_output=True, text=True, timeout=20)
            assert uploaded.returncode == 0, uploaded.stderr
            assert not state_path.exists() and Path(str(state_path) + ".lock").exists()
            object_id = uploaded.stdout.strip()
            assert len(object_id) == 36, uploaded.stdout

            output = root / "download.bin"
            downloaded = subprocess.run(
                [client, "download", address, object_id, str(output)],
                capture_output=True, text=True, timeout=20)
            assert downloaded.returncode == 0, downloaded.stderr
            assert output.read_bytes() == payload

            old = b"existing file must survive"
            output.write_bytes(old)
            refused = subprocess.run(
                [client, "download", address, object_id, str(output)],
                capture_output=True, text=True, timeout=20)
            assert refused.returncode == 1, refused.stderr
            assert "File exists" in refused.stderr, refused.stderr
            assert output.read_bytes() == old
            assert not list(root.glob(".object-download-*"))

            data_files = list((root / "node" / "chunks").rglob("data"))
            assert len(data_files) == 1, data_files
            with data_files[0].open("r+b") as stored:
                original = stored.read(1)
                stored.seek(0)
                stored.write(bytes([original[0] ^ 0xFF]))
                stored.flush()
                os.fsync(stored.fileno())
            corrupt_target = root / "corrupt-download.bin"
            corrupt = subprocess.run(
                [client, "download", address, object_id, str(corrupt_target)],
                capture_output=True, text=True, timeout=20)
            assert corrupt.returncode == 1, corrupt.stderr
            assert "SHA-256 mismatch" in corrupt.stderr, corrupt.stderr
            assert not corrupt_target.exists()
            assert not list(root.glob(".object-download-*"))
        finally:
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
