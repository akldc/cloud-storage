#!/usr/bin/env python3
"""Stage E: recover after intentionally discarded Begin, PUT and Commit replies."""

import hashlib
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
from storage_node_coordinator_test import NODE_ID, stop, wait_listening, wait_node, wait_node_port
from storage_node_test import exchange, free_port, head


def call(probe, address, *arguments):
    result = subprocess.run([probe, address, *map(str, arguments)],
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, (arguments, result.stderr)
    return result.stdout.strip()


def state_file(source, request_id):
    state = Path(str(source) + ".object-upload")
    contents = f"object-upload-v1\n{source.stat().st_size}\n{request_id}\n{source.resolve()}"
    with state.open("w") as file:
        file.write(contents)
        file.flush()
        os.fsync(file.fileno())
    directory = os.open(source.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return state


def plan(db, request_id):
    with sqlite3.connect(db) as connection:
        return connection.execute(
            "SELECT o.object_id,o.upload_id,c.chunk_id FROM objects o JOIN chunks c "
            "ON c.object_id=o.object_id WHERE o.request_id=?", (request_id,)).fetchone()


def headers(object_id, upload_id, size, sha):
    return {"X-Upload-Id": upload_id, "X-Object-Id": object_id,
            "X-Object-Version": "1", "X-Chunk-Index": "0",
            "X-Logical-Offset": "0", "X-Chunk-Size": str(size),
            "X-Checksum-Sha256": sha, "X-Generation": "1", "X-Offset": "0"}


def drop_put_reply(port, chunk_id, descriptor, payload):
    request_headers = dict(descriptor)
    request_headers["Content-Length"] = str(len(payload))
    wire = (f"PUT /v1/chunks/{chunk_id} HTTP/1.1\r\nHost: localhost\r\n"
            + "".join(f"{key}: {value}\r\n" for key, value in request_headers.items())
            + "\r\n").encode()
    with socket.create_connection(("127.0.0.1", port), timeout=10) as connection:
        connection.sendall(wire + payload)
        connection.shutdown(socket.SHUT_WR)
        # Deliberately close without reading the eventual HTTP 201.


def wait_head(port, chunk_id, upload_id):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        code, fields, _ = head(port, chunk_id, upload_id)
        if code == 200:
            return fields
        assert code in (404, 409, 308), (code, fields)
        time.sleep(0.05)
    raise AssertionError("Chunk did not become committed after lost PUT reply")


def main(coordinator, storage_node, probe, client):
    with tempfile.TemporaryDirectory(prefix="object-lost-reply-") as directory:
        root = Path(directory)
        address = f"127.0.0.1:{free_port()}"
        db = root / "objects.sqlite"
        coord = None
        node = None
        try:
            coord = subprocess.Popen(
                [coordinator, address, "100", "500", "50", "--db", str(db)],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            wait_listening(coord, int(address.rsplit(":", 1)[1]))
            node = subprocess.Popen(
                [storage_node, str(root / "node"), "0", address, NODE_ID, "127.0.0.1",
                 "50", "1000"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            port = wait_node_port(node)
            wait_node(probe, address, "ONLINE")

            source = root / "begin-and-put-lost.bin"
            payload = os.urandom(256 * 1024)
            source.write_bytes(payload)
            sha = hashlib.sha256(payload).hexdigest()
            request_id = str(uuid.uuid4())
            state = state_file(source, request_id)
            assert call(probe, address, "begin-single", request_id, len(payload), sha) == "OK"
            object_id, upload_id, chunk_id = plan(db, request_id)
            assert call(probe, address, "get-object", object_id) == "ERROR NOT_FOUND"
            descriptor = headers(object_id, upload_id, len(payload), sha)
            drop_put_reply(port, chunk_id, descriptor, payload)
            assert wait_head(port, chunk_id, upload_id)["x-checksum-sha256"] == sha
            recovered = subprocess.run([client, "upload", address, str(source)],
                                       capture_output=True, text=True, timeout=15)
            assert recovered.returncode == 0 and recovered.stdout.strip() == object_id, recovered.stderr
            assert not state.exists()
            assert call(probe, address, "get-object", object_id) == "OK"
            code, _, _ = exchange(port, "PUT", chunk_id, descriptor, payload)
            assert code == 200, code
            assert call(probe, address, "confirm", upload_id, 0) == "OK"
            assert call(probe, address, "commit", upload_id) == "OK"

            second = root / "commit-lost.bin"
            second_payload = os.urandom(128 * 1024)
            second.write_bytes(second_payload)
            second_sha = hashlib.sha256(second_payload).hexdigest()
            second_request = str(uuid.uuid4())
            second_state = state_file(second, second_request)
            assert call(probe, address, "begin-single", second_request,
                        len(second_payload), second_sha) == "OK"
            second_object, second_upload, second_chunk = plan(db, second_request)
            code, _, _ = exchange(port, "PUT", second_chunk,
                                  headers(second_object, second_upload,
                                          len(second_payload), second_sha), second_payload)
            assert code == 201, code
            assert call(probe, address, "confirm", second_upload, 0) == "OK"
            assert call(probe, address, "commit", second_upload) == "OK"
            assert second_state.exists()  # Simulate losing the Commit RPC response.
            resumed = subprocess.run([client, "upload", address, str(second)],
                                     capture_output=True, text=True, timeout=15)
            assert resumed.returncode == 0 and resumed.stdout.strip() == second_object, resumed.stderr
            assert not second_state.exists()
            assert call(probe, address, "get-upload", second_request).split()[-1] == "2"
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
