#!/usr/bin/env python3
"""Socket-level regression for the single-node streaming HTTP service."""

import hashlib
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


CHUNK = "01234567-89ab-cdef-0123-456789abcdef"
OBJECT = "11111111-2222-3333-4444-555555555555"
UPLOAD = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, executable, root, extra_env=None):
        self.port = free_port()
        environment = os.environ.copy()
        environment.update(extra_env or {})
        self.proc = subprocess.Popen(
            [executable, str(root), str(self.port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"server exited: {self.proc.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), 0.2):
                    return
            except OSError:
                time.sleep(0.02)
        raise AssertionError("server did not listen")

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                raise
        assert self.proc.returncode == 0, self.proc.returncode

    def rss_kib(self):
        for line in Path(f"/proc/{self.proc.pid}/status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
        raise AssertionError("VmHWM unavailable")


def exchange(port, method, chunk, headers=None, body=b"", send_piece=65536,
             delay=0, recv_piece=65536, recv_delay=0):
    headers = dict(headers or {})
    headers.setdefault("Content-Length", str(len(body)))
    wire = (f"{method} /v1/chunks/{chunk} HTTP/1.1\r\n"
            + "Host: localhost\r\n"
            + "".join(f"{key}: {value}\r\n" for key, value in headers.items())
            + "\r\n").encode()
    with socket.create_connection(("127.0.0.1", port), timeout=20) as sock:
        sock.settimeout(20)
        sock.sendall(wire)
        for pos in range(0, len(body), send_piece):
            sock.sendall(body[pos:pos + send_piece])
            if delay:
                time.sleep(delay)
        response = bytearray()
        while True:
            part = sock.recv(recv_piece)
            if not part:
                break
            response.extend(part)
            if recv_delay:
                time.sleep(recv_delay)
    head, sep, content = bytes(response).partition(b"\r\n\r\n")
    assert sep, response[:200]
    lines = head.decode("ascii").split("\r\n")
    code = int(lines[0].split()[1])
    fields = {}
    for line in lines[1:]:
        key, value = line.split(":", 1)
        fields[key.lower()] = value.strip()
    return code, fields, content


def put_headers(payload, offset, chunk_size, upload=UPLOAD):
    return {
        "X-Upload-Id": upload,
        "X-Object-Id": OBJECT,
        "X-Object-Version": "1",
        "X-Chunk-Index": "0",
        "X-Logical-Offset": "0",
        "X-Chunk-Size": str(chunk_size),
        "X-Checksum-Sha256": hashlib.sha256(payload).hexdigest(),
        "X-Generation": "1",
        "X-Offset": str(offset),
    }


def head(port, chunk, upload=UPLOAD):
    return exchange(port, "HEAD", chunk,
                    {"X-Generation": "1", "X-Upload-Id": upload})


def assert_descriptor(fields, chunk, headers):
    expected = {
        "x-chunk-id": chunk,
        "x-object-id": headers["X-Object-Id"],
        "x-object-version": headers["X-Object-Version"],
        "x-chunk-index": headers["X-Chunk-Index"],
        "x-logical-offset": headers["X-Logical-Offset"],
        "x-chunk-size": headers["X-Chunk-Size"],
        "x-checksum-sha256": headers["X-Checksum-Sha256"],
        "x-generation": headers["X-Generation"],
    }
    for name, value in expected.items():
        assert fields.get(name) == value, (name, fields)


def test_main(executable):
    with tempfile.TemporaryDirectory(prefix="storage-node-test-") as root:
        server = Server(executable, root)
        try:
            contender = subprocess.run(
                [executable, str(root), str(free_port())],
                capture_output=True, text=True, timeout=5,
            )
            assert contender.returncode == 1, contender
            assert "storage root is already in use" in contender.stderr

            baseline_rss = server.rss_kib()
            payload = os.urandom(8 * 1024 * 1024)
            headers = put_headers(payload, 0, len(payload))
            first = payload[:1024 * 1024]
            code, fields, _ = exchange(server.port, "PUT", CHUNK, headers, first)
            assert code == 202 and fields["x-durable-offset"] == str(len(first)), (code, fields)
            code, fields, _ = exchange(server.port, "PUT", CHUNK, headers, first)
            assert code == 409 and fields["x-durable-offset"] == str(len(first)), (code, fields)
            code, fields, _ = head(server.port, CHUNK)
            assert code == 308 and fields["x-durable-offset"] == str(len(first)), (code, fields)
            assert_descriptor(fields, CHUNK, headers)
            rest = payload[len(first):]
            code, fields, _ = exchange(server.port, "PUT", CHUNK,
                                       put_headers(payload, len(first), len(payload)), rest)
            assert code == 201 and fields["x-durable-offset"] == str(len(payload)), (code, fields)
            code, fields, _ = head(server.port, CHUNK)
            assert code == 200 and fields["x-checksum-sha256"] == headers["X-Checksum-Sha256"]
            assert_descriptor(fields, CHUNK, headers)
            code, fields, content = exchange(server.port, "GET", CHUNK, {"X-Generation": "1"})
            assert code == 200 and content == payload, (code, len(content))
            assert_descriptor(fields, CHUNK, headers)
            code, fields, content = exchange(server.port, "GET", CHUNK,
                                              {"X-Generation": "1", "Range": "bytes=12345-54321"})
            assert code == 206 and content == payload[12345:54322], (code, len(content))
            assert fields["content-range"] == f"bytes 12345-54321/{len(payload)}"
            code, _, content = exchange(server.port, "GET", CHUNK,
                                         {"X-Generation": "1", "Range": "bytes=-100"})
            assert code == 206 and content == payload[-100:]
            code, _, _ = exchange(server.port, "GET", CHUNK,
                                   {"X-Generation": "1", "Range": "bytes=99999999-"})
            assert code == 416
            code, _, _ = exchange(server.port, "PUT", CHUNK, headers, payload)
            assert code == 200, code
            assert server.rss_kib() - baseline_rss < 32 * 1024, server.rss_kib()

            # Disconnect after transmitting only a prefix; the durable offset
            # is reported by HEAD and survives a clean service restart.
            interrupted = "22222222-3333-4444-5555-666666666666"
            resume_data = os.urandom(2 * 1024 * 1024)
            partial = resume_data[:200000]
            wire_headers = put_headers(resume_data, 0, len(resume_data))
            wire_headers["Content-Length"] = str(1024 * 1024)
            wire = (f"PUT /v1/chunks/{interrupted} HTTP/1.1\r\nHost: localhost\r\n"
                    + "".join(f"{key}: {value}\r\n" for key, value in wire_headers.items())
                    + "\r\n").encode()
            with socket.create_connection(("127.0.0.1", server.port), timeout=5) as sock:
                sock.sendall(wire + partial)
            deadline = time.monotonic() + 5
            while True:
                code, fields, _ = head(server.port, interrupted)
                if code == 308 and int(fields["x-durable-offset"]) > 0:
                    break
                assert time.monotonic() < deadline, (code, fields)
                time.sleep(0.05)
            durable = int(fields["x-durable-offset"])
            assert durable <= len(partial)
            server.stop()
            server = Server(executable, root)
            code, fields, _ = head(server.port, interrupted)
            assert code == 308 and int(fields["x-durable-offset"]) == durable, (code, fields)
            code, _, _ = exchange(server.port, "PUT", interrupted,
                                   put_headers(resume_data, durable, len(resume_data)),
                                   resume_data[durable:])
            assert code == 201, code
            code, _, content = exchange(server.port, "GET", interrupted,
                                         {"X-Generation": "1"})
            assert code == 200 and content == resume_data

            # Separate chunks share one upload ID and are served concurrently.
            failures = []
            def concurrent(index):
                chunk = f"33333333-4444-5555-6666-{index:012x}"
                data = bytes([index]) * (256 * 1024)
                try:
                    code, _, _ = exchange(server.port, "PUT", chunk,
                                           put_headers(data, 0, len(data)), data)
                    assert code == 201, code
                    code, _, content = exchange(server.port, "GET", chunk,
                                                 {"X-Generation": "1"})
                    assert code == 200 and content == data
                except Exception as exc:
                    failures.append(exc)
            threads = [threading.Thread(target=concurrent, args=(i,)) for i in range(8)]
            for thread in threads: thread.start()
            for thread in threads: thread.join()
            assert not failures, failures

            # A client that reads slowly must not cause an unbounded send queue.
            before = server.rss_kib()
            code, _, content = exchange(server.port, "GET", CHUNK,
                                         {"X-Generation": "1", "Range": "bytes=0-4194303"},
                                         recv_piece=4096, recv_delay=0.002)
            assert code == 206 and content == payload[:4194304]
            assert server.rss_kib() - before < 8 * 1024

            code, _, _ = exchange(server.port, "DELETE", interrupted,
                                   {"X-Generation": "1"})
            assert code == 204
            code, _, _ = head(server.port, interrupted)
            assert code == 404

            server.stop()
            server = Server(executable, root, {"STORAGE_NODE_DISK_DELAY_MS": "20"})
            before = server.rss_kib()
            slow_chunk = "44444444-5555-6666-7777-888888888888"
            slow_data = os.urandom(1024 * 1024)
            code, _, _ = exchange(server.port, "PUT", slow_chunk,
                                   put_headers(slow_data, 0, len(slow_data)), slow_data)
            assert code == 201, code
            code, _, content = exchange(server.port, "GET", slow_chunk,
                                         {"X-Generation": "1"})
            assert code == 200 and content == slow_data
            assert server.rss_kib() - before < 16 * 1024

            # Delete must conflict while an upload owns the Chunk, and service
            # shutdown must drain its in-flight disk work before Store teardown.
            active_chunk = "55555555-6666-7777-8888-999999999999"
            active_data = os.urandom(1024 * 1024)
            active_headers = put_headers(active_data, 0, len(active_data))
            active_headers["Content-Length"] = str(len(active_data))
            wire = (f"PUT /v1/chunks/{active_chunk} HTTP/1.1\r\nHost: localhost\r\n"
                    + "".join(f"{key}: {value}\r\n" for key, value in active_headers.items())
                    + "\r\n").encode()
            active_socket = socket.create_connection(("127.0.0.1", server.port), timeout=5)
            try:
                active_socket.sendall(wire + active_data[:65536])
                deadline = time.monotonic() + 5
                while True:
                    code, _, _ = head(server.port, active_chunk)
                    if code == 409:
                        break
                    assert time.monotonic() < deadline, code
                    time.sleep(0.02)
                code, _, _ = exchange(server.port, "DELETE", active_chunk,
                                       {"X-Generation": "1"})
                assert code == 409, code
                server.stop()
            finally:
                active_socket.close()
            server = Server(executable, root)
            code, fields, _ = head(server.port, active_chunk)
            assert code == 308, (code, fields)
            durable = int(fields["x-durable-offset"])
            assert 0 < durable <= 65536, durable
            code, _, _ = exchange(server.port, "PUT", active_chunk,
                                   put_headers(active_data, durable, len(active_data)),
                                   active_data[durable:])
            assert code == 201, code
            code, _, content = exchange(server.port, "GET", active_chunk,
                                         {"X-Generation": "1"})
            assert code == 200 and content == active_data

            server.stop()
            server = Server(executable, root, {"STORAGE_NODE_IDLE_TIMEOUT_SEC": "1"})
            with socket.create_connection(("127.0.0.1", server.port), timeout=5) as idle:
                idle.settimeout(5)
                assert idle.recv(1) == b""
        finally:
            server.stop()


if __name__ == "__main__":
    test_main(sys.argv[1])
