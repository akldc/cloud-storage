#!/usr/bin/env python3
"""One-node control-plane integration; multi-node placement belongs to stage E."""

import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from storage_node_test import CHUNK, exchange, free_port, put_headers


NODE_ID = "12345678-1234-1234-1234-123456789abc"


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        assert process.wait(timeout=5) == 0
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)
        raise


def wait_listening(process, port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert process.poll() is None, f"coordinator exited: {process.returncode}"
        try:
            with socket.create_connection(("127.0.0.1", port), 0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError("coordinator did not listen")


def node_info(probe, address):
    result = subprocess.run([probe, address, NODE_ID], capture_output=True,
                            text=True, timeout=2)
    if result.returncode == 3:
        return None
    assert result.returncode == 0, result.stderr
    state, instance, host, port = result.stdout.strip().split()
    return state, instance, host, int(port)


def wait_node(probe, address, desired):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        info = node_info(probe, address)
        if info is not None and info[0] == desired:
            return info
        time.sleep(0.05)
    raise AssertionError(f"node did not become {desired}")


def wait_node_port(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=5), "storage node did not announce its port"
        line = process.stdout.readline()
    assert line.startswith("storage_node listening on "), line
    return int(line.rsplit(" ", 1)[1])


def main(coordinator, storage_node, probe):
    with tempfile.TemporaryDirectory(prefix="storage-node-coordinator-") as root:
        address = f"127.0.0.1:{free_port()}"
        coordinator_command = [coordinator, address, "100", "500", "50"]
        coord = None
        node = None
        try:
            invalid = subprocess.run(
                [storage_node, root, "0", address, "bad-id", "127.0.0.1"],
                capture_output=True, timeout=3)
            assert invalid.returncode == 2

            coord = subprocess.Popen(coordinator_command, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.PIPE, text=True)
            wait_listening(coord, int(address.rsplit(":", 1)[1]))
            node = subprocess.Popen(
                [storage_node, root, "0", address, NODE_ID, "127.0.0.1", "50", "1000"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            port = wait_node_port(node)
            first = wait_node(probe, address, "ONLINE")
            assert first[2:] == ("127.0.0.1", port), first

            stop(coord)
            coord.stderr.close()
            payload = b"coordinator outage does not block HTTP" * 1024
            code, _, _ = exchange(port, "PUT", CHUNK,
                                   put_headers(payload, 0, len(payload)), payload)
            assert code == 201, code
            code, _, contents = exchange(port, "GET", CHUNK, {"X-Generation": "1"})
            assert code == 200 and contents == payload

            coord = subprocess.Popen(coordinator_command, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.PIPE, text=True)
            wait_listening(coord, int(address.rsplit(":", 1)[1]))
            recovered = wait_node(probe, address, "ONLINE")
            assert recovered[1] == first[1] and recovered[3] == port
            code, _, contents = exchange(port, "GET", CHUNK, {"X-Generation": "1"})
            assert code == 200 and contents == payload

            stop(node)
            offline = wait_node(probe, address, "OFFLINE")
            assert offline[1] == first[1]
        finally:
            for process in (node, coord):
                if process is not None:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=3)
                    if process.stdout:
                        process.stdout.close()
                    if process.stderr:
                        process.stderr.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
