#!/usr/bin/env python3
"""Stage E: real Coordinator and Storage Node process membership/placement checks."""

import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.dont_write_bytecode = True
from storage_node_test import exchange, free_port, put_headers


IDS = [f"{index:08x}-0000-0000-0000-000000000000" for index in range(1, 6)]
INSTANCE = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CHUNK = "01234567-89ab-cdef-0123-456789abcdef"
LEASE_SECONDS = 1.5


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 0


def kill(process):
    if process.poll() is None:
        process.kill()
        process.wait(timeout=3)


def wait_coordinator(process, port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert process.poll() is None, f"Coordinator exited: {process.returncode}"
        try:
            with socket.create_connection(("127.0.0.1", port), 0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError("Coordinator did not listen")


def read_node_port(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=5), "Storage Node did not announce its port"
        line = process.stdout.readline().decode()
    assert line.startswith("storage_node listening on "), line
    return int(line.rsplit(" ", 1)[1])


def probe(executable, address, *arguments):
    result = subprocess.run([executable, address, *map(str, arguments)],
                            capture_output=True, text=True, timeout=2)
    assert result.returncode == 0, (arguments, result.returncode, result.stderr)
    lines = result.stdout.strip().splitlines()
    if lines and lines[0].startswith("ERROR "):
        assert len(lines) == 1, lines
        return lines[0], []
    return None, [tuple(line.split()) for line in lines]


def get_node(executable, address, node_id):
    error, rows = probe(executable, address, "get", node_id)
    if error in ("ERROR NOT_FOUND", "ERROR UNAVAILABLE", "ERROR DEADLINE_EXCEEDED"):
        return None
    assert error is None and len(rows) == 1, (error, rows)
    return rows[0]  # node_id, instance_id, host, port, state


def wait_until(predicate, description, timeout=9):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")


def wait_state(executable, address, node_id, state):
    return wait_until(
        lambda: (row if row and row[4] == state else None)
        if (row := get_node(executable, address, node_id)) else None,
        f"{node_id} {state}",
    )


def placement(executable, address, count):
    return probe(executable, address, "placement", CHUNK, count)


def check_http(port, index):
    chunk_id = f"33333333-4444-5555-6666-{index:012x}"
    payload = (f"node-{index}-payload".encode()) * 1024
    code, _, _ = exchange(port, "PUT", chunk_id,
                           put_headers(payload, 0, len(payload)), payload)
    assert code == 201, (port, code)
    code, _, contents = exchange(port, "GET", chunk_id, {"X-Generation": "1"})
    assert code == 200 and contents == payload, (port, code)


def main(coordinator_exe, node_exe, probe_exe):
    with tempfile.TemporaryDirectory(prefix="coordinator-multi-process-") as directory:
        root = Path(directory)
        address_port = free_port()
        address = f"127.0.0.1:{address_port}"
        coordinator_command = [coordinator_exe, address, "100", "1500", "50"]
        processes = []
        logs = []

        def launch(command, name, capture_port=False):
            path = root / f"{name}.log"
            output = path.open("ab")
            logs.append((path, output))
            process = subprocess.Popen(command,
                                       stdout=subprocess.PIPE if capture_port else output,
                                       stderr=output)
            processes.append(process)
            return process

        def start_node(name, node_id, storage_root):
            process = launch([node_exe, str(storage_root), "0", address, node_id,
                              "127.0.0.1", "50", "3500"], name, True)
            return process, read_node_port(process)

        try:
            coord = launch(coordinator_command, "coordinator-1")
            wait_coordinator(coord, address_port)
            nodes = {}
            for index, name in enumerate(("a", "b", "c")):
                nodes[name] = start_node(name, IDS[index], root / name)
            initial = {name: wait_state(probe_exe, address, IDS[index], "ONLINE")
                       for index, name in enumerate(("a", "b", "c"))}
            error, listed = probe(probe_exe, address, "list")
            assert error is None and len(listed) == 3
            assert {row[0] for row in listed} == set(IDS[:3])
            for index, name in enumerate(("a", "b", "c")):
                assert initial[name][3] == str(nodes[name][1])
                check_http(nodes[name][1], index + 1)

            first = placement(probe_exe, address, 2)
            assert first[0] is None and len(first[1]) == 2
            assert len({row[0] for row in first[1]}) == 2
            assert all(row[4] == "ONLINE" for row in first[1])
            for _ in range(3):
                assert placement(probe_exe, address, 2) == first

            # A second Node ID cannot claim A's live advertised endpoint.
            error, _ = probe(probe_exe, address, "register", IDS[3], INSTANCE,
                             "127.0.0.1", nodes["a"][1])
            assert error == "ERROR ALREADY_EXISTS", error
            assert get_node(probe_exe, address, IDS[3]) is None

            # Kill B without unregistering; a replacement must wait for expiry.
            killed_at = time.monotonic()
            kill(nodes["b"][0])
            assert get_node(probe_exe, address, IDS[1])[1] == initial["b"][1]
            replacement_b = start_node("b-replacement", IDS[1], root / "b")
            nodes["b2"] = replacement_b
            new_b = wait_until(
                lambda: (row if row and row[1] != initial["b"][1] and row[4] == "ONLINE"
                         else None)
                if (row := get_node(probe_exe, address, IDS[1])) else None,
                "B replacement", 7)
            assert time.monotonic() - killed_at >= LEASE_SECONDS - 0.25
            assert new_b[3] == str(replacement_b[1])
            for command in ("heartbeat", "unregister"):
                error, _ = probe(probe_exe, address, command, IDS[1], initial["b"][1])
                assert error == "ERROR FAILED_PRECONDITION", (command, error)

            # Pause C so its old process survives; the new instance takes over
            # only after expiry. Resuming C must not steal the lease back.
            nodes["c"][0].send_signal(signal.SIGSTOP)
            paused_at = time.monotonic()
            replacement_c = start_node("c-replacement", IDS[2], root / "c-replacement")
            nodes["c2"] = replacement_c
            new_c = wait_until(
                lambda: (row if row and row[1] != initial["c"][1] and row[4] == "ONLINE"
                         else None)
                if (row := get_node(probe_exe, address, IDS[2])) else None,
                "C replacement", 7)
            assert time.monotonic() - paused_at >= LEASE_SECONDS - 0.25
            nodes["c"][0].send_signal(signal.SIGCONT)
            for command in ("heartbeat", "unregister"):
                error, _ = probe(probe_exe, address, command, IDS[2], initial["c"][1])
                assert error == "ERROR FAILED_PRECONDITION", (command, error)
            time.sleep(0.4)  # Old agent receives its own heartbeat rejection.
            assert get_node(probe_exe, address, IDS[2])[1] == new_c[1]
            check_http(nodes["c"][1], 4)  # Old process still serves its own data.

            stop(replacement_c[0])
            wait_state(probe_exe, address, IDS[2], "OFFLINE")
            time.sleep(LEASE_SECONDS + 0.3)
            assert get_node(probe_exe, address, IDS[2])[4] == "OFFLINE"

            # An unregistered endpoint can be reused by another Node ID.
            error, rows = probe(probe_exe, address, "register", IDS[3], INSTANCE,
                                "127.0.0.1", replacement_c[1])
            assert error is None and rows == [("OK",)]
            error, rows = probe(probe_exe, address, "unregister", IDS[3], INSTANCE)
            assert error is None and rows == [("OK",)]

            stop(nodes["c"][0])
            nodes["c3"] = start_node("c-third", IDS[2], root / "c-replacement")
            active_c = wait_state(probe_exe, address, IDS[2], "ONLINE")
            assert active_c[1] != new_c[1]

            # After Coordinator restart, all three live agents re-register.
            active = {IDS[0]: initial["a"][1], IDS[1]: new_b[1], IDS[2]: active_c[1]}
            stop(coord)
            coord = launch(coordinator_command, "coordinator-2")
            wait_coordinator(coord, address_port)
            wait_until(
                lambda: all((row := get_node(probe_exe, address, node_id)) and
                            row[4] == "ONLINE" and row[1] == instance
                            for node_id, instance in active.items()),
                "three agents re-register", 10)
            error, listed = probe(probe_exe, address, "list")
            assert error is None and {row[0] for row in listed if row[4] == "ONLINE"} == set(active)
            for index, name in enumerate(("a", "b2", "c3")):
                check_http(nodes[name][1], index + 5)
            error, chosen = placement(probe_exe, address, 2)
            assert error is None and len(chosen) == 2 and chosen[0][0] != chosen[1][0]
            assert placement(probe_exe, address, 2) == (error, chosen)

            # Expired B is excluded; with only A left, two candidates fail
            # atomically rather than returning a partial list.
            kill(nodes["b2"][0])
            stop(nodes["c3"][0])
            wait_state(probe_exe, address, IDS[1], "OFFLINE")
            wait_state(probe_exe, address, IDS[2], "OFFLINE")
            error, chosen = placement(probe_exe, address, 1)
            assert error is None and len(chosen) == 1 and chosen[0][0] == IDS[0]
            error, chosen = placement(probe_exe, address, 2)
            assert error == "ERROR FAILED_PRECONDITION" and chosen == []

            # An expired endpoint is reusable too.
            error, rows = probe(probe_exe, address, "register", IDS[4], INSTANCE,
                                "127.0.0.1", replacement_b[1])
            assert error is None and rows == [("OK",)]
            error, rows = probe(probe_exe, address, "unregister", IDS[4], INSTANCE)
            assert error is None and rows == [("OK",)]
        except Exception:
            for path, output in logs:
                output.flush()
                if path.exists():
                    print(f"\n--- {path.name} ---\n{path.read_text(errors='replace')[-3000:]}",
                          file=sys.stderr)
            raise
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()  # SIGKILL also releases a paused process.
                    process.wait(timeout=3)
                if process.stdout:
                    process.stdout.close()
            for _, output in logs:
                output.close()


if __name__ == "__main__":
    main(*sys.argv[1:])
