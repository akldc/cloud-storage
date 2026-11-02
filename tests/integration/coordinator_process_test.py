#!/usr/bin/env python3
"""Check command-line validation and bounded SIGTERM shutdown."""

import selectors
import signal
import subprocess
import sys


def main(executable):
    for arguments in (
        [],
        ["127.0.0.1:0", "100", "350", "50"],
        ["127.0.0.1:0", "abc", "500", "50"],
        ["127.0.0.1:0", "4294967296", "500", "50"],
    ):
        result = subprocess.run([executable, *arguments], capture_output=True, timeout=3)
        assert result.returncode == 2, (arguments, result.stderr)

    process = subprocess.Popen(
        [executable, "127.0.0.1:0", "100", "500", "50"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), "coordinator did not announce its port"
            line = process.stdout.readline()
        assert line.startswith("coordinator listening on port "), line
        assert int(line.rsplit(" ", 1)[1]) > 0
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=5) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=3)
        process.stdout.close()
        process.stderr.close()


if __name__ == "__main__":
    main(sys.argv[1])
