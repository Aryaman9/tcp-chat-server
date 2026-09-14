"""Script two real C++ clients and print their conversation; no Python chat client."""
import contextlib
import os
import selectors
import signal
import subprocess
import sys
import time


class Output:
    def __init__(self, process, label):
        self.process, self.label, self.pending = process, label, b""

    def expect(self, expected=None):
        deadline = time.monotonic() + 5
        while b"\n" not in self.pending:
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                if not selector.select(max(0, deadline - time.monotonic())):
                    raise RuntimeError(f"{self.label}: output timeout")
            chunk = os.read(self.process.stdout.fileno(), 4096)
            if not chunk:
                raise RuntimeError(f"{self.label}: unexpected EOF")
            self.pending += chunk
        raw, self.pending = self.pending.split(b"\n", 1)
        line = raw.decode()
        print(f"[{self.label}] {line}", flush=True)
        if expected is not None and line != expected:
            raise RuntimeError(f"expected {expected!r}, got {line!r}")
        return line


@contextlib.contextmanager
def running(args):
    process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    try:
        yield process
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        try:
            _, err = process.communicate(timeout=4)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise RuntimeError("demo process failed to stop")
        if process.returncode != 0:
            raise RuntimeError(f"demo process failed ({process.returncode}): {err.decode()}")


def say(process, label, text):
    print(f"[{label} input] {text}", flush=True)
    process.stdin.write((text + "\n").encode())
    process.stdin.flush()


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: demo_chat.py PATH_TO_CHAT_SERVER PATH_TO_CHAT_CLIENT")
    with running([sys.argv[1], "0"]) as server:
        port = Output(server, "server").expect().rsplit(":", 1)[1]
        with running([sys.argv[2], "alice", port]) as alice:
            a = Output(alice, "alice")
            a.expect("OK alice")
            a.expect("JOIN alice")
            with running([sys.argv[2], "bob", port]) as bob:
                b = Output(bob, "bob")
                b.expect("OK bob")
                b.expect("JOIN bob")
                a.expect("JOIN bob")
                say(alice, "alice", "hello Bob")
                a.expect("MSG alice hello Bob")
                b.expect("MSG alice hello Bob")
                say(bob, "bob", "hello Alice")
                a.expect("MSG bob hello Alice")
                b.expect("MSG bob hello Alice")
                say(alice, "alice", "/users")
                a.expect("USERS alice bob")
                say(alice, "alice", "/quit")
                a.expect("BYE")
                alice.wait(timeout=3)
                b.expect("LEAVE alice")
                print("[bob input] EOF", flush=True)
                bob.stdin.close()
                bob.stdin = None
                b.expect("BYE")
                bob.wait(timeout=3)
    print("Demo passed; server and both C++ clients stopped.")


if __name__ == "__main__":
    main()
