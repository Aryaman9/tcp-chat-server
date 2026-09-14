"""Real TCP and C++ terminal-client integration tests; Python standard library only."""
import concurrent.futures
import os
import selectors
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest

SERVER, CLIENT = sys.argv[1:3]
del sys.argv[1:3]


class Lines:
    """Unbuffered descriptor reader: select never overlooks prefetched lines."""
    def __init__(self, stream):
        self.stream = stream
        self.pending = b""

    def line(self, timeout=5):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.pending:
            with selectors.DefaultSelector() as selector:
                selector.register(self.stream, selectors.EVENT_READ)
                if not selector.select(max(0, deadline - time.monotonic())):
                    raise AssertionError("timed out waiting for a line")
            chunk = os.read(self.stream.fileno(), 4096)
            if not chunk:
                if self.pending:
                    raise AssertionError("truncated response: " + repr(self.pending))
                return ""
            self.pending += chunk
        line, self.pending = self.pending.split(b"\n", 1)
        return line.decode("utf-8")

    def expect(self, expected, timeout=5):
        actual = self.line(timeout)
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}")


class Peer(Lines):
    def send(self, text):
        self.stream.sendall(text.encode("utf-8"))


class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.process = subprocess.Popen([SERVER, "0", "4", "1"], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
        self.addCleanup(self.stop)
        line = Lines(self.process.stdout).line(10)
        self.assertTrue(line.startswith("LISTENING 127.0.0.1:"), line)
        self.port = int(line.rsplit(":", 1)[1])

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        try:
            _, err = self.process.communicate(timeout=4)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            self.fail("server failed to stop within four seconds")
        self.assertEqual(self.process.returncode, 0, err.decode())

    def peer(self, name=None, slow=False):
        sock = socket.socket()
        self.addCleanup(sock.close)
        sock.settimeout(5)
        if slow:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        sock.connect(("127.0.0.1", self.port))
        peer = Peer(sock)
        if name:
            peer.send(f"NICK {name}\n")
            peer.expect(f"OK {name}")
            peer.expect(f"JOIN {name}")
        return peer

    def cpp_client(self, name):
        process = subprocess.Popen([CLIENT, name, str(self.port)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.terminate()
            try:
                _, err = process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                self.fail("C++ client cleanup timed out")
            # Tests wait for normal exits; only a failed test needs termination.
            if process.returncode not in (0, -signal.SIGTERM):
                self.fail(f"C++ client failed: {err.decode()}")
        self.addCleanup(cleanup)
        lines = Lines(process.stdout)
        lines.expect(f"OK {name}")
        lines.expect(f"JOIN {name}")
        return process, lines

    def test_exchange_users_clean_and_abrupt_leave(self):
        alice = self.peer("alice")
        bob = self.peer("bob")
        alice.expect("JOIN bob")
        alice.send("MSG hello Bob\n")
        alice.expect("MSG alice hello Bob")
        bob.expect("MSG alice hello Bob")
        bob.send("MSG hello Alice\nUSERS\n")
        alice.expect("MSG bob hello Alice")
        bob.expect("MSG bob hello Alice")
        bob.expect("USERS alice bob")
        bob.send("QUIT\n")
        bob.expect("BYE")
        bob.expect("")
        alice.expect("LEAVE bob")
        bob2 = self.peer("bob")
        alice.expect("JOIN bob")
        bob2.stream.close()
        alice.expect("LEAVE bob")
        alice.send("USERS\n")
        alice.expect("USERS alice")

    def test_simultaneous_duplicate_nickname_and_retry(self):
        peers = [self.peer(), self.peer()]
        barrier = threading.Barrier(2)
        def register(peer):
            barrier.wait()
            peer.send("NICK same\n")
            return peer.line()
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(register, peers))
        self.assertCountEqual(results, ["OK same", "ERR nickname_in_use"])
        winner = peers[results.index("OK same")]
        loser = peers[results.index("ERR nickname_in_use")]
        winner.expect("JOIN same")
        winner.send("QUIT\n")
        winner.expect("BYE")
        winner.expect("")  # Removal is complete before the retry is dispatched.
        loser.send("NICK same\n")
        loser.expect("OK same")
        loser.expect("JOIN same")
        loser.send("NICK different\n")
        loser.expect("ERR already_registered")

    def test_split_coalesced_crlf_and_utf8(self):
        peer = self.peer()
        peer.send("NI")
        peer.send("CK alice\r\nMSG hello world\nUSERS\nMSG café\nQUIT\n")
        for line in ["OK alice", "JOIN alice", "MSG alice hello world",
                     "USERS alice", "MSG alice café", "BYE", ""]:
            peer.expect(line)

    def test_invalid_commands_and_message_boundaries(self):
        peer = self.peer()
        peer.send("\nBOGUS\nNICK bad/name\nUSERS\nMSG hi\nQUIT extra\n")
        for line in ["ERR command", "ERR command", "ERR nickname", "ERR register_first",
                     "ERR register_first", "ERR command"]:
            peer.expect(line)
        peer.send("NICK a\n")
        peer.expect("OK a")
        peer.expect("JOIN a")
        peer.send("MSG\nMSG \nMSG " + "x" * 1025 + "\nMSG " + "y" * 1024 + "\n")
        for _ in range(3):
            peer.expect("ERR message")
        peer.expect("MSG a " + "y" * 1024)

    def test_fatal_framing_errors(self):
        for payload, error in [(b"x" * 1101, "ERR line_too_long"),
                               (b"x" * 1101 + b"\n", "ERR line_too_long"),
                               (b"NICK a\x00b\n", "ERR binary_input"),
                               (b"NICK a\tb\n", "ERR control_input")]:
            with self.subTest(error=error, length=len(payload)):
                peer = self.peer()
                peer.stream.sendall(payload)
                peer.expect(error)
                peer.expect("")

    def test_concurrent_order_with_one_slot_event_queue(self):
        peers = [self.peer("a"), self.peer("b"), self.peer("c")]
        peers[0].expect("JOIN b")
        peers[0].expect("JOIN c")
        peers[1].expect("JOIN c")
        count = 100
        barrier = threading.Barrier(3)
        def send(index):
            barrier.wait()
            peers[index].send("".join(f"MSG {n}\n" for n in range(count)))
        def read(peer):
            lines = [peer.line() for _ in range(3 * count)]
            for name in ("a", "b", "c"):
                self.assertEqual([int(line.split()[2]) for line in lines
                                  if line.startswith(f"MSG {name} ")], list(range(count)))
            return lines
        with concurrent.futures.ThreadPoolExecutor(6) as pool:
            readers = [pool.submit(read, p) for p in peers]
            writers = [pool.submit(send, i) for i in range(3)]
            for future in writers:
                future.result(timeout=10)
            sequences = [future.result(timeout=10) for future in readers]
        self.assertEqual(sequences[0], sequences[1])
        self.assertEqual(sequences[1], sequences[2])

    def test_idle_registered_user_and_registration_deadline(self):
        registered = self.peer("idle")
        unregistered = self.peer()
        # Waiting for the documented deadline also exceeds the old idle limit.
        unregistered.expect("ERR registration_timeout", timeout=12)
        unregistered.expect("")
        registered.send("MSG still here\n")
        registered.expect("MSG idle still here")

    def test_capacity_and_shutdown_with_idle_clients(self):
        peers = []
        for i in range(4):
            peers.append(self.peer(f"p{i}"))
        rejected = self.peer()
        rejected.expect("ERR capacity")
        rejected.expect("")
        self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=3)
        with socket.socket() as rebound:
            rebound.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rebound.bind(("127.0.0.1", self.port))

    def test_shutdown_with_queued_work(self):
        peers = [self.peer(f"p{i}") for i in range(4)]
        for peer in peers:
            peer.send(("MSG " + "x" * 1024 + "\n") * 64)
        self.process.send_signal(signal.SIGTERM)
        self.process.wait(timeout=3)

    def test_slow_receiver_is_removed_without_losing_healthy_messages(self):
        slow = self.peer("a_slow", slow=True)
        sender = self.peer("sender")
        slow.expect("JOIN sender")
        count = 300
        payload = "x" * 1024
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            writer = pool.submit(sender.send, "".join(f"MSG {payload}\n" for _ in range(count)))
            messages, departed = 0, False
            deadline = time.monotonic() + 10
            while messages < count or not departed:
                line = sender.line(max(0, deadline - time.monotonic()))
                if line == "LEAVE a_slow":
                    self.assertFalse(departed, "duplicate departure")
                    departed = True
                else:
                    self.assertEqual(line, f"MSG sender {payload}")
                    messages += 1
                self.assertLessEqual(messages, count)
            writer.result(timeout=2)
        sender.send("USERS\n")
        sender.expect("USERS sender")
        replacement = self.peer("a_slow")
        sender.expect("JOIN a_slow")
        replacement.send("MSG back\n")
        sender.expect("MSG a_slow back")

    def test_actual_cpp_clients_exchange_while_stdin_idle_and_eof(self):
        alice, a = self.cpp_client("alice")
        bob, b = self.cpp_client("bob")
        a.expect("JOIN bob")
        alice.stdin.write(b"hello Bob\n")
        alice.stdin.flush()
        a.expect("MSG alice hello Bob")
        b.expect("MSG alice hello Bob")  # Bob has supplied no stdin at all.
        bob.stdin.write(b"hello Alice\n/users\n")
        bob.stdin.flush()
        a.expect("MSG bob hello Alice")
        b.expect("MSG bob hello Alice")
        b.expect("USERS alice bob")
        alice.stdin.write(b"/quit\n")
        alice.stdin.flush()
        a.expect("BYE")
        self.assertEqual(alice.wait(timeout=3), 0)
        b.expect("LEAVE alice")
        bob.stdin.close()
        bob.stdin = None
        b.expect("BYE")
        self.assertEqual(bob.wait(timeout=3), 0)

    def test_cpp_client_server_disconnect(self):
        client, _ = self.cpp_client("alice")
        self.process.send_signal(signal.SIGTERM)
        self.process.wait(timeout=3)
        self.assertEqual(client.wait(timeout=3), 0)

    def test_cpp_client_partial_stdin_and_long_line_recovery(self):
        client, lines = self.cpp_client("alice")
        peer = self.peer("bob")
        lines.expect("JOIN bob")
        client.stdin.write(b"unfinished")
        client.stdin.flush()
        peer.send("MSG while typing\n")
        lines.expect("MSG bob while typing")
        peer.expect("MSG bob while typing")
        client.stdin.write(b"\n" + b"x" * 1025 + b"\nvalid\n")
        client.stdin.flush()
        for expected in ["MSG alice unfinished", "MSG alice valid"]:
            lines.expect(expected)
            peer.expect(expected)
        client.stdin.write(b"last line without newline")
        client.stdin.close()
        client.stdin = None
        lines.expect("MSG alice last line without newline")
        lines.expect("BYE")
        self.assertEqual(client.wait(timeout=3), 0)

    def test_invalid_cli_arguments(self):
        for binary, args in [(SERVER, ["-1"]), (SERVER, ["0", "0"]),
                             (SERVER, ["0", "4", "0"]), (CLIENT, []),
                             (CLIENT, ["bad/name"]), (CLIENT, ["alice", "0"])]:
            result = subprocess.run([binary, *args], capture_output=True, timeout=3)
            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
