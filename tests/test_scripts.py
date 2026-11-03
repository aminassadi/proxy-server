"""Regression checks for the Python test harness (no proxy required)."""
import socket
import subprocess
import sys
import threading
import unittest
from pathlib import Path

import client
import server


class ScriptTests(unittest.TestCase):
    def test_truncated_response_is_not_a_clean_rejection(self):
        for response in (b"HTTP/1.1 400", b"HTTP/1.1 400 Bad Request\r\nContent-Length: 3\r\n\r\nx"):
            with self.subTest(response=response):
                reader, writer = socket.socketpair()
                with reader, writer:
                    writer.sendall(response)
                    writer.shutdown(socket.SHUT_WR)
                    with self.assertRaises(ValueError):
                        client.read_response(reader)

    def test_origin_chunked_empty_leftover_and_keepalive(self):
        with server.Server(("127.0.0.1", 0), server.Handler) as origin:
            thread = threading.Thread(target=origin.serve_forever, daemon=True)
            thread.start()
            try:
                with socket.create_connection(origin.server_address, timeout=2) as sock:
                    sock.sendall(b"POST /chunked HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n3;ext=yes\r\nabc\r\n0\r\n\r\n")
                    status, _, body, leftover = client.read_response(sock)
                    self.assertEqual(status, 200)
                    self.assertIn(b"body_len=3", body.split())
                    sock.sendall(client.build_request("GET", "/next", "test", close=True))
                    status, _, body, _ = client.read_response(sock, leftover)
                    self.assertEqual(status, 200)
                    self.assertIn(b"path=/next", body.split())
            finally:
                origin.shutdown()
                thread.join()

    def test_client_options_before_and_after_command(self):
        import run_all
        with server.Server(("127.0.0.1", 0), server.Handler) as origin:
            thread = threading.Thread(target=origin.serve_forever, daemon=True)
            thread.start()
            try:
                options = ["--port", str(origin.server_address[1]), "--timeout", "2"]
                for args in (options + ["simple"], ["simple"] + options):
                    result = subprocess.run(
                        [sys.executable, str(Path(client.__file__)), *args],
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                passed, _, _, output = run_all.run_one(["simple"], options, 5)
                self.assertTrue(passed, output)
            finally:
                origin.shutdown()
                thread.join()


if __name__ == "__main__":
    unittest.main()
