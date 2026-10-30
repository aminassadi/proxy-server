#!/usr/bin/env python3
"""Minimal origin HTTP server for testing the proxy.

Supports: keep-alive, pipelining, chunked requests, arbitrary body sizes.
Response body: 'method=... path=... version=... body_len=N body_md5=XXXXXXXX'
"""
import argparse
import hashlib
import socketserver


def read_until(sock, buf, marker):
    while marker not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            return None, buf
        buf += chunk
    head, buf = buf.split(marker, 1)
    return head, buf


def read_exact(sock, buf, n):
    while len(buf) < n:
        chunk = sock.recv(65536)
        if not chunk:
            return None, buf
        buf += chunk
    return buf[:n], buf[n:]


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        sock = self.request
        buf = b""
        while True:
            head, buf = read_until(sock, buf, b"\r\n\r\n")
            if head is None:
                return
            lines = head.split(b"\r\n")
            try:
                method, path, version = lines[0].decode().split(" ", 2)
            except ValueError:
                return

            headers = {}
            for line in lines[1:]:
                k, _, v = line.partition(b":")
                headers[k.strip().lower().decode()] = v.strip().decode()

            chunked = "chunked" in headers.get("transfer-encoding", "").lower()
            cl = int(headers.get("content-length", "0"))

            if chunked:
                body = b""
                while True:
                    size_line, buf = read_until(sock, buf, b"\r\n")
                    if size_line is None:
                        return
                    size = int(size_line, 16)
                    if size == 0:
                        # consume trailers until empty line
                        while True:
                            trailer, buf = read_until(sock, buf, b"\r\n")
                            if trailer is None:
                                return
                            if trailer == b"":
                                break
                        break
                    data, buf = read_exact(sock, buf, size + 2)  # body + CRLF
                    if data is None:
                        return
                    body += data[:size]
                cl = len(body)
            else:
                data, buf = read_exact(sock, buf, cl)
                if data is None:
                    return
                body = data

            digest = hashlib.md5(body).hexdigest()[:8]
            resp_body = (
                f"method={method} path={path} version={version} "
                f"body_len={len(body)} body_md5={digest}\n"
            ).encode()

            keep_alive = True
            if version == "HTTP/1.0":
                keep_alive = headers.get("connection", "").lower() == "keep-alive"
            elif headers.get("connection", "").lower() == "close":
                keep_alive = False

            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: text/plain\r\n"
                f"Content-Length: {len(resp_body)}\r\n"
                f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n"
                f"\r\n"
            ).encode() + resp_body
            sock.sendall(resp)

            if not keep_alive:
                return


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    a = p.parse_args()
    print(f"origin listening on {a.host}:{a.port}")
    with Server((a.host, a.port), Handler) as srv:
        srv.serve_forever()


if __name__ == "__main__":
    main()