#!/usr/bin/env python3
"""Minimal origin HTTP server for testing the proxy.

Supports: keep-alive, pipelining, chunked requests, arbitrary body sizes.
Response body: 'method=... path=... version=... body_len=N body_md5=XXXXXXXX'
"""
import argparse
import hashlib
import itertools
import logging
import socketserver
import threading
import time

log = logging.getLogger("origin")

# ── instrumentation ────────────────────────────────────────────────
_conn_counter = itertools.count(1)
_accepts = 0
_closes = 0
_requests = 0
_stats_lock = threading.Lock()


def bump(name):
    global _accepts, _closes, _requests
    with _stats_lock:
        if name == "accept":
            _accepts += 1
        elif name == "close":
            _closes += 1
        elif name == "request":
            _requests += 1


# ── helpers ────────────────────────────────────────────────────────
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
        cid = next(_conn_counter)
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        bump("accept")
        log.info(f"[c{cid}] accept from {peer}")
        t0 = time.monotonic()
        try:
            self._serve(cid, peer)
        finally:
            dur = time.monotonic() - t0
            bump("close")
            log.info(f"[c{cid}] close (lifetime {dur*1000:.0f} ms)")

    def _serve(self, cid, peer):
        sock = self.request
        buf = b""
        req_n = 0
        while True:
            head, buf = read_until(sock, buf, b"\r\n\r\n")
            if head is None:
                log.info(f"[c{cid}] peer EOF")
                return
            lines = head.split(b"\r\n")
            try:
                method, path, version = lines[0].decode().split(" ", 2)
            except ValueError:
                log.warning(f"[c{cid}] malformed request line: {lines[0]!r}")
                return
            req_n += 1
            bump("request")

            headers = {}
            for line in lines[1:]:
                k, _, v = line.partition(b":")
                headers[k.strip().lower().decode()] = v.strip().decode()

            chunked = "chunked" in headers.get("transfer-encoding", "").lower()
            cl = int(headers.get("content-length", "0"))

            if chunked:
                body = b""
                chunk_count = 0
                while True:
                    size_line, buf = read_until(sock, buf, b"\r\n")
                    if size_line is None:
                        log.warning(f"[c{cid}] peer EOF mid-chunk")
                        return
                    size = int(size_line, 16)
                    if size == 0:
                        while True:
                            trailer, buf = read_until(sock, buf, b"\r\n")
                            if trailer is None:
                                return
                            if trailer == b"":
                                break
                        break
                    data, buf = read_exact(sock, buf, size + 2)
                    if data is None:
                        log.warning(f"[c{cid}] peer EOF mid-chunk-data")
                        return
                    body += data[:size]
                    chunk_count += 1
                cl = len(body)
            else:
                data, buf = read_exact(sock, buf, cl)
                if data is None:
                    log.warning(f"[c{cid}] peer EOF mid-body (wanted {cl})")
                    return
                body = data
                chunk_count = 0

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

            # ── one-line request log ──────────────────────────────
            te = "chunked" if chunked else "-"
            conn = headers.get("connection", "-") or "-"
            extra = f" chunks={chunk_count}" if chunked else ""
            log.info(
                f"[c{cid}] #{req_n} {method} {path} {version} "
                f"host={headers.get('host','-')} cl={cl} te={te} conn={conn} "
                f"-> 200 body_len={len(body)} md5={digest}{extra}"
            )
            log.debug(
                f"[c{cid}] #{req_n} headers: "
                + ", ".join(f"{k}={v}" for k, v in headers.items())
            )

            if not keep_alive:
                log.info(f"[c{cid}] closing after response (Connection: close)")
                return


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("-v", "--verbose", action="store_true",
                   help="log every header (debug level)")
    a = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info(f"origin listening on {a.host}:{a.port} (verbose={a.verbose})")
    try:
        with Server((a.host, a.port), Handler) as srv:
            srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        log.info(
            f"shutdown: accepts={_accepts} closes={_closes} "
            f"requests={_requests} open={_accepts - _closes}"
        )


if __name__ == "__main__":
    main()