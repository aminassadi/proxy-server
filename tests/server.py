#!/usr/bin/env python3
"""Minimal origin HTTP server for testing the proxy.

Supports: keep-alive, pipelining, chunked requests, arbitrary body sizes.
Response body embeds a fingerprint of what was received:
    'method=... path=... version=... body_len=N body_md5=XXXXXXXX'

Use it as the proxy's outbound target. Run with --verbose to log headers.
"""
import argparse
import hashlib
import itertools
import logging
import socketserver
import threading
import time

log = logging.getLogger("origin")

# ── instrumentation (safe to read on shutdown) ─────────────────────
_conn_counter = itertools.count(1)
_accepts = 0
_closes = 0
_requests = 0
_stats_lock = threading.Lock()


def _bump(name):
    global _accepts, _closes, _requests
    with _stats_lock:
        if name == "accept":
            _accepts += 1
        elif name == "close":
            _closes += 1
        elif name == "request":
            _requests += 1


def _snapshot():
    with _stats_lock:
        return _accepts, _closes, _requests


# ── socket helpers ─────────────────────────────────────────────────
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


# ── request handler ────────────────────────────────────────────────
class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        cid = next(_conn_counter)
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        _bump("accept")
        log.info(f"[c{cid}] accept from {peer}")
        t0 = time.monotonic()
        try:
            self._serve(cid)
        except Exception as e:
            log.exception(f"[c{cid}] unhandled: {e}")
        finally:
            dur = time.monotonic() - t0
            _bump("close")
            log.info(f"[c{cid}] close (lifetime {dur*1000:.0f} ms)")

    def _serve(self, cid):
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
            _bump("request")

            headers = {}
            for line in lines[1:]:
                k, _, v = line.partition(b":")
                headers[k.strip().lower().decode(errors="replace")] = \
                    v.strip().decode(errors="replace")

            chunked = "chunked" in headers.get("transfer-encoding", "").lower()

            if chunked:
                body, chunk_count, ok = self._read_chunked(sock, buf, cid)
                if not ok:
                    return
                buf = ok  # _read_chunked returns leftover in 3rd slot
            else:
                try:
                    cl = int(headers.get("content-length", "0"))
                except ValueError:
                    cl = 0
                body, buf = read_exact(sock, buf, cl)
                if body is None:
                    log.warning(f"[c{cid}] peer EOF mid-body (wanted {cl})")
                    return
                chunk_count = 0

            cl_final = len(body)
            digest = hashlib.md5(body).hexdigest()[:8]
            resp_body = (
                f"method={method} path={path} version={version} "
                f"body_len={cl_final} body_md5={digest}\n"
            ).encode()

            keep_alive = True
            conn_hdr = headers.get("connection", "").lower()
            if version == "HTTP/1.0":
                keep_alive = conn_hdr == "keep-alive"
            elif conn_hdr == "close":
                keep_alive = False

            resp = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: text/plain\r\n"
                f"Content-Length: {len(resp_body)}\r\n"
                f"Connection: {'keep-alive' if keep_alive else 'close'}\r\n"
                f"\r\n"
            ).encode() + resp_body
            sock.sendall(resp)

            te = "chunked" if chunked else "-"
            conn = conn_hdr or "-"
            extra = f" chunks={chunk_count}" if chunked else ""
            log.info(
                f"[c{cid}] #{req_n} {method} {path} {version} "
                f"host={headers.get('host','-')} cl={cl_final} te={te} "
                f"conn={conn} -> 200 body_len={cl_final} md5={digest}{extra}"
            )
            log.debug(
                f"[c{cid}] #{req_n} headers: "
                + ", ".join(f"{k}={v}" for k, v in headers.items())
            )

            if not keep_alive:
                log.info(f"[c{cid}] closing (Connection: close)")
                return

    def _read_chunked(self, sock, buf, cid):
        """Return (body, chunk_count, leftover_buf). On failure: (b'', 0, None)."""
        body = b""
        chunks = 0
        while True:
            size_line, buf = read_until(sock, buf, b"\r\n")
            if size_line is None:
                log.warning(f"[c{cid}] peer EOF mid-chunk-header")
                return b"", 0, None
            try:
                size = int(size_line, 16)
            except ValueError:
                log.warning(f"[c{cid}] bad chunk size: {size_line!r}")
                return b"", 0, None
            if size == 0:
                # consume trailers until an empty line
                while True:
                    trailer, buf = read_until(sock, buf, b"\r\n")
                    if trailer is None:
                        return b"", 0, None
                    if trailer == b"":
                        break
                return body, chunks, buf
            data, buf = read_exact(sock, buf, size + 2)  # body + CRLF
            if data is None:
                log.warning(f"[c{cid}] peer EOF mid-chunk-data")
                return b"", 0, None
            body += data[:size]
            chunks += 1


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


# ── main ───────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("-v", "--verbose", action="store_true",
                   help="log all headers per request (debug level)")
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
        acc, cls, req = _snapshot()
        log.info(
            f"shutdown: accepts={acc} closes={cls} requests={req} "
            f"open={acc - cls}"
        )


if __name__ == "__main__":
    main()