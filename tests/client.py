#!/usr/bin/env python3
"""Test clients for the proxy.

Each subcommand runs one scenario, asserts on the proxied response, prints
PASS/FAIL per check, and exits 0 (all passed) or 1 (any failed).

Some scenarios assert spec-compliant behavior the proxy may not yet implement
(bad-request -> 400, length-required -> 411, chunked -> reject). They fail
loudly until the Rust side adds those responses -- that's intentional.
"""
import argparse
import hashlib
import socket
import sys
import time


# ── assertion helper ───────────────────────────────────────────────
def check(errors, cond, msg):
    print(f"  {'PASS' if cond else 'FAIL'}  {msg}")
    if not cond:
        errors.append(msg)


# ── HTTP helpers ───────────────────────────────────────────────────
def read_response(sock, buf=b""):
    """Read one full HTTP response. Returns (status, headers, body, leftover)."""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            if buf:
                raise ValueError("truncated response headers")
            raise ConnectionError("closed reading headers")
        buf += chunk
    head, buf = buf.split(b"\r\n\r\n", 1)
    lines = head.split(b"\r\n")
    status = int(lines[0].split(b" ", 2)[1])
    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(b":")
        headers[k.strip().lower().decode(errors="replace")] = \
            v.strip().decode(errors="replace")
    if "content-length" not in headers:
        raise ValueError("expected Content-Length in test response")
    cl = int(headers["content-length"])
    if cl < 0:
        raise ValueError("negative response Content-Length")
    while len(buf) < cl:
        chunk = sock.recv(65536)
        if not chunk:
            raise ValueError("truncated response body")
        buf += chunk
    return status, headers, buf[:cl], buf[cl:]


def build_request(method, path, host_header, body=b"", close=False,
                  extra_headers=()):
    lines = [f"{method} {path} HTTP/1.1", f"Host: {host_header}"]
    if body:
        lines.append(f"Content-Length: {len(body)}")
    if close:
        lines.append("Connection: close")
    for h in extra_headers:
        lines.append(h)
    return "\r\n".join(lines).encode() + b"\r\n\r\n" + body


def connect(a):
    return socket.create_connection((a.host, a.port), timeout=a.timeout)


# ── scenarios ──────────────────────────────────────────────────────
def cmd_simple(a):
    errors = []
    try:
        with connect(a) as s:
            s.sendall(build_request("GET", "/simple", a.host_header, close=True))
            status, _, body, _ = read_response(s)
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, b"method=GET" in body.split(), "body mentions GET")
    check(errors, b"path=/simple" in body.split(), "body mentions /simple")
    check(errors, b"body_len=0" in body.split(), "body_len == 0")
    return 0 if not errors else 1


def cmd_keepalive(a):
    errors = []
    try:
        with connect(a) as s:
            buf = b""
            for i in range(a.n):
                s.sendall(build_request("GET", f"/keepalive/{i}", a.host_header))
                status, _, body, buf = read_response(s, buf)
                check(errors, status == 200, f"req {i}: status 200 (got {status})")
                check(errors, f"path=/keepalive/{i}".encode() in body.split(),
                      f"req {i}: response matches request")
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    return 0 if not errors else 1


def cmd_pipelined(a):
    errors = []
    try:
        with connect(a) as s:
            batch = b"".join(
                build_request("GET", f"/pipe/{i}", a.host_header) for i in range(a.n)
            )
            s.sendall(batch)  # ALL requests in one syscall
            buf = b""
            for i in range(a.n):
                status, _, body, buf = read_response(s, buf)
                check(errors, status == 200, f"req {i}: status 200 (got {status})")
                check(errors, f"path=/pipe/{i}".encode() in body.split(),
                      f"req {i}: correct response order (got {body[:40]!r}...)")
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    return 0 if not errors else 1


def cmd_large_body(a):
    errors = []
    seed = hashlib.sha256(b"seed").digest()
    body = (seed * (a.size // len(seed) + 1))[: a.size]
    req = build_request("POST", "/large", a.host_header, body=body, close=True)
    expected = hashlib.md5(body).hexdigest()[:8]
    try:
        with connect(a) as s:
            for i in range(0, len(req), a.chunk):
                s.sendall(req[i : i + a.chunk])
                time.sleep(0.001)
            status, _, resp_body, _ = read_response(s)
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, f"body_len={len(body)}".encode() in resp_body.split(),
          f"body_len == {len(body)} (got {resp_body!r})")
    check(errors, f"body_md5={expected}".encode() in resp_body.split(),
          f"body_md5 == {expected} (no corruption)")
    return 0 if not errors else 1


def cmd_glued(a):
    errors = []
    body = b"glued-body-content"
    req = build_request("POST", "/glued", a.host_header, body=body, close=True)
    expected = hashlib.md5(body).hexdigest()[:8]
    try:
        with connect(a) as s:
            s.sendall(req)  # headers + body in ONE syscall -- tests "glued" bug
            status, _, resp_body, _ = read_response(s)
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, f"body_len={len(body)}".encode() in resp_body.split(),
          "body_len correct")
    check(errors, f"body_md5={expected}".encode() in resp_body.split(),
          "body_md5 matches")
    return 0 if not errors else 1


def cmd_partial(a):
    errors = []
    req = build_request("GET", "/partial", a.host_header, close=True)
    try:
        with connect(a) as s:
            for b in req:
                s.sendall(bytes([b]))
                time.sleep(a.delay)
            status, _, body, _ = read_response(s)
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, b"path=/partial" in body.split(), "path echoed")
    return 0 if not errors else 1


def cmd_chunked(a):
    """Chunked POST. --expect reject: proxy must reject (connection close, 400, or 501).
       --expect accept: proxy must forward -> 200 with body_len=12."""
    errors = []
    payload = [b"hello ", b"world", b"!"]
    total_len = sum(len(c) for c in payload)
    s = None
    got_response = False
    status, body = None, b""
    try:
        s = connect(a)
        s.sendall(
            (f"POST /chunked HTTP/1.1\r\n"
             f"Host: {a.host_header}\r\n"
             f"Transfer-Encoding: chunked\r\n"
             f"Connection: close\r\n"
             f"\r\n").encode()
        )
        for chunk in payload:
            s.sendall(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
        s.sendall(b"0\r\n\r\n")
        try:
            status, _, body, _ = read_response(s)
            got_response = True
        except ConnectionError:
            pass
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass

    if a.expect == "reject":
        ok = (not got_response) or status in (400, 501)
        check(errors, ok,
              f"proxy rejected chunked (got {'connection closed' if not got_response else f'status {status}'})")
    else:
        check(errors, got_response, "got a response")
        if got_response:
            check(errors, status == 200, f"status == 200 (got {status})")
            check(errors, f"body_len={total_len}".encode() in body.split(),
                  f"body_len == {total_len} (got {body!r})")
    return 0 if not errors else 1


def cmd_bad_request(a):
    """Send a malformed request line. Proxy must respond 400 (not just close)."""
    errors = []
    s = None
    got_response = False
    status, body = None, b""
    try:
        s = connect(a)
        # Garbage request line: no method/path/version structure.
        s.sendall(b"\x00\x01\x02 GARBAGE\r\n\r\n")
        try:
            status, _, body, _ = read_response(s)
            got_response = True
        except ConnectionError:
            pass
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass

    check(errors, got_response,
          f"proxy sent a response (received={got_response})")
    if got_response:
        check(errors, status == 400, f"status == 400 (got {status})")
    return 0 if not errors else 1


def cmd_length_required(a):
    """POST with no Content-Length and no Transfer-Encoding. Proxy must 411."""
    errors = []
    s = None
    got_response = False
    status, body = None, b""
    try:
        s = connect(a)
        s.sendall(
            (f"POST /nolength HTTP/1.1\r\n"
             f"Host: {a.host_header}\r\n"
             f"Connection: close\r\n"
             f"\r\n").encode()
        )
        try:
            status, _, body, _ = read_response(s)
            got_response = True
        except ConnectionError:
            pass
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    finally:
        if s:
            try:
                s.close()
            except Exception:
                pass

    check(errors, got_response,
          f"proxy sent a response (received={got_response})")
    if got_response:
        check(errors, status == 411, f"status == 411 (got {status})")
    return 0 if not errors else 1


# ── entrypoint ─────────────────────────────────────────────────────
SCENARIO_FN = {
    "simple": cmd_simple,
    "keepalive": cmd_keepalive,
    "pipelined": cmd_pipelined,
    "large-body": cmd_large_body,
    "glued": cmd_glued,
    "partial": cmd_partial,
    "chunked": cmd_chunked,
    "bad-request": cmd_bad_request,
    "length-required": cmd_length_required,
}


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--host", default="127.0.0.1")
    common.add_argument("--port", type=int, default=8080)
    common.add_argument("--host-header", default="example.test",
                        help="must match the proxy --outbound value")
    common.add_argument("--timeout", type=float, default=10.0)

    p = argparse.ArgumentParser(parents=[common])
    # Subparser defaults must not overwrite options given before the command.
    command_common = argparse.ArgumentParser(add_help=False)
    for action in common._actions:
        command_common.add_argument(
            *action.option_strings, dest=action.dest, type=action.type,
            default=argparse.SUPPRESS,
        )
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("simple", parents=[command_common])

    k = sub.add_parser("keepalive", parents=[command_common])
    k.add_argument("--n", type=int, default=5)

    pl = sub.add_parser("pipelined", parents=[command_common])
    pl.add_argument("--n", type=int, default=3)

    lb = sub.add_parser("large-body", parents=[command_common])
    lb.add_argument("--size", type=int, default=100_000)
    lb.add_argument("--chunk", type=int, default=8192)

    sub.add_parser("glued", parents=[command_common])

    pa = sub.add_parser("partial", parents=[command_common])
    pa.add_argument("--delay", type=float, default=0.001)

    ch = sub.add_parser("chunked", parents=[command_common])
    ch.add_argument("--expect", choices=["reject", "accept"], default="reject")

    sub.add_parser("bad-request", parents=[command_common])
    sub.add_parser("length-required", parents=[command_common])

    a = p.parse_args()
    if a.timeout <= 0:
        p.error("--timeout must be positive")
    for name in ("n", "chunk"):
        if hasattr(a, name) and getattr(a, name) <= 0:
            p.error(f"--{name} must be positive")
    for name in ("size", "delay"):
        if hasattr(a, name) and getattr(a, name) < 0:
            p.error(f"--{name} must be nonnegative")
    sys.exit(SCENARIO_FN[a.cmd](a))


if __name__ == "__main__":
    main()
