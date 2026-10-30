#!/usr/bin/env python3
"""Test clients for the proxy. Each subcommand asserts and exits 0 (pass) / 1 (fail)."""
import argparse
import hashlib
import socket
import sys
import time


def check(errors, cond, msg):
    """Print PASS/FAIL for one assertion; record failure in `errors`."""
    print(f"  {'PASS' if cond else 'FAIL'}  {msg}")
    if not cond:
        errors.append(msg)


def read_response(sock, buf=b""):
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed reading headers")
        buf += chunk
    head, buf = buf.split(b"\r\n\r\n", 1)
    lines = head.split(b"\r\n")
    status = int(lines[0].split(b" ", 2)[1])
    headers = {}
    for line in lines[1:]:
        k, _, v = line.partition(b":")
        headers[k.strip().lower().decode()] = v.strip().decode()
    cl = int(headers.get("content-length", "0"))
    while len(buf) < cl:
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("closed reading body")
        buf += chunk
    return status, headers, buf[:cl], buf[cl:]


def build_request(method, path, host_header, body=b"", close=False):
    lines = [f"{method} {path} HTTP/1.1", f"Host: {host_header}"]
    if body:
        lines.append(f"Content-Length: {len(body)}")
    if close:
        lines.append("Connection: close")
    return "\r\n".join(lines).encode() + b"\r\n\r\n" + body


def connect(a):
    return socket.create_connection((a.host, a.port), timeout=a.timeout)


# ---------- scenarios ----------

def cmd_simple(a):
    errors = []
    try:
        s = connect(a)
        s.sendall(build_request("GET", "/simple", a.host_header, close=True))
        status, _, body, _ = read_response(s)
        s.close()
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, b"method=GET" in body, "body mentions GET")
    check(errors, b"path=/simple" in body, "body mentions /simple")
    check(errors, b"body_len=0" in body, "body_len == 0")
    return 0 if not errors else 1


def cmd_keepalive(a):
    errors = []
    try:
        s = connect(a)
        buf = b""
        for i in range(a.n):
            s.sendall(build_request("GET", f"/keepalive/{i}", a.host_header))
            status, _, body, buf = read_response(s, buf)
            check(errors, status == 200, f"req {i}: status 200 (got {status})")
            check(errors, f"path=/keepalive/{i}".encode() in body,
                  f"req {i}: correct path echoed")
        s.close()
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    return 0 if not errors else 1


def cmd_pipelined(a):
    errors = []
    try:
        s = connect(a)
        batch = b"".join(
            build_request("GET", f"/pipe/{i}", a.host_header) for i in range(a.n)
        )
        s.sendall(batch)  # ALL requests in one syscall
        buf = b""
        for i in range(a.n):
            status, _, body, buf = read_response(s, buf)
            check(errors, status == 200, f"req {i}: status 200 (got {status})")
            check(errors, f"path=/pipe/{i}".encode() in body,
                  f"req {i}: response order correct (got {body[:40]!r}...)")
        s.close()
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
        s = connect(a)
        for i in range(0, len(req), a.chunk):
            s.sendall(req[i : i + a.chunk])
            time.sleep(0.001)
        status, _, resp_body, _ = read_response(s)
        s.close()
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, f"body_len={len(body)}".encode() in resp_body,
          f"body_len == {len(body)} (got {resp_body!r})")
    check(errors, f"body_md5={expected}".encode() in resp_body,
          f"body_md5 == {expected} (no corruption)")
    return 0 if not errors else 1


def cmd_glued(a):
    errors = []
    body = b"glued-body-content"
    req = build_request("POST", "/glued", a.host_header, body=body, close=True)
    expected = hashlib.md5(body).hexdigest()[:8]
    try:
        s = connect(a)
        s.sendall(req)  # headers + body in ONE syscall — tests "glued body" bug
        status, _, resp_body, _ = read_response(s)
        s.close()
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, f"body_len={len(body)}".encode() in resp_body, "body_len correct")
    check(errors, f"body_md5={expected}".encode() in resp_body, "body_md5 matches")
    return 0 if not errors else 1


def cmd_partial(a):
    errors = []
    req = build_request("GET", "/partial", a.host_header, close=True)
    try:
        s = connect(a)
        for b in req:
            s.sendall(bytes([b]))
            time.sleep(a.delay)
        status, _, body, _ = read_response(s)
        s.close()
    except Exception as e:
        print(f"  FAIL  exception: {e}")
        return 1
    check(errors, status == 200, f"status == 200 (got {status})")
    check(errors, b"path=/partial" in body, "path echoed")
    return 0 if not errors else 1


def cmd_chunked(a):
    """Proxy currently rejects chunked requests; verify that.

    Use --expect accept to instead assert a full 200 response (when you add
    chunked support later, or when testing against the origin directly).
    """
    errors = []
    s = None
    try:
        s = connect(a)
        s.sendall(
            (f"POST /chunked HTTP/1.1\r\n"
             f"Host: {a.host_header}\r\n"
             f"Transfer-Encoding: chunked\r\n"
             f"Connection: close\r\n"
             f"\r\n").encode()
        )
        for chunk in [b"hello ", b"world", b"!"]:
            s.sendall(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
        s.sendall(b"0\r\n\r\n")

        try:
            status, _, body, _ = read_response(s)
            got_response = True
        except ConnectionError:
            got_response = False
            status, body = None, b""
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
        check(errors, not got_response, "proxy rejected chunked (connection closed)")
    else:
        check(errors, got_response, "got a response")
        if got_response:
            check(errors, status == 200, f"status == 200 (got {status})")
            check(errors, b"body_len=12" in body,
                  f"body_len == 12 (got {body!r})")
    return 0 if not errors else 1


# ---------- entrypoint ----------

def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--host", default="127.0.0.1")
    common.add_argument("--port", type=int, default=8080)
    common.add_argument("--host-header", default="example.test")
    common.add_argument("--timeout", type=float, default=10.0)

    p = argparse.ArgumentParser(parents=[common])
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("simple", parents=[common])

    k = sub.add_parser("keepalive", parents=[common])
    k.add_argument("--n", type=int, default=5)

    pl = sub.add_parser("pipelined", parents=[common])
    pl.add_argument("--n", type=int, default=3)

    lb = sub.add_parser("large-body", parents=[common])
    lb.add_argument("--size", type=int, default=100_000)
    lb.add_argument("--chunk", type=int, default=8192)

    sub.add_parser("glued", parents=[common])

    pa = sub.add_parser("partial", parents=[common])
    pa.add_argument("--delay", type=float, default=0.001)

    ch = sub.add_parser("chunked", parents=[common])
    ch.add_argument("--expect", choices=["reject", "accept"], default="reject")

    a = p.parse_args()
    rc = {
        "simple": cmd_simple,
        "keepalive": cmd_keepalive,
        "pipelined": cmd_pipelined,
        "large-body": cmd_large_body,
        "glued": cmd_glued,
        "partial": cmd_partial,
        "chunked": cmd_chunked,
    }[a.cmd](a)
    sys.exit(rc)


if __name__ == "__main__":
    main()