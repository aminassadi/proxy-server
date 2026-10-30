#!/usr/bin/env python3
"""Run every client scenario against the proxy and report a pass/fail summary.

Exit code: 0 if all scenarios pass, 1 otherwise (CI-friendly).
"""
import argparse
import subprocess
import sys
import time

# (name, client args, description)
SCENARIOS = [
    ("simple",     ["simple"],                                    "one GET, one response"),
    ("keepalive",  ["keepalive", "--n", "5"],                     "5 sequential reqs, one socket"),
    ("pipelined",  ["pipelined", "--n", "3"],                     "3 reqs sent before any read"),
    ("large-body", ["large-body", "--size", "100000"],            "100 KB body (>> MTU)"),
    ("glued",      ["glued"],                                     "headers + body in one syscall"),
    ("partial",    ["partial", "--delay", "0.001"],               "headers byte-by-byte"),
    ("chunked",    ["chunked", "--expect", "reject"],             "proxy rejects chunked"),
]


def run_one(name, client_args, base, timeout):
    """Run one scenario; return (passed: bool, duration: float, summary: str)."""
    cmd = [sys.executable, "client.py", *client_args, *base]
    start = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        dur = time.monotonic() - start
        # Echo the client's own PASS/FAIL lines (indented)
        if proc.stdout.strip():
            for line in proc.stdout.rstrip().splitlines():
                print(f"  {line}")
        if proc.stderr.strip():
            for line in proc.stderr.rstrip().splitlines():
                print(f"  stderr: {line}")
        passed = proc.returncode == 0
        return passed, dur, "ok" if passed else f"exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        dur = time.monotonic() - start
        return False, dur, f"TIMEOUT after {timeout}s"
    except Exception as e:
        dur = time.monotonic() - start
        return False, dur, f"exception: {e}"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--proxy-host", "--host", dest="proxy_host", default="127.0.0.1")
    p.add_argument("--proxy-port", "--port", dest="proxy_port", type=int, default=8080)
    p.add_argument("--host-header", default="example.test")
    p.add_argument("--timeout", type=float, default=30.0,
                   help="per-scenario timeout (seconds)")
    p.add_argument("--only", action="append", default=None,
                   help="run only these scenario(s), repeatable")
    a = p.parse_args()

    base = [
        "--host", a.proxy_host,
        "--port", str(a.proxy_port),
        "--host-header", a.host_header,
    ]

    selected = [s for s in SCENARIOS if a.only is None or s[0] in a.only]
    if not selected:
        print(f"no scenarios matched --only {a.only}", file=sys.stderr)
        sys.exit(2)

    results = []
    overall_start = time.monotonic()
    for name, client_args, desc in selected:
        print(f"\n=== {name}: {desc} ===")
        passed, dur, summary = run_one(name, client_args, base, a.timeout)
        results.append((name, passed, dur, summary))
        print(f"  -> {'PASS' if passed else 'FAIL'}  ({dur*1000:.0f} ms, {summary})")

    total = time.monotonic() - overall_start
    passed = sum(1 for _, ok, _, _ in results if ok)
    failed = len(results) - passed

    print("\n" + "=" * 60)
    print(f"{'SCENARIO':<14} {'RESULT':<8} {'TIME':>8}  DETAIL")
    print("-" * 60)
    for name, ok, dur, summary in results:
        print(f"{name:<14} {'PASS' if ok else 'FAIL':<8} {dur*1000:>7.0f}ms  {summary}")
    print("-" * 60)
    print(f"{'TOTAL':<14} {passed}/{len(results)} passed  ({total:.2f}s)")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()