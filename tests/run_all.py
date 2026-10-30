#!/usr/bin/env python3
"""Run every client scenario against the proxy and report a pass/fail summary.

Exit code: 0 if all selected scenarios pass, 1 otherwise (CI-friendly).

Some scenarios (bad-request, length-required, chunked --expect reject) assert
spec-compliant error responses that the proxy may not yet implement. They fail
loudly until the Rust side sends 400/411 instead of just closing. Use --only
to run a subset while developing.
"""
import argparse
import subprocess
import sys
import time

# (name, client args, description, is_aspirational)
# is_aspirational=True means "requires Rust-side changes to pass".
SCENARIOS = [
    ("simple",          ["simple"],                                    "one GET, one response",                 False),
    ("keepalive",       ["keepalive", "--n", "5"],                     "5 sequential reqs, one socket",          False),
    ("pipelined",       ["pipelined", "--n", "3"],                     "3 reqs sent before any read",            False),
    ("large-body",      ["large-body", "--size", "100000"],            "100 KB body (>> MTU)",                   False),
    ("glued",           ["glued"],                                     "headers + body in one syscall",          False),
    ("partial",         ["partial", "--delay", "0.001"],               "headers byte-by-byte",                   False),
    ("chunked",         ["chunked", "--expect", "reject"],             "proxy rejects chunked (400/close)",      True),
    ("bad-request",     ["bad-request"],                               "malformed request -> 400",               True),
    ("length-required", ["length-required"],                           "POST no CL/TE -> 411",                   True),
]


def run_one(client_args, base, timeout):
    """Run one scenario; return (passed, duration_s, summary_str, captured_output)."""
    cmd = [sys.executable, "client.py", *client_args, *base]
    start = time.monotonic()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        dur = time.monotonic() - start
        out = proc.stdout
        if proc.stderr.strip():
            out += "\nSTDERR:\n" + proc.stderr
        passed = proc.returncode == 0
        summary = "ok" if passed else f"exit {proc.returncode}"
        return passed, dur, summary, out
    except subprocess.TimeoutExpired:
        dur = time.monotonic() - start
        return False, dur, f"TIMEOUT after {timeout}s", ""
    except Exception as e:
        dur = time.monotonic() - start
        return False, dur, f"exception: {e}", ""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--proxy-host", "--host", dest="proxy_host", default="127.0.0.1")
    p.add_argument("--proxy-port", "--port", dest="proxy_port", type=int, default=8080)
    p.add_argument("--host-header", default="example.test")
    p.add_argument("--timeout", type=float, default=30.0,
                   help="per-scenario timeout (seconds)")
    p.add_argument("--only", action="append", default=None,
                   help="run only these scenario(s), repeatable")
    p.add_argument("--skip-aspirational", action="store_true",
                   help="skip scenarios that require Rust-side changes")
    a = p.parse_args()

    base = [
        "--host", a.proxy_host,
        "--port", str(a.proxy_port),
        "--host-header", a.host_header,
    ]

    selected = []
    for name, args, desc, aspirational in SCENARIOS:
        if a.only is not None and name not in a.only:
            continue
        if a.skip_aspirational and aspirational:
            continue
        selected.append((name, args, desc, aspirational))

    if not selected:
        print(f"no scenarios matched --only {a.only}", file=sys.stderr)
        sys.exit(2)

    results = []
    overall_start = time.monotonic()
    for name, client_args, desc, aspirational in selected:
        tag = " [aspirational]" if aspirational else ""
        print(f"\n=== {name}: {desc}{tag} ===")
        passed, dur, summary, out = run_one(client_args, base, a.timeout)
        # Echo the client's own PASS/FAIL lines (already indented)
        if out.strip():
            for line in out.rstrip().splitlines():
                if line.startswith("STDERR:"):
                    print(f"  {line}")
                else:
                    print(f"  {line}")
        results.append((name, passed, dur, summary, aspirational))
        print(f"  -> {'PASS' if passed else 'FAIL'}  ({dur*1000:.0f} ms, {summary})")

    total = time.monotonic() - overall_start
    passed = sum(1 for _, ok, _, _, _ in results if ok)
    failed = len(results) - passed

    print("\n" + "=" * 66)
    print(f"{'SCENARIO':<18} {'RESULT':<8} {'TIME':>8}  DETAIL")
    print("-" * 66)
    for name, ok, dur, summary, aspirational in results:
        tag = "*" if aspirational else " "
        print(f"{name+tag:<18} {'PASS' if ok else 'FAIL':<8} {dur*1000:>7.0f}ms  {summary}")
    print("-" * 66)
    print(f"{'TOTAL':<18} {passed}/{len(results)} passed  ({total:.2f}s)")
    if any(asp for *_, asp in results):
        print("\n* = aspirational (requires Rust-side changes to pass)")

    if failed:
        print("\nFAILURES:")
        for name, ok, _, summary, aspirational in results:
            if not ok:
                note = "  (requires Rust-side changes)" if aspirational else ""
                print(f"  - {name}: {summary}{note}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()