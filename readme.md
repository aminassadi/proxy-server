# HTTP Reverse Proxy Server

A multi-process, crash-resilient HTTP/1.1 reverse proxy written in Rust.

## Build

To build the project, run:

```bash
cargo b
```

## Run
```bash
./target/debug/proxy-server --inbound <IP>:<PORT> --outbound <HOSTNAME>:<PORT>
```

## Testing
start the origin server
```bash
python tests/server.py --host 127.0.0.1 --port 9000
```
start the proxy
```bash
/target/debug/proxy-server --inbound 127.0.0.1:8080 --outbound upstream:9000
```

run tests
```bash
python tests/run_all.py --host 127.0.0.1 --port 8080 --host-header upstream.com
```

### Architecture Summary
The proxy is designed around a Master-Worker multi-process model to guarantee crash resilience, maximize performance, and avoid data races, fully utilizing modern multi-core Linux systems.

1. Kernel-Level Load Balancing (SO_REUSEPORT)
 every worker process binds to the exact same inbound IP and port using the SO_REUSEPORT socket option. This delegates the responsibility of load balancing entirely to the Linux kernel. Incoming connections are distributed evenly and efficiently across the worker processes with zero user-space overhead.

2. Single-Threaded Tokio Workers
Each worker process is pinned to a specific CPU core via core_affinity. Inside the worker, we run a current-thread (single-threaded) Tokio runtime. Because the process is already isolated to a single CPU core, running multiple OS threads would only introduce context-switching overhead and cache misses. The single-threaded async runtime allows each worker to handle thousands of concurrent connections effortlessly through non-blocking I/O and state-machine based forwarding.

3. Unix Domain Socket (IPC) for Log Aggregation
To satisfy the /.svc/collect_logs requirement without using external storage (like disks or databases), we use a Unix Domain Socket (UDS) for Inter-Process Communication.

