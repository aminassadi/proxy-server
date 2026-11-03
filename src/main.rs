use core::panic;
use std::os::unix::net::UnixListener;
use std::println;
use std::process;

use clap::Parser;
use nix::sys::signal::Signal;
use nix::sys::signal::kill;

use nix::sys::wait::waitpid;
use nix::unistd::ForkResult;
use nix::unistd::Pid;
use nix::unistd::fork;
use proxy_server;
use proxy_server::log_server::logs::Logs;
use proxy_server::log_server::server::LogServer;
use tokio::runtime::Builder;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio_util::sync::CancellationToken;
use std::net::SocketAddr;

const LOG_SERVER_SOURCE: &str = "log-source.sock";
const LOG_SERVER_SINK: &str = "log-sink.sock";

#[derive(Parser, Debug)]
#[command(name = "proxy-server", version, about = "Simple proxy config")]
struct Args {
    #[arg(long)]
    inbound: String,

    #[arg(long)]
    outbound: String,
}

fn main() {
    let args = Args::parse();
    println!("inbound={} outbound={}", args.inbound, args.outbound);
    let inbound: SocketAddr = args.inbound.parse().unwrap_or_else(|e| {
        panic!(
            "invalid --inbound value '{}': {} (expected IP:PORT, e.g. 127.0.0.1:8080)",
            args.inbound, e
        )
    });

    let input = UnixListener::bind(LOG_SERVER_SOURCE).unwrap_or_else(|e|{
          panic!(
            "Binding to the unix socket filed, address:{}, Error:{}",
            LOG_SERVER_SOURCE, e
        )
    });
    let output = match UnixListener::bind(LOG_SERVER_SINK) {
        Ok(output) => output,
        Err(e) => {
            drop(input);
            let _ = std::fs::remove_file(LOG_SERVER_SOURCE);
           panic!(
            "Binding to the unix socket filed, address:{}, Error:{}",
            LOG_SERVER_SINK, e
        )
        }
    };

    let core_ids = core_affinity::get_core_ids().unwrap();
    println!("number of cores: {}", core_ids.len());
    let mut children = Vec::new();

    for id in core_ids {
        match unsafe { fork() }.expect("Fork failed") {
            ForkResult::Parent { child } => {
                children.push(child);
            }
            ForkResult::Child => {
                if !core_affinity::set_for_current(id) {
                    println!("core_affinity failed");
                }
                let proxy = proxy_server::proxy::AsyncProxy::new(inbound, args.outbound, LOG_SERVER_SOURCE.into(), LOG_SERVER_SINK.into())
                    .unwrap_or_else(|e| panic!("Initiling worker process failed with error: {e}"));
                let _ =proxy.run().unwrap_or_else(|e|{panic!("running proxy server failed with error: {e}")});
                process::exit(0);
            }
        }
    }
    run_log_server(input, output);
    stop_workers(&children);
    let _ = std::fs::remove_file(LOG_SERVER_SOURCE);
    let _ = std::fs::remove_file(LOG_SERVER_SINK);
}

fn run_log_server(input: UnixListener, output: UnixListener) -> std::io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async move {
        input.set_nonblocking(true)?;
        output.set_nonblocking(true)?;
        let server = LogServer::from_listeners(
            tokio::net::UnixListener::from_std(input)?,
            tokio::net::UnixListener::from_std(output)?,
            Logs::new(),
        );
        let shutdown = CancellationToken::new();
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let result = tokio::select! {
            result = server.run(shutdown.clone()) => result,
            _ = sigint.recv() => Ok(()),
            _ = sigterm.recv() => Ok(()),
        };
        shutdown.cancel();
        result
    })
}


fn stop_workers(children: &[Pid]) {
    for &pid in children {
        let _ = kill(pid, Signal::SIGTERM);
    }

    for &pid in children {
        loop {
            match waitpid(pid, None) {
                Ok(_) => break,
                Err(nix::errno::Errno::EINTR) => continue,
                Err(_) => break,
            }
        }
    }
}
