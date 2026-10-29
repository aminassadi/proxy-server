use core::panic;
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
use std::net::SocketAddr;

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

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

    let outbound: SocketAddr = args.outbound.parse().unwrap_or_else(|e| {
        panic!(
            "invalid --outbound value '{}': {} (expected IP:PORT, e.g. 127.0.0.1:9090)",
            args.outbound, e
        )
    });

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
                let proxy = proxy_server::proxy::AsyncProxy::new(inbound, outbound)
                    .unwrap_or_else(|e| panic!("Initiling worker process failed with error: {e}"));
                let _ =proxy.run().unwrap_or_else(|e|{panic!("running proxy server failed with error: {e}")});
                process::exit(0);
            }
        }
    }
    println!("this is parent, pid: {}", process::id());
    wait_for_shutdown(&children);
}

fn wait_for_shutdown(children: &[Pid]) -> std::io::Result<()> {
    let mut signals = Signals::new([SIGINT, SIGTERM])?;

    let _ = signals.forever().next();

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

    Ok(())
}
