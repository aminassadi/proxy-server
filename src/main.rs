use std::println;
use std::process;

use clap::Parser;
use nix::unistd::ForkResult;
use nix::unistd::fork;
use std::net::SocketAddr;

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
                process::exit(0);
            }
        }
    }

    println!("this is parent, pid: {}", process::id());
}
