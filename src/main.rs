use std::println;
use std::process;

use clap::Parser;
use nix::unistd::ForkResult;
use nix::unistd::fork;

#[derive(Parser, Debug)]
#[command(name = "proxy-server", version, about = "Simple proxy config")]
struct Args {
    #[arg(long)]
    inbound: String,

    #[arg(long)]
    outbound: String,
}

fn main() {
    let core_ids = core_affinity::get_core_ids().unwrap();
    println!("number of cores: {}", core_ids.len());
    let args = Args::parse();
    println!("inbound={} outbound={}", args.inbound, args.outbound);
    let mut children = Vec::new();

    for id in core_ids {
        match unsafe { fork() }.expect("Fork failed") {
            ForkResult::Parent { child } => {
                println!(
                    "Parent process (PID: {}) forked child (PID: {})",
                    std::process::id(),
                    child
                );
                children.push(child);
            }
            ForkResult::Child => {
                println!("Child process (PID: {}) starting on core: {}", std::process::id(), id.id);
                if !core_affinity::set_for_current(id) {
                    println!("core_affinity failed");
                }
                process::exit(0);
            }
        }
    }
    
}
