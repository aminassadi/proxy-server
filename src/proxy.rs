use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, println, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio::{
    io,
    net::TcpListener,
    runtime::{Builder, Runtime},
};
use tokio_util::sync::CancellationToken;

use tokio::signal::unix::{SignalKind, signal};
pub struct AsyncProxy {
    inbound: SocketAddr,
    outbound: SocketAddr,
    rt: Runtime,
    shutdown: CancellationToken,
}

const MAX_BACKLOG: i32 = 1024;

impl AsyncProxy {
    pub fn new(inbound: SocketAddr, outbound: SocketAddr) -> std::io::Result<Self> {
        let rt = Builder::new_current_thread().enable_all().build()?;

        Ok(Self {
            inbound,
            outbound,
            rt,
            shutdown: CancellationToken::new(),
        })
    }

    pub fn run(self) -> std::io::Result<()> {
        self.rt
            .block_on(Self::runtime(self.inbound, self.outbound, self.shutdown))
    }

    async fn runtime(
        inbound: SocketAddr,
        outbound: SocketAddr,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let listener = bind_reuseport(inbound).await?;
        let outbound = Arc::new(outbound);
        tokio::spawn(install_shutdown_signals(shutdown.clone()));
        loop {
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    // stop accepting; in-flight tasks get dropped when we return
                    break;
                }
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let outbound = Arc::clone(&outbound);
                    let shutdown = shutdown.clone();

                    tokio::spawn(async move {
                        println!("incomming socket: {peer}, pid: {}", std::process::id());
                        tokio::select! {
                            _ = shutdown.cancelled() => {}
                            r = handle_connection(stream, peer, &outbound) => {
                                if let Err(e) = r {
                                    eprintln!("[{peer}] {e}");
                                }
                             }
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

pub async fn bind_reuseport(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    socket.listen(MAX_BACKLOG)?;

    let std_listener: std::net::TcpListener = socket.into();
    std_listener.set_nonblocking(true)?;
    TcpListener::from_std(std_listener)
}

async fn install_shutdown_signals(shutdown: CancellationToken) {
    let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");

    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    }
    println!("shutdown received");
    shutdown.cancel();
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    _outbound: &std::net::SocketAddr,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(8192);
    let mut headers = [httparse::EMPTY_HEADER; 64];

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }

        let header_len = {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut req = httparse::Request::new(&mut headers);

            match req.parse(&buf) {
                Ok(httparse::Status::Complete(n)) => {
                    println!(
                        "[{peer}] {} {} HTTP/1.{}",
                        req.method.unwrap_or("?"),
                        req.path.unwrap_or("?"),
                        req.version.unwrap_or(0),
                    );

                    for h in req.headers.iter() {
                        println!("  {}: {}", h.name, String::from_utf8_lossy(h.value));
                    }

                    n
                }
                Ok(httparse::Status::Partial) => {
                    if buf.len() > 64 * 1024 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "request headers exceed 64 KiB",
                        ));
                    }
                    continue;
                }
                Err(e) => {
                    eprintln!("[{peer}] malformed: {e}");
                    return Ok(());
                }
            }
        }; // req and headers drop here; buf can be mutated again

        println!(
            "  headers_end={header_len}, body_bytes_so_far={}",
            buf.len() - header_len,
        );

        // Process the request before reading more, or consume the processed bytes.
        return Ok(());
    }
}
