use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io,
    net::TcpListener,
    runtime::{Builder, Runtime},
};
use tokio_util::sync::CancellationToken;
struct AsyncProxy {
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
                        tokio::select! {
                            _ = shutdown.cancelled() => {}
                            // r = handle_connection(stream, peer, &outbound) => {
                            //     if let Err(e) = r {
                            //         eprintln!("[{peer}] {e}");
                            //     }
                            // }
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
