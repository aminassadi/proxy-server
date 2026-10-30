use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, println, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    io,
    net::TcpListener,
    runtime::{Builder, Runtime},
};
use tokio_util::sync::CancellationToken;

use tokio::net::{TcpSocket, TcpStream};
use tokio::signal::unix::{SignalKind, signal};

const MAX_BACKLOG: i32 = 1024;

pub struct AsyncProxy {
    inbound: SocketAddr,
    outbound: SocketAddr,
    rt: Runtime,
    shutdown: CancellationToken,
}

impl AsyncProxy {
    pub fn new(inbound: SocketAddr, outbound: SocketAddr) -> std::io::Result<Self> {
        let rt: Runtime = Builder::new_current_thread().enable_all().build()?;

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
                            r = handle_connection(stream, peer, &outbound, shutdown.clone()) => {
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

async fn handle_connection(
    mut client_stream: tokio::net::TcpStream,
    peer: std::net::SocketAddr,
    outbound: &std::net::SocketAddr,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut client_buff = Vec::with_capacity(64 * 1024);
    let mut server_buff = vec![0u8; 8192];
    let mut content_len: Option<usize> = None;
    let mut body_received: usize = 0;

    let mut server_stream = TcpStream::connect(outbound).await?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            r = client_stream.read_buf(&mut client_buff) => {
                let incomming_packet_len = r?;
                if incomming_packet_len == 0 {
                    return Ok(());
                }
                while !client_buff.is_empty(){
                    if content_len.is_none(){
                        let mut headers = [httparse::EMPTY_HEADER; 64];
                        let mut req = httparse::Request::new(&mut headers);
                        match req.parse(&client_buff) {
                            Ok(httparse::Status::Complete(n)) => {
                                println!(
                                    "[{peer}] {} {} HTTP/1.{}",
                                    req.method.unwrap_or("?"),
                                    req.path.unwrap_or("?"),
                                    req.version.unwrap_or(0),
                                );
                                let mut cl = 0usize;
                                for h in req.headers.iter() {
                                    println!("  {}: {}", h.name, String::from_utf8_lossy(h.value));
                                    if h.name.eq_ignore_ascii_case("transfer-encoding") {
                                            let v = String::from_utf8_lossy(h.value).to_ascii_lowercase();
                                            if v.split(',').any(|t| t.trim() == "chunked") {
                                                return Err(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    "chunked not yet supported",
                                                ));
                                            }
                                        }
                                    if h.name.eq_ignore_ascii_case("content-length") {
                                        cl = std::str::from_utf8(h.value)
                                        .ok()
                                        .and_then(|s| s.trim().parse().ok())
                                        .ok_or_else(|| std::io::Error::new(
                                            std::io::ErrorKind::InvalidData, "bad Content-Length"))?;
                                    }
                                }
                                server_stream.write_all(&client_buff[..n]).await?;
                                client_buff.drain(..n);
                                content_len = Some(cl);
                            }
                            Ok(httparse::Status::Partial) => {
                                if client_buff.len() > 64 * 1024 {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "request headers exceed 64 KiB",
                                    ));
                                }
                                break;
                            }
                            Err(e) => {
                                eprintln!("[{peer}] malformed: {e}");
                                return Ok(());
                            }
                        }
                    }
                      if let Some(cl) = content_len {
                        let remaining = cl - body_received;
                        let take = remaining.min(client_buff.len());
                        if take > 0 {
                            server_stream.write_all(&client_buff[..take]).await?;
                            client_buff.drain(..take);             // BytesMut: use .advance(take)
                            body_received += take;
                        }
                        if body_received >= cl {
                            content_len = None;
                            body_received = 0;
                        }else{
                            break;
                        }
                 }
                    }



            }
            r = server_stream.read(&mut server_buff) =>{
                let n = r?;
                if n==0{
                    return Ok(()) //todo: this is error server connection failed, may response with 500
                }
                client_stream.write_all(&server_buff[..n]).await?
            }
        }
    }
    return Ok(());
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

pub fn validate_request() {}
