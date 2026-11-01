use httparse::parse_headers;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use std::println;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Runtime};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use crate::http::error::{bad_request, error_response, length_required};
use crate::http::parser::{MAX_HEADER_SIZE, ParseResult};

const MAX_BACKLOG: i32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyState {
    ReadingHeaders,
    ForwardingBody { remaining: usize },
}

async fn handle_connection(
    mut client_stream: TcpStream,
    peer: SocketAddr,
    outbound: &str,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let mut client_buff = Vec::with_capacity(8192);
    let mut server_buff = vec![0u8; 8192];
    let mut state = ProxyState::ReadingHeaders;
     let connect_addr = if outbound.contains(':') {
        outbound.to_string()
    } else {
        format!("{}:80", outbound)
    };

    // 3. Connect using the properly formatted address
    let mut server_stream = TcpStream::connect(&connect_addr).await?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,

            r = client_stream.read_buf(&mut client_buff) => {
                let n = r?;
                if n == 0 { return Ok(()); } // Client closed connection

                loop {
                    match state {
                        ProxyState::ReadingHeaders => {
                            if client_buff.is_empty() { break; }

                            if client_buff.len() > MAX_HEADER_SIZE {
                                let _ = client_stream.write_all(&bad_request("Request Header Fields Too Large")).await;
                                return Ok(());
                            }

                            match crate::http::parser::parse_headers(&client_buff, &outbound) {
                                ParseResult::Incomplete => break,
                                ParseResult::Invalid(e) => {
                                    eprintln!("[{peer}] Invalid: {e}");
                                    let _ = client_stream.write_all(&bad_request(&e)).await;
                                    return Ok(());
                                }
                                ParseResult::Unsupported(e) => {
                                    eprintln!("[{peer}] Unsupported: {e}");
                                    let _ = client_stream.write_all(&error_response(501, "Not Implemented", &e)).await;
                                    return Ok(());
                                }
                                ParseResult::LengthRequired => {
                                    let _ = client_stream.write_all(&length_required()).await;
                                    return Ok(());
                                }
                                ParseResult::Complete { header_len, content_length } => {
                                    server_stream.write_all(&client_buff[..header_len]).await?;
                                    client_buff.drain(..header_len);

                                    if content_length == 0 {
                                        state = ProxyState::ReadingHeaders;
                                    } else {
                                        state = ProxyState::ForwardingBody { remaining: content_length };
                                    }
                                }
                            }
                        }

                        ProxyState::ForwardingBody { mut remaining } => {
                            let take = remaining.min(client_buff.len());
                            if take > 0 {
                                server_stream.write_all(&client_buff[..take]).await?;
                                client_buff.drain(..take);
                                remaining -= take;
                            }

                            if remaining == 0 {
                                state = ProxyState::ReadingHeaders;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }

            r = server_stream.read(&mut server_buff) => {
                let n = r?;
                if n == 0 {
                    return Ok(());
                }
                client_stream.write_all(&server_buff[..n]).await?;
            }
        }
    }
    Ok(())
}

pub async fn bind_reuseport(addr: SocketAddr) -> std::io::Result<TcpListener> {
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

pub struct AsyncProxy {
    inbound: SocketAddr,
    outbound: String,
    rt: Runtime,
    shutdown: CancellationToken,
}

impl AsyncProxy {
    // Note: outbound is now a String, not a SocketAddr
    pub fn new(inbound: SocketAddr, outbound: String) -> std::io::Result<Self> {
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
        outbound: String,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let listener = bind_reuseport(inbound).await?;
        let outbound = Arc::new(outbound);

        tokio::spawn(install_shutdown_signals(shutdown.clone()));

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let outbound = Arc::clone(&outbound);
                    let shutdown = shutdown.clone();

                    tokio::spawn(async move {
                        tokio::select! {
                            _ = shutdown.cancelled() => {}
                            r = handle_connection(stream, peer, &outbound, shutdown.clone()) => {
                                if let Err(e) = r {
                                    eprintln!("[{peer}] Connection Error: {e}");
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
