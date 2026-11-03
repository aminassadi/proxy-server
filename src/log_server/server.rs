use std::io;
use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

use super::logs::Logs;

pub struct LogServer {
    input: UnixListener,
    output: UnixListener,
    logs: Logs,
}

impl LogServer {

    pub fn from_listeners(input: UnixListener, output: UnixListener, logs: Logs) -> Self {
        Self {
            input,
            output,
            logs,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                accepted = self.input.accept() => {
                    let (stream, _) = accepted?;
                    let logs = self.logs.clone();
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        if let Err(error) = receive_logs(stream, logs, shutdown).await {
                            eprintln!("Log connection error: {error}");
                        }
                    });
                }
                accepted = self.output.accept() => {
                    let (stream, _) = accepted?;
                    let logs = self.logs.clone();
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        if let Err(error) = send_logs(stream, logs, shutdown).await {
                            eprintln!("Log snapshot error: {error}");
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

async fn receive_logs(
    stream: UnixStream,
    logs: Logs,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            result = stream.read_until(b'\n', &mut line) => result?,
        };
        if n == 0 {
            return Ok(());
        }

        // The newline separates messages on the socket; it is not part of the stored log.
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        let line = std::str::from_utf8(&line)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        logs.add(line.to_owned())?;
    }
}

async fn send_logs(
    mut stream: UnixStream,
    logs: Logs,
    shutdown: CancellationToken,
) -> io::Result<()> {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => Ok(()),
        result = async {
            let entries = logs.get()?;
            for entry in entries {
                stream.write_all(entry.as_bytes()).await?;
                stream.write_all(b"\n").await?;
            }
            stream.shutdown().await
        } => result,
    }
}
