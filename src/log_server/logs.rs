use std::sync::Arc;
use std::sync::Mutex;

const MAX_LOGS: usize = 100;
#[derive(Clone)]
pub struct Logs {
    logs_holder: Arc<Mutex<Vec<String>>>,
}

impl Logs {
    pub fn new() -> Self {
        let logs_holder = Arc::new(Mutex::new(Vec::<String>::new()));
        Self { logs_holder }
    }

    pub fn add(&self, log: String) -> std::io::Result<()> {
        let mut logs = self
            .logs_holder
            .lock()
            .map_err(|_| std::io::Error::other("logs mutex poisoned"))?;
        if logs.len() >= MAX_LOGS {
            return Err(std::io::Error::new(std::io::ErrorKind::StorageFull, "full"));
        }
        if log.contains(['\r', '\n']) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "log must be one line",
            ));
        }
        logs.push(log);
        Ok(())
    }

    pub fn get(&self) -> std::io::Result<Vec<String>> {
        let mut logs = self
            .logs_holder
            .lock()
            .map_err(|_| std::io::Error::other("logs mutex poisoned"))?;
        let snapshot = logs.clone();
        logs.clear();
        Ok(snapshot)
    }
}
