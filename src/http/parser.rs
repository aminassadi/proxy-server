pub const MAX_HEADER_SIZE: usize = 64 * 1024;
pub const COLLECT_LOG_URI: &str = ".svc/collect-log";
#[derive(Debug)]
pub enum ParseResult {
    Incomplete,
    Complete { header_len: usize, content_length: usize },
    CollectLog,
    Invalid(String),
    Unsupported(String),
    LengthRequired,
}

pub fn parse_headers(buf: &[u8], expected_host: &str) -> ParseResult {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut req = httparse::Request::new(&mut headers);

    match req.parse(buf) {
        Ok(httparse::Status::Partial) => ParseResult::Incomplete,
        Ok(httparse::Status::Complete(header_len)) => {
            let mut content_length = None;
            let mut host = "";
            let method = req.method.unwrap_or("");

            for h in req.headers.iter() {
                if h.name.eq_ignore_ascii_case("host") {
                    host = std::str::from_utf8(h.value).unwrap_or("");
                }
                if h.name.eq_ignore_ascii_case("content-length") {
                    content_length = std::str::from_utf8(h.value)
                        .ok()
                        .and_then(|s| s.trim().parse::<usize>().ok());
                }
            }

            if req.path == Some(COLLECT_LOG_URI){
                if method != "GET"{
                    return ParseResult::Invalid("Invalid method".into());
                }
                return ParseResult::CollectLog;
            }

            if host != expected_host {
                return ParseResult::Invalid(format!("Invalid or missing Host (expected {})", expected_host));
            }

            if content_length.is_none() && matches!(method, "POST" | "PUT" | "PATCH") {
                return ParseResult::Unsupported("Missing Content-Length (or unsupported framing)".into());
            }

            ParseResult::Complete {
                header_len,
                content_length: content_length.unwrap_or(0), // default 0 for GET
            }
        }
        Err(_) => ParseResult::Invalid("Malformed HTTP request".into()),
    }
}