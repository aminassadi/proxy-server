pub fn error_response(status: u16, reason: &str, detail: &str) -> Vec<u8> {
    let body = format!("{status} {reason}\n{detail}\n");
    let body = body.replace('\r', "").replace('\n', " ").trim_end().to_string() + "\n";
    let mut out = Vec::with_capacity(128 + body.len());
    out.extend_from_slice(
        format!(
            "HTTP/1.1 {status} {reason}\r\n\
             Content-Type: text/plain; charset=utf-8\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            body.len()
        )
        .as_bytes(),
    );
    out.extend_from_slice(body.as_bytes());
    out
}

pub fn bad_request(reason: &str) -> Vec<u8> {
    error_response(400, "Bad Request", reason)
}

pub fn length_required() -> Vec<u8> {
    error_response(411, "Length Required", "request body length could not be determined")
}