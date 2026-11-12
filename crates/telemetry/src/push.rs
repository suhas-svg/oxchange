//! Pushing a batch job's metrics to a Prometheus Pushgateway. Prometheus
//! cannot scrape a job that has already exited (the auditor runs for
//! seconds), so the job pushes its results before it ends.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TIMEOUT: Duration = Duration::from_secs(10);

/// Replaces the metrics stored for `job` on the Pushgateway at `base_url`
/// (`http://host:port`) with `body`, in Prometheus text format.
pub async fn push(base_url: &str, job: &str, body: &str) -> Result<(), String> {
    let rest = base_url
        .trim_end_matches('/')
        .strip_prefix("http://")
        .ok_or_else(|| format!("{base_url:?}: only http:// Pushgateway URLs are supported"))?;
    let (host, prefix) = match rest.split_once('/') {
        Some((h, p)) => (h, format!("/{p}")),
        None => (rest, String::new()),
    };
    let addr = if host.contains(':') {
        host.to_owned()
    } else {
        format!("{host}:80")
    };
    let request = format!(
        "PUT {prefix}/metrics/job/{job} HTTP/1.1\r\nHost: {host}\r\n\
         Content-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let exchange = async {
        let mut s = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("connect {addr}: {e}"))?;
        s.write_all(request.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut reply = Vec::new();
        s.read_to_end(&mut reply).await.map_err(|e| e.to_string())?;
        Ok::<_, String>(reply)
    };
    let reply = tokio::time::timeout(TIMEOUT, exchange)
        .await
        .map_err(|_| format!("{addr}: no answer in {TIMEOUT:?}"))??;
    let status_line = String::from_utf8_lossy(&reply)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    if status_line.contains(" 200 ") || status_line.contains(" 202 ") {
        Ok(())
    } else {
        Err(format!("Pushgateway answered {status_line:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn sends_a_put_and_accepts_200() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = s.read(&mut buf).await.unwrap();
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            String::from_utf8_lossy(&buf[..n]).into_owned()
        });
        push(&format!("http://127.0.0.1:{port}"), "auditor", "x 1\n")
            .await
            .unwrap();
        let req = server.await.unwrap();
        assert!(req.starts_with("PUT /metrics/job/auditor HTTP/1.1"));
        assert!(req.ends_with("x 1\n"));
    }
}
