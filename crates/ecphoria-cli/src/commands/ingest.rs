//! `ecphoria ingest` — send events from a local file to the server.
//!
//! The file is read and parsed here rather than passed to the server as a path: the server would
//! resolve it against *its own* filesystem, which is both surprising for the caller and an
//! arbitrary-file-read primitive.
//!
//! Accepts either a JSON array of event objects, a single event object, or JSON Lines (one object
//! per line) — the three shapes a log export actually arrives in.

use crate::client::EcphoriaClient;
use crate::output;

/// Server limit (`MAX_INGEST_EVENTS`); larger files are sent in several requests.
const BATCH: usize = 10_000;

pub async fn run(url: &str, source: &str, file: &str) -> anyhow::Result<()> {
    let raw =
        std::fs::read_to_string(file).map_err(|e| anyhow::anyhow!("failed to read {file}: {e}"))?;
    let events = parse_events(&raw).map_err(|e| anyhow::anyhow!("{file}: {e}"))?;
    if events.is_empty() {
        anyhow::bail!("{file} contains no events");
    }

    let client = EcphoriaClient::new(url);
    let mut ingested = 0u64;
    for chunk in events.chunks(BATCH) {
        let result = client.ingest(source, chunk.to_vec()).await?;
        ingested += result.get("ingested").and_then(|v| v.as_u64()).unwrap_or(0);
        if let Some(err) = result.get("error") {
            anyhow::bail!("{err}");
        }
    }
    output::print_json(
        &serde_json::json!({ "source": source, "sent": events.len(), "ingested": ingested }),
        false,
    );
    Ok(())
}

/// Parse a JSON array, a single object, or JSON Lines into a list of events.
fn parse_events(raw: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    // Whole-document JSON first: an array is the common export shape, a bare object a single event.
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return match v {
            serde_json::Value::Array(items) => Ok(items),
            obj @ serde_json::Value::Object(_) => Ok(vec![obj]),
            other => anyhow::bail!("expected an array or object of events, got {other}"),
        };
    }
    // Otherwise JSON Lines. Report the offending line rather than a generic parse failure.
    let mut out = Vec::new();
    for (i, line) in trimmed.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| anyhow::anyhow!("line {}: {e}", i + 1))?;
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_three_shapes() {
        let array = parse_events(r#"[{"a":1},{"a":2}]"#).unwrap();
        assert_eq!(array.len(), 2);

        let single = parse_events(r#"{"a":1}"#).unwrap();
        assert_eq!(single.len(), 1);

        let lines = parse_events("{\"a\":1}\n\n{\"a\":2}\n").unwrap();
        assert_eq!(lines.len(), 2, "blank lines must be skipped");

        assert!(parse_events("   ").unwrap().is_empty());
    }

    #[test]
    fn reports_the_offending_line() {
        let err = parse_events("{\"a\":1}\nnot json\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("line 2"), "{err}");
    }

    #[test]
    fn rejects_a_bare_scalar() {
        assert!(parse_events("42").is_err());
    }
}
