use std::collections::HashMap;
use std::path::Path;

use serde_json::Value;

use crate::client::EcphoriaClient;

/// GDPR entity export: dump every episodic event mentioning `entity` as NDJSON.
pub async fn run(url: &str, entity: &str) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);

    // Query all episodic events for this entity
    let sql =
        format!("SELECT * FROM episodic WHERE payload::VARCHAR LIKE '%{entity}%' ORDER BY ts");
    let result = client.query(&sql).await?;

    let rows = result["rows"].as_array();
    let count = rows.map(|r| r.len()).unwrap_or(0);

    if count == 0 {
        println!("No data found for entity: {entity}");
    } else {
        // Output as NDJSON (one JSON object per line) for GDPR export
        if let Some(rows) = rows {
            for row in rows {
                println!("{}", serde_json::to_string(row)?);
            }
        }
        eprintln!("Exported {count} records for entity: {entity}");
    }

    Ok(())
}

/// Export active memories to an Obsidian-style markdown vault — the inverse of
/// `import --from obsidian`, completing the round-trip. One note per memory (filename from the
/// subject, else the id), YAML frontmatter carrying the memory's fields, and `[[wikilinks]]`
/// reconstructed from the knowledge-graph edges (so Obsidian's graph view lights up).
pub async fn run_obsidian(url: &str, vault: &str, user: Option<&str>) -> anyhow::Result<()> {
    let client = EcphoriaClient::new(url);

    let mut mpath = "/api/v1/memories?limit=10000".to_string();
    if let Some(u) = user {
        mpath.push_str(&format!("&user_id={u}"));
    }
    let mems = client.get_json(&mpath).await?;
    let memories = mems["memories"].as_array().cloned().unwrap_or_default();
    if memories.is_empty() {
        println!("No memories to export.");
        return Ok(());
    }

    // All graph edges → a map of source-subject → outgoing link targets.
    let edges = client
        .get_json("/api/v1/memories/edges?limit=100000")
        .await
        .unwrap_or(Value::Null);
    let links = link_map(&edges);

    let root = Path::new(vault);
    std::fs::create_dir_all(root)?;
    let mut used: HashMap<String, u32> = HashMap::new();
    let mut written = 0u32;
    for m in &memories {
        // Unique filename: sanitized title, disambiguated with a counter on collision.
        let base = sanitize_filename(&note_title(m));
        let n = used.entry(base.clone()).or_insert(0);
        let fname = if *n == 0 {
            format!("{base}.md")
        } else {
            format!("{base}-{n}.md")
        };
        *n += 1;
        std::fs::write(root.join(&fname), render_note(m, &links))?;
        written += 1;
    }

    println!("Exported {written} memory note(s) to {vault} (Obsidian vault).");
    Ok(())
}

/// Build `src → [dst, …]` from the `{ "edges": [...] }` payload.
fn link_map(edges: &Value) -> HashMap<String, Vec<String>> {
    let mut links: HashMap<String, Vec<String>> = HashMap::new();
    if let Some(arr) = edges.get("edges").and_then(|e| e.as_array()) {
        for e in arr {
            if let (Some(src), Some(dst)) = (e["src"].as_str(), e["dst"].as_str()) {
                links
                    .entry(src.to_string())
                    .or_default()
                    .push(dst.to_string());
            }
        }
    }
    links
}

/// Note title: the memory's subject if present, else a short id.
fn note_title(m: &Value) -> String {
    m["subject"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            let id = m["id"].as_str().unwrap_or("memory");
            format!("memory-{}", id.split('-').next().unwrap_or(id))
        })
}

/// Filesystem-safe note name: strip path separators and characters Obsidian/OSes dislike.
fn sanitize_filename(title: &str) -> String {
    let cleaned: String = title
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '#' | '^' | '[' | ']' => '-',
            c if c.is_control() => '-',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim().trim_matches('.').trim();
    if trimmed.is_empty() {
        "untitled".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Render one memory as a markdown note: YAML frontmatter + content + a `## Links` section of
/// `[[wikilinks]]` for the memory's outgoing graph edges.
fn render_note(m: &Value, links: &HashMap<String, Vec<String>>) -> String {
    let mut out = String::from("---\n");
    fm_str(&mut out, "id", &m["id"]);
    if let Some(s) = m["subject"].as_str() {
        fm_str(&mut out, "subject", &Value::String(s.to_string()));
    }
    fm_str(&mut out, "type", &m["mem_type"]);
    fm_raw(&mut out, "importance", &m["importance"]);
    fm_str(&mut out, "valid_from", &m["valid_from"]);
    if !m["valid_to"].is_null() {
        fm_str(&mut out, "valid_to", &m["valid_to"]);
    }
    fm_str(&mut out, "state", &m["state"]);
    for key in ["tenant_id", "user_id", "agent_id", "session_id"] {
        if let Some(s) = m[key].as_str() {
            fm_str(&mut out, key, &Value::String(s.to_string()));
        }
    }
    out.push_str("source: ecphoria\n---\n\n");

    out.push_str(m["content"].as_str().unwrap_or(""));
    out.push('\n');

    // Wikilinks from edges whose source is this note's subject.
    if let Some(subj) = m["subject"].as_str() {
        if let Some(dsts) = links.get(subj).filter(|d| !d.is_empty()) {
            out.push_str("\n## Links\n");
            for d in dsts {
                out.push_str(&format!("- [[{d}]]\n"));
            }
        }
    }
    out
}

/// Emit a quoted-string frontmatter line (skips null/non-strings).
fn fm_str(out: &mut String, key: &str, v: &Value) {
    if let Some(s) = v.as_str() {
        out.push_str(&format!("{key}: \"{}\"\n", s.replace('"', "\\\"")));
    }
}

/// Emit a raw (unquoted) frontmatter line for numbers/bools (skips null).
fn fm_raw(out: &mut String, key: &str, v: &Value) {
    if !v.is_null() {
        out.push_str(&format!("{key}: {v}\n"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_frontmatter_content_and_wikilinks() {
        let m = json!({
            "id": "abcd1234-0000-0000-0000-000000000000",
            "subject": "coffee",
            "content": "Alice drinks coffee",
            "mem_type": "semantic",
            "importance": 0.7,
            "valid_from": "2026-01-01T00:00:00Z",
            "state": "active",
            "user_id": "alice"
        });
        let mut links = HashMap::new();
        links.insert(
            "coffee".to_string(),
            vec!["caffeine".to_string(), "morning".to_string()],
        );

        let note = render_note(&m, &links);
        assert!(note.starts_with("---\n"));
        assert!(note.contains("id: \"abcd1234-0000-0000-0000-000000000000\""));
        assert!(note.contains("subject: \"coffee\""));
        assert!(note.contains("type: \"semantic\""));
        assert!(note.contains("importance: 0.7")); // unquoted number
        assert!(note.contains("user_id: \"alice\""));
        assert!(note.contains("Alice drinks coffee"));
        assert!(note.contains("## Links"));
        assert!(note.contains("- [[caffeine]]"));
        assert!(note.contains("- [[morning]]"));
    }

    #[test]
    fn title_falls_back_to_short_id_and_sanitizes() {
        let no_subject = json!({ "id": "deadbeef-1111-2222-3333-444444444444", "content": "x" });
        assert_eq!(note_title(&no_subject), "memory-deadbeef");
        assert_eq!(sanitize_filename("a/b:c*?"), "a-b-c--");
        assert_eq!(sanitize_filename("  ..  "), "untitled");
        assert_eq!(
            sanitize_filename("user.favorite_color"),
            "user.favorite_color"
        );
    }

    #[test]
    fn link_map_groups_by_source() {
        let edges = json!({ "edges": [
            { "src": "coffee", "dst": "caffeine" },
            { "src": "coffee", "dst": "morning" },
            { "src": "tea", "dst": "caffeine" }
        ]});
        let lm = link_map(&edges);
        assert_eq!(lm["coffee"], vec!["caffeine", "morning"]);
        assert_eq!(lm["tea"], vec!["caffeine"]);
    }
}
