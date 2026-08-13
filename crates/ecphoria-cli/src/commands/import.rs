//! `ecphoria import` — bring external note/knowledge stores into Ecphoria's memory.
//!
//! - **Obsidian**: each Markdown note becomes a memory (subject = note title), YAML frontmatter is
//!   attached as metadata, and every `[[wikilink]]` becomes a knowledge-graph edge
//!   (`note --links_to--> target`).
//! - **Mem0**: a `mem0` export (JSON) — a `get_all()` dump or a plain array of memory objects; each
//!   `memory` text becomes a memory, carrying its `user_id`/`metadata`/`created_at`.
//! - **Zep**: a `zep` export (JSON) — graph `facts` and/or session `messages`; each fact/message
//!   becomes a memory.
//! - **Git**: every tracked Markdown file in a repository, chunked server-side into
//!   independently-addressed sections, with each file's **last commit date** as the memory's
//!   valid-time. Re-running it supersedes only the sections that changed — so a documentation
//!   corpus keeps a real per-section history instead of a pile of import-time duplicates.
//!
//! - **GitHub**: historical backfill of closed issues and merged pull requests. The webhook path
//!   only ever sees events from the moment it is wired up; this fills in everything before that.
//!
//! All variants run over the REST API, so they work against any Ecphoria server.

use crate::client::EcphoriaClient;

/// A parsed Obsidian note.
struct Note {
    title: String,
    body: String,
    frontmatter: serde_json::Value,
    links: Vec<String>,
}

/// A memory record extracted from an external store, ready to POST to `/api/v1/memories`.
struct MemRecord {
    content: String,
    subject: Option<String>,
    user_id: Option<String>,
    metadata: serde_json::Value,
}

pub async fn run(
    url: &str,
    from: &str,
    path: &str,
    user: Option<&str>,
    watch: bool,
) -> anyhow::Result<()> {
    match from {
        "obsidian" if watch => import_obsidian_watch(url, path, user).await,
        "obsidian" => import_obsidian(url, path, user).await,
        "git" if watch => import_git_watch(url, path, user).await,
        "git" => import_git(url, path, user).await,
        "github" if watch => {
            anyhow::bail!("--watch is not supported for --from github; wire the webhook instead")
        }
        "github" => import_github(url, path, user).await,
        "mem0" | "zep" if watch => {
            anyhow::bail!("--watch is only supported for --from obsidian and --from git")
        }
        "mem0" => import_records(url, path, user, "mem0", parse_mem0).await,
        "zep" => import_records(url, path, user, "zep", parse_zep).await,
        other => {
            anyhow::bail!(
                "unknown import source '{other}' (supported: git, github, obsidian, mem0, zep)"
            )
        }
    }
}

/// Generic JSON-file importer: read `path`, run `parse` to extract records, POST each as a memory.
/// The record's own `user_id` wins; otherwise the CLI `--user` fallback applies. `source` is stamped
/// into each memory's metadata.
async fn import_records(
    url: &str,
    path: &str,
    user: Option<&str>,
    source: &str,
    parse: fn(&serde_json::Value) -> anyhow::Result<Vec<MemRecord>>,
) -> anyhow::Result<()> {
    let raw =
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("cannot read '{path}': {e}"))?;
    let doc: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("'{path}' is not valid JSON: {e}"))?;
    let records = parse(&doc)?;
    if records.is_empty() {
        println!("No memories found in {path}.");
        return Ok(());
    }

    let client = EcphoriaClient::new(url);
    let (mut imported, mut errors) = (0u32, 0u32);
    for rec in &records {
        let mut metadata = rec.metadata.clone();
        if !metadata.is_object() {
            metadata = serde_json::json!({});
        }
        if let serde_json::Value::Object(ref mut m) = metadata {
            m.entry("source").or_insert(serde_json::json!(source));
        }
        let mut body = serde_json::json!({
            "content": rec.content,
            "metadata": metadata,
        });
        if let Some(s) = &rec.subject {
            body["subject"] = serde_json::json!(s);
        }
        // Record's own user_id wins; else the CLI fallback.
        if let Some(u) = rec.user_id.as_deref().or(user) {
            body["user_id"] = serde_json::json!(u);
        }
        match client.post_json("/api/v1/memories", body).await {
            Ok(_) => imported += 1,
            Err(e) => {
                eprintln!("  memory failed: {e}");
                errors += 1;
            }
        }
    }
    println!("Imported {imported} memory(ies) from {source}, {errors} error(s).");
    Ok(())
}

/// Parse a Mem0 export. Accepts a top-level array, or an object wrapping the list under `results`,
/// `memories`, or `data`. Each item's text comes from `memory` (Mem0's field) or `text`/`content`.
fn parse_mem0(doc: &serde_json::Value) -> anyhow::Result<Vec<MemRecord>> {
    let items = as_list(doc, &["results", "memories", "data"]).ok_or_else(|| {
        anyhow::anyhow!("mem0 export: expected an array or a {{results:[…]}} object")
    })?;
    let mut out = Vec::new();
    for item in items {
        let Some(content) = str_field(item, &["memory", "text", "content"]) else {
            continue; // skip entries with no text
        };
        if content.trim().is_empty() {
            continue;
        }
        let mut metadata = item
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        // Preserve Mem0's timestamps in metadata (Ecphoria assigns its own valid_from).
        if let serde_json::Value::Object(ref mut m) = metadata {
            for key in ["created_at", "updated_at", "hash", "categories"] {
                if let Some(v) = item.get(key) {
                    m.entry(key).or_insert(v.clone());
                }
            }
        }
        out.push(MemRecord {
            content,
            subject: None,
            user_id: str_field(item, &["user_id"]),
            metadata,
        });
    }
    Ok(out)
}

/// Parse a Zep export. Accepts graph `facts` (each a string or `{fact|content}` object) and/or
/// session `messages` (each `{role, content}`), at the top level or nested. Facts and messages both
/// become memories; a message's `role` is kept in metadata.
fn parse_zep(doc: &serde_json::Value) -> anyhow::Result<Vec<MemRecord>> {
    let mut out = Vec::new();
    let user = str_field(doc, &["user_id", "session_id"]);
    if let Some(facts) = as_list(doc, &["facts"]) {
        for f in facts {
            let content = match f {
                serde_json::Value::String(s) => Some(s.clone()),
                _ => str_field(f, &["fact", "content"]),
            };
            if let Some(c) = content.filter(|c| !c.trim().is_empty()) {
                out.push(MemRecord {
                    content: c,
                    subject: None,
                    user_id: user.clone(),
                    metadata: serde_json::json!({ "kind": "fact" }),
                });
            }
        }
    }
    if let Some(messages) = as_list(doc, &["messages"]) {
        for m in messages {
            let Some(content) = str_field(m, &["content", "message"]) else {
                continue;
            };
            if content.trim().is_empty() {
                continue;
            }
            let role = str_field(m, &["role", "role_type"]).unwrap_or_else(|| "user".into());
            out.push(MemRecord {
                content,
                subject: None,
                user_id: user.clone(),
                metadata: serde_json::json!({ "kind": "message", "role": role }),
            });
        }
    }
    if out.is_empty() {
        anyhow::bail!("zep export: found neither `facts` nor `messages`");
    }
    Ok(out)
}

/// Return a JSON array from `doc` itself (if it is an array) or from the first present key in `keys`.
fn as_list<'a>(doc: &'a serde_json::Value, keys: &[&str]) -> Option<&'a Vec<serde_json::Value>> {
    if let Some(arr) = doc.as_array() {
        return Some(arr);
    }
    keys.iter()
        .find_map(|k| doc.get(*k).and_then(|v| v.as_array()))
}

/// First present, non-null string among `keys`.
fn str_field(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| v.get(*k).and_then(|x| x.as_str()))
        .map(|s| s.to_string())
}

async fn import_obsidian(url: &str, vault: &str, user: Option<&str>) -> anyhow::Result<()> {
    let root = std::path::Path::new(vault);
    if !root.is_dir() {
        anyhow::bail!("vault path '{vault}' is not a directory");
    }
    let mut files = Vec::new();
    collect_markdown(root, &mut files)?;
    if files.is_empty() {
        println!("No .md files found under {vault}.");
        return Ok(());
    }

    let client = EcphoriaClient::new(url);
    let (mut memories, mut edges, mut errors) = (0u32, 0u32, 0u32);
    for file in &files {
        let (m, e, err) = import_note(&client, file, user).await;
        memories += m;
        edges += e;
        errors += err;
    }

    println!(
        "Imported {} note(s): {memories} memories, {edges} graph edges, {errors} error(s).",
        files.len()
    );
    Ok(())
}

/// Import one Obsidian note → a memory (+ a graph edge per `[[wikilink]]`). Returns
/// `(memories, edges, errors)` so both the batch import and the watcher can reuse it.
async fn import_note(
    client: &EcphoriaClient,
    file: &std::path::Path,
    user: Option<&str>,
) -> (u32, u32, u32) {
    let content = match std::fs::read_to_string(file) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  skip {}: {e}", file.display());
            return (0, 0, 1);
        }
    };
    let note = parse_note(file, &content);
    let (mut memories, mut edges, mut errors) = (0u32, 0u32, 0u32);

    let mut metadata = note.frontmatter.clone();
    if let serde_json::Value::Object(ref mut m) = metadata {
        m.insert("source".into(), serde_json::json!("obsidian"));
        m.insert("path".into(), serde_json::json!(file.display().to_string()));
    }
    let mut body = serde_json::json!({
        "content": if note.body.trim().is_empty() { note.title.clone() } else { note.body.clone() },
        "subject": note.title,
        "metadata": metadata,
        "mem_type": "semantic",
    });
    if let Some(u) = user {
        body["user_id"] = serde_json::json!(u);
    }
    match client.post_json("/api/v1/memories", body).await {
        Ok(_) => memories += 1,
        Err(e) => {
            eprintln!("  memory failed for '{}': {e}", note.title);
            return (0, 0, 1);
        }
    }

    for target in &note.links {
        let edge = serde_json::json!({ "src": note.title, "relation": "links_to", "dst": target });
        match client.post_json("/api/v1/memories/link", edge).await {
            Ok(_) => edges += 1,
            Err(e) => {
                eprintln!("  edge {} -> {} failed: {e}", note.title, target);
                errors += 1;
            }
        }
    }
    (memories, edges, errors)
}

/// A vault file that should be synced: a Markdown file not under Obsidian's `.obsidian` config dir.
fn is_syncable_md(path: &std::path::Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some("md")
        && !path.components().any(|c| c.as_os_str() == ".obsidian")
}

/// Live import: do the initial batch, then watch `vault` and re-import each Markdown note as it
/// changes — the human→agent half of an Obsidian sync (the agent→human half is `export --to
/// obsidian`). Runs until interrupted.
async fn import_obsidian_watch(url: &str, vault: &str, user: Option<&str>) -> anyhow::Result<()> {
    use notify::{RecursiveMode, Watcher};

    import_obsidian(url, vault, user).await?;

    let root = std::path::Path::new(vault).to_path_buf();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            let _ = tx.send(event);
        }
    })?;
    watcher.watch(&root, RecursiveMode::Recursive)?;
    println!("Watching {vault} for changes (Ctrl-C to stop)…");

    let client = EcphoriaClient::new(url);
    // Block for the first event, then debounce: drain everything that arrives within a short window
    // so a burst of saves (Obsidian writes several times) becomes one re-import per file. Ends when
    // the watcher is dropped (recv errors).
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        while let Ok(ev) = rx.try_recv() {
            batch.push(ev);
        }
        // Unique syncable .md paths that still exist.
        let mut paths: Vec<std::path::PathBuf> = batch
            .into_iter()
            .flat_map(|e| e.paths)
            .filter(|p| is_syncable_md(p) && p.is_file())
            .collect();
        paths.sort();
        paths.dedup();
        for p in paths {
            let (m, e, _) = import_note(&client, &p, user).await;
            println!("  synced {} ({m} memory, {e} edges)", p.display());
        }
    }
    Ok(())
}

// ── GitHub historical backfill ──────────────────────────────────────

/// Backfill closed issues and merged pull requests from the GitHub API.
///
/// The webhook path (`POST /api/v1/webhook/github` + `memory.promotion`) only ever sees events
/// from the moment it is wired up. Everything a team decided before that — which is most of it —
/// needs pulling once. Both paths converge on the **same subject scheme**
/// (`owner/repo#issue-N`, `owner/repo#pr-N`), so a backfilled ticket that later receives a webhook
/// is confirmed or superseded rather than duplicated.
///
/// `path` is the repository slug (`owner/repo`). Needs `GITHUB_TOKEN` for anything private and to
/// avoid the 60 requests/hour unauthenticated rate limit.
async fn import_github(url: &str, repo: &str, user: Option<&str>) -> anyhow::Result<()> {
    if repo.split('/').count() != 2 {
        anyhow::bail!("--path must be a repository slug like 'owner/repo', got {repo:?}");
    }
    let token = std::env::var("GITHUB_TOKEN").ok().filter(|t| !t.is_empty());
    if token.is_none() {
        eprintln!("warning: GITHUB_TOKEN not set — public repos only, 60 requests/hour");
    }
    let gh = reqwest::Client::builder()
        .user_agent("ecphoria-import")
        .build()?;
    let client = EcphoriaClient::new(url);

    let mut memories: Vec<serde_json::Value> = Vec::new();
    let mut page = 1u32;
    // `/issues` returns pull requests too (GitHub models a PR as an issue), which is convenient:
    // one paginated walk covers both, distinguished by the `pull_request` key.
    loop {
        let api = format!(
            "https://api.github.com/repos/{repo}/issues\
             ?state=closed&per_page=100&page={page}&sort=updated&direction=desc"
        );
        let mut req = gh.get(&api).header("Accept", "application/vnd.github+json");
        if let Some(t) = &token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "GitHub API {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        let items: Vec<serde_json::Value> = resp.json().await?;
        if items.is_empty() {
            break;
        }
        let count = items.len();
        for item in items {
            if let Some(m) = github_memory(repo, &item, user) {
                memories.push(m);
            }
        }
        print!("\r  fetched page {page} ({} memories)…", memories.len());
        use std::io::Write;
        let _ = std::io::stdout().flush();
        if count < 100 {
            break;
        }
        page += 1;
    }
    println!();

    if memories.is_empty() {
        println!("No closed issues or merged pull requests in {repo}.");
        return Ok(());
    }
    // The server caps a batch at 10 000; stay well under so one slow request can't time out.
    let mut added = 0usize;
    for chunk in memories.chunks(500) {
        let resp = client
            .post_json(
                "/api/v1/memories/batch",
                serde_json::json!({ "memories": chunk }),
            )
            .await?;
        if let Some(err) = resp.get("error") {
            anyhow::bail!("{err}");
        }
        added += resp.get("added").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    }
    println!("Imported {added} tickets from {repo}.");
    Ok(())
}

/// Shape one closed issue or merged PR into a memory, or `None` if it should be skipped.
///
/// Matches the subject scheme the webhook promotion path uses, so the two converge on one record
/// per ticket. An unmerged closed PR is deliberately kept — "we decided not to do this" is often
/// the more useful memory.
fn github_memory(
    repo: &str,
    item: &serde_json::Value,
    user: Option<&str>,
) -> Option<serde_json::Value> {
    let number = item.get("number")?.as_u64()?;
    let title = item
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("(no title)");
    let who = item
        .pointer("/user/login")
        .and_then(|v| v.as_str())
        .unwrap_or("someone");
    let body = item.get("body").and_then(|v| v.as_str()).unwrap_or("");
    let is_pr = item.get("pull_request").is_some();
    let closed_at = item.get("closed_at").and_then(|v| v.as_str());

    let (subject, mut content) = if is_pr {
        let merged = item
            .pointer("/pull_request/merged_at")
            .is_some_and(|v| !v.is_null());
        let outcome = if merged {
            "merged"
        } else {
            "closed without merging"
        };
        (
            format!("{repo}#pr-{number}"),
            format!("Pull request #{number} in {repo} — \"{title}\" — {outcome}, opened by {who}."),
        )
    } else {
        (
            format!("{repo}#issue-{number}"),
            format!("Issue #{number} in {repo} — \"{title}\" — closed, opened by {who}."),
        )
    };
    // The body carries the actual reasoning; without it the memory is a status line. Bounded so a
    // single enormous ticket cannot dominate the corpus.
    let body = body.trim();
    if !body.is_empty() {
        content.push_str("\n\n");
        content.extend(body.chars().take(4_000));
    }

    let mut m = serde_json::json!({
        "content": content,
        "subject": subject,
        "mem_type": "episodic",
        // The repository slug is the project, so tickets sit alongside that repo's documentation
        // and a project-scoped search finds both.
        "project": repo,
        "metadata": { "source": format!("github/{repo}"), "backfilled": true },
    });
    // Valid-time is when the ticket closed, so a backfill reconstructs the real timeline rather
    // than stacking every ticket at import time.
    if let Some(t) = closed_at {
        m["valid_from"] = serde_json::json!(t);
    }
    if let Some(u) = user {
        m["user_id"] = serde_json::json!(u);
    }
    Some(m)
}

// ── Git repository import ───────────────────────────────────────────

/// Run a `git` subcommand in `repo` and return stdout.
///
/// Shelling out rather than linking a git library is deliberate: the CLI is a pure HTTP client by
/// design, `git` is already installed wherever a repo is being imported from, and it handles
/// worktrees, submodules and `.gitignore` correctly without a dependency.
fn git(repo: &str, args: &[&str]) -> anyhow::Result<String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run git (is it installed?): {e}"))?;
    if !out.status.success() {
        anyhow::bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Last commit date (RFC 3339) per tracked Markdown file.
///
/// One `git log` pass rather than one per file: `--name-only` lists the files each commit touched
/// and the log is newest-first, so the first time a path appears is its last modification. The
/// `\x01` sentinel on the format line distinguishes date lines from filenames unambiguously
/// (a filename cannot begin with a control character).
fn git_last_modified(repo: &str) -> anyhow::Result<std::collections::HashMap<String, String>> {
    let log = git(
        repo,
        &[
            "log",
            "--format=%x01%aI",
            "--name-only",
            "--diff-filter=ACMRT",
            "--",
            "*.md",
        ],
    )?;
    let mut dates = std::collections::HashMap::new();
    let mut current: Option<String> = None;
    for line in log.lines() {
        if let Some(date) = line.strip_prefix('\x01') {
            current = Some(date.trim().to_string());
        } else if !line.trim().is_empty() {
            if let Some(d) = &current {
                dates.entry(line.to_string()).or_insert_with(|| d.clone());
            }
        }
    }
    Ok(dates)
}

/// Import every tracked Markdown file in a git repository as a chunked document.
///
/// Documents are addressed `<project>/<repo-relative-path>`, where `<project>` defaults to the
/// repository's directory name. The namespace is **not** cosmetic: a document's identity drives
/// both supersession and the removal sweep, so two repositories that each contain `README.md`
/// would otherwise be the same document — importing the second one expires the first one's
/// sections. Override it with `ECPHORIA_PROJECT` when the directory name is not distinctive.
async fn import_git(url: &str, repo: &str, user: Option<&str>) -> anyhow::Result<()> {
    let root = git(repo, &["rev-parse", "--show-toplevel"])?
        .trim()
        .to_string();
    let project = std::env::var("ECPHORIA_PROJECT").ok().unwrap_or_else(|| {
        std::path::Path::new(&root)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "repo".into())
    });
    let files: Vec<String> = git(repo, &["ls-files", "--", "*.md"])?
        .lines()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if files.is_empty() {
        anyhow::bail!("no tracked Markdown files under {repo}");
    }
    let dates = git_last_modified(repo)?;
    println!(
        "Importing {} Markdown files from {root} as project '{project}'…",
        files.len()
    );

    let client = EcphoriaClient::new(url);
    let mut totals = (0u64, 0u64, 0u64, 0u64, 0u64); // chunks, inserted, superseded, confirmed, removed
    let mut failed = 0usize;
    for rel in &files {
        let abs = std::path::Path::new(&root).join(rel);
        let Ok(content) = std::fs::read_to_string(&abs) else {
            // Tracked but unreadable (deleted from the worktree, or not valid UTF-8).
            continue;
        };
        if content.trim().is_empty() {
            continue;
        }
        // Namespaced by project: the document path is its identity, and identity drives both
        // supersession and the removal sweep. Two repositories that each contain `README.md` would
        // otherwise be the same document — importing the second expires the first one's sections.
        let doc_path = format!("{project}/{rel}");
        match post_document(
            &client,
            &doc_path,
            &content,
            dates.get(rel).map(|s| s.as_str()),
            user,
            Some(&project),
        )
        .await
        {
            Ok(r) => {
                totals.0 += r.0;
                totals.1 += r.1;
                totals.2 += r.2;
                totals.3 += r.3;
                totals.4 += r.4;
            }
            Err(e) => {
                eprintln!("  {rel}: {e}");
                failed += 1;
            }
        }
    }
    println!(
        "Imported {} files → {} sections ({} new, {} updated, {} unchanged, {} removed)",
        files.len() - failed,
        totals.0,
        totals.1,
        totals.2,
        totals.3,
        totals.4
    );
    if failed > 0 {
        eprintln!("{failed} file(s) failed — see errors above");
    }
    Ok(())
}

/// POST one document, returning `(chunks, inserted, superseded, confirmed, removed)`.
async fn post_document(
    client: &EcphoriaClient,
    path: &str,
    content: &str,
    valid_from: Option<&str>,
    user: Option<&str>,
    project: Option<&str>,
) -> anyhow::Result<(u64, u64, u64, u64, u64)> {
    let mut body = serde_json::json!({
        "path": path,
        "content": content,
        "metadata": { "source": "git" },
    });
    if let Some(p) = project {
        body["project"] = serde_json::json!(p);
    }
    if let Some(v) = valid_from {
        body["valid_from"] = serde_json::json!(v);
    }
    if let Some(u) = user {
        body["user_id"] = serde_json::json!(u);
    }
    let resp = client.post_json("/api/v1/documents", body).await?;
    if let Some(err) = resp.get("error") {
        anyhow::bail!("{}", err);
    }
    let n = |k: &str| resp.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
    Ok((
        n("chunks"),
        n("inserted"),
        n("superseded"),
        n("confirmed"),
        n("removed"),
    ))
}

/// Live import: do the initial pass, then re-import each Markdown file as it changes on disk.
///
/// Working-tree edits have no commit date, so they are recorded with valid-time = now. The next
/// full `import --from git` run re-anchors them to their real commit dates.
async fn import_git_watch(url: &str, repo: &str, user: Option<&str>) -> anyhow::Result<()> {
    use notify::{RecursiveMode, Watcher};

    import_git(url, repo, user).await?;
    let root = std::path::PathBuf::from(git(repo, &["rev-parse", "--show-toplevel"])?.trim());
    let project = std::env::var("ECPHORIA_PROJECT").ok().unwrap_or_else(|| {
        root.file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "repo".into())
    });

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            let _ = tx.send(event);
        }
    })?;
    watcher.watch(&root, RecursiveMode::Recursive)?;
    println!("Watching {} for changes (Ctrl-C to stop)…", root.display());

    let client = EcphoriaClient::new(url);
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        while let Ok(ev) = rx.try_recv() {
            batch.push(ev);
        }
        let mut paths: Vec<std::path::PathBuf> = batch
            .into_iter()
            .flat_map(|e| e.paths)
            // Skip `.git/` itself — every commit rewrites it and would trigger a storm.
            .filter(|p| {
                is_syncable_md(p) && p.is_file() && !p.components().any(|c| c.as_os_str() == ".git")
            })
            .collect();
        paths.sort();
        paths.dedup();
        for p in paths {
            let rel = format!(
                "{project}/{}",
                p.strip_prefix(&root).unwrap_or(&p).to_string_lossy()
            );
            let Ok(content) = std::fs::read_to_string(&p) else {
                continue;
            };
            match post_document(&client, &rel, &content, None, user, Some(&project)).await {
                Ok((chunks, ins, sup, conf, rem)) => println!(
                    "  synced {rel} ({chunks} sections: {ins} new, {sup} updated, {conf} unchanged, {rem} removed)"
                ),
                Err(e) => eprintln!("  {rel}: {e}"),
            }
        }
    }
    Ok(())
}

/// Recursively collect `.md` files, skipping the Obsidian `.obsidian` config dir.
fn collect_markdown(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some(".obsidian") {
                continue;
            }
            collect_markdown(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("md") {
            out.push(path);
        }
    }
    Ok(())
}

/// Parse a note into (title, body, frontmatter, wikilinks). Title = frontmatter `title` or filename.
fn parse_note(file: &std::path::Path, content: &str) -> Note {
    let (frontmatter, body) = split_frontmatter(content);
    let title = frontmatter
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            file.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("untitled")
                .to_string()
        });
    let links = extract_wikilinks(body);
    Note {
        title,
        body: body.to_string(),
        frontmatter,
        links,
    }
}

/// Split a leading `---\n…\n---` YAML-ish frontmatter block from the body. The block is parsed
/// line-by-line into a JSON object (`key: value`) — no YAML dependency; nested/complex YAML is kept
/// as raw strings. Returns `({}, whole)` when there is no frontmatter.
fn split_frontmatter(content: &str) -> (serde_json::Value, &str) {
    let rest = match content.strip_prefix("---\n") {
        Some(r) => r,
        None => return (serde_json::json!({}), content),
    };
    let Some(end) = rest
        .find("\n---\n")
        .or_else(|| rest.strip_suffix("\n---").map(|_| rest.len() - 4))
    else {
        return (serde_json::json!({}), content);
    };
    let (fm, after) = rest.split_at(end);
    let body = after.strip_prefix("\n---\n").unwrap_or("");
    let mut map = serde_json::Map::new();
    for line in fm.lines() {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim();
            let v = v.trim().trim_matches('"');
            if !k.is_empty() {
                map.insert(k.to_string(), serde_json::json!(v));
            }
        }
    }
    (serde_json::Value::Object(map), body)
}

/// Extract `[[wikilink]]` targets, dropping any `|alias` and `#heading` fragments and deduplicating.
fn extract_wikilinks(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'[' && bytes[i + 1] == b'[' {
            if let Some(close) = text[i + 2..].find("]]") {
                let inner = &text[i + 2..i + 2 + close];
                // Strip Obsidian alias (`Target|Alias`) and heading (`Target#Section`) parts.
                let target = inner
                    .split('|')
                    .next()
                    .unwrap_or(inner)
                    .split('#')
                    .next()
                    .unwrap_or(inner)
                    .trim();
                if !target.is_empty() && !out.contains(&target.to_string()) {
                    out.push(target.to_string());
                }
                i += 2 + close + 2;
                continue;
            }
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parses_frontmatter_body_and_links() {
        let content = "---\ntitle: My Note\ntags: a, b\n---\nHello [[Other Note]] and [[Third|alias]] and [[Fourth#Heading]].";
        let note = parse_note(Path::new("/vault/my-note.md"), content);
        assert_eq!(note.title, "My Note");
        assert_eq!(note.frontmatter["tags"], "a, b");
        assert!(note.body.starts_with("Hello"));
        assert_eq!(note.links, vec!["Other Note", "Third", "Fourth"]);
    }

    #[test]
    fn title_falls_back_to_filename() {
        let note = parse_note(Path::new("/vault/Some Idea.md"), "no frontmatter here");
        assert_eq!(note.title, "Some Idea");
        assert!(note.frontmatter.as_object().unwrap().is_empty());
        assert!(note.links.is_empty());
    }

    #[test]
    fn syncable_md_filter() {
        use std::path::Path;
        assert!(is_syncable_md(Path::new("/vault/note.md")));
        assert!(is_syncable_md(Path::new("/vault/sub/deep.md")));
        assert!(!is_syncable_md(Path::new("/vault/note.txt")));
        assert!(!is_syncable_md(Path::new("/vault/image.png")));
        // Obsidian config dir is ignored.
        assert!(!is_syncable_md(Path::new("/vault/.obsidian/workspace.md")));
    }

    #[test]
    fn wikilinks_dedupe_and_strip() {
        let links = extract_wikilinks("[[A]] [[A]] [[B|x]] plain [[ C ]]");
        assert_eq!(links, vec!["A", "B", "C"]);
    }

    #[test]
    fn mem0_parses_results_wrapper_and_bare_array() {
        // get_all()-style {results:[…]}
        let doc = serde_json::json!({
            "results": [
                {"memory": "likes tea", "user_id": "alice", "metadata": {"topic": "drink"},
                 "created_at": "2026-01-01T00:00:00Z"},
                {"memory": "  ", "user_id": "bob"},        // blank text → skipped
                {"user_id": "carol"}                        // no text → skipped
            ]
        });
        let recs = parse_mem0(&doc).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].content, "likes tea");
        assert_eq!(recs[0].user_id.as_deref(), Some("alice"));
        assert_eq!(recs[0].metadata["topic"], "drink");
        assert_eq!(recs[0].metadata["created_at"], "2026-01-01T00:00:00Z");

        // Bare array with alternate text key.
        let bare = serde_json::json!([{"text": "prefers window seats", "user_id": "dave"}]);
        let recs = parse_mem0(&bare).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].content, "prefers window seats");
    }

    #[test]
    fn zep_parses_facts_and_messages() {
        let doc = serde_json::json!({
            "user_id": "u1",
            "facts": [
                "the sky is blue",
                {"fact": "grass is green"},
                {"fact": "   "}                              // blank → skipped
            ],
            "messages": [
                {"role": "assistant", "content": "hello"},
                {"role_type": "user", "message": "hi there"},
                {"role": "user", "content": ""}              // blank → skipped
            ]
        });
        let recs = parse_zep(&doc).unwrap();
        // 2 facts + 2 messages
        assert_eq!(recs.len(), 4);
        assert!(recs.iter().all(|r| r.user_id.as_deref() == Some("u1")));
        assert_eq!(recs[0].metadata["kind"], "fact");
        assert_eq!(recs[2].metadata["kind"], "message");
        assert_eq!(recs[2].metadata["role"], "assistant");
    }

    #[test]
    fn zep_errors_when_empty() {
        assert!(parse_zep(&serde_json::json!({"other": 1})).is_err());
    }
}

#[cfg(test)]
mod git_tests {
    use super::*;

    /// The `git log` parser must attribute each file to its *most recent* commit.
    ///
    /// The log is newest-first and one commit lists many files, so a naive parse that overwrites
    /// on every sighting would record each file's *oldest* date instead — silently inverting the
    /// timeline that the whole backdated-import feature depends on.
    #[test]
    fn last_modified_takes_the_newest_commit_per_file() {
        // Shape of `git log --format=%x01%aI --name-only`.
        let log = "\u{1}2026-06-02T10:00:00+02:00\n\
                   \n\
                   docs/runbook.md\n\
                   docs/architecture.md\n\
                   \n\
                   \u{1}2026-03-10T09:00:00+02:00\n\
                   \n\
                   docs/runbook.md\n\
                   \n\
                   \u{1}2026-01-15T08:00:00+02:00\n\
                   \n\
                   docs/architecture.md\n";
        let mut dates = std::collections::HashMap::new();
        let mut current: Option<String> = None;
        for line in log.lines() {
            if let Some(date) = line.strip_prefix('\u{1}') {
                current = Some(date.trim().to_string());
            } else if !line.trim().is_empty() {
                if let Some(d) = &current {
                    dates.entry(line.to_string()).or_insert_with(|| d.clone());
                }
            }
        }
        assert_eq!(dates["docs/runbook.md"], "2026-06-02T10:00:00+02:00");
        assert_eq!(dates["docs/architecture.md"], "2026-06-02T10:00:00+02:00");
        assert_eq!(dates.len(), 2);
    }

    /// Reading a real repository must work end to end — this one.
    #[test]
    fn reads_this_repository() {
        let repo = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let files = git(repo, &["ls-files", "--", "*.md"]).expect("ls-files");
        assert!(
            files.lines().any(|l| l == "README.md"),
            "expected README.md among tracked markdown"
        );
        let dates = git_last_modified(repo).expect("git log");
        let readme = dates.get("README.md").expect("README.md has a commit date");
        chrono::DateTime::parse_from_rfc3339(readme)
            .unwrap_or_else(|e| panic!("git date {readme:?} is not RFC 3339: {e}"));
    }
}

#[cfg(test)]
mod github_tests {
    use super::*;

    fn issue(json: serde_json::Value) -> serde_json::Value {
        json
    }

    /// Backfilled tickets must land on the *same* subject the webhook path uses, or the two
    /// sources produce two records per ticket and the history is split in half.
    #[test]
    fn subjects_match_the_webhook_promotion_scheme() {
        let pr = github_memory(
            "acme/api",
            &issue(serde_json::json!({
                "number": 42, "title": "Fix the shard router",
                "user": {"login": "alice"},
                "pull_request": {"merged_at": "2026-05-01T00:00:00Z"},
                "closed_at": "2026-05-01T00:00:00Z"
            })),
            None,
        )
        .unwrap();
        assert_eq!(pr["subject"], "acme/api#pr-42");
        assert!(pr["content"].as_str().unwrap().contains("merged"));

        let iss = github_memory(
            "acme/api",
            &issue(serde_json::json!({
                "number": 7, "title": "Quorum loss",
                "user": {"login": "bob"},
                "body": "Root cause: stale term.",
                "closed_at": "2026-03-01T00:00:00Z"
            })),
            None,
        )
        .unwrap();
        assert_eq!(iss["subject"], "acme/api#issue-7");
        assert!(iss["content"].as_str().unwrap().contains("Root cause"));
    }

    /// Valid-time is when the ticket closed — otherwise a backfill stacks years of history at
    /// import time and every as-of query returns the same thing.
    #[test]
    fn valid_from_is_the_close_date() {
        let m = github_memory(
            "acme/api",
            &issue(serde_json::json!({
                "number": 1, "title": "t", "user": {"login": "x"},
                "closed_at": "2026-03-01T00:00:00Z"
            })),
            None,
        )
        .unwrap();
        assert_eq!(m["valid_from"], "2026-03-01T00:00:00Z");

        // Still importable without one, just undated.
        let m = github_memory(
            "acme/api",
            &issue(serde_json::json!({"number": 2, "title": "t", "user": {"login": "x"}})),
            None,
        )
        .unwrap();
        assert!(m.get("valid_from").is_none());
    }

    /// A closed-but-unmerged PR is kept: "we decided not to do this" is often the more useful
    /// memory, and dropping it would silently lose rejected proposals.
    #[test]
    fn unmerged_pull_requests_are_kept_and_labelled() {
        let m = github_memory(
            "acme/api",
            &issue(serde_json::json!({
                "number": 9, "title": "Switch to Postgres",
                "user": {"login": "carol"},
                "pull_request": {"merged_at": serde_json::Value::Null}
            })),
            None,
        )
        .unwrap();
        assert!(
            m["content"]
                .as_str()
                .unwrap()
                .contains("closed without merging"),
            "{}",
            m["content"]
        );
    }

    #[test]
    fn enormous_bodies_are_bounded() {
        let m = github_memory(
            "acme/api",
            &issue(serde_json::json!({
                "number": 1, "title": "t", "user": {"login": "x"},
                "body": "x".repeat(50_000)
            })),
            None,
        )
        .unwrap();
        assert!(m["content"].as_str().unwrap().len() < 5_000);
    }

    #[test]
    fn malformed_items_are_skipped_not_fatal() {
        assert!(github_memory(
            "acme/api",
            &issue(serde_json::json!({"title": "no number"})),
            None
        )
        .is_none());
    }
}
