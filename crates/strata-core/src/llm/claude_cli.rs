//! Claude Code CLI completion provider — shells out to the authenticated `claude -p` (non-interactive
//! "print" mode). Uses the local CLI's existing auth (subscription/OAuth), so **no API key is
//! needed**. Slower per call than the HTTP API (process startup), but handy for evals/dev on a
//! machine where `claude` is logged in.

use std::process::Stdio;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// Completion provider that invokes `claude -p --append-system-prompt <system> --model <model>` with
/// the user message piped on stdin.
pub struct ClaudeCliCompletion {
    bin: String,
    model: String,
}

impl ClaudeCliCompletion {
    pub fn new(model: String) -> Self {
        Self {
            bin: std::env::var("STRATA_CLAUDE_BIN").unwrap_or_else(|_| "claude".into()),
            model,
        }
    }
}

#[async_trait::async_trait]
impl super::CompletionProvider for ClaudeCliCompletion {
    async fn complete(&self, system: &str, user: &str) -> crate::Result<String> {
        let mut child = Command::new(&self.bin)
            .arg("-p")
            .arg("--append-system-prompt")
            .arg(system)
            .arg("--model")
            .arg(&self.model)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| crate::Error::Llm(format!("claude CLI spawn failed: {e}")))?;

        // Feed the user prompt on stdin (avoids ARG_MAX for large RAG contexts), then close it (EOF).
        {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| crate::Error::Llm("claude CLI: no stdin handle".into()))?;
            stdin
                .write_all(user.as_bytes())
                .await
                .map_err(|e| crate::Error::Llm(format!("claude CLI stdin write: {e}")))?;
        }

        let out = child
            .wait_with_output()
            .await
            .map_err(|e| crate::Error::Llm(format!("claude CLI wait: {e}")))?;
        if !out.status.success() {
            return Err(crate::Error::Llm(format!(
                "claude CLI failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}
