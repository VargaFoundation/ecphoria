//! OpenAI-compatible /v1/chat/completions proxy with automatic RAG.
//!
//! Flow:
//! 1. Receive OpenAI-format chat completion request
//! 2. Extract the last user message
//! 3. Search semantic memory for relevant context
//! 4. Prepend context to system message
//! 5. Forward enriched request to configured LLM provider
//! 6. Return the provider's response

use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use reqwest::Client;
use strata_core::StrataEngine;

use super::providers::LlmProvider;

/// OpenAI-compatible chat completion request.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// OpenAI-compatible chat completion response.
#[derive(Debug, serde::Serialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub usage: Usage,
}

#[derive(Debug, serde::Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: ChatMessage,
    pub finish_reason: String,
}

#[derive(Debug, serde::Serialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

/// Handle /v1/chat/completions — OpenAI-compatible endpoint with auto-RAG.
pub async fn chat_completions(
    State(engine): State<Arc<StrataEngine>>,
    Json(mut req): Json<ChatCompletionRequest>,
) -> Json<serde_json::Value> {
    // 1. Extract last user message for context search
    let user_query = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.clone())
        .unwrap_or_default();

    // 2. Search semantic memory for relevant context (if we have entries)
    if engine.semantic_count() > 0 && !user_query.is_empty() {
        // We'd need an embedding to search — for now, skip auto-RAG
        // In production: embed(user_query) → semantic_search(vector, 5) → build context
        // This would require an EmbeddingProvider on the engine
    }

    // 3. Search episodic memory for recent relevant events
    let recent_events = engine
        .query_sql("SELECT source, event_type, payload, ts FROM episodic ORDER BY ts DESC LIMIT 5")
        .await
        .unwrap_or_default();

    if !recent_events.is_empty() {
        // Build context from recent events
        let context_lines: Vec<String> = recent_events
            .iter()
            .filter_map(|row| {
                let source = row.get("source")?.as_str()?;
                let event_type = row.get("event_type")?.as_str()?;
                Some(format!("- [{source}] {event_type}"))
            })
            .collect();

        if !context_lines.is_empty() {
            let context_block = format!(
                "## Context from Strata (recent events)\n{}",
                context_lines.join("\n")
            );

            // Prepend context to system message or create one
            if let Some(sys_msg) = req.messages.iter_mut().find(|m| m.role == "system") {
                sys_msg.content = format!("{}\n\n{}", context_block, sys_msg.content);
            } else {
                req.messages.insert(
                    0,
                    ChatMessage {
                        role: "system".into(),
                        content: context_block,
                    },
                );
            }
        }
    }

    // 4. Determine provider and forward (shared client for connection reuse)
    let config = engine.config();
    let provider = determine_provider(&req.model);
    // Use a thread-local shared client to avoid per-request allocation
    static HTTP_CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
    let http = HTTP_CLIENT.get_or_init(Client::new);

    match provider {
        LlmProvider::OpenAi => {
            forward_to_openai(http, &config.embedding.openai_api_key, &req).await
        }
        LlmProvider::Ollama => forward_to_ollama(http, &config.embedding.ollama_url, &req).await,
        LlmProvider::Anthropic => forward_to_anthropic(http, &req).await,
    }
}

fn determine_provider(model: &str) -> LlmProvider {
    if model.starts_with("claude") {
        LlmProvider::Anthropic
    } else if model.starts_with("gpt") || model.starts_with("o1") || model.starts_with("o3") {
        LlmProvider::OpenAi
    } else {
        // Default to Ollama for local models
        LlmProvider::Ollama
    }
}

async fn forward_to_openai(
    http: &Client,
    api_key: &str,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    match http
        .post("https://api.openai.com/v1/chat/completions")
        .bearer_auth(api_key)
        .json(req)
        .send()
        .await
    {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => Json(body),
            Err(e) => error_response(&format!("failed to parse OpenAI response: {e}")),
        },
        Err(e) => error_response(&format!("OpenAI request failed: {e}")),
    }
}

async fn forward_to_ollama(
    http: &Client,
    base_url: &str,
    req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    match http.post(&url).json(req).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => Json(body),
            Err(e) => error_response(&format!("failed to parse Ollama response: {e}")),
        },
        Err(e) => error_response(&format!("Ollama request failed: {e}")),
    }
}

async fn forward_to_anthropic(
    _http: &Client,
    _req: &ChatCompletionRequest,
) -> Json<serde_json::Value> {
    // Anthropic uses a different API format — would need translation
    // For now, return an informative error
    error_response("Anthropic provider requires API translation (use OpenAI or Ollama)")
}

fn error_response(message: &str) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "error": {
            "message": message,
            "type": "proxy_error",
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn determine_openai_provider() {
        assert!(matches!(determine_provider("gpt-4"), LlmProvider::OpenAi));
        assert!(matches!(
            determine_provider("gpt-3.5-turbo"),
            LlmProvider::OpenAi
        ));
    }

    #[test]
    fn determine_anthropic_provider() {
        assert!(matches!(
            determine_provider("claude-sonnet-4-20250514"),
            LlmProvider::Anthropic
        ));
    }

    #[test]
    fn determine_ollama_provider() {
        assert!(matches!(determine_provider("llama3"), LlmProvider::Ollama));
        assert!(matches!(determine_provider("mistral"), LlmProvider::Ollama));
    }

    #[test]
    fn chat_message_serialization() {
        let msg = ChatMessage {
            role: "user".into(),
            content: "Hello".into(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["role"], "user");
        assert_eq!(json["content"], "Hello");
    }

    #[test]
    fn chat_request_deserialization() {
        let json = serde_json::json!({
            "model": "gpt-4",
            "messages": [
                {"role": "user", "content": "Hi"}
            ]
        });
        let req: ChatCompletionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.model, "gpt-4");
        assert_eq!(req.messages.len(), 1);
    }

    #[test]
    fn chat_request_with_optional_fields() {
        let json = serde_json::json!({
            "model": "llama3",
            "messages": [{"role": "user", "content": "test"}],
            "temperature": 0.7,
            "max_tokens": 1000,
            "stream": false
        });
        let req: ChatCompletionRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(1000));
        assert_eq!(req.stream, Some(false));
    }
}
