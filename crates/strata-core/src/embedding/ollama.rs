//! Ollama embedding provider.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Ollama-based embedding provider.
pub struct OllamaProvider {
    client: Client,
    url: String,
    model: String,
    dimension: usize,
}

#[derive(Serialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

impl OllamaProvider {
    pub fn new(url: String, model: String, dimension: usize) -> Self {
        Self {
            client: Client::new(),
            url: url.trim_end_matches('/').to_string(),
            model,
            dimension,
        }
    }
}

#[async_trait::async_trait]
impl super::EmbeddingProvider for OllamaProvider {
    async fn embed(&self, texts: &[String]) -> crate::Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        let request = EmbedRequest {
            model: self.model.clone(),
            input: texts.to_vec(),
        };

        let response = self
            .client
            .post(format!("{}/api/embed", self.url))
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Embedding(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Embedding(format!(
                "Ollama returned {status}: {body}"
            )));
        }

        let embed_response: EmbedResponse = response
            .json()
            .await
            .map_err(|e| crate::Error::Embedding(format!("failed to parse response: {e}")))?;

        Ok(embed_response.embeddings)
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::EmbeddingProvider;

    #[test]
    fn provider_metadata() {
        let provider = OllamaProvider::new(
            "http://localhost:11434".into(),
            "nomic-embed-text".into(),
            768,
        );
        assert_eq!(provider.dimension(), 768);
        assert_eq!(provider.model_name(), "nomic-embed-text");
    }

    #[tokio::test]
    async fn embed_empty_input() {
        let provider = OllamaProvider::new(
            "http://localhost:11434".into(),
            "nomic-embed-text".into(),
            768,
        );
        let result = provider.embed(&[]).await.unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn url_trimming() {
        let provider = OllamaProvider::new("http://localhost:11434/".into(), "model".into(), 768);
        assert_eq!(provider.url, "http://localhost:11434");
    }
}
