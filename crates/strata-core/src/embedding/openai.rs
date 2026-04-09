//! OpenAI embedding provider.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// OpenAI-based embedding provider.
pub struct OpenAiProvider {
    client: Client,
    api_key: String,
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
    data: Vec<EmbedData>,
}

#[derive(Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

impl OpenAiProvider {
    pub fn new(api_key: String, model: String, dimension: usize) -> Self {
        Self {
            client: Client::new(),
            api_key,
            model,
            dimension,
        }
    }
}

#[async_trait::async_trait]
impl super::EmbeddingProvider for OpenAiProvider {
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
            .post("https://api.openai.com/v1/embeddings")
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Embedding(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Embedding(format!(
                "OpenAI returned {status}: {body}"
            )));
        }

        let embed_response: EmbedResponse = response
            .json()
            .await
            .map_err(|e| crate::Error::Embedding(format!("failed to parse response: {e}")))?;

        Ok(embed_response
            .data
            .into_iter()
            .map(|d| d.embedding)
            .collect())
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
        let provider = OpenAiProvider::new("sk-test".into(), "text-embedding-3-small".into(), 1536);
        assert_eq!(provider.dimension(), 1536);
        assert_eq!(provider.model_name(), "text-embedding-3-small");
    }

    #[tokio::test]
    async fn embed_empty_input() {
        let provider = OpenAiProvider::new("sk-test".into(), "model".into(), 1536);
        let result = provider.embed(&[]).await.unwrap();
        assert!(result.is_empty());
    }
}
