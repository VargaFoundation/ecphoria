//! OpenAI embedding provider.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// OpenAI-based embedding provider.
pub struct OpenAiProvider {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
    dimension: usize,
    query_prefix: String,
    document_prefix: String,
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
            base_url: "https://api.openai.com/v1".into(),
            model,
            dimension,
            query_prefix: String::new(),
            document_prefix: String::new(),
        }
    }

    /// Point the provider at any OpenAI-compatible endpoint (LiteLLM, vLLM, a local shim).
    ///
    /// A trailing slash is tolerated: an operator copying a gateway URL from a dashboard should
    /// not have to know that it matters.
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        let url = base_url.into();
        let trimmed = url.trim_end_matches('/');
        if !trimmed.is_empty() {
            self.base_url = trimmed.to_string();
        }
        self
    }

    /// Set the asymmetric retrieval task prefixes (query / document). OpenAI `text-embedding-3-*`
    /// needs none, but this stays configurable for prefix-trained models served OpenAI-style.
    pub fn with_prefixes(mut self, query_prefix: String, document_prefix: String) -> Self {
        self.query_prefix = query_prefix;
        self.document_prefix = document_prefix;
        self
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
            .post(format!("{}/embeddings", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| crate::Error::Embedding(format!("HTTP request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Embedding(format!(
                "{} returned {status}: {body}",
                self.base_url
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

    fn query_prefix(&self) -> &str {
        &self.query_prefix
    }

    fn document_prefix(&self) -> &str {
        &self.document_prefix
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

    #[test]
    fn defaults_to_openai_and_accepts_any_compatible_endpoint() {
        let default = OpenAiProvider::new("sk-test".into(), "model".into(), 1536);
        assert_eq!(default.base_url, "https://api.openai.com/v1");

        let gateway = OpenAiProvider::new("sk-test".into(), "model".into(), 1536)
            .with_base_url("http://litellm:4000/v1");
        assert_eq!(gateway.base_url, "http://litellm:4000/v1");
    }

    #[test]
    fn a_trailing_slash_is_not_an_operator_error() {
        let provider = OpenAiProvider::new("sk-test".into(), "model".into(), 1536)
            .with_base_url("http://litellm:4000/v1/");
        assert_eq!(provider.base_url, "http://litellm:4000/v1");
    }

    #[test]
    fn an_empty_base_url_keeps_the_default() {
        let provider =
            OpenAiProvider::new("sk-test".into(), "model".into(), 1536).with_base_url("");
        assert_eq!(provider.base_url, "https://api.openai.com/v1");
    }
}
