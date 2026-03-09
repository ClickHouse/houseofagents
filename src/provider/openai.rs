use super::{HttpProviderBase, Provider, ProviderKind, SendFuture};
use crate::error::AppError;

pub struct OpenAIProvider {
    base: HttpProviderBase,
}

impl OpenAIProvider {
    pub fn new(base: HttpProviderBase) -> Self {
        Self { base }
    }
}

fn extract_content(resp_body: &serde_json::Value) -> Result<String, AppError> {
    let choices = resp_body
        .get("choices")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| AppError::Provider {
            provider: "OpenAI".into(),
            message: "Missing `choices` array in response".into(),
        })?;

    for choice in choices {
        let content = choice
            .get("message")
            .and_then(|msg| msg.get("content"))
            .ok_or_else(|| AppError::Provider {
                provider: "OpenAI".into(),
                message: "Missing `message.content` in response choice".into(),
            })?;

        if let Some(text) = content.as_str() {
            return Ok(text.to_string());
        }

        if let Some(parts) = content.as_array() {
            let joined = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .concat();
            if !joined.is_empty() {
                return Ok(joined);
            }
        }
    }

    Err(AppError::Provider {
        provider: "OpenAI".into(),
        message: "No text content found in response".into(),
    })
}

impl Provider for OpenAIProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::OpenAI
    }

    fn send(&mut self, message: &str) -> SendFuture<'_> {
        let message = message.to_string();
        Box::pin(async move {
            self.base.prepare_send(&message);
            let messages = self.base.format_messages_standard();

            let body = if let Some(ref effort) = self.base.effort {
                serde_json::json!({
                    "model": self.base.model,
                    "max_completion_tokens": self.base.max_tokens,
                    "reasoning_effort": effort,
                    "messages": messages,
                })
            } else {
                serde_json::json!({
                    "model": self.base.model,
                    "max_tokens": self.base.max_tokens,
                    "messages": messages,
                })
            };

            let request = self
                .base
                .client
                .post("https://api.openai.com/v1/chat/completions")
                .header("Authorization", format!("Bearer {}", self.base.api_key))
                .header("content-type", "application/json")
                .json(&body)
                .build()?;

            let resp_body = self.base.execute_request(request, "OpenAI").await?;
            let content = extract_content(&resp_body)?;
            Ok(self.base.finish_send(content))
        })
    }
}

pub async fn list_models(api_key: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let request = client
        .get("https://api.openai.com/v1/models")
        .header("Authorization", format!("Bearer {}", api_key))
        .build()
        .map_err(|e| e.to_string())?;

    let body = HttpProviderBase::fetch_models(client, request).await?;

    let mut entries: Vec<(String, i64)> = body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let id = m["id"].as_str()?.to_string();
                    let created = m["created"].as_i64().unwrap_or(0);
                    Some((id, created))
                })
                .collect()
        })
        .unwrap_or_default();

    entries.sort_by(|a, b| b.1.cmp(&a.1));
    Ok(entries.into_iter().map(|(id, _)| id).collect())
}

#[cfg(test)]
mod tests {
    use super::extract_content;
    use serde_json::json;

    #[test]
    fn extract_content_reads_string_content() {
        let body = json!({
            "choices": [
                { "message": { "content": "answer" } }
            ]
        });
        let content = extract_content(&body).expect("extract");
        assert_eq!(content, "answer");
    }

    #[test]
    fn extract_content_reads_text_parts_array() {
        let body = json!({
            "choices": [
                { "message": { "content": [
                    { "type": "output_text", "text": "part 1 "},
                    { "type": "output_text", "text": "part 2"}
                ] } }
            ]
        });
        let content = extract_content(&body).expect("extract");
        assert_eq!(content, "part 1 part 2");
    }

    #[test]
    fn extract_content_errors_when_choices_missing() {
        let body = json!({});
        let err = extract_content(&body).expect_err("expected error");
        assert!(err.to_string().contains("Missing `choices` array"));
    }

    #[test]
    fn extract_content_errors_when_no_text() {
        let body = json!({
            "choices": [
                { "message": { "content": [] } }
            ]
        });
        let err = extract_content(&body).expect_err("expected error");
        assert!(err
            .to_string()
            .contains("No text content found in response"));
    }
}
