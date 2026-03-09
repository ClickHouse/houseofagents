use super::{
    effort_to_budget, validate_effort_config, HttpProviderBase, Provider, ProviderKind, SendFuture,
};
use crate::error::AppError;

pub struct AnthropicProvider {
    base: HttpProviderBase,
}

impl AnthropicProvider {
    pub fn new(base: HttpProviderBase) -> Self {
        Self { base }
    }
}

fn extract_text_content(resp_body: &serde_json::Value) -> Result<String, AppError> {
    let content = resp_body
        .get("content")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| AppError::Provider {
            provider: "Anthropic".into(),
            message: "Missing `content` array in response".into(),
        })?;

    let text_blocks: Vec<&str> = content
        .iter()
        .filter_map(|block| {
            let block_type = block.get("type").and_then(serde_json::Value::as_str);
            if block_type == Some("text") {
                block.get("text").and_then(serde_json::Value::as_str)
            } else {
                None
            }
        })
        .collect();

    if !text_blocks.is_empty() {
        return Ok(text_blocks.concat());
    }

    // Legacy fallback where `type` is omitted but `text` still exists.
    if let Some(text) = content
        .iter()
        .find_map(|block| block.get("text").and_then(serde_json::Value::as_str))
    {
        return Ok(text.to_string());
    }

    if let Some(text) = content.first().and_then(serde_json::Value::as_str) {
        return Ok(text.to_string());
    }

    Err(AppError::Provider {
        provider: "Anthropic".into(),
        message: "No text content found in response".into(),
    })
}

impl Provider for AnthropicProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Anthropic
    }

    fn send(&mut self, message: &str) -> SendFuture<'_> {
        let message = message.to_string();
        Box::pin(async move {
            if let Err(message) = validate_effort_config(
                ProviderKind::Anthropic,
                false,
                None,
                self.base.effort.as_deref(),
            ) {
                return Err(AppError::Provider {
                    provider: "Anthropic".into(),
                    message,
                });
            }

            self.base.prepare_send(&message);
            let messages = self.base.format_messages_standard();

            let mut body = serde_json::json!({
                "model": self.base.model,
                "max_tokens": self.base.max_tokens,
                "messages": messages,
            });

            if let Some(ref effort) = self.base.effort {
                let budget = effort_to_budget(effort).map_err(|e| AppError::Provider {
                    provider: "Anthropic".into(),
                    message: e,
                })?;
                body["thinking"] = serde_json::json!({
                    "type": "enabled",
                    "budget_tokens": budget,
                });
            }

            let request = self
                .base
                .client
                .post("https://api.anthropic.com/v1/messages")
                .header("x-api-key", &self.base.api_key)
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .json(&body)
                .build()?;

            let resp_body = self.base.execute_request(request, "Anthropic").await?;
            let content = extract_text_content(&resp_body)?;
            Ok(self.base.finish_send(content))
        })
    }
}

pub async fn list_models(api_key: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let request = client
        .get("https://api.anthropic.com/v1/models")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .build()
        .map_err(|e| e.to_string())?;

    let body = HttpProviderBase::fetch_models(client, request).await?;

    let mut entries: Vec<(String, i64)> = body["data"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|m| {
                    let id = m["id"].as_str()?.to_string();
                    let created = m["created_at"]
                        .as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.timestamp())
                        .unwrap_or(0);
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
    use super::extract_text_content;
    use serde_json::json;

    #[test]
    fn extract_text_content_uses_text_block_when_thinking_block_is_first() {
        let body = json!({
            "content": [
                { "type": "thinking", "thinking": "internal" },
                { "type": "text", "text": "final answer" }
            ]
        });
        let content = extract_text_content(&body).expect("extract");
        assert_eq!(content, "final answer");
    }

    #[test]
    fn extract_text_content_concatenates_multiple_text_blocks() {
        let body = json!({
            "content": [
                { "type": "text", "text": "part 1 " },
                { "type": "text", "text": "part 2" }
            ]
        });
        let content = extract_text_content(&body).expect("extract");
        assert_eq!(content, "part 1 part 2");
    }

    #[test]
    fn extract_text_content_accepts_legacy_text_without_type() {
        let body = json!({
            "content": [
                { "text": "legacy answer" }
            ]
        });
        let content = extract_text_content(&body).expect("extract");
        assert_eq!(content, "legacy answer");
    }

    #[test]
    fn extract_text_content_errors_when_no_text_exists() {
        let body = json!({
            "content": [
                { "type": "tool_use", "name": "x" }
            ]
        });
        let err = extract_text_content(&body).expect_err("should fail");
        assert!(err
            .to_string()
            .contains("No text content found in response"));
    }
}
