use super::{
    effort_to_budget, prune_history, validate_effort_config, CompletionResponse, Message, Provider,
    ProviderKind, Role,
};
use crate::error::AppError;
use async_trait::async_trait;

pub struct AnthropicProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
    max_tokens: u32,
    max_history_messages: usize,
    thinking_effort: Option<String>,
    history: Vec<Message>,
}

impl AnthropicProvider {
    pub fn new(
        api_key: String,
        model: String,
        client: reqwest::Client,
        max_tokens: u32,
        max_history_messages: usize,
        thinking_effort: Option<String>,
    ) -> Self {
        Self {
            api_key,
            model,
            client,
            max_tokens,
            max_history_messages,
            thinking_effort,
            history: Vec::new(),
        }
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

#[async_trait]
impl Provider for AnthropicProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Anthropic
    }

    async fn send(&mut self, message: &str) -> Result<CompletionResponse, AppError> {
        if let Err(message) = validate_effort_config(
            ProviderKind::Anthropic,
            false,
            None,
            self.thinking_effort.as_deref(),
        ) {
            return Err(AppError::Provider {
                provider: "Anthropic".into(),
                message,
            });
        }

        self.history.push(Message {
            role: Role::User,
            content: message.to_string(),
        });

        prune_history(&mut self.history, self.max_history_messages);

        let messages: Vec<serde_json::Value> = self
            .history
            .iter()
            .map(|m| {
                serde_json::json!({
                    "role": match m.role { Role::User => "user", Role::Assistant => "assistant" },
                    "content": m.content,
                })
            })
            .collect();

        let mut body = serde_json::json!({
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": messages,
        });

        if let Some(ref effort) = self.thinking_effort {
            let budget = effort_to_budget(effort).map_err(|e| AppError::Provider {
                provider: "Anthropic".into(),
                message: e,
            })?;
            body["thinking"] = serde_json::json!({
                "type": "enabled",
                "budget_tokens": budget,
            });
        }

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        let resp_text = resp.text().await?;

        if !status.is_success() {
            return Err(AppError::Provider {
                provider: "Anthropic".into(),
                message: format!("{status}: {resp_text}"),
            });
        }

        let resp_body: serde_json::Value =
            serde_json::from_str(&resp_text).map_err(|e| AppError::Provider {
                provider: "Anthropic".into(),
                message: format!("Failed to parse response: {e}"),
            })?;

        let content = extract_text_content(&resp_body)?;

        self.history.push(Message {
            role: Role::Assistant,
            content,
        });

        let content = self.history.last().unwrap().content.clone();
        Ok(CompletionResponse {
            content,
            debug_logs: Vec::new(),
        })
    }
}

pub async fn list_models(api_key: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let resp = client
        .get("https://api.anthropic.com/v1/models")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = resp.status();

    if !status.is_success() {
        let text = resp.text().await.map_err(|e| e.to_string())?;
        return Err(format!("{status}: {text}"));
    }

    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;

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
