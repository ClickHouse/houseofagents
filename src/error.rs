use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Config error: {0}")]
    Config(String),

    #[error("Provider error ({provider}): {message}")]
    Provider { provider: String, message: String },

    #[error("API request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::AppError;

    #[test]
    fn config_error_display() {
        let err = AppError::Config("bad".to_string());
        assert_eq!(err.to_string(), "Config error: bad");
    }

    #[test]
    fn provider_error_display() {
        let err = AppError::Provider {
            provider: "OpenAI".to_string(),
            message: "oops".to_string(),
        };
        assert_eq!(err.to_string(), "Provider error (OpenAI): oops");
    }

    #[test]
    fn io_error_conversion() {
        let io = std::io::Error::other("disk");
        let err: AppError = io.into();
        assert!(err.to_string().contains("IO error: disk"));
    }

    #[test]
    fn json_error_conversion() {
        let json_err = serde_json::from_str::<serde_json::Value>("{").expect_err("json err");
        let err: AppError = json_err.into();
        assert!(err.to_string().contains("JSON error:"));
    }

    #[test]
    fn http_variant_can_be_constructed_from_reqwest_error() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let req_err = rt.block_on(async {
            reqwest::Client::new()
                .get("http://127.0.0.1:9")
                .send()
                .await
                .expect_err("expected connect failure")
        });
        let err: AppError = req_err.into();
        assert!(err.to_string().starts_with("API request failed:"));
    }
}
