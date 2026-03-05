use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub output_dir: String,
    #[serde(default = "default_max_tokens")]
    pub default_max_tokens: u32,
    #[serde(default = "default_max_history_messages")]
    pub max_history_messages: usize,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub api_key: String,
    pub model: String,
}

fn default_max_tokens() -> u32 {
    4096
}

fn default_max_history_messages() -> usize {
    50
}

impl AppConfig {
    pub fn load() -> Result<Self, AppError> {
        let path = Self::config_path()?;
        if !path.exists() {
            return Err(AppError::Config(format!(
                "Config file not found at {}. Create it with provider API keys.",
                path.display()
            )));
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| AppError::Config(format!("Failed to read config: {e}")))?;
        let config: AppConfig = toml::from_str(&content)
            .map_err(|e| AppError::Config(format!("Failed to parse config: {e}")))?;
        Ok(config)
    }

    pub fn config_path() -> Result<PathBuf, AppError> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| AppError::Config("Cannot determine config directory".into()))?;
        Ok(config_dir.join("houseofagents").join("config.toml"))
    }

    pub fn resolved_output_dir(&self) -> PathBuf {
        let expanded = if self.output_dir.starts_with("~/") {
            if let Some(home) = dirs::home_dir() {
                home.join(&self.output_dir[2..])
            } else {
                PathBuf::from(&self.output_dir)
            }
        } else {
            PathBuf::from(&self.output_dir)
        };
        expanded
    }
}
