use crate::config::{AppConfig, ProviderConfig};
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::provider::ProviderKind;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Screen {
    Home,
    Prompt,
    Order,
    Running,
    Results,
}

pub struct App {
    pub config: AppConfig,
    pub session_overrides: HashMap<String, ProviderConfig>,
    pub screen: Screen,
    pub should_quit: bool,

    // Home screen state
    pub selected_agents: Vec<ProviderKind>,
    pub selected_mode: ExecutionMode,
    pub home_cursor: usize,
    pub home_section: HomeSection,

    // Prompt screen state
    pub prompt_text: String,
    pub session_name: String,
    pub iterations: u32,
    pub prompt_focus: PromptFocus,

    // Order screen state (relay only)
    pub order_cursor: usize,
    pub order_grabbed: Option<usize>,

    // Running screen state
    pub progress_events: Vec<ProgressEvent>,
    pub is_running: bool,
    pub run_error: Option<String>,

    // Results screen state
    pub result_files: Vec<PathBuf>,
    pub result_cursor: usize,
    pub result_preview: String,

    // Edit popup
    pub show_edit_popup: bool,
    pub edit_popup_cursor: usize,
    pub edit_popup_field: EditField,
    pub edit_buffer: String,

    // Error modal
    pub error_modal: Option<String>,

    // Execution channel state (not part of UI state)
    pub progress_rx: Option<mpsc::UnboundedReceiver<ProgressEvent>>,
    pub cancel_flag: Arc<AtomicBool>,
    pub run_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeSection {
    Agents,
    Mode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptFocus {
    Text,
    SessionName,
    Iterations,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditField {
    ApiKey,
    Model,
}

impl App {
    pub fn new(config: AppConfig) -> Self {
        Self {
            config,
            session_overrides: HashMap::new(),
            screen: Screen::Home,
            should_quit: false,
            selected_agents: Vec::new(),
            selected_mode: ExecutionMode::Solo,
            home_cursor: 0,
            home_section: HomeSection::Agents,
            prompt_text: String::new(),
            session_name: String::new(),
            iterations: 1,
            prompt_focus: PromptFocus::Text,
            order_cursor: 0,
            order_grabbed: None,
            progress_events: Vec::new(),
            is_running: false,
            run_error: None,
            result_files: Vec::new(),
            result_cursor: 0,
            result_preview: String::new(),
            show_edit_popup: false,
            edit_popup_cursor: 0,
            edit_popup_field: EditField::ApiKey,
            edit_buffer: String::new(),
            error_modal: None,
            progress_rx: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
            run_dir: None,
        }
    }

    pub fn available_providers(&self) -> Vec<(ProviderKind, bool)> {
        ProviderKind::all()
            .iter()
            .map(|&kind| {
                let has_key = self
                    .effective_provider_config(kind)
                    .map(|c| !c.api_key.is_empty())
                    .unwrap_or(false);
                (kind, has_key)
            })
            .collect()
    }

    pub fn effective_provider_config(&self, kind: ProviderKind) -> Option<ProviderConfig> {
        let key = kind.config_key();
        if let Some(override_config) = self.session_overrides.get(key) {
            return Some(override_config.clone());
        }
        self.config.providers.get(key).cloned()
    }

    pub fn toggle_agent(&mut self, kind: ProviderKind) {
        if let Some(pos) = self.selected_agents.iter().position(|&k| k == kind) {
            self.selected_agents.remove(pos);
        } else {
            self.selected_agents.push(kind);
        }
    }

    pub fn move_order_up(&mut self) {
        if self.order_cursor > 0 {
            if let Some(grabbed) = self.order_grabbed {
                self.selected_agents.swap(grabbed, grabbed - 1);
                self.order_grabbed = Some(grabbed - 1);
            }
            self.order_cursor -= 1;
        }
    }

    pub fn move_order_down(&mut self) {
        if self.order_cursor < self.selected_agents.len().saturating_sub(1) {
            if let Some(grabbed) = self.order_grabbed {
                self.selected_agents.swap(grabbed, grabbed + 1);
                self.order_grabbed = Some(grabbed + 1);
            }
            self.order_cursor += 1;
        }
    }
}
