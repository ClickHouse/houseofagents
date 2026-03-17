mod consolidation;
mod diagnostics;
mod execution;
mod input;
mod results;
mod resume;
mod setup_analysis;
mod text_edit;

#[cfg(test)]
mod tests;

use crate::app::{
    App, BatchRunGroup, ConsolidationPhase, ConsolidationTarget, EditField, EditPopupSection,
    HomeSection, PipelineDialogMode, PipelineEditField, PipelineFocus, PipelineLoopEditField,
    PromptFocus, RunState, RunStatus, RunStepStatus, Screen,
};
use crate::config::{AgentConfig, AppConfig, ProviderConfig};
use crate::event::{Event, EventHandler};
use crate::execution::multi::run_multi;
use crate::execution::pipeline::{self as pipeline_mod, BlockId};
use crate::execution::relay::run_relay;
use crate::execution::swarm::run_swarm;
use crate::execution::{
    BatchProgressEvent, ExecutionMode, ProgressEvent, PromptRuntimeContext, RunOutcome,
};
use crate::output::{AgentSessionInfo, OutputManager};
use crate::provider::{self, ProviderKind};

use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, KeyCode, KeyEvent, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::collections::HashMap;
use std::io::{self, stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub fn restore_terminal() -> io::Result<()> {
    terminal::disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen, DisableBracketedPaste)?;
    Ok(())
}

pub async fn run(app: &mut App) -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen, EnableBracketedPaste)?;
    let _terminal_guard = TerminalRestoreGuard;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut events = EventHandler::new(Duration::from_millis(100));

    loop {
        terminal.draw(|f| crate::screen::draw(f, app))?;

        // After draw updates viewport dimensions (e.g. pipeline_file_visible),
        // sync scroll offsets so the cursor stays visible.  This matters after
        // terminal resize: the draw above captures the new height in the Cell,
        // and this call adjusts scroll to match.
        input::adjust_file_dialog_scroll(app);

        tokio::select! {
            Some(event) = events.next() => {
                match event {
                    Event::Key(key) => input::handle_key(app, key),
                    Event::Paste(text) => input::handle_paste(app, &text),
                    Event::Tick => {}
                    Event::Resize(_, _) => {}
                }
            }
            Some(progress) = async {
                if let Some(ref mut rx) = app.running.progress_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<ProgressEvent>>().await
                }
            } => {
                execution::handle_progress(app, progress);
            }
            Some(progress) = async {
                if let Some(ref mut rx) = app.running.batch_progress_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<BatchProgressEvent>>().await
                }
            } => {
                execution::handle_batch_progress(app, progress);
            }
            Some(result) = async {
                if let Some(ref mut rx) = app.edit_popup.model_picker_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<Vec<String>, String>>>().await
                }
            } => {
                input::handle_model_list_result(app, result);
            }
            Some(save_result) = async {
                if let Some(ref mut rx) = app.edit_popup.config_save_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<(), String>>>().await
                }
            } => {
                input::handle_config_save_result(app, save_result);
            }
            Some(consolidation_result) = async {
                if let Some(ref mut rx) = app.running.consolidation_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<String, String>>>().await
                }
            } => {
                consolidation::handle_consolidation_result(app, consolidation_result);
            }
            Some(diagnostic_result) = async {
                if let Some(ref mut rx) = app.running.diagnostic_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<String, String>>>().await
                }
            } => {
                diagnostics::handle_diagnostic_result(app, diagnostic_result);
            }
            Some(analysis_result) = async {
                if let Some(ref mut rx) = app.setup_analysis.rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<String, String>>>().await
                }
            } => {
                setup_analysis::handle_setup_analysis_result(app, analysis_result);
            }
            Some(results_result) = async {
                if let Some(ref mut rx) = app.results.results_load_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<crate::app::ResultsLoadPayload, String>>>().await
                }
            } => {
                results::handle_results_load_result(app, results_result);
            }
            Some(preview_result) = async {
                if let Some(ref mut rx) = app.results.preview_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<crate::app::PreviewLoadResult>>().await
                }
            } => {
                results::handle_preview_result(app, preview_result);
            }
            Some(resume_result) = async {
                if let Some(ref mut rx) = app.running.resume_prepare_rx {
                    rx.recv().await
                } else {
                    std::future::pending::<Option<Result<crate::app::ResumePreparation, String>>>().await
                }
            } => {
                execution::handle_resume_preparation_result(app, resume_result);
            }
            extraction_msg = async {
                if app.memory.extraction_rx.is_empty() {
                    std::future::pending::<(usize, Option<Result<Vec<crate::memory::types::ExtractedMemory>, String>>)>().await
                } else {
                    let futs: Vec<_> = app.memory.extraction_rx.iter_mut().enumerate().map(|(i, rx)| {
                        Box::pin(async move { (i, rx.recv().await) })
                    }).collect();
                    let ((idx, msg), _, _) = futures_util::future::select_all(futs).await;
                    (idx, msg)
                }
            } => {
                let (idx, msg) = extraction_msg;
                match msg {
                    Some(result) => {
                        app.memory.extraction_rx.remove(idx);
                        execution::handle_extraction_result(app, result);
                    }
                    None => { app.memory.extraction_rx.remove(idx); } // task panicked or dropped
                }
            }
        }

        if app.should_quit {
            // Drain all in-flight memory extractions before exit so spawned tasks
            // (which write _memories.json and insert into SQLite) aren't cancelled
            // when the tokio runtime drops. Use max of HTTP and CLI timeouts since
            // extraction may use either provider type.
            if !app.memory.extraction_rx.is_empty() {
                // Cap at 30s so a huge cli/http timeout doesn't freeze the terminal
                // on quit. The spawned tasks write to SQLite and _memories.json only
                // after the provider call completes, so this drain keeps the runtime
                // alive long enough for typical extraction API calls.
                let drain_secs = (app
                    .effective_http_timeout_seconds()
                    .max(app.effective_cli_timeout_seconds())
                    .max(10)
                    + 5)
                .min(30);
                let timeout = std::time::Duration::from_secs(drain_secs);
                let futs: Vec<_> = app
                    .memory
                    .extraction_rx
                    .iter_mut()
                    .map(|rx| rx.recv())
                    .collect();
                let _ = tokio::time::timeout(timeout, futures_util::future::join_all(futs)).await;
            }
            break;
        }
    }

    Ok(())
}

struct TerminalRestoreGuard;

impl Drop for TerminalRestoreGuard {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}
