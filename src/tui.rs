use crate::app::{App, EditField, HomeSection, PromptFocus, Screen};
use crate::config::ProviderConfig;
use crate::event::{Event, EventHandler};
use crate::execution::relay::run_relay;
use crate::execution::solo::run_solo;
use crate::execution::swarm::run_swarm;
use crate::execution::{ExecutionMode, ProgressEvent};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io::{self, stdout};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub fn restore_terminal() -> io::Result<()> {
    terminal::disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen)?;
    Ok(())
}

pub async fn run(app: &mut App) -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let mut events = EventHandler::new(Duration::from_millis(100));

    loop {
        terminal.draw(|f| crate::screen::draw(f, app))?;

        tokio::select! {
            Some(event) = events.next() => {
                match event {
                    Event::Key(key) => {
                        handle_key(app, key);
                    }
                    Event::Tick => {}
                    Event::Resize(_, _) => {}
                }
            }
            Some(progress) = async {
                if let Some(ref mut rx) = app.progress_rx {
                    rx.recv().await
                } else {
                    // Never resolves — park this branch
                    std::future::pending::<Option<ProgressEvent>>().await
                }
            } => {
                handle_progress(app, progress);
            }
        }

        if app.should_quit {
            break;
        }
    }

    restore_terminal()?;
    Ok(())
}

fn handle_key(app: &mut App, key: KeyEvent) {
    // Ctrl+C: graceful quit from anywhere
    if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
        if app.is_running {
            app.cancel_flag.store(true, Ordering::Relaxed);
        }
        app.should_quit = true;
        return;
    }

    // Dismiss error modal with any key
    if app.error_modal.is_some() {
        app.error_modal = None;
        return;
    }

    // Edit popup handling
    if app.show_edit_popup {
        handle_edit_popup_key(app, key);
        return;
    }

    match app.screen {
        Screen::Home => handle_home_key(app, key),
        Screen::Prompt => handle_prompt_key(app, key),
        Screen::Order => handle_order_key(app, key),
        Screen::Running => handle_running_key(app, key),
        Screen::Results => handle_results_key(app, key),
    }
}

fn handle_home_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Char('e') => {
            app.show_edit_popup = true;
            app.edit_popup_cursor = 0;
        }
        KeyCode::Tab => {
            app.home_section = match app.home_section {
                HomeSection::Agents => HomeSection::Mode,
                HomeSection::Mode => HomeSection::Agents,
            };
            app.home_cursor = 0;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            app.home_cursor = app.home_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let max = match app.home_section {
                HomeSection::Agents => ProviderKind::all().len().saturating_sub(1),
                HomeSection::Mode => ExecutionMode::all().len().saturating_sub(1),
            };
            if app.home_cursor < max {
                app.home_cursor += 1;
            }
        }
        KeyCode::Char(' ') => match app.home_section {
            HomeSection::Agents => {
                let providers = app.available_providers();
                if let Some((kind, available)) = providers.get(app.home_cursor) {
                    if *available {
                        app.toggle_agent(*kind);
                    }
                }
            }
            HomeSection::Mode => {
                if let Some(mode) = ExecutionMode::all().get(app.home_cursor) {
                    app.selected_mode = *mode;
                }
            }
        },
        KeyCode::Enter => {
            if app.selected_agents.is_empty() {
                app.error_modal = Some("Select at least one agent".into());
            } else {
                app.screen = Screen::Prompt;
                app.prompt_focus = PromptFocus::Text;
            }
        }
        _ => {}
    }
}

fn handle_prompt_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Home;
        }
        KeyCode::Tab => {
            app.prompt_focus = match app.prompt_focus {
                PromptFocus::Text => PromptFocus::SessionName,
                PromptFocus::SessionName => PromptFocus::Iterations,
                PromptFocus::Iterations => PromptFocus::Text,
            };
        }
        KeyCode::BackTab => {
            app.prompt_focus = match app.prompt_focus {
                PromptFocus::Text => PromptFocus::Iterations,
                PromptFocus::SessionName => PromptFocus::Text,
                PromptFocus::Iterations => PromptFocus::SessionName,
            };
        }
        KeyCode::Enter if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.prompt_text.trim().is_empty() {
                app.error_modal = Some("Enter a prompt first".into());
                return;
            }
            if app.selected_mode == ExecutionMode::Relay && app.selected_agents.len() > 1 {
                app.screen = Screen::Order;
                app.order_cursor = 0;
                app.order_grabbed = None;
            } else {
                start_execution(app);
            }
        }
        _ => match app.prompt_focus {
            PromptFocus::Text => match key.code {
                KeyCode::Char(c) => app.prompt_text.push(c),
                KeyCode::Backspace => {
                    app.prompt_text.pop();
                }
                KeyCode::Enter => app.prompt_text.push('\n'),
                _ => {}
            },
            PromptFocus::SessionName => match key.code {
                KeyCode::Char(c) => app.session_name.push(c),
                KeyCode::Backspace => {
                    app.session_name.pop();
                }
                _ => {}
            },
            PromptFocus::Iterations => match key.code {
                KeyCode::Char('+') | KeyCode::Up => {
                    if app.selected_mode != ExecutionMode::Solo {
                        app.iterations = (app.iterations + 1).min(20);
                    }
                }
                KeyCode::Char('-') | KeyCode::Down => {
                    if app.selected_mode != ExecutionMode::Solo {
                        app.iterations = app.iterations.saturating_sub(1).max(1);
                    }
                }
                _ => {}
            },
        },
    }
}

fn handle_order_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::Prompt;
            app.order_grabbed = None;
        }
        KeyCode::Up | KeyCode::Char('k') => app.move_order_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_order_down(),
        KeyCode::Char(' ') => {
            if app.order_grabbed.is_some() {
                app.order_grabbed = None;
            } else {
                app.order_grabbed = Some(app.order_cursor);
            }
        }
        KeyCode::Enter => {
            app.order_grabbed = None;
            start_execution(app);
        }
        _ => {}
    }
}

fn handle_running_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc if app.is_running => {
            app.cancel_flag.store(true, Ordering::Relaxed);
        }
        KeyCode::Enter if !app.is_running => {
            app.screen = Screen::Results;
            load_results(app);
        }
        KeyCode::Char('q') if !app.is_running => {
            app.should_quit = true;
        }
        _ => {}
    }
}

fn handle_results_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Char('q') => app.should_quit = true,
        KeyCode::Up | KeyCode::Char('k') => {
            app.result_cursor = app.result_cursor.saturating_sub(1);
            update_preview(app);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.result_cursor < app.result_files.len().saturating_sub(1) {
                app.result_cursor += 1;
            }
            update_preview(app);
        }
        KeyCode::Esc => {
            app.screen = Screen::Home;
            app.prompt_text.clear();
            app.session_name.clear();
            app.selected_agents.clear();
            app.progress_events.clear();
            app.result_files.clear();
            app.result_preview.clear();
            app.iterations = 1;
        }
        _ => {}
    }
}

fn handle_edit_popup_key(app: &mut App, key: KeyEvent) {
    match key.code {
        KeyCode::Esc => {
            app.show_edit_popup = false;
        }
        KeyCode::Up | KeyCode::Char('k') if app.edit_buffer.is_empty() => {
            app.edit_popup_cursor = app.edit_popup_cursor.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') if app.edit_buffer.is_empty() => {
            let max = ProviderKind::all().len().saturating_sub(1);
            if app.edit_popup_cursor < max {
                app.edit_popup_cursor += 1;
            }
        }
        KeyCode::Char('a') if app.edit_buffer.is_empty() => {
            app.edit_popup_field = EditField::ApiKey;
            app.edit_buffer.clear();
        }
        KeyCode::Char('m') if app.edit_buffer.is_empty() => {
            app.edit_popup_field = EditField::Model;
            app.edit_buffer.clear();
        }
        KeyCode::Enter => {
            if !app.edit_buffer.is_empty() {
                if let Some(kind) = ProviderKind::all().get(app.edit_popup_cursor) {
                    let key = kind.config_key().to_string();
                    let existing = app.effective_provider_config(*kind);
                    let mut config = existing.unwrap_or(ProviderConfig {
                        api_key: String::new(),
                        model: String::new(),
                    });
                    match app.edit_popup_field {
                        EditField::ApiKey => config.api_key = app.edit_buffer.clone(),
                        EditField::Model => config.model = app.edit_buffer.clone(),
                    }
                    app.session_overrides.insert(key, config);
                    app.edit_buffer.clear();
                }
            }
        }
        KeyCode::Backspace => {
            app.edit_buffer.pop();
        }
        KeyCode::Char(c) if !app.edit_buffer.is_empty() || c != 'j' && c != 'k' && c != 'a' && c != 'm' => {
            app.edit_buffer.push(c);
        }
        _ => {}
    }
}

fn start_execution(app: &mut App) {
    app.screen = Screen::Running;
    app.progress_events.clear();
    app.is_running = true;
    app.run_error = None;

    let config = app.config.clone();
    let prompt = app.prompt_text.clone();
    let agents = app.selected_agents.clone();
    let mode = app.selected_mode;
    let iterations = if mode == ExecutionMode::Solo {
        1
    } else {
        app.iterations
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .expect("Failed to create HTTP client");

    let mut providers: Vec<Box<dyn provider::Provider>> = Vec::new();
    for kind in &agents {
        if let Some(pconfig) = app.effective_provider_config(*kind) {
            providers.push(provider::create_provider(
                *kind,
                &pconfig,
                client.clone(),
                config.default_max_tokens,
                config.max_history_messages,
            ));
        }
    }

    let output_dir = config.resolved_output_dir();
    let session_name = if app.session_name.is_empty() {
        None
    } else {
        Some(app.session_name.as_str())
    };
    let output = match OutputManager::new(&output_dir, session_name) {
        Ok(o) => o,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create output dir: {e}"));
            app.screen = Screen::Prompt;
            app.is_running = false;
            return;
        }
    };

    let _ = output.write_prompt(&prompt);
    let _ = output.write_session_info(&mode, &agents, iterations, session_name);

    // Store run dir for results screen
    app.run_dir = Some(output.run_dir().clone());

    // Create progress channel
    let (tx, rx) = mpsc::unbounded_channel::<ProgressEvent>();
    let cancel = Arc::new(AtomicBool::new(false));

    app.progress_rx = Some(rx);
    app.cancel_flag = cancel.clone();

    tokio::spawn(async move {
        let result = match mode {
            ExecutionMode::Solo => {
                run_solo(&prompt, providers, &output, tx.clone(), cancel).await
            }
            ExecutionMode::Relay => {
                run_relay(&prompt, providers, iterations, &output, tx.clone(), cancel).await
            }
            ExecutionMode::Swarm => {
                run_swarm(&prompt, providers, iterations, &output, tx.clone(), cancel).await
            }
        };
        if let Err(e) = result {
            let _ = tx.send(ProgressEvent::AgentError {
                kind: ProviderKind::Anthropic,
                iteration: 0,
                error: e.to_string(),
            });
            let _ = tx.send(ProgressEvent::AllDone);
        }
    });
}

fn handle_progress(app: &mut App, event: ProgressEvent) {
    let is_done = matches!(event, ProgressEvent::AllDone);
    app.progress_events.push(event);
    if is_done {
        app.is_running = false;
        app.progress_rx = None;
    }
}

fn load_results(app: &mut App) {
    if let Some(ref run_dir) = app.run_dir {
        let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(run_dir) {
            for entry in entries.flatten() {
                if entry.path().is_file() {
                    files.push(entry.path());
                }
            }
        }
        files.sort();
        app.result_files = files;
    }
    app.result_cursor = 0;
    update_preview(app);
}

fn update_preview(app: &mut App) {
    if let Some(path) = app.result_files.get(app.result_cursor) {
        app.result_preview =
            std::fs::read_to_string(path).unwrap_or_else(|e| format!("Error: {e}"));
    } else {
        app.result_preview = String::new();
    }
}
