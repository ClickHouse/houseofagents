use crate::app::App;
use crate::execution::ProgressEvent;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, Paragraph};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Length(3), // Progress gauge
            Constraint::Min(0),   // Event log
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let title = Paragraph::new(format!("Running — {} mode", app.selected_mode))
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Progress gauge
    let total_steps = compute_total_steps(app);
    let completed = count_completed_steps(app);
    let ratio = if total_steps > 0 {
        (completed as f64 / total_steps as f64).min(1.0)
    } else {
        0.0
    };

    let gauge = Gauge::default()
        .block(
            Block::default()
                .title(" Progress ")
                .borders(Borders::ALL),
        )
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(ratio)
        .label(format!("{completed}/{total_steps}"));
    f.render_widget(gauge, chunks[1]);

    // Event log
    let items: Vec<ListItem> = app
        .progress_events
        .iter()
        .map(|evt| match evt {
            ProgressEvent::AgentStarted { kind, iteration } => ListItem::new(Line::from(vec![
                Span::styled("▶ ", Style::default().fg(Color::Yellow)),
                Span::raw(format!("{} starting (iter {})", kind.display_name(), iteration)),
            ])),
            ProgressEvent::AgentFinished {
                kind, iteration,
            } => ListItem::new(Line::from(vec![
                Span::styled("✓ ", Style::default().fg(Color::Green)),
                Span::raw(format!(
                    "{} finished (iter {})",
                    kind.display_name(),
                    iteration
                )),
            ])),
            ProgressEvent::AgentError {
                kind,
                iteration,
                error,
            } => ListItem::new(Line::from(vec![
                Span::styled("✗ ", Style::default().fg(Color::Red)),
                Span::raw(format!(
                    "{} error (iter {}): {}",
                    kind.display_name(),
                    iteration,
                    error
                )),
            ])),
            ProgressEvent::IterationComplete { iteration } => {
                ListItem::new(Line::from(vec![Span::styled(
                    format!("── Iteration {iteration} complete ──"),
                    Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::ITALIC),
                )]))
            }
            ProgressEvent::AllDone => ListItem::new(Line::from(vec![Span::styled(
                "All done!",
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            )])),
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Activity ")
            .borders(Borders::ALL),
    );
    f.render_widget(list, chunks[2]);

    // Help bar
    let help_text = if app.is_running {
        "Esc: cancel run"
    } else {
        "Enter: view results  q: quit"
    };
    let help = Paragraph::new(help_text).block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[3]);
}

fn compute_total_steps(app: &App) -> usize {
    let agents = app.selected_agents.len();
    match app.selected_mode {
        crate::execution::ExecutionMode::Solo => agents,
        crate::execution::ExecutionMode::Relay => agents * app.iterations as usize,
        crate::execution::ExecutionMode::Swarm => agents * app.iterations as usize,
    }
}

fn count_completed_steps(app: &App) -> usize {
    app.progress_events
        .iter()
        .filter(|e| matches!(e, ProgressEvent::AgentFinished { .. } | ProgressEvent::AgentError { .. }))
        .count()
}
