use crate::app::{App, PromptFocus};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // Title
            Constraint::Min(0),    // Prompt text area
            Constraint::Length(3), // Session name
            Constraint::Length(3), // Iterations
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let agents_str: Vec<&str> = app.selected_agents.iter().map(|a| a.display_name()).collect();
    let title = Paragraph::new(format!(
        "Prompt — {} mode with {}",
        app.selected_mode,
        agents_str.join(", ")
    ))
    .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
    .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Prompt text area
    let prompt_border = if app.prompt_focus == PromptFocus::Text {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let display_text = if app.prompt_text.is_empty() {
        "Enter your prompt here...".to_string()
    } else {
        app.prompt_text.clone()
    };

    let text_style = if app.prompt_text.is_empty() {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let prompt_area = Paragraph::new(display_text)
        .style(text_style)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(" Prompt ")
                .borders(Borders::ALL)
                .border_style(prompt_border),
        );
    f.render_widget(prompt_area, chunks[1]);

    // Session name
    let name_border = if app.prompt_focus == PromptFocus::SessionName {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let name_display = if app.session_name.is_empty() {
        if app.prompt_focus == PromptFocus::SessionName {
            "_".to_string()
        } else {
            "(optional)".to_string()
        }
    } else if app.prompt_focus == PromptFocus::SessionName {
        format!("{}_", app.session_name)
    } else {
        app.session_name.clone()
    };

    let name_style = if app.session_name.is_empty() && app.prompt_focus != PromptFocus::SessionName {
        Style::default().fg(Color::DarkGray)
    } else {
        Style::default()
    };

    let session_name = Paragraph::new(name_display).style(name_style).block(
        Block::default()
            .title(" Session Name ")
            .borders(Borders::ALL)
            .border_style(name_border),
    );
    f.render_widget(session_name, chunks[2]);

    // Iterations
    let iter_border = if app.prompt_focus == PromptFocus::Iterations {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let iter_info = if app.selected_mode == crate::execution::ExecutionMode::Solo {
        "(Solo mode: always 1 iteration)".to_string()
    } else {
        format!("Iterations: {}", app.iterations)
    };

    let iterations = Paragraph::new(iter_info).block(
        Block::default()
            .title(" Iterations ")
            .borders(Borders::ALL)
            .border_style(iter_border),
    );
    f.render_widget(iterations, chunks[3]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::raw(": switch focus  "),
        Span::styled("Ctrl+Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": submit  "),
        Span::styled("Esc", Style::default().fg(Color::Yellow)),
        Span::raw(": back  "),
        Span::styled("+/-", Style::default().fg(Color::Yellow)),
        Span::raw(": iterations"),
    ]))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[4]);
}
