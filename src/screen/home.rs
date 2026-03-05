use crate::app::{App, HomeSection};
use crate::execution::ExecutionMode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),   // Main content
            Constraint::Length(3), // Help bar
        ])
        .split(f.area());

    // Title
    let title = Paragraph::new("House of Agents")
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::BOTTOM));
    f.render_widget(title, chunks[0]);

    // Main content: agents list + mode picker side by side
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    draw_agents_panel(f, app, main_chunks[0]);
    draw_mode_panel(f, app, main_chunks[1]);

    // Help bar
    let help = Paragraph::new(Line::from(vec![
        Span::styled("Space", Style::default().fg(Color::Yellow)),
        Span::raw(": toggle  "),
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::raw(": switch panel  "),
        Span::styled("Enter", Style::default().fg(Color::Yellow)),
        Span::raw(": proceed  "),
        Span::styled("e", Style::default().fg(Color::Yellow)),
        Span::raw(": edit config  "),
        Span::styled("q", Style::default().fg(Color::Yellow)),
        Span::raw(": quit"),
    ]))
    .block(Block::default().borders(Borders::TOP));
    f.render_widget(help, chunks[2]);

    // Error modal overlay
    if let Some(ref err) = app.error_modal {
        draw_error_modal(f, err);
    }

    // Edit popup overlay
    if app.show_edit_popup {
        draw_edit_popup(f, app);
    }
}

fn draw_agents_panel(f: &mut Frame, app: &App, area: Rect) {
    let providers = app.available_providers();
    let is_focused = app.home_section == HomeSection::Agents;

    let items: Vec<ListItem> = providers
        .iter()
        .enumerate()
        .map(|(i, (kind, available))| {
            let selected = app.selected_agents.contains(kind);
            let marker = if selected { "[x]" } else { "[ ]" };
            let status = if *available { "" } else { " (no key)" };
            let name = kind.display_name();

            let style = if !available {
                Style::default().fg(Color::DarkGray)
            } else if is_focused && i == app.home_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };

            ListItem::new(format!("{marker} {name}{status}")).style(style)
        })
        .collect();

    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let list = List::new(items).block(
        Block::default()
            .title(" Agents ")
            .borders(Borders::ALL)
            .border_style(border_style),
    );
    f.render_widget(list, area);
}

fn draw_mode_panel(f: &mut Frame, app: &App, area: Rect) {
    let is_focused = app.home_section == HomeSection::Mode;
    let modes = ExecutionMode::all();

    let items: Vec<ListItem> = modes
        .iter()
        .enumerate()
        .map(|(i, mode)| {
            let selected = app.selected_mode == *mode;
            let marker = if selected { "(o)" } else { "( )" };

            let style = if is_focused && i == app.home_cursor {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else if selected {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };

            ListItem::new(Line::from(vec![
                Span::styled(format!("{marker} {mode}"), style),
                Span::styled(format!("  {}", mode.description()), Style::default().fg(Color::DarkGray)),
            ]))
        })
        .collect();

    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let list = List::new(items).block(
        Block::default()
            .title(" Mode ")
            .borders(Borders::ALL)
            .border_style(border_style),
    );
    f.render_widget(list, area);
}

fn draw_error_modal(f: &mut Frame, message: &str) {
    let area = centered_rect(60, 20, f.area());
    let block = Block::default()
        .title(" Error ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Red));
    let text = Paragraph::new(message)
        .style(Style::default().fg(Color::Red))
        .block(block);
    f.render_widget(ratatui::widgets::Clear, area);
    f.render_widget(text, area);
}

fn draw_edit_popup(f: &mut Frame, app: &App) {
    let area = centered_rect(70, 60, f.area());
    let providers = app.available_providers();

    let mut lines = vec![
        Line::from(Span::styled(
            "Session Provider Overrides",
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "j/k: navigate  a: edit key  m: edit model  Enter: apply  Esc: close",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(""),
    ];

    for (i, (kind, _available)) in providers.iter().enumerate() {
        let config = app.effective_provider_config(*kind);
        let (key, model) = match &config {
            Some(c) => (mask_key(&c.api_key), c.model.clone()),
            None => ("(not set)".into(), "(not set)".into()),
        };

        let is_selected = i == app.edit_popup_cursor;
        let style = if is_selected {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };

        lines.push(Line::from(Span::styled(
            format!("{}", kind.display_name()),
            style,
        )));
        lines.push(Line::from(format!("  Key:   {key}")));
        lines.push(Line::from(format!("  Model: {model}")));

        // Show edit buffer if this provider is selected and buffer is non-empty
        if is_selected && !app.edit_buffer.is_empty() {
            let field_name = match app.edit_popup_field {
                crate::app::EditField::ApiKey => "key",
                crate::app::EditField::Model => "model",
            };
            lines.push(Line::from(Span::styled(
                format!("  Editing {}: {}_", field_name, app.edit_buffer),
                Style::default().fg(Color::Yellow),
            )));
        }

        lines.push(Line::from(""));
    }

    let block = Block::default()
        .title(" Edit Providers ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));
    let text = Paragraph::new(lines).block(block);
    f.render_widget(ratatui::widgets::Clear, area);
    f.render_widget(text, area);
}

fn mask_key(key: &str) -> String {
    if key.len() <= 8 {
        "****".into()
    } else {
        format!("{}...{}", &key[..4], &key[key.len() - 4..])
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
