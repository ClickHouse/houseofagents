pub mod help;
pub mod home;
pub mod order;
pub mod pipeline;
pub mod prompt;
pub mod results;
pub mod running;

use crate::app::{App, Screen};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    match app.screen {
        Screen::Home => home::draw(f, app),
        Screen::Prompt => prompt::draw(f, app),
        Screen::Order => order::draw(f, app),
        Screen::Running => running::draw(f, app),
        Screen::Results => results::draw(f, app),
        Screen::Pipeline => pipeline::draw(f, app),
    }
}

pub fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centered_rect_returns_expected_size() {
        let area = Rect::new(0, 0, 100, 40);
        let centered = centered_rect(60, 50, area);
        assert_eq!(centered.width, 60);
        assert_eq!(centered.height, 20);
    }

    #[test]
    fn centered_rect_is_positioned_in_center() {
        let area = Rect::new(0, 0, 80, 24);
        let centered = centered_rect(50, 50, area);
        assert_eq!(centered.x, 20);
        assert_eq!(centered.y, 6);
    }
}
