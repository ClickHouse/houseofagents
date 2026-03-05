pub mod home;
pub mod order;
pub mod prompt;
pub mod results;
pub mod running;

use crate::app::{App, Screen};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    match app.screen {
        Screen::Home => home::draw(f, app),
        Screen::Prompt => prompt::draw(f, app),
        Screen::Order => order::draw(f, app),
        Screen::Running => running::draw(f, app),
        Screen::Results => results::draw(f, app),
    }
}
