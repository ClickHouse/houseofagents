mod app;
mod config;
mod error;
mod event;
mod execution;
mod output;
mod provider;
mod screen;
mod tui;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "houseofagents", about = "Multi-agent prompt runner with TUI")]
struct Cli {
    /// Path to config file (default: ~/.config/houseofagents/config.toml)
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install panic hook that restores terminal before printing panic
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = tui::restore_terminal();
        original_hook(panic_info);
    }));

    let _cli = Cli::parse();

    let config = config::AppConfig::load()?;

    let mut app = app::App::new(config);
    tui::run(&mut app).await?;

    Ok(())
}
