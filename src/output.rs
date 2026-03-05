use crate::error::AppError;
use crate::execution::ExecutionMode;
use crate::provider::ProviderKind;
use chrono::Local;
use rand::Rng;
use std::path::PathBuf;

pub struct OutputManager {
    run_dir: PathBuf,
}

impl OutputManager {
    pub fn new(base_dir: &PathBuf, session_name: Option<&str>) -> Result<Self, AppError> {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let suffix: u16 = rand::thread_rng().gen_range(100..999);
        let dir_name = match session_name {
            Some(name) if !name.is_empty() => {
                let sanitized: String = name
                    .chars()
                    .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
                    .collect();
                format!("{}_{}_{}", timestamp, suffix, sanitized)
            }
            _ => format!("{}_{}", timestamp, suffix),
        };
        let run_dir = base_dir.join(dir_name);
        std::fs::create_dir_all(&run_dir)?;
        Ok(Self { run_dir })
    }

    pub fn run_dir(&self) -> &PathBuf {
        &self.run_dir
    }

    pub fn write_prompt(&self, prompt: &str) -> Result<(), AppError> {
        let path = self.run_dir.join("prompt.md");
        std::fs::write(path, prompt)?;
        Ok(())
    }

    pub fn write_session_info(
        &self,
        mode: &ExecutionMode,
        agents: &[ProviderKind],
        iterations: u32,
        session_name: Option<&str>,
    ) -> Result<(), AppError> {
        let agents_str: Vec<&str> = agents.iter().map(|a| a.config_key()).collect();
        let name_line = match session_name {
            Some(name) if !name.is_empty() => format!("name = \"{}\"\n", name),
            _ => String::new(),
        };
        let content = format!(
            "{}mode = \"{}\"\nagents = {:?}\niterations = {}\n",
            name_line,
            mode.as_str(),
            agents_str,
            iterations,
        );
        let path = self.run_dir.join("session.toml");
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn write_agent_output(
        &self,
        kind: ProviderKind,
        iteration: u32,
        content: &str,
    ) -> Result<PathBuf, AppError> {
        let filename = format!(
            "{}_iter{}.md",
            kind.config_key(),
            iteration
        );
        let path = self.run_dir.join(&filename);
        std::fs::write(&path, content)?;
        Ok(path)
    }

    pub fn append_error(&self, error: &str) -> Result<(), AppError> {
        use std::io::Write;
        let path = self.run_dir.join("_errors.log");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let ts = Local::now().format("%H:%M:%S");
        writeln!(file, "[{ts}] {error}")?;
        Ok(())
    }

}
