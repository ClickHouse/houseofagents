use crate::error::AppError;
use crate::execution::ProgressEvent;
use crate::output::OutputManager;
use crate::provider::{Provider, ProviderKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn run_relay(
    prompt: &str,
    mut providers: Vec<Box<dyn Provider>>,
    iterations: u32,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut last_output = String::new();

    // Pre-compute agent kinds for building messages without borrowing providers
    let agent_kinds: Vec<ProviderKind> = providers.iter().map(|p| p.kind()).collect();
    let num_agents = providers.len();

    for iteration in 1..=iterations {
        for i in 0..num_agents {
            if cancel.load(Ordering::Relaxed) {
                let _ = progress_tx.send(ProgressEvent::AllDone);
                return Ok(());
            }

            let kind = agent_kinds[i];
            let _ = progress_tx.send(ProgressEvent::AgentStarted { kind, iteration });

            let message = if iteration == 1 && i == 0 {
                prompt.to_string()
            } else {
                let prev_kind = if i == 0 {
                    agent_kinds[num_agents - 1]
                } else {
                    agent_kinds[i - 1]
                };
                format!(
                    "Here is the output from {} (the previous agent):\n\n---\n{}\n---\n\nPlease build upon and improve this work.",
                    prev_kind.display_name(), last_output
                )
            };

            match providers[i].send(&message).await {
                Ok(resp) => {
                    let _ = output.write_agent_output(kind, iteration, &resp.content);
                    let _ = progress_tx.send(ProgressEvent::AgentFinished {
                        kind,
                        iteration,
                    });
                    last_output = resp.content;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let _ = output.append_error(&format!("{kind} iter{iteration}: {err_str}"));
                    let _ = progress_tx.send(ProgressEvent::AgentError {
                        kind,
                        iteration,
                        error: err_str,
                    });
                }
            }
        }

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}
