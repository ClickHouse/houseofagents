use crate::error::AppError;
use crate::execution::ProgressEvent;
use crate::output::OutputManager;
use crate::provider::Provider;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn run_solo(
    prompt: &str,
    mut providers: Vec<Box<dyn Provider>>,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    let mut handles = Vec::new();

    for provider in providers.drain(..) {
        let prompt = prompt.to_string();
        let tx = progress_tx.clone();
        let cancel = cancel.clone();
        let run_dir = output.run_dir().clone();

        handles.push(tokio::spawn(async move {
            solo_agent(provider, &prompt, &run_dir, tx, cancel).await
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

async fn solo_agent(
    mut provider: Box<dyn Provider>,
    prompt: &str,
    run_dir: &std::path::Path,
    tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) {
    let kind = provider.kind();

    if cancel.load(Ordering::Relaxed) {
        return;
    }

    let _ = tx.send(ProgressEvent::AgentStarted { kind, iteration: 1 });

    match provider.send(prompt).await {
        Ok(resp) => {
            let filename = format!("{}_iter1.md", kind.config_key());
            let path = run_dir.join(&filename);
            let _ = std::fs::write(&path, &resp.content);
            let _ = tx.send(ProgressEvent::AgentFinished {
                kind,
                iteration: 1,
            });
        }
        Err(e) => {
            let _ = tx.send(ProgressEvent::AgentError {
                kind,
                iteration: 1,
                error: e.to_string(),
            });
        }
    }
}
