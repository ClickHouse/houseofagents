use super::consolidation::PostRunPromptBudget;
use super::execution::validate_agent_runtime;
use super::results::batch_run_directories;
use super::*;

pub(super) fn maybe_start_diagnostics(app: &mut App) {
    if app.running.cancel_flag.load(Ordering::Relaxed)
        || app.running.diagnostic_running
        || app.running.diagnostic_rx.is_some()
    {
        return;
    }

    let diag_agent_name = match app.config.diagnostic_provider.as_deref() {
        Some(name) => name.to_string(),
        None => return,
    };
    let run_dir = match app.running.run_dir.clone() {
        Some(path) => path,
        None => return,
    };
    let agent_config = match app.effective_agent_config(&diag_agent_name).cloned() {
        Some(cfg) => cfg,
        None => {
            app.error_modal = Some(format!(
                "Diagnostic agent '{}' is not configured",
                diag_agent_name
            ));
            return;
        }
    };
    let diagnostic_kind = agent_config.provider;
    if let Err(message) = validate_agent_runtime(
        app,
        &format!("Diagnostic agent '{}'", diag_agent_name),
        &agent_config,
    ) {
        app.error_modal = Some(message);
        return;
    }
    let pconfig = agent_config.to_provider_config();
    let base_errors = app.error_ledger().iter().cloned().collect::<Vec<_>>();

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(
            app.effective_http_timeout_seconds().max(1),
        ))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            app.error_modal = Some(format!("Failed to create HTTP client: {e}"));
            return;
        }
    };
    let provider = provider::create_provider(
        diagnostic_kind,
        &pconfig,
        client,
        app.config.default_max_tokens,
        app.config.max_history_messages,
        app.config.max_history_bytes,
        app.effective_cli_timeout_seconds().max(1),
    );

    app.record_progress(ProgressEvent::AgentLog {
        agent: diag_agent_name,
        kind: diagnostic_kind,
        iteration: 0,
        message: "analyzing reports for errors".into(),
    });
    app.running.diagnostic_running = true;
    app.running.is_running = true;

    let output_path = run_dir.join("errors.md");
    let (tx, rx) = mpsc::unbounded_channel();
    app.running.diagnostic_rx = Some(rx);

    tokio::spawn(async move {
        let mut provider = provider;
        let prompt_result = tokio::task::spawn_blocking(move || {
            let report_files = collect_report_files(&run_dir);
            let app_errors = collect_application_errors(&base_errors, &run_dir);
            build_diagnostic_prompt(&report_files, &app_errors, pconfig.use_cli)
        })
        .await;

        let result = match prompt_result {
            Ok(Ok(prompt)) => match provider.send(&prompt).await {
                Ok(resp) => match tokio::fs::write(&output_path, &resp.content).await {
                    Ok(()) => Ok(output_path.display().to_string()),
                    Err(e) => Err(format!("Failed to write errors.md: {e}")),
                },
                Err(e) => Err(e.to_string()),
            },
            Ok(Err(error)) => Err(error),
            Err(e) => Err(format!("Diagnostic preparation task failed: {e}")),
        };
        let _ = tx.send(result);
    });
}

pub(super) fn handle_diagnostic_result(app: &mut App, result: Result<String, String>) {
    app.running.diagnostic_running = false;
    app.running.is_running = false;
    app.running.diagnostic_rx = None;

    let agent_name = app
        .config
        .diagnostic_provider
        .clone()
        .unwrap_or_else(|| "diagnostics".into());
    let kind = app
        .effective_agent_config(&agent_name)
        .map(|a| a.provider)
        .unwrap_or(ProviderKind::Anthropic);

    match result {
        Ok(path) => {
            app.record_progress(ProgressEvent::AgentLog {
                agent: agent_name,
                kind,
                iteration: 0,
                message: format!("Diagnostic report saved to {path}"),
            });
        }
        Err(e) => {
            app.record_progress(ProgressEvent::AgentError {
                agent: agent_name,
                kind,
                iteration: 0,
                error: e.clone(),
                details: Some(e),
            });
        }
    }
}

pub(super) fn collect_report_files(run_dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut dirs = vec![run_dir.to_path_buf()];
    if OutputManager::is_batch_root(run_dir) {
        dirs.extend(batch_run_directories(run_dir));
    }

    for dir in dirs {
        let mut dir_files = std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flat_map(|it| it.flatten())
            .filter_map(|entry| {
                let path = entry.path();
                if !path.is_file() {
                    return None;
                }
                let name = path.file_name()?.to_str()?.to_string();
                let is_md = path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("md"))
                    .unwrap_or(false);
                if is_md && name != "prompt.md" && name != "errors.md" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        files.append(&mut dir_files);
    }
    files.sort();
    files
}

pub(super) fn collect_application_errors(
    base_errors: &[String],
    run_dir: &std::path::Path,
) -> Vec<String> {
    let mut errors = base_errors.to_vec();

    let mut dirs = vec![run_dir.to_path_buf()];
    if OutputManager::is_batch_root(run_dir) {
        dirs.extend(batch_run_directories(run_dir));
    }

    for dir in dirs {
        let log_path = dir.join("_errors.log");
        if let Ok(content) = std::fs::read_to_string(&log_path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    errors.push(trimmed.to_string());
                }
            }
        }
    }

    let mut seen = std::collections::HashSet::new();
    errors.retain(|e| seen.insert(e.clone()));
    errors
}

pub(super) fn build_diagnostic_prompt(
    report_files: &[std::path::PathBuf],
    app_errors: &[String],
    use_cli: bool,
) -> Result<String, String> {
    let mut prompt = String::from(
        "Analyze all reports for OPERATIONAL errors only and produce a markdown report.\n",
    );
    let mut budget = PostRunPromptBudget::new();
    prompt.push_str(
        "Focus exclusively on errors that prevented an agent from completing its task:\n",
    );
    prompt.push_str("- API failures, timeouts, authentication errors\n");
    prompt.push_str("- CLI tool crashes, missing binaries, permission errors\n");
    prompt.push_str("- Agent permission denials (e.g. tool use blocked, sandbox restrictions, file access denied)\n");
    prompt.push_str("- Rate limits, network errors, malformed responses\n");
    prompt.push_str("- Provider returning empty or truncated output due to a fault\n\n");
    prompt.push_str("Do NOT report on:\n");
    prompt.push_str("- Quality or correctness of the agent's response content\n");
    prompt.push_str("- Whether the agent answered the user's prompt well\n");
    prompt.push_str("- Logical errors, hallucinations, or wrong answers in the output\n");
    prompt.push_str("- Style, formatting, or completeness of the response text\n\n");
    prompt.push_str("Write only the diagnostic report content.\n");
    prompt.push_str("Do not write files and do not ask for filesystem permissions.\n");
    prompt.push_str("The application will save your response to errors.md.\n\n");
    prompt.push_str(
        "Report structure:\n1) Summary\n2) Detected Issues\n3) Evidence\n4) Suggested Fixes\n\n",
    );
    prompt.push_str("If there are no operational errors, write a short summary stating all agents completed successfully.\n\n");

    prompt.push_str("Application-generated errors:\n");
    if app_errors.is_empty() {
        prompt.push_str("- none reported by application\n");
    } else {
        for err in app_errors {
            budget.add_text(err, "Application error diagnostics input")?;
            prompt.push_str("- ");
            prompt.push_str(err);
            prompt.push('\n');
        }
    }

    prompt.push_str("\nReports to analyze:\n");
    if report_files.is_empty() {
        prompt.push_str("- none\n");
        return Ok(prompt);
    }
    for path in report_files {
        prompt.push_str("- ");
        prompt.push_str(&path.display().to_string());
        prompt.push('\n');
    }

    if use_cli {
        prompt.push_str(
            "\nRead each listed file from disk before writing the report. Include permission/tool errors explicitly.\n",
        );
        return Ok(prompt);
    }

    prompt.push_str("\nReport contents:\n");
    for path in report_files {
        budget.add_file_sync(path, "Diagnostic report input")?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.md");
        match std::fs::read_to_string(path) {
            Ok(content) => {
                prompt.push_str(&format!("\n=== BEGIN {name} ===\n"));
                prompt.push_str(&content);
                prompt.push_str(&format!("\n=== END {name} ===\n"));
            }
            Err(e) => {
                prompt.push_str(&format!("\n=== BEGIN {name} ===\n"));
                prompt.push_str(&format!("Failed to read file: {e}\n"));
                prompt.push_str(&format!("=== END {name} ===\n"));
            }
        }
    }

    Ok(prompt)
}
