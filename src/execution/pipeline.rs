use crate::config::AppConfig;
use crate::error::AppError;
use crate::execution::{
    finish_live_log_forwarder, wait_for_cancel, ProgressEvent, PromptRuntimeContext,
};
use crate::output::OutputManager;
use crate::provider::{self, ProviderKind};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub type BlockId = u32;
#[allow(dead_code)]
type ProviderPool = HashMap<(String, String), Arc<Mutex<Box<dyn provider::Provider>>>>;
type PipelineAgentConfigs = HashMap<String, (ProviderKind, crate::config::ProviderConfig, bool)>;

#[derive(Debug, Clone)]
struct PipelineTaskMetadata {
    runtime_id: u32,
    source_block_id: BlockId,
    agent_name: String,
    label: String,
    iteration: u32,
}

struct PipelineMessageContext<'a> {
    def: &'a PipelineDefinition,
    iteration: u32,
    block_outputs: &'a HashMap<u32, String>,
    previous_terminal_outputs: &'a str,
    output: &'a OutputManager,
    prompt_context: &'a PromptRuntimeContext,
    runtime_table: &'a RuntimeReplicaTable,
}

// ---------------------------------------------------------------------------
// Runtime Replica Table
// ---------------------------------------------------------------------------

pub(crate) struct RuntimeReplicaInfo {
    pub runtime_id: u32,
    pub source_block_id: BlockId,
    pub replica_index: u32,
    pub agent: String,
    pub display_label: String,
    pub session_key: String,
    pub filename_stem: String,
}

pub(crate) struct RuntimeReplicaTable {
    pub entries: Vec<RuntimeReplicaInfo>,
    pub logical_to_runtime: HashMap<BlockId, Vec<u32>>,
    pub keep_policy: HashMap<(String, String), bool>,
}

pub(crate) fn build_runtime_table(def: &PipelineDefinition) -> RuntimeReplicaTable {
    let mut entries = Vec::new();
    let mut logical_to_runtime: HashMap<BlockId, Vec<u32>> = HashMap::new();
    let mut keep_policy: HashMap<(String, String), bool> = HashMap::new();
    let mut next_id: u32 = 0;

    for block in &def.blocks {
        let base_session_key = block.effective_session_key();
        let base_keep = def.keep_session_across_iterations(&block.agent, &base_session_key);
        let block_name_key = if block.name.trim().is_empty() {
            format!("block{}", block.id)
        } else {
            format!(
                "{}_b{}",
                OutputManager::sanitize_session_name(&block.name),
                block.id
            )
        };
        let agent_file_key = OutputManager::sanitize_session_name(&block.agent);

        let mut runtime_ids = Vec::new();
        for ri in 0..block.replicas {
            let runtime_id = next_id;
            next_id += 1;

            let (display_label, session_key, filename_stem) = if block.replicas == 1 {
                (
                    block_label(block),
                    base_session_key.clone(),
                    format!("{}_{}", block_name_key, agent_file_key),
                )
            } else {
                let ord = ri + 1;
                (
                    format!("{} (r{})", block_label(block), ord),
                    format!("{}_r{}", base_session_key, ord),
                    format!("{}_{}_r{}", block_name_key, agent_file_key, ord),
                )
            };

            keep_policy.insert((block.agent.clone(), session_key.clone()), base_keep);

            entries.push(RuntimeReplicaInfo {
                runtime_id,
                source_block_id: block.id,
                replica_index: ri,
                agent: block.agent.clone(),
                display_label,
                session_key,
                filename_stem,
            });

            runtime_ids.push(runtime_id);
        }
        logical_to_runtime.insert(block.id, runtime_ids);
    }

    RuntimeReplicaTable {
        entries,
        logical_to_runtime,
        keep_policy,
    }
}

fn replica_filename(info: &RuntimeReplicaInfo, iteration: u32) -> String {
    format!("{}_iter{}.md", info.filename_stem, iteration)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineBlock {
    pub id: BlockId,
    #[serde(default)]
    pub name: String,
    pub agent: String,
    #[serde(default)]
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub position: (u16, u16), // grid coordinates (col, row)
    #[serde(default = "default_one", skip_serializing_if = "is_one")]
    pub replicas: u32,
}

fn default_one() -> u32 {
    1
}

fn is_one(v: &u32) -> bool {
    *v == 1
}

impl PipelineBlock {
    pub fn effective_session_key(&self) -> String {
        self.session_id
            .clone()
            .unwrap_or_else(|| format!("__block_{}", self.id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConnection {
    pub from: BlockId,
    pub to: BlockId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionConfig {
    pub agent: String,
    pub session_key: String,
    #[serde(default = "default_keep_across_iterations")]
    pub keep_across_iterations: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveSession {
    pub agent: String,
    pub session_key: String,
    pub display_label: String,
    pub block_ids: Vec<BlockId>,
    pub keep_across_iterations: bool,
    pub total_replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    #[serde(default)]
    pub initial_prompt: String,
    #[serde(default = "default_iterations")]
    pub iterations: u32,
    #[serde(default)]
    pub blocks: Vec<PipelineBlock>,
    #[serde(default)]
    pub connections: Vec<PipelineConnection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub session_configs: Vec<SessionConfig>,
}

fn default_iterations() -> u32 {
    1
}

fn default_keep_across_iterations() -> bool {
    true
}

impl Default for PipelineDefinition {
    fn default() -> Self {
        Self {
            initial_prompt: String::new(),
            iterations: 1,
            blocks: Vec::new(),
            connections: Vec::new(),
            session_configs: Vec::new(),
        }
    }
}

impl PipelineDefinition {
    pub fn effective_sessions(&self) -> Vec<EffectiveSession> {
        let mut map: HashMap<(String, String), (String, Vec<BlockId>, u32)> = HashMap::new();
        for block in &self.blocks {
            let key = (block.agent.clone(), block.effective_session_key());
            let entry = map.entry(key).or_insert_with(|| {
                let label = if block.session_id.is_some() {
                    block.session_id.clone().unwrap()
                } else if !block.name.is_empty() {
                    block.name.clone()
                } else {
                    format!("Block {}", block.id)
                };
                (label, Vec::new(), 0)
            });
            entry.1.push(block.id);
            entry.2 += block.replicas;
        }
        let mut sessions: Vec<EffectiveSession> = map
            .into_iter()
            .map(|((agent, session_key), (display_label, block_ids, total_replicas))| {
                let keep = self.keep_session_across_iterations(&agent, &session_key);
                EffectiveSession {
                    agent,
                    session_key,
                    display_label,
                    block_ids,
                    keep_across_iterations: keep,
                    total_replicas,
                }
            })
            .collect();
        sessions.sort_by(|a, b| (&a.agent, &a.session_key).cmp(&(&b.agent, &b.session_key)));

        // Disambiguate rows where agent + display_label would look identical
        let mut label_counts: HashMap<(String, String), usize> = HashMap::new();
        for s in &sessions {
            *label_counts
                .entry((s.agent.clone(), s.display_label.clone()))
                .or_default() += 1;
        }
        for s in &mut sessions {
            if label_counts
                .get(&(s.agent.clone(), s.display_label.clone()))
                .copied()
                .unwrap_or(0)
                > 1
            {
                let ids: Vec<String> = s.block_ids.iter().map(|id| id.to_string()).collect();
                // Prefix with block IDs so truncation never hides the distinguishing part
                s.display_label = format!("#{}: {}", ids.join(","), s.display_label);
            }
        }

        sessions
    }

    pub fn keep_session_across_iterations(&self, agent: &str, session_key: &str) -> bool {
        self.session_configs
            .iter()
            .find(|c| c.agent == agent && c.session_key == session_key)
            .map(|c| c.keep_across_iterations)
            .unwrap_or(true)
    }

    pub fn set_keep_session_across_iterations(
        &mut self,
        agent: &str,
        session_key: &str,
        keep: bool,
    ) {
        if keep {
            // Remove explicit override (true is the default)
            self.session_configs
                .retain(|c| !(c.agent == agent && c.session_key == session_key));
        } else if let Some(existing) = self
            .session_configs
            .iter_mut()
            .find(|c| c.agent == agent && c.session_key == session_key)
        {
            existing.keep_across_iterations = false;
        } else {
            self.session_configs.push(SessionConfig {
                agent: agent.to_string(),
                session_key: session_key.to_string(),
                keep_across_iterations: false,
            });
        }
    }

    pub fn normalize_session_configs(&mut self) {
        // Collect valid effective session keys
        let valid: HashSet<(String, String)> = self
            .blocks
            .iter()
            .map(|b| (b.agent.clone(), b.effective_session_key()))
            .collect();

        // Drop stale rows and rows with keep=true (default)
        self.session_configs
            .retain(|c| !c.keep_across_iterations && valid.contains(&(c.agent.clone(), c.session_key.clone())));

        // Sort for stability
        self.session_configs
            .sort_by(|a, b| (&a.agent, &a.session_key).cmp(&(&b.agent, &b.session_key)));

        // Deduplicate
        self.session_configs
            .dedup_by(|a, b| a.agent == b.agent && a.session_key == b.session_key);
    }
}

// ---------------------------------------------------------------------------
// Graph utilities
// ---------------------------------------------------------------------------

/// Blocks with no incoming connections (DAG roots).
#[allow(dead_code)]
pub fn root_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_incoming: HashSet<BlockId> = def.connections.iter().map(|c| c.to).collect();
    def.blocks
        .iter()
        .filter(|b| !has_incoming.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Blocks with no outgoing connections (DAG terminals).
pub fn terminal_blocks(def: &PipelineDefinition) -> Vec<BlockId> {
    let has_outgoing: HashSet<BlockId> = def.connections.iter().map(|c| c.from).collect();
    def.blocks
        .iter()
        .filter(|b| !has_outgoing.contains(&b.id))
        .map(|b| b.id)
        .collect()
}

/// Direct predecessors of a block.
pub fn upstream_of(def: &PipelineDefinition, id: BlockId) -> Vec<BlockId> {
    def.connections
        .iter()
        .filter(|c| c.to == id)
        .map(|c| c.from)
        .collect()
}

/// Kahn's algorithm: returns parallelizable layers or Err on cycle.
pub fn topological_layers(def: &PipelineDefinition) -> Result<Vec<Vec<BlockId>>, CycleError> {
    let block_ids: HashSet<BlockId> = def.blocks.iter().map(|b| b.id).collect();
    let mut in_degree: HashMap<BlockId, usize> = block_ids.iter().map(|&id| (id, 0)).collect();
    let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();

    for conn in &def.connections {
        *in_degree.entry(conn.to).or_default() += 1;
        downstream.entry(conn.from).or_default().push(conn.to);
    }

    let mut queue: VecDeque<BlockId> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&id, _)| id)
        .collect();

    // Sort for determinism
    let mut sorted_queue: Vec<BlockId> = queue.drain(..).collect();
    sorted_queue.sort();
    queue.extend(sorted_queue);

    let mut layers: Vec<Vec<BlockId>> = Vec::new();
    let mut visited = 0usize;

    while !queue.is_empty() {
        let mut layer: Vec<BlockId> = queue.drain(..).collect();
        layer.sort();
        visited += layer.len();

        let mut next_queue = Vec::new();
        for &id in &layer {
            if let Some(children) = downstream.get(&id) {
                for &child in children {
                    let deg = in_degree.get_mut(&child).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        next_queue.push(child);
                    }
                }
            }
        }
        next_queue.sort();
        queue.extend(next_queue);
        layers.push(layer);
    }

    if visited != block_ids.len() {
        Err(CycleError)
    } else {
        Ok(layers)
    }
}

#[derive(Debug, Clone)]
pub struct CycleError;

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pipeline contains a cycle")
    }
}

/// DFS reachability: would adding `from → to` create a cycle?
/// Checks if `from` is reachable from `to` in the existing graph.
pub fn would_create_cycle(def: &PipelineDefinition, from: BlockId, to: BlockId) -> bool {
    if from == to {
        return true;
    }
    // BFS from `to` — if we reach `from`, adding from→to would create a cycle
    let downstream: HashMap<BlockId, Vec<BlockId>> = {
        let mut map: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
        for conn in &def.connections {
            map.entry(conn.from).or_default().push(conn.to);
        }
        map
    };
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(to);
    visited.insert(to);
    while let Some(node) = queue.pop_front() {
        if let Some(children) = downstream.get(&node) {
            for &child in children {
                if child == from {
                    return true;
                }
                if visited.insert(child) {
                    queue.push_back(child);
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Auto-position
// ---------------------------------------------------------------------------

/// Scan grid left-to-right, top-to-bottom for first unoccupied slot.
pub fn next_free_position(def: &PipelineDefinition) -> (u16, u16) {
    let occupied: HashSet<(u16, u16)> = def.blocks.iter().map(|b| b.position).collect();
    for row in 0u16..100 {
        for col in 0u16..100 {
            if !occupied.contains(&(col, row)) {
                return (col, row);
            }
        }
    }
    (0, 0)
}

// ---------------------------------------------------------------------------
// TOML save/load
// ---------------------------------------------------------------------------

pub fn pipelines_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("houseofagents")
        .join("pipelines")
}

pub fn ensure_pipelines_dir() -> io::Result<PathBuf> {
    let dir = pipelines_dir();
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn save_pipeline(def: &PipelineDefinition, path: &Path) -> Result<(), AppError> {
    let mut normalized = def.clone();
    normalized.normalize_session_configs();
    let content = toml::to_string_pretty(&normalized)
        .map_err(|e| AppError::Config(format!("Failed to serialize pipeline: {e}")))?;
    std::fs::write(path, content)?;
    Ok(())
}

pub fn load_pipeline(path: &Path) -> Result<PipelineDefinition, AppError> {
    let content = std::fs::read_to_string(path)?;
    let mut def: PipelineDefinition = toml::from_str(&content)
        .map_err(|e| AppError::Config(format!("Failed to parse pipeline: {e}")))?;
    def.normalize_session_configs();
    validate_pipeline(&def)?;
    Ok(def)
}

pub(crate) fn validate_replicas(def: &PipelineDefinition) -> Result<(), AppError> {
    for block in &def.blocks {
        if block.replicas < 1 {
            return Err(AppError::Config(format!(
                "Block '{}' has replicas < 1",
                block.name
            )));
        }
        if block.replicas > 32 {
            return Err(AppError::Config(format!(
                "Block '{}' has replicas > 32 (max allowed)",
                block.name
            )));
        }
    }
    // Session sharing restriction: blocks with replicas > 1 cannot share session_id
    for block in &def.blocks {
        if block.replicas > 1 {
            if let Some(ref sid) = block.session_id {
                for other in &def.blocks {
                    if other.id != block.id && other.session_id.as_deref() == Some(sid) {
                        return Err(AppError::Config(format!(
                            "Session '{}' is used by replicated block '{}' and cannot be shared",
                            sid, block.name
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_pipeline(def: &PipelineDefinition) -> Result<(), AppError> {
    // Check duplicate block IDs
    let mut seen = HashSet::new();
    for block in &def.blocks {
        if !seen.insert(block.id) {
            return Err(AppError::Config(format!(
                "Duplicate block ID: {}",
                block.id
            )));
        }
    }

    // Check dangling connection references
    for conn in &def.connections {
        if !seen.contains(&conn.from) {
            return Err(AppError::Config(format!(
                "Connection references non-existent block: {}",
                conn.from
            )));
        }
        if !seen.contains(&conn.to) {
            return Err(AppError::Config(format!(
                "Connection references non-existent block: {}",
                conn.to
            )));
        }
        // Self-edges
        if conn.from == conn.to {
            return Err(AppError::Config(format!(
                "Self-edge on block {}",
                conn.from
            )));
        }
    }

    // Check for cycles
    topological_layers(def)
        .map_err(|_| AppError::Config("Pipeline contains a cycle".to_string()))?;

    // Check for duplicate session config entries
    {
        let mut seen_configs = HashSet::new();
        for cfg in &def.session_configs {
            if !seen_configs.insert((&cfg.agent, &cfg.session_key)) {
                return Err(AppError::Config(format!(
                    "Duplicate session config for ({}, {})",
                    cfg.agent, cfg.session_key
                )));
            }
        }
    }

    // Replica validation
    validate_replicas(def)?;

    Ok(())
}

pub fn list_pipeline_files() -> io::Result<Vec<PathBuf>> {
    let dir = pipelines_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                Some(path)
            } else {
                None
            }
        })
        .collect();
    files.sort();
    Ok(files)
}

// ---------------------------------------------------------------------------
// Execution engine
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    def: &PipelineDefinition,
    config: &AppConfig,
    agent_configs: PipelineAgentConfigs,
    client: reqwest::Client,
    cli_timeout_secs: u64,
    prompt_context: &PromptRuntimeContext,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
) -> Result<(), AppError> {
    run_pipeline_with_provider_factory(
        def,
        config.pipeline_block_concurrency,
        agent_configs,
        prompt_context,
        output,
        progress_tx,
        cancel,
        |kind, cfg| {
            provider::create_provider(
                kind,
                cfg,
                client.clone(),
                config.default_max_tokens,
                config.max_history_messages,
                config.max_history_bytes,
                cli_timeout_secs,
            )
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_pipeline_with_provider_factory<F>(
    def: &PipelineDefinition,
    max_block_concurrency: u32,
    agent_configs: PipelineAgentConfigs,
    prompt_context: &PromptRuntimeContext,
    output: &OutputManager,
    progress_tx: mpsc::UnboundedSender<ProgressEvent>,
    cancel: Arc<AtomicBool>,
    provider_factory: F,
) -> Result<(), AppError>
where
    F: Fn(ProviderKind, &crate::config::ProviderConfig) -> Box<dyn provider::Provider>,
{
    let rt = build_runtime_table(def);
    let total_tasks = rt.entries.len();
    if total_tasks == 0 {
        let _ = progress_tx.send(ProgressEvent::AllDone);
        return Ok(());
    }

    let concurrency_sem = Arc::new(tokio::sync::Semaphore::new(if max_block_concurrency == 0 {
        tokio::sync::Semaphore::MAX_PERMITS
    } else {
        max_block_concurrency as usize
    }));

    // Build provider pool keyed by (agent, runtime_session_key)
    let mut provider_pool: ProviderPool = HashMap::new();
    for entry in &rt.entries {
        let pool_key = (entry.agent.clone(), entry.session_key.clone());
        if let std::collections::hash_map::Entry::Vacant(vacant) = provider_pool.entry(pool_key) {
            if let Some((kind, cfg, _use_cli)) = agent_configs.get(&entry.agent) {
                let p = provider_factory(*kind, cfg);
                vacant.insert(Arc::new(Mutex::new(p)));
            }
        }
    }

    // Build adjacency structures with replica-weighted in-degree
    let mut in_degree: HashMap<BlockId, usize> = def.blocks.iter().map(|b| (b.id, 0)).collect();
    let mut downstream: HashMap<BlockId, Vec<BlockId>> = HashMap::new();
    for conn in &def.connections {
        let from_block = def.blocks.iter().find(|b| b.id == conn.from).unwrap();
        *in_degree.entry(conn.to).or_default() += from_block.replicas as usize;
        downstream.entry(conn.from).or_default().push(conn.to);
    }

    let mut previous_terminal_outputs = String::new();

    for iteration in 1..=def.iterations {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        // Clear history for sessions configured to reset between iterations
        if iteration > 1 {
            for ((agent, session_key), provider_arc) in &provider_pool {
                let keep = rt
                    .keep_policy
                    .get(&(agent.clone(), session_key.clone()))
                    .copied()
                    .unwrap_or(true);
                if !keep {
                    let mut guard = provider_arc.lock().await;
                    guard.clear_history();
                }
            }
        }

        let mut current_in_degree = in_degree.clone();
        let mut failed_replicas: HashSet<u32> = HashSet::new();
        let mut failed_logical: HashSet<BlockId> = HashSet::new();
        let mut replica_outputs: HashMap<u32, String> = HashMap::new();
        let mut completed = 0usize;

        // Seed ready queue with root blocks (logical IDs)
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<BlockId>();
        let mut roots: Vec<BlockId> = current_in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        roots.sort();
        for &id in &roots {
            let _ = ready_tx.send(id);
        }

        let mut tasks: tokio::task::JoinSet<(u32, Result<String, String>)> =
            tokio::task::JoinSet::new();
        let mut task_metadata: HashMap<tokio::task::Id, PipelineTaskMetadata> = HashMap::new();

        while completed < total_tasks {
            tokio::select! {
                Some(block_id) = ready_rx.recv() => {
                    if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }

                    let block = match def.blocks.iter().find(|b| b.id == block_id) {
                        Some(b) => b,
                        None => {
                            let replica_count = rt.logical_to_runtime.get(&block_id)
                                .map(|v| v.len()).unwrap_or(1);
                            completed += replica_count;
                            continue;
                        }
                    };
                    let replica_count = block.replicas as usize;

                    // Group-aware failure: skip only if ALL replicas of an upstream block failed
                    let failed_upstream: Vec<BlockId> = upstream_of(def, block_id)
                        .into_iter()
                        .filter(|u| failed_logical.contains(u))
                        .collect();

                    if !failed_upstream.is_empty() {
                        let reason = format!("all replicas of upstream Block {} failed", failed_upstream[0]);
                        if let Some(rids) = rt.logical_to_runtime.get(&block_id) {
                            for &rid in rids {
                                let info = &rt.entries[rid as usize];
                                let _ = progress_tx.send(ProgressEvent::BlockSkipped {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    label: info.display_label.clone(),
                                    iteration,
                                    reason: reason.clone(),
                                });
                                failed_replicas.insert(rid);
                            }
                        }
                        failed_logical.insert(block_id);
                        completed += replica_count;
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                let deg = current_in_degree.get_mut(&child).unwrap();
                                *deg = deg.saturating_sub(replica_count);
                                if *deg == 0 {
                                    let _ = ready_tx.send(child);
                                }
                            }
                        }
                        continue;
                    }

                    let use_cli = agent_configs
                        .get(&block.agent)
                        .map(|(_, _, cli)| *cli)
                        .unwrap_or(false);

                    // Build message ONCE (shared by all replicas of this block)
                    let message = build_pipeline_block_message(
                        block,
                        use_cli,
                        &PipelineMessageContext {
                            def,
                            iteration,
                            block_outputs: &replica_outputs,
                            previous_terminal_outputs: &previous_terminal_outputs,
                            output,
                            prompt_context,
                            runtime_table: &rt,
                        },
                    );

                    // Check provider availability (all replicas share the same agent)
                    let rids = match rt.logical_to_runtime.get(&block_id) {
                        Some(rids) => rids,
                        None => { completed += replica_count; continue; }
                    };

                    if !agent_configs.contains_key(&block.agent) {
                        for &rid in rids {
                            let info = &rt.entries[rid as usize];
                            let _ = progress_tx.send(ProgressEvent::BlockError {
                                block_id: rid,
                                agent_name: info.agent.clone(),
                                label: info.display_label.clone(),
                                iteration,
                                error: "No provider available".into(),
                                details: None,
                            });
                            failed_replicas.insert(rid);
                        }
                        failed_logical.insert(block_id);
                        completed += replica_count;
                        if let Some(children) = downstream.get(&block_id) {
                            for &child in children {
                                let deg = current_in_degree.get_mut(&child).unwrap();
                                *deg = deg.saturating_sub(replica_count);
                                if *deg == 0 {
                                    let _ = ready_tx.send(child);
                                }
                            }
                        }
                        continue;
                    }

                    // Spawn one task per replica
                    for &rid in rids {
                        let info = &rt.entries[rid as usize];
                        let pool_key = (info.agent.clone(), info.session_key.clone());
                        let provider_arc = match provider_pool.get(&pool_key) {
                            Some(p) => p.clone(),
                            None => {
                                let _ = progress_tx.send(ProgressEvent::BlockError {
                                    block_id: rid,
                                    agent_name: info.agent.clone(),
                                    label: info.display_label.clone(),
                                    iteration,
                                    error: "No provider available".into(),
                                    details: None,
                                });
                                failed_replicas.insert(rid);
                                completed += 1;
                                if let Some(children) = downstream.get(&block_id) {
                                    for &child in children {
                                        let deg = current_in_degree.get_mut(&child).unwrap();
                                        *deg -= 1;
                                        if *deg == 0 {
                                            let _ = ready_tx.send(child);
                                        }
                                    }
                                }
                                continue;
                            }
                        };

                        let _ = progress_tx.send(ProgressEvent::BlockStarted {
                            block_id: rid,
                            agent_name: info.agent.clone(),
                            label: info.display_label.clone(),
                            iteration,
                        });

                        let ptx = progress_tx.clone();
                        let cancel_clone = cancel.clone();
                        let task_output = output.clone();
                        let task_filename = replica_filename(info, iteration);
                        let task_agent_name = info.agent.clone();
                        let task_label = info.display_label.clone();
                        let message_clone = message.clone();
                        let sem_clone = concurrency_sem.clone();
                        let task_handle = tasks.spawn(async move {
                            let mut guard = provider_arc.lock().await;
                            let _permit = sem_clone.acquire().await.expect("semaphore closed");

                            let (live_tx, mut live_rx) = mpsc::unbounded_channel::<String>();
                            guard.set_live_log_sender(Some(live_tx));

                            let bid = rid;
                            let an = task_agent_name.clone();
                            let it = iteration;
                            let ptx2 = ptx.clone();
                            let live_forward = tokio::spawn(async move {
                                while let Some(line) = live_rx.recv().await {
                                    let _ = ptx2.send(ProgressEvent::BlockLog {
                                        block_id: bid,
                                        agent_name: an.clone(),
                                        iteration: it,
                                        message: format!("CLI {line}"),
                                    });
                                }
                            });

                            let result = tokio::select! {
                                res = crate::execution::send_with_streaming(
                                    &mut **guard,
                                    &message_clone,
                                    &ptx,
                                    {
                                        let agent_name = task_agent_name.clone();
                                        let bid = rid;
                                        let it = iteration;
                                        move |chunk| ProgressEvent::BlockStreamChunk {
                                            block_id: bid,
                                            agent_name: agent_name.clone(),
                                            iteration: it,
                                            chunk,
                                        }
                                    },
                                ) => Some(res),
                                _ = wait_for_cancel(&cancel_clone) => None
                            };

                            guard.set_live_log_sender(None);
                            let cancelled = result.is_none();
                            drop(guard);
                            finish_live_log_forwarder(live_forward, cancelled).await;

                            match result {
                                None => (rid, Err("Cancelled".to_string())),
                                Some(Ok(resp)) => {
                                    for log in &resp.debug_logs {
                                        let _ = ptx.send(ProgressEvent::BlockLog {
                                            block_id: rid,
                                            agent_name: task_agent_name.clone(),
                                            iteration,
                                            message: log.clone(),
                                        });
                                    }
                                    let path = task_output.run_dir().join(&task_filename);
                                    if let Err(e) = std::fs::write(&path, &resp.content) {
                                        let error = format!("Failed to write output: {e}");
                                        let _ = task_output.append_error(&format!(
                                            "runtime {rid} {task_agent_name} iter{iteration}: {error}"
                                        ));
                                        let _ = ptx.send(ProgressEvent::BlockError {
                                            block_id: rid,
                                            agent_name: task_agent_name,
                                            label: task_label,
                                            iteration,
                                            error: error.clone(),
                                            details: Some(error.clone()),
                                        });
                                        (rid, Err(error))
                                    } else {
                                        let _ = ptx.send(ProgressEvent::BlockFinished {
                                            block_id: rid,
                                            agent_name: task_agent_name,
                                            label: task_label,
                                            iteration,
                                        });
                                        (rid, Ok(resp.content))
                                    }
                                }
                                Some(Err(e)) => {
                                    let error = e.to_string();
                                    let _ = task_output.append_error(&format!(
                                        "runtime {rid} {task_agent_name} iter{iteration}: {error}"
                                    ));
                                    let _ = ptx.send(ProgressEvent::BlockError {
                                        block_id: rid,
                                        agent_name: task_agent_name,
                                        label: task_label,
                                        iteration,
                                        error: error.clone(),
                                        details: Some(error.clone()),
                                    });
                                    (rid, Err(error))
                                }
                            }
                        });
                        task_metadata.insert(
                            task_handle.id(),
                            PipelineTaskMetadata {
                                runtime_id: rid,
                                source_block_id: block_id,
                                agent_name: info.agent.clone(),
                                label: info.display_label.clone(),
                                iteration,
                            },
                        );
                    }
                }
                Some(result) = tasks.join_next() => {
                    completed += 1;
                    match result {
                        Ok((runtime_id, outcome)) => {
                            let source_id = rt.entries[runtime_id as usize].source_block_id;
                            match outcome {
                                Ok(content) => {
                                    replica_outputs.insert(runtime_id, content);
                                }
                                Err(_) => {
                                    failed_replicas.insert(runtime_id);
                                    if let Some(rids) = rt.logical_to_runtime.get(&source_id) {
                                        if rids.iter().all(|r| failed_replicas.contains(r)) {
                                            failed_logical.insert(source_id);
                                        }
                                    }
                                }
                            }
                            // Decrement downstream by 1 per replica completion
                            if let Some(children) = downstream.get(&source_id) {
                                for &child in children {
                                    let deg = current_in_degree.get_mut(&child).unwrap();
                                    *deg -= 1;
                                    if *deg == 0 {
                                        let _ = ready_tx.send(child);
                                    }
                                }
                            }
                        }
                        Err(join_error) => {
                            let error = format!("Pipeline worker panicked: {join_error}");
                            if let Some(metadata) = task_metadata.get(&join_error.id()).cloned() {
                                let _ = progress_tx.send(ProgressEvent::BlockError {
                                    block_id: metadata.runtime_id,
                                    agent_name: metadata.agent_name.clone(),
                                    label: metadata.label.clone(),
                                    iteration: metadata.iteration,
                                    error: error.clone(),
                                    details: Some(error.clone()),
                                });
                                let _ = output.append_error(&format!(
                                    "runtime {} {} iter{}: {}",
                                    metadata.runtime_id,
                                    metadata.agent_name,
                                    metadata.iteration,
                                    error
                                ));
                                failed_replicas.insert(metadata.runtime_id);
                                if let Some(rids) = rt.logical_to_runtime.get(&metadata.source_block_id) {
                                    if rids.iter().all(|r| failed_replicas.contains(r)) {
                                        failed_logical.insert(metadata.source_block_id);
                                    }
                                }
                                if let Some(children) = downstream.get(&metadata.source_block_id) {
                                    for &child in children {
                                        let deg = current_in_degree.get_mut(&child).unwrap();
                                        *deg -= 1;
                                        if *deg == 0 {
                                            let _ = ready_tx.send(child);
                                        }
                                    }
                                }
                            } else {
                                let _ = output.append_error(&format!(
                                    "pipeline panic could not be attributed to a block: {error}"
                                ));
                            }
                        }
                    }
                }
                else => break,
            }
        }

        let _ = progress_tx.send(ProgressEvent::IterationComplete { iteration });

        // Collect labeled terminal outputs for next iteration
        let terminals = terminal_blocks(def);
        previous_terminal_outputs.clear();
        for &tid in &terminals {
            if let Some(rids) = rt.logical_to_runtime.get(&tid) {
                let upstream_replicas = def.blocks.iter().find(|b| b.id == tid).map(|b| b.replicas).unwrap_or(1);
                for &rid in rids {
                    if let Some(content) = replica_outputs.get(&rid) {
                        if !previous_terminal_outputs.is_empty() {
                            previous_terminal_outputs.push_str("\n\n---\n\n");
                        }
                        if upstream_replicas > 1 {
                            let info = &rt.entries[rid as usize];
                            previous_terminal_outputs.push_str(&format!(
                                "--- Output from {} ---\n{}", info.display_label, content
                            ));
                        } else {
                            previous_terminal_outputs.push_str(content);
                        }
                    }
                }
            }
        }
    }

    let _ = progress_tx.send(ProgressEvent::AllDone);
    Ok(())
}

fn block_label(block: &PipelineBlock) -> String {
    if block.name.trim().is_empty() {
        format!("Block {}", block.id)
    } else {
        block.name.clone()
    }
}

fn build_pipeline_block_message(
    block: &PipelineBlock,
    use_cli: bool,
    context: &PipelineMessageContext<'_>,
) -> String {
    let is_root = upstream_of(context.def, block.id).is_empty();
    let base_message = if is_root && context.iteration == 1 {
        if block.prompt.is_empty() {
            context.def.initial_prompt.clone()
        } else {
            format!("{}\n\n{}", block.prompt, context.def.initial_prompt)
        }
    } else if is_root {
        let base = if block.prompt.is_empty() {
            context.def.initial_prompt.clone()
        } else {
            block.prompt.clone()
        };
        format!(
            "{base}\n\n--- Previous iteration outputs ---\n{}",
            context.previous_terminal_outputs
        )
    } else {
        let upstream_ids = upstream_of(context.def, block.id);
        let prefix = if block.prompt.is_empty() {
            String::new()
        } else {
            format!("{}\n\n", block.prompt)
        };
        if use_cli {
            let mut file_refs = String::new();
            for uid in &upstream_ids {
                if let Some(rids) = context.runtime_table.logical_to_runtime.get(uid) {
                    for &rid in rids {
                        let info = &context.runtime_table.entries[rid as usize];
                        let filename = replica_filename(info, context.iteration);
                        let path = context.output.run_dir().join(&filename);
                        if path.exists() {
                            file_refs.push_str(&format!("- {}\n", path.display()));
                        }
                    }
                }
            }
            format!(
                "{prefix}Read these upstream output files:\n{file_refs}\nRead each file before responding."
            )
        } else {
            let mut upstream_content = String::new();
            for uid in &upstream_ids {
                let upstream_block = context.def.blocks.iter().find(|b| b.id == *uid);
                if let Some(rids) = context.runtime_table.logical_to_runtime.get(uid) {
                    for &rid in rids {
                        if let Some(content) = context.block_outputs.get(&rid) {
                            if !upstream_content.is_empty() {
                                upstream_content.push_str("\n\n---\n\n");
                            }
                            if upstream_block.map(|b| b.replicas).unwrap_or(1) > 1 {
                                let info = &context.runtime_table.entries[rid as usize];
                                upstream_content.push_str(&format!(
                                    "--- Output from {} ---\n{}", info.display_label, content
                                ));
                            } else {
                                upstream_content.push_str(content);
                            }
                        }
                    }
                }
            }
            format!("{prefix}--- Upstream outputs ---\n{upstream_content}")
        }
    };

    context
        .prompt_context
        .augment_prompt_for_agent(&base_message, use_cli)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;
    use crate::execution::test_utils::{collect_progress_events, MockProvider, PanicProvider};
    use crate::output::OutputManager;
    use crate::provider::ProviderKind;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    fn block(id: BlockId, col: u16, row: u16) -> PipelineBlock {
        PipelineBlock {
            id,
            name: format!("Block#{id}"),
            agent: "Claude".into(),
            prompt: format!("block {id}"),
            session_id: None,
            position: (col, row),
            replicas: 1,
        }
    }

    fn conn(from: BlockId, to: BlockId) -> PipelineConnection {
        PipelineConnection { from, to }
    }

    fn def_with(
        blocks: Vec<PipelineBlock>,
        connections: Vec<PipelineConnection>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            initial_prompt: "test".into(),
            iterations: 1,
            blocks,
            connections,
            session_configs: Vec::new(),
        }
    }

    // -- root_blocks / terminal_blocks --

    #[test]
    fn root_blocks_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        assert_eq!(root_blocks(&d), vec![1]);
    }

    #[test]
    fn terminal_blocks_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        assert_eq!(terminal_blocks(&d), vec![3]);
    }

    #[test]
    fn root_and_terminal_isolated_blocks() {
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let roots = root_blocks(&d);
        let terms = terminal_blocks(&d);
        assert!(roots.contains(&1) && roots.contains(&2));
        assert!(terms.contains(&1) && terms.contains(&2));
    }

    #[test]
    fn root_and_terminal_diamond() {
        // 1 → 2, 1 → 3, 2 → 4, 3 → 4
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        assert_eq!(root_blocks(&d), vec![1]);
        assert_eq!(terminal_blocks(&d), vec![4]);
    }

    // -- upstream_of --

    #[test]
    fn upstream_of_returns_direct_predecessors() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 4), conn(2, 4), conn(3, 4)],
        );
        let mut ups = upstream_of(&d, 4);
        ups.sort();
        assert_eq!(ups, vec![1, 2, 3]);
    }

    // -- topological_layers --

    #[test]
    fn topo_layers_linear_chain() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1], vec![2], vec![3]]);
    }

    #[test]
    fn topo_layers_diamond() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4), conn(3, 4)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec![1]);
        assert_eq!(layers[1], vec![2, 3]);
        assert_eq!(layers[2], vec![4]);
    }

    #[test]
    fn topo_layers_fan_out() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 1, 2),
            ],
            vec![conn(1, 2), conn(1, 3), conn(1, 4)],
        );
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1], vec![2, 3, 4]]);
    }

    #[test]
    fn topo_layers_isolated_blocks() {
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        let layers = topological_layers(&d).unwrap();
        assert_eq!(layers, vec![vec![1, 2]]);
    }

    #[test]
    fn topo_layers_empty_graph() {
        let d = PipelineDefinition::default();
        let layers = topological_layers(&d).unwrap();
        assert!(layers.is_empty());
    }

    #[test]
    fn topo_layers_rejects_cycle_triangle() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3), conn(3, 1)],
        );
        assert!(topological_layers(&d).is_err());
    }

    #[test]
    fn topo_layers_rejects_self_edge() {
        let d = def_with(vec![block(1, 0, 0)], vec![conn(1, 1)]);
        assert!(topological_layers(&d).is_err());
    }

    // -- would_create_cycle --

    #[test]
    fn would_create_cycle_self_edge() {
        let d = def_with(vec![block(1, 0, 0)], vec![]);
        assert!(would_create_cycle(&d, 1, 1));
    }

    #[test]
    fn would_create_cycle_back_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 3→1 would create 1→2→3→1
        assert!(would_create_cycle(&d, 3, 1));
    }

    #[test]
    fn would_create_cycle_valid_forward_edge() {
        let d = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![conn(1, 2), conn(2, 3)],
        );
        // Adding 1→3 (skip-edge) is valid
        assert!(!would_create_cycle(&d, 1, 3));
    }

    #[test]
    fn would_create_cycle_diamond_no_cycle() {
        let d = def_with(
            vec![
                block(1, 0, 0),
                block(2, 1, 0),
                block(3, 1, 1),
                block(4, 2, 0),
            ],
            vec![conn(1, 2), conn(1, 3), conn(2, 4)],
        );
        // Adding 3→4 is valid (diamond)
        assert!(!would_create_cycle(&d, 3, 4));
    }

    // -- next_free_position --

    #[test]
    fn next_free_position_empty() {
        let d = PipelineDefinition::default();
        assert_eq!(next_free_position(&d), (0, 0));
    }

    #[test]
    fn next_free_position_fills_gaps() {
        let d = def_with(vec![block(1, 0, 0), block(2, 2, 0)], vec![]);
        // (1, 0) is the first gap
        assert_eq!(next_free_position(&d), (1, 0));
    }

    #[test]
    fn next_free_position_wraps_to_next_row() {
        // Fill entire row 0 cols 0..100? That's too many. Let's check a smaller scenario.
        let d = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![]);
        assert_eq!(next_free_position(&d), (2, 0));
    }

    // -- save/load roundtrip --

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let def = def_with(vec![block(1, 0, 0), block(2, 1, 0)], vec![conn(1, 2)]);
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.blocks.len(), 2);
        assert_eq!(loaded.connections.len(), 1);
        assert_eq!(loaded.iterations, 1);
    }

    // -- load_pipeline validation --

    #[test]
    fn load_rejects_duplicate_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[blocks]]
id = 1
agent = "Claude"
prompt = "b"
position = [1, 0]
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("Duplicate block ID"));
    }

    #[test]
    fn load_rejects_dangling_connection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dangle.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[connections]]
from = 1
to = 99
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("non-existent block"));
    }

    #[test]
    fn load_rejects_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cycle.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[blocks]]
id = 2
agent = "Claude"
prompt = "b"
position = [1, 0]

[[connections]]
from = 1
to = 2

[[connections]]
from = 2
to = 1
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn load_rejects_self_edge() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("self.toml");
        let content = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
agent = "Claude"
prompt = "a"
position = [0, 0]

[[connections]]
from = 1
to = 1
"#;
        std::fs::write(&path, content).unwrap();
        let err = load_pipeline(&path).unwrap_err();
        assert!(err.to_string().contains("Self-edge"));
    }

    #[test]
    fn default_pipeline_definition() {
        let d = PipelineDefinition::default();
        assert_eq!(d.iterations, 1);
        assert!(d.blocks.is_empty());
        assert!(d.connections.is_empty());
        assert!(d.initial_prompt.is_empty());
    }

    #[test]
    fn build_pipeline_block_message_only_adds_cli_prefix_for_cli_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-msg")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agent: "Claude".into(),
                prompt: "block prompt".into(),
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
        };
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);

        let block_outputs = HashMap::new();
        let rt = build_runtime_table(&def);
        let message_context = PipelineMessageContext {
            def: &def,
            iteration: 1,
            block_outputs: &block_outputs,
            previous_terminal_outputs: "",
            output: &output,
            prompt_context: &context,
            runtime_table: &rt,
        };
        let cli_message = build_pipeline_block_message(&def.blocks[0], true, &message_context);
        let api_message = build_pipeline_block_message(&def.blocks[0], false, &message_context);

        assert!(cli_message.contains("Working directory:"));
        assert!(cli_message.ends_with("block prompt\n\nbase prompt"));
        assert_eq!(api_message, "block prompt\n\nbase prompt");
    }

    #[tokio::test]
    async fn run_pipeline_panics_emit_block_error_and_append_error_log() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-panic")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agent: "Claude".into(),
                prompt: String::new(),
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
        };
        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, rx) = mpsc::unbounded_channel();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            |_kind, _cfg| {
                Box::new(PanicProvider::new(
                    ProviderKind::Anthropic,
                    "pipeline panic",
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProgressEvent::BlockError {
                    block_id,
                    agent_name,
                    error,
                    ..
                } if *block_id == 0 && agent_name == "Claude" && error.contains("panicked")
            )
        }));
        assert!(events
            .iter()
            .any(|event| matches!(event, ProgressEvent::AllDone)));

        let log = std::fs::read_to_string(output.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("runtime 0 Claude iter1"));
        assert!(log.contains("pipeline panic"));
    }

    #[tokio::test]
    async fn run_pipeline_provider_error_appends_error_log() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("pipeline-provider-error")).unwrap();
        let def = PipelineDefinition {
            initial_prompt: "base prompt".into(),
            iterations: 1,
            blocks: vec![PipelineBlock {
                id: 1,
                name: "Root".into(),
                agent: "Claude".into(),
                prompt: String::new(),
                session_id: None,
                position: (0, 0),
                replicas: 1,
            }],
            connections: vec![],
            session_configs: Vec::new(),
        };
        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let received = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = mpsc::unbounded_channel();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            |_kind, _cfg| {
                Box::new(MockProvider::err(
                    ProviderKind::Anthropic,
                    "provider failed",
                    received.clone(),
                ))
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ProgressEvent::BlockError {
                    block_id,
                    agent_name,
                    error,
                    ..
                } if *block_id == 0 && agent_name == "Claude" && error.contains("provider failed")
            )
        }));

        let log = std::fs::read_to_string(output.run_dir().join("_errors.log")).expect("log");
        assert!(log.contains("runtime 0 Claude iter1"));
        assert!(log.contains("provider failed"));
    }

    #[tokio::test]
    async fn pipeline_concurrency_limit_enforced() {
        use crate::provider::{CompletionResponse, Provider, SendFuture};
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ConcurrencyTracker {
            kind: ProviderKind,
            active: Arc<AtomicUsize>,
            peak: Arc<AtomicUsize>,
        }

        impl Provider for ConcurrencyTracker {
            fn kind(&self) -> ProviderKind {
                self.kind
            }

            fn clear_history(&mut self) {}

            fn send(&mut self, _message: &str) -> SendFuture<'_> {
                let active = self.active.clone();
                let peak = self.peak.clone();
                Box::pin(async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(CompletionResponse {
                        content: "ok".to_string(),
                        debug_logs: Vec::new(),
                    })
                })
            }
        }

        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![], // All independent roots
        );

        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let agent_configs = {
            let mut m: PipelineAgentConfigs = HashMap::new();
            m.insert(
                "Claude".to_string(),
                (
                    ProviderKind::Anthropic,
                    ProviderConfig {
                        api_key: "k".to_string(),
                        model: "m".to_string(),
                        reasoning_effort: None,
                        thinking_effort: None,
                        use_cli: false,
                        cli_print_mode: true,
                        extra_cli_args: String::new(),
                    },
                    false,
                ),
            );
            m
        };

        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let dir = tempfile::tempdir().expect("tempdir");
        let output = OutputManager::new(dir.path(), Some("pipeline-concurrency")).unwrap();
        let (tx, rx) = mpsc::unbounded_channel();

        let active_clone = active.clone();
        let peak_clone = peak.clone();

        run_pipeline_with_provider_factory(
            &def,
            1, // max_block_concurrency = 1
            agent_configs,
            &context,
            &output,
            tx,
            Arc::new(AtomicBool::new(false)),
            move |_kind, _cfg| -> Box<dyn Provider> {
                Box::new(ConcurrencyTracker {
                    kind: ProviderKind::Anthropic,
                    active: active_clone.clone(),
                    peak: peak_clone.clone(),
                })
            },
        )
        .await
        .expect("run");

        let events = collect_progress_events(rx);
        let finished_count = events
            .iter()
            .filter(|e| matches!(e, ProgressEvent::BlockFinished { .. }))
            .count();
        assert_eq!(finished_count, 3);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    // -- effective_session_key --

    #[test]
    fn effective_session_key_returns_explicit_session_id() {
        let b = PipelineBlock {
            id: 1,
            name: "B".into(),
            agent: "Claude".into(),
            prompt: String::new(),
            session_id: Some("shared".into()),
            position: (0, 0),
            replicas: 1,
        };
        assert_eq!(b.effective_session_key(), "shared");
    }

    #[test]
    fn effective_session_key_falls_back_to_block_id() {
        let b = block(5, 0, 0);
        assert_eq!(b.effective_session_key(), "__block_5");
    }

    // -- effective_sessions --

    #[test]
    fn effective_sessions_groups_shared_sessions() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![conn(1, 2)],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_key, "shared");
        assert!(sessions[0].block_ids.contains(&1));
        assert!(sessions[0].block_ids.contains(&2));
    }

    #[test]
    fn effective_sessions_separates_different_agents_same_session() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agent: "GPT".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn effective_sessions_isolated_blocks_get_separate_rows() {
        let def = def_with(
            vec![block(1, 0, 0), block(2, 1, 0), block(3, 2, 0)],
            vec![],
        );
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 3);
    }

    #[test]
    fn effective_sessions_sorted_by_agent_then_key() {
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "Z".into(),
                    agent: "GPT".into(),
                    prompt: String::new(),
                    session_id: None,
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "A".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: None,
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        def.session_configs = Vec::new();
        let sessions = def.effective_sessions();
        assert_eq!(sessions[0].agent, "Claude");
        assert_eq!(sessions[1].agent, "GPT");
    }

    #[test]
    fn effective_sessions_disambiguates_identical_labels() {
        let def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "Worker".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: None,
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "Worker".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: None,
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![],
        );
        let sessions = def.effective_sessions();
        assert_eq!(sessions.len(), 2);
        // Both have same agent+name, so labels must be disambiguated with block IDs
        assert_ne!(sessions[0].display_label, sessions[1].display_label);
        assert!(sessions[0].display_label.contains("Worker"));
        assert!(sessions[1].display_label.contains("Worker"));
        // Each label is prefixed with #id so truncation never hides the distinguishing part
        assert!(
            sessions[0].display_label.starts_with('#'),
            "label should be prefixed with block ID: {}",
            sessions[0].display_label
        );
    }

    // -- keep_session_across_iterations --

    #[test]
    fn keep_session_defaults_to_true() {
        let def = def_with(vec![block(1, 0, 0)], vec![]);
        assert!(def.keep_session_across_iterations("Claude", "__block_1"));
    }

    #[test]
    fn set_keep_false_adds_explicit_entry() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        assert!(!def.keep_session_across_iterations("Claude", "__block_1"));
        assert_eq!(def.session_configs.len(), 1);
    }

    #[test]
    fn toggle_back_to_true_removes_explicit_entry() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        assert_eq!(def.session_configs.len(), 1);
        def.set_keep_session_across_iterations("Claude", "__block_1", true);
        assert!(def.session_configs.is_empty());
        assert!(def.keep_session_across_iterations("Claude", "__block_1"));
    }

    // -- normalize_session_configs --

    #[test]
    fn normalize_drops_stale_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "nonexistent".into(),
            keep_across_iterations: false,
        });
        def.normalize_session_configs();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn normalize_drops_true_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "__block_1".into(),
            keep_across_iterations: true,
        });
        def.normalize_session_configs();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn normalize_keeps_valid_false_rows() {
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.session_configs.push(SessionConfig {
            agent: "Claude".into(),
            session_key: "__block_1".into(),
            keep_across_iterations: false,
        });
        def.normalize_session_configs();
        assert_eq!(def.session_configs.len(), 1);
    }

    // -- serde --

    #[test]
    fn old_toml_without_session_configs_loads_empty() {
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]
"#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert!(def.session_configs.is_empty());
    }

    #[test]
    fn session_config_missing_keep_defaults_to_true() {
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
"#;
        let def: PipelineDefinition = toml::from_str(toml_str).unwrap();
        assert_eq!(def.session_configs.len(), 1);
        assert!(def.session_configs[0].keep_across_iterations);
    }

    #[test]
    fn save_load_roundtrip_preserves_false_session_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_session.toml");
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.set_keep_session_across_iterations("Claude", "__block_1", false);
        save_pipeline(&def, &path).unwrap();
        let loaded = load_pipeline(&path).unwrap();
        assert!(!loaded.keep_session_across_iterations("Claude", "__block_1"));
    }

    #[test]
    fn load_deduplicates_session_configs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.toml");
        let toml_str = r#"
initial_prompt = "test"
iterations = 1

[[blocks]]
id = 1
name = "B"
agent = "Claude"
prompt = ""
position = [0, 0]

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_iterations = false

[[session_configs]]
agent = "Claude"
session_key = "__block_1"
keep_across_iterations = false
"#;
        std::fs::write(&path, toml_str).unwrap();
        // Normalization deduplicates before validation
        let loaded = load_pipeline(&path).unwrap();
        assert_eq!(loaded.session_configs.len(), 1);
        assert!(!loaded.keep_session_across_iterations("Claude", "__block_1"));
    }

    // -- execution: clear_history --

    struct ClearCountProvider {
        kind: ProviderKind,
        responses: std::sync::Mutex<
            VecDeque<Result<crate::provider::CompletionResponse, AppError>>,
        >,
        clear_count: Arc<AtomicUsize>,
    }

    impl crate::provider::Provider for ClearCountProvider {
        fn kind(&self) -> ProviderKind {
            self.kind
        }

        fn clear_history(&mut self) {
            self.clear_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        fn send(&mut self, _message: &str) -> crate::provider::SendFuture<'_> {
            Box::pin(async {
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| {
                        Ok(crate::provider::CompletionResponse {
                            content: "response".to_string(),
                            debug_logs: Vec::new(),
                        })
                    })
            })
        }
    }

    #[tokio::test]
    async fn iteration_clears_non_keep_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("clear-test")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 2;
        def.set_keep_session_across_iterations("Claude", "__block_1", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // Should have cleared once before iteration 2
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn single_iteration_never_clears() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("no-clear")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 1;
        def.set_keep_session_across_iterations("Claude", "__block_1", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn all_keep_sessions_never_clear() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("keep-all")).unwrap();
        let mut def = def_with(vec![block(1, 0, 0)], vec![]);
        def.iterations = 2;
        // Default is keep=true, so no explicit config needed

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn shared_session_clears_once_per_provider() {
        let dir = tempfile::tempdir().unwrap();
        let output = OutputManager::new(dir.path(), Some("shared-clear")).unwrap();
        let mut def = def_with(
            vec![
                PipelineBlock {
                    id: 1,
                    name: "A".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (0, 0),
                    replicas: 1,
                },
                PipelineBlock {
                    id: 2,
                    name: "B".into(),
                    agent: "Claude".into(),
                    prompt: String::new(),
                    session_id: Some("shared".into()),
                    position: (1, 0),
                    replicas: 1,
                },
            ],
            vec![conn(1, 2)],
        );
        def.iterations = 2;
        def.set_keep_session_across_iterations("Claude", "shared", false);

        let agent_configs = HashMap::from([(
            "Claude".to_string(),
            (
                ProviderKind::Anthropic,
                ProviderConfig {
                    api_key: String::new(),
                    model: "test".to_string(),
                    reasoning_effort: None,
                    thinking_effort: None,
                    use_cli: false,
                    cli_print_mode: true,
                    extra_cli_args: String::new(),
                },
                false,
            ),
        )]);
        let context = PromptRuntimeContext::new(def.initial_prompt.clone(), false);
        let (tx, _rx) = mpsc::unbounded_channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let clear_count = Arc::new(AtomicUsize::new(0));
        let cc = clear_count.clone();

        run_pipeline_with_provider_factory(
            &def,
            0,
            agent_configs,
            &context,
            &output,
            tx,
            cancel,
            move |_kind, _cfg| {
                Box::new(ClearCountProvider {
                    kind: ProviderKind::Anthropic,
                    responses: std::sync::Mutex::new(VecDeque::new()),
                    clear_count: cc.clone(),
                })
            },
        )
        .await
        .unwrap();

        // Two blocks share one provider, cleared once before iteration 2
        assert_eq!(clear_count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
