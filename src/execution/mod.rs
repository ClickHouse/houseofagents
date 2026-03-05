pub mod relay;
pub mod solo;
pub mod swarm;

use crate::provider::ProviderKind;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Relay,
    Swarm,
    Solo,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "relay",
            ExecutionMode::Swarm => "swarm",
            ExecutionMode::Solo => "solo",
        }
    }

    pub fn description(&self) -> &'static str {
        match self {
            ExecutionMode::Relay => "Sequential cooperative - agents build on each other",
            ExecutionMode::Swarm => "Parallel cooperative - agents share results between rounds",
            ExecutionMode::Solo => "Independent parallel - each agent works alone",
        }
    }

    pub fn all() -> &'static [ExecutionMode] {
        &[ExecutionMode::Relay, ExecutionMode::Swarm, ExecutionMode::Solo]
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            ExecutionMode::Relay => "Relay",
            ExecutionMode::Swarm => "Swarm",
            ExecutionMode::Solo => "Solo",
        })
    }
}

#[derive(Debug, Clone)]
pub enum ProgressEvent {
    AgentStarted {
        kind: ProviderKind,
        iteration: u32,
    },
    AgentFinished {
        kind: ProviderKind,
        iteration: u32,
    },
    AgentError {
        kind: ProviderKind,
        iteration: u32,
        error: String,
    },
    IterationComplete {
        iteration: u32,
    },
    AllDone,
}

