use super::store::MemoryStore;
use super::types::{Memory, RecalledSet};
use crate::execution::pipeline::PipelineDefinition;
use std::collections::HashSet;
use std::sync::LazyLock;

static STOP: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    [
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may", "might", "shall", "can",
        "this", "that", "these", "those", "it", "its", "i", "me", "my", "we", "our", "you", "your",
        "he", "she", "they", "them", "his", "her", "and", "or", "but", "not", "no", "so", "if",
        "then", "than", "too", "very", "just", "about", "up", "out", "on", "off", "in", "of", "to",
        "for", "with", "at", "by", "from", "as", "into", "all", "each", "any", "some", "such",
        "what", "which", "who", "when", "where", "how", "why", "more", "most", "other", "over",
    ]
    .into_iter()
    .collect()
});

pub fn extract_keywords(prompt: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut words: Vec<String> = prompt
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 2 && !STOP.contains(w))
        .filter(|w| seen.insert(w.to_string()))
        .map(|w| w.to_string())
        .collect();
    // Sort by length descending (longer words are more specific)
    words.sort_by_key(|w| std::cmp::Reverse(w.len()));
    words.truncate(20);
    words
}

/// Extract recall keywords from a pipeline definition by merging keywords
/// from the initial prompt with keywords from all block/finalization prompts.
///
/// Uses seek-based round-robin merge: each section (initial_prompt + each block
/// prompt) produces a keyword list sorted by length descending. A cursor per
/// section advances round-robin, emitting one keyword per section per round,
/// skipping duplicates. A final bonus pass backfills any remaining short terms
/// (≤6 chars) from sections that still have unused keywords, also round-robin.
///
/// The result is truncated to 20 terms total (same as extract_keywords).
pub(crate) fn extract_pipeline_keywords(pipeline_def: &PipelineDefinition) -> Vec<String> {
    // Collect per-section keyword lists (each already sorted longest-first, deduped)
    let mut sections: Vec<Vec<String>> = Vec::new();

    // Section 0: initial_prompt
    let ip_kws = extract_keywords(&pipeline_def.initial_prompt);
    if !ip_kws.is_empty() {
        sections.push(ip_kws);
    }

    // Execution blocks
    for block in &pipeline_def.blocks {
        if !block.prompt.is_empty() {
            let kws = extract_keywords(&block.prompt);
            if !kws.is_empty() {
                sections.push(kws);
            }
        }
        // Recurse into sub-pipeline blocks
        if let Some(ref sub) = block.sub_pipeline {
            if !sub.initial_prompt.is_empty() {
                let kws = extract_keywords(&sub.initial_prompt);
                if !kws.is_empty() {
                    sections.push(kws);
                }
            }
            for sb in &sub.blocks {
                if !sb.prompt.is_empty() {
                    let kws = extract_keywords(&sb.prompt);
                    if !kws.is_empty() {
                        sections.push(kws);
                    }
                }
            }
            for fb in &sub.finalization_blocks {
                if !fb.prompt.is_empty() {
                    let kws = extract_keywords(&fb.prompt);
                    if !kws.is_empty() {
                        sections.push(kws);
                    }
                }
            }
            // Sub-pipeline loop connections (same structure as top-level)
            for lc in &sub.loop_connections {
                if !lc.prompt.is_empty() {
                    let kws = extract_keywords(&lc.prompt);
                    if !kws.is_empty() {
                        sections.push(kws);
                    }
                }
                if !lc.break_condition.is_empty() {
                    let kws = extract_keywords(&lc.break_condition);
                    if !kws.is_empty() {
                        sections.push(kws);
                    }
                }
            }
        }
    }

    // Finalization blocks
    for block in &pipeline_def.finalization_blocks {
        if !block.prompt.is_empty() {
            let kws = extract_keywords(&block.prompt);
            if !kws.is_empty() {
                sections.push(kws);
            }
        }
    }

    // Loop connection prompts and break conditions
    for lc in &pipeline_def.loop_connections {
        if !lc.prompt.is_empty() {
            let kws = extract_keywords(&lc.prompt);
            if !kws.is_empty() {
                sections.push(kws);
            }
        }
        if !lc.break_condition.is_empty() {
            let kws = extract_keywords(&lc.break_condition);
            if !kws.is_empty() {
                sections.push(kws);
            }
        }
    }

    if sections.is_empty() {
        return Vec::new();
    }

    const MAX_TERMS: usize = 20;
    let mut result: Vec<String> = Vec::with_capacity(MAX_TERMS);
    let mut seen = HashSet::new();

    // Seek-based cursors: one per section
    let mut cursors: Vec<usize> = vec![0; sections.len()];

    // Main round-robin pass
    loop {
        let mut any_advanced = false;
        for (i, cursor) in cursors.iter_mut().enumerate() {
            if result.len() >= MAX_TERMS {
                break;
            }
            // Seek forward to the next unseen keyword
            while *cursor < sections[i].len() {
                let kw = &sections[i][*cursor];
                *cursor += 1;
                if seen.insert(kw.clone()) {
                    result.push(kw.clone());
                    any_advanced = true;
                    break;
                }
            }
        }
        if result.len() >= MAX_TERMS || !any_advanced {
            break;
        }
    }

    // Bonus pass: backfill short terms (≤6 chars) round-robin
    // This ensures short but specific terms from later sections aren't starved
    if result.len() < MAX_TERMS {
        loop {
            let mut any_advanced = false;
            for (i, cursor) in cursors.iter_mut().enumerate() {
                if result.len() >= MAX_TERMS {
                    break;
                }
                while *cursor < sections[i].len() {
                    let kw = &sections[i][*cursor];
                    *cursor += 1;
                    if kw.len() <= 6 && seen.insert(kw.clone()) {
                        result.push(kw.clone());
                        any_advanced = true;
                        break;
                    }
                }
            }
            if result.len() >= MAX_TERMS || !any_advanced {
                break;
            }
        }
    }

    result
}

/// Compute recall terms for a given execution mode.
/// Reused by prompt-time recall and extraction-time recall to keep both
/// paths aligned.
pub(crate) fn compute_recall_terms(
    mode: crate::execution::ExecutionMode,
    prompt_text: &str,
    pipeline_def: Option<&PipelineDefinition>,
) -> Vec<String> {
    if mode == crate::execution::ExecutionMode::Pipeline {
        if let Some(def) = pipeline_def {
            let terms = extract_pipeline_keywords(def);
            if !terms.is_empty() {
                return terms;
            }
        }
    }
    extract_keywords(prompt_text)
}

pub fn recall_for_prompt(
    store: &MemoryStore,
    project_id: &str,
    raw_prompt: &str,
    max: usize,
    max_bytes: usize,
    max_summary: usize,
    recall_terms: Option<&[String]>,
) -> Result<RecalledSet, String> {
    let own_terms;
    let terms = match recall_terms {
        Some(t) if !t.is_empty() => t,
        _ => {
            own_terms = extract_keywords(raw_prompt);
            &own_terms
        }
    };
    if terms.is_empty() {
        return Ok(RecalledSet {
            memories: vec![],
            total_bytes: 0,
        });
    }
    store.recall(project_id, terms, max, max_bytes, max_summary)
}

pub fn format_memory_context(recalled: &RecalledSet) -> String {
    if recalled.memories.is_empty() {
        return String::new();
    }
    let mut out = String::from("<project_memory>\n");
    out.push_str("The following memories were recalled from previous runs in this project:\n\n");
    for mem in &recalled.memories {
        format_memory_entry(&mut out, mem);
    }
    out.push_str("</project_memory>");
    out
}

/// Count memory entries from a previously formatted context string.
/// Colocated with `format_memory_entry` so the two stay in sync.
pub fn count_entries_in_context(context: &str) -> usize {
    context.matches("\n[").count()
}

/// Escape characters that could break out of XML wrapper tags.
fn escape_xml(s: &str) -> std::borrow::Cow<'_, str> {
    // Fast path: skip allocation if no special chars
    if !s.contains('&') && !s.contains('<') && !s.contains('>') {
        return std::borrow::Cow::Borrowed(s);
    }
    std::borrow::Cow::Owned(
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;"),
    )
}

fn format_memory_entry(out: &mut String, mem: &Memory) {
    out.push_str(&format!(
        "[{}] {}\n",
        mem.kind.as_str().to_uppercase(),
        escape_xml(&mem.content)
    ));
    if !mem.reasoning.is_empty() {
        out.push_str(&format!("  Reasoning: {}\n", escape_xml(&mem.reasoning)));
    }
    if mem.evidence_count > 1 {
        out.push_str(&format!("  (Reinforced {} times)\n", mem.evidence_count));
    }
    out.push('\n');
}

/// Format a memory entry as plain text (no XML escaping).
/// Used by extraction prompts where content fidelity matters more
/// than XML safety.
pub(crate) fn format_memory_entry_plain(out: &mut String, mem: &Memory) {
    out.push_str(&format!(
        "[{}] {}\n",
        mem.kind.as_str().to_uppercase(),
        &mem.content
    ));
    if !mem.reasoning.is_empty() {
        out.push_str(&format!("  Reasoning: {}\n", &mem.reasoning));
    }
    if mem.evidence_count > 1 {
        out.push_str(&format!("  (Reinforced {} times)\n", mem.evidence_count));
    }
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::types::MemoryKind;

    #[test]
    fn extract_keywords_filters_stop_words() {
        let kw = extract_keywords("the quick brown fox jumps over a lazy dog");
        assert!(!kw.contains(&"the".to_string()));
        assert!(!kw.contains(&"over".to_string()));
        assert!(kw.contains(&"quick".to_string()));
        assert!(kw.contains(&"brown".to_string()));
        assert!(kw.contains(&"jumps".to_string()));
    }

    #[test]
    fn extract_keywords_dedup() {
        let kw = extract_keywords("rust rust rust python python");
        assert_eq!(kw.iter().filter(|w| *w == "rust").count(), 1);
    }

    #[test]
    fn extract_keywords_length_ordering() {
        let kw = extract_keywords("go python rust typescript");
        // "typescript" should come before shorter words
        assert_eq!(kw[0], "typescript");
    }

    #[test]
    fn extract_keywords_short_words_filtered() {
        let kw = extract_keywords("go is ok");
        assert!(kw.is_empty());
    }

    #[test]
    fn format_memory_context_empty() {
        let recalled = RecalledSet {
            memories: vec![],
            total_bytes: 0,
        };
        assert!(format_memory_context(&recalled).is_empty());
    }

    #[test]
    fn format_memory_context_with_entries() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Decision,
                content: "Use X".into(),
                reasoning: "Because Y".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 1,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 10,
        };
        let ctx = format_memory_context(&recalled);
        assert!(ctx.contains("<project_memory>"));
        assert!(ctx.contains("[DECISION] Use X"));
        assert!(ctx.contains("Reasoning: Because Y"));
        assert!(ctx.contains("</project_memory>"));
    }

    #[test]
    fn format_memory_context_reinforced() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Principle,
                content: "Validate input".into(),
                reasoning: "Security".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 3,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 20,
        };
        let ctx = format_memory_context(&recalled);
        assert!(ctx.contains("Reinforced 3 times"));
    }

    #[test]
    fn count_entries_matches_format_output() {
        let recalled = RecalledSet {
            memories: vec![
                Memory {
                    id: 1,
                    project_id: "p".into(),
                    kind: MemoryKind::Decision,
                    content: "Use X".into(),
                    reasoning: "Because Y".into(),
                    source_run: "r".into(),
                    source_agent: "a".into(),
                    evidence_count: 1,
                    tags: String::new(),
                    created_at: String::new(),
                    expires_at: None,
                    updated_at: String::new(),
                    recall_count: 0,
                    last_recalled_at: None,
                    archived: false,
                },
                Memory {
                    id: 2,
                    project_id: "p".into(),
                    kind: MemoryKind::Principle,
                    content: "Always validate".into(),
                    reasoning: "Security".into(),
                    source_run: "r".into(),
                    source_agent: "a".into(),
                    evidence_count: 3,
                    tags: String::new(),
                    created_at: String::new(),
                    expires_at: None,
                    updated_at: String::new(),
                    recall_count: 0,
                    last_recalled_at: None,
                    archived: false,
                },
            ],
            total_bytes: 30,
        };
        let ctx = format_memory_context(&recalled);
        assert_eq!(count_entries_in_context(&ctx), 2);
    }

    #[test]
    fn count_entries_empty_context() {
        assert_eq!(count_entries_in_context(""), 0);
        assert_eq!(count_entries_in_context("no entries here"), 0);
    }

    #[test]
    fn format_memory_context_escapes_xml() {
        let recalled = RecalledSet {
            memories: vec![Memory {
                id: 1,
                project_id: "p".into(),
                kind: MemoryKind::Decision,
                content: "Use </project_memory> injection & <script>".into(),
                reasoning: "Because <evil>".into(),
                source_run: "r".into(),
                source_agent: "a".into(),
                evidence_count: 1,
                tags: String::new(),
                created_at: String::new(),
                expires_at: None,
                updated_at: String::new(),
                recall_count: 0,
                last_recalled_at: None,
                archived: false,
            }],
            total_bytes: 10,
        };
        let ctx = format_memory_context(&recalled);
        // Content and reasoning must be escaped
        assert!(ctx.contains("&lt;/project_memory&gt;"));
        assert!(ctx.contains("injection &amp; &lt;script&gt;"));
        assert!(ctx.contains("Reasoning: Because &lt;evil&gt;"));
        // Wrapper tags must remain intact
        assert!(ctx.starts_with("<project_memory>"));
        assert!(ctx.ends_with("</project_memory>"));
    }

    #[test]
    fn escape_xml_no_alloc_fast_path() {
        // Plain text should return identical string (no special chars)
        let plain = "hello world 123";
        assert_eq!(escape_xml(plain), plain);
    }

    #[test]
    fn recall_for_prompt_end_to_end() {
        use crate::config::MemoryConfig;
        use crate::memory::types::ExtractedMemory;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();

        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for persistence layer".into(),
                    reasoning: "ACID needed".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        let result = recall_for_prompt(
            &store,
            "proj1",
            "What database should we use for PostgreSQL migration?",
            15,
            8192,
            0,
            None,
        )
        .unwrap();
        assert!(!result.memories.is_empty());
    }

    fn make_block(id: u32, name: &str, prompt: &str) -> crate::execution::pipeline::PipelineBlock {
        crate::execution::pipeline::PipelineBlock {
            raw: false,
            fresh: false,
            model: None,
            effort: None,
            id,
            name: name.into(),
            agents: vec!["c".into()],
            prompt: prompt.into(),
            position: (0, 0),
            profiles: vec![],
            session_id: None,
            replicas: 1,
            command: None,
            schema: None,
            sub_pipeline: None,
        }
    }

    fn make_empty_block(
        id: u32,
        name: &str,
        sub: Option<PipelineDefinition>,
    ) -> crate::execution::pipeline::PipelineBlock {
        crate::execution::pipeline::PipelineBlock {
            raw: false,
            fresh: false,
            model: None,
            effort: None,
            id,
            name: name.into(),
            agents: vec![],
            prompt: String::new(),
            position: (0, 0),
            profiles: vec![],
            session_id: None,
            replicas: 1,
            command: None,
            schema: None,
            sub_pipeline: sub,
        }
    }

    #[test]
    fn extract_pipeline_keywords_round_robin() {
        let def = PipelineDefinition {
            initial_prompt: "Analyze database performance optimization".into(),
            blocks: vec![make_block(
                1,
                "Worker",
                "Review caching strategy and latency metrics",
            )],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        // Should contain terms from BOTH sections, not just initial_prompt
        assert!(terms.contains(&"optimization".to_string()));
        assert!(terms.contains(&"caching".to_string()));
        // Both should appear in top terms
        let opt_idx = terms.iter().position(|t| t == "optimization").unwrap();
        let cache_idx = terms.iter().position(|t| t == "caching").unwrap();
        assert!(opt_idx < 20);
        assert!(cache_idx < 20);
    }

    #[test]
    fn extract_pipeline_keywords_shared_leaders_uses_seek() {
        // Both sections share "parallelization" (17 chars) as longest term
        let def = PipelineDefinition {
            initial_prompt: "parallelization strategy for workers".into(),
            blocks: vec![make_block(1, "W", "parallelization throughput measurement")],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        // "parallelization" appears once (deduped)
        assert_eq!(terms.iter().filter(|t| *t == "parallelization").count(), 1);
        // Section B's cursor seeks past the duplicate to "throughput"
        assert!(terms.contains(&"throughput".to_string()));
        // Section A contributes "strategy"
        assert!(terms.contains(&"strategy".to_string()));
    }

    #[test]
    fn extract_pipeline_keywords_bonus_pass_is_fair() {
        // Section A: 1 long term + many short terms
        let def = PipelineDefinition {
            initial_prompt: "infrastructure abc def ghi jkl mno pqr stu vwx".into(),
            blocks: vec![make_block(1, "W", "optimization xyz uvw")],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        // Section B's short terms should not be starved by Section A's many short terms
        let b_shorts: Vec<&str> = vec!["xyz", "uvw"];
        let has_b_short = terms.iter().any(|t| b_shorts.contains(&t.as_str()));
        assert!(
            has_b_short,
            "Section B's short terms should appear in bonus pass"
        );
    }

    #[test]
    fn enriched_recall_finds_block_prompt_memories() {
        use crate::config::MemoryConfig;
        use crate::memory::types::ExtractedMemory;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();
        // Insert a memory about "caching" — term NOT in initial_prompt
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use Redis for caching layer".into(),
                    reasoning: "Low latency needed".into(),
                    tags: vec!["caching".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        // Pipeline: initial_prompt has zero overlap with "caching"
        let def = PipelineDefinition {
            initial_prompt: "Analyze production infrastructure".into(),
            blocks: vec![make_block(1, "W", "Review caching strategy")],
            ..Default::default()
        };

        let terms = extract_pipeline_keywords(&def);
        let result = recall_for_prompt(
            &store,
            "proj1",
            &def.initial_prompt,
            10,
            8192,
            0,
            Some(&terms),
        )
        .unwrap();
        // Should find the caching memory because block prompt contributed "caching"
        assert!(!result.memories.is_empty());
        assert!(result.memories[0].content.contains("caching"));
    }

    #[test]
    fn extract_pipeline_keywords_includes_sub_pipelines() {
        let sub = PipelineDefinition {
            initial_prompt: "Validate kubernetes manifests".into(),
            blocks: vec![make_block(100, "Inner", "Check helm chart configuration")],
            ..Default::default()
        };
        let def = PipelineDefinition {
            initial_prompt: "Orchestrate deployment".into(),
            blocks: vec![make_empty_block(1, "Sub", Some(sub))],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        assert!(terms.contains(&"kubernetes".to_string()));
        assert!(terms.contains(&"helm".to_string()));
        assert!(terms.contains(&"configuration".to_string()));
    }

    #[test]
    fn extract_pipeline_keywords_includes_loop_prompts() {
        use crate::execution::pipeline::{LoopConnection, PipelineConnection};

        let def = PipelineDefinition {
            initial_prompt: "Review code quality".into(),
            blocks: vec![
                make_block(1, "A", "Analyze coverage"),
                make_block(2, "B", "Check results"),
            ],
            connections: vec![PipelineConnection::new(1, 2)],
            loop_connections: vec![LoopConnection {
                from: 2,
                to: 1,
                count: 3,
                prompt: "Refine mutation testing analysis".into(),
                break_condition: "convergence achieved".into(),
                break_agent: String::new(),
                break_command: String::new(),
            }],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        assert!(terms.contains(&"mutation".to_string()));
        assert!(terms.contains(&"convergence".to_string()));
    }

    #[test]
    fn extract_pipeline_keywords_includes_sub_pipeline_loop_prompts() {
        use crate::execution::pipeline::LoopConnection;

        let sub = PipelineDefinition {
            initial_prompt: "Inner task description".into(),
            blocks: vec![
                make_block(10, "Planner", "Create implementation plan"),
                make_block(11, "Critic", "Review proposed changes"),
            ],
            connections: vec![crate::execution::pipeline::PipelineConnection::new(10, 11)],
            loop_connections: vec![LoopConnection {
                from: 11,
                to: 10,
                count: 5,
                prompt: "Refine serialization strategy".into(),
                break_condition: "stabilization reached".into(),
                break_agent: String::new(),
                break_command: String::new(),
            }],
            ..Default::default()
        };
        let def = PipelineDefinition {
            initial_prompt: "Orchestrate planning".into(),
            blocks: vec![make_empty_block(1, "Sub", Some(sub))],
            ..Default::default()
        };
        let terms = extract_pipeline_keywords(&def);
        // Keywords from sub-pipeline loop prompt should appear
        assert!(terms.contains(&"serialization".to_string()));
        // Keywords from sub-pipeline loop break_condition should appear
        assert!(terms.contains(&"stabilization".to_string()));
        // Keywords from sub-pipeline block prompts should also appear
        assert!(terms.contains(&"implementation".to_string()));
    }

    #[test]
    fn extract_pipeline_keywords_empty_pipeline() {
        let def = PipelineDefinition::default();
        let terms = extract_pipeline_keywords(&def);
        assert!(terms.is_empty());
    }

    #[test]
    fn recall_for_prompt_empty_terms_falls_back_to_prompt() {
        use crate::config::MemoryConfig;
        use crate::memory::types::ExtractedMemory;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use Redis for caching layer".into(),
                    reasoning: "Low latency needed".into(),
                    tags: vec!["caching".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        // Empty slice should fall back to prompt-based extraction
        let empty: Vec<String> = vec![];
        let result = recall_for_prompt(
            &store,
            "proj1",
            "What caching layer should we use?",
            10,
            8192,
            0,
            Some(&empty),
        )
        .unwrap();
        assert!(!result.memories.is_empty());
        assert!(result.memories[0].content.contains("caching"));
    }

    #[test]
    fn recall_for_prompt_with_explicit_terms() {
        use crate::config::MemoryConfig;
        use crate::memory::types::ExtractedMemory;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use Redis for caching layer".into(),
                    reasoning: "Low latency needed".into(),
                    tags: vec!["caching".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        // When recall_terms is provided, it should use those instead of extracting from prompt
        let terms = vec!["caching".to_string()];
        let result = recall_for_prompt(
            &store,
            "proj1",
            "irrelevant prompt text",
            10,
            8192,
            0,
            Some(&terms),
        )
        .unwrap();
        assert!(!result.memories.is_empty());
        assert!(result.memories[0].content.contains("caching"));
    }

    #[test]
    fn format_memory_entry_plain_preserves_special_chars() {
        let mem = Memory {
            id: 1,
            project_id: "p".into(),
            kind: MemoryKind::Decision,
            content: "Use Vec<String> & HashMap<K, V>".into(),
            reasoning: "Because <generics> are useful".into(),
            source_run: "r".into(),
            source_agent: "a".into(),
            evidence_count: 2,
            tags: String::new(),
            created_at: String::new(),
            expires_at: None,
            updated_at: String::new(),
            recall_count: 0,
            last_recalled_at: None,
            archived: false,
        };
        let mut out = String::new();
        format_memory_entry_plain(&mut out, &mem);
        // Raw characters preserved — no XML escaping
        assert!(out.contains("Vec<String>"));
        assert!(out.contains("& HashMap<K, V>"));
        assert!(out.contains("<generics>"));
        assert!(out.contains("Reinforced 2 times"));
        // Negative: no XML entities
        assert!(!out.contains("&lt;"));
        assert!(!out.contains("&amp;"));
    }

    #[test]
    fn compute_recall_terms_relay_uses_prompt() {
        let terms = compute_recall_terms(
            crate::execution::ExecutionMode::Relay,
            "Review database performance optimization",
            None,
        );
        assert!(terms.contains(&"optimization".to_string()));
        assert!(terms.contains(&"performance".to_string()));
        assert!(terms.contains(&"database".to_string()));
    }

    #[test]
    fn compute_recall_terms_swarm_uses_prompt() {
        let terms = compute_recall_terms(
            crate::execution::ExecutionMode::Swarm,
            "Analyze caching infrastructure throughput",
            None,
        );
        assert!(terms.contains(&"infrastructure".to_string()));
        assert!(terms.contains(&"caching".to_string()));
    }

    #[test]
    fn compute_recall_terms_pipeline_with_def_uses_enriched() {
        let def = PipelineDefinition {
            initial_prompt: "Analyze production infrastructure".into(),
            blocks: vec![make_block(1, "W", "Review caching strategy")],
            ..Default::default()
        };
        let terms = compute_recall_terms(
            crate::execution::ExecutionMode::Pipeline,
            &def.initial_prompt,
            Some(&def),
        );
        // Should have terms from block prompts (enriched)
        assert!(terms.contains(&"caching".to_string()));
        assert!(terms.contains(&"infrastructure".to_string()));
    }

    #[test]
    fn compute_recall_terms_pipeline_no_def_falls_back() {
        let terms = compute_recall_terms(
            crate::execution::ExecutionMode::Pipeline,
            "Review database performance optimization",
            None,
        );
        // Falls back to prompt-based extraction
        assert!(terms.contains(&"optimization".to_string()));
        assert!(terms.contains(&"performance".to_string()));
    }
}
