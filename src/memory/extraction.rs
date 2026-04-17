use super::types::ExtractedMemory;
use crate::post_run::{PostRunPromptBudget, EXTRACTION_MAX_INPUT_BYTES};

/// Query the memory store for existing memories relevant to the current run.
/// Used by both TUI and headless extraction paths to provide dedup context
/// to the extraction LLM. Returns an empty Vec on any failure.
pub(crate) fn recall_for_extraction(
    store: &crate::memory::store::MemoryStore,
    project_id: &str,
    terms: &[String],
    max_recall: usize,
    max_summary_recall: usize,
) -> Vec<crate::memory::types::Memory> {
    use crate::post_run::EXTRACTION_MEMORY_MAX_BYTES;

    if terms.is_empty() {
        return vec![];
    }
    match store.recall(
        project_id,
        terms,
        max_recall,
        EXTRACTION_MEMORY_MAX_BYTES,
        max_summary_recall,
    ) {
        Ok(recalled) => recalled.memories,
        Err(_) => vec![],
    }
}

pub(crate) fn build_extraction_prompt(
    files: &[(String, std::path::PathBuf)],
    observation_ttl_days: u32,
    summary_ttl_days: u32,
    existing_memories: &[crate::memory::types::Memory],
) -> Result<(String, usize), String> {
    // Part 1: Instruction header
    let mut prompt = format!(
        "Extract reusable memories from these agent outputs. Return a JSON array.\n\n\
         Each object: {{\"kind\": \"decision|observation|summary|principle\", \
         \"content\": \"...\", \"reasoning\": \"...\", \"tags\": [\"...\"]}}\n\n\
         Kinds:\n\
         - decision: a choice made and why (permanent). Be specific about what was chosen and the alternatives rejected.\n\
         - observation: a factual finding about the environment, APIs, or tools (temporary, expires after {observation_ttl_days} days).\n\
         - summary: high-level run summary capturing the goal and outcome (temporary, expires after {summary_ttl_days} days). Limit to 1 per run.\n\
         - principle: a reusable rule or pattern worth following again (permanent, reinforced if repeated).\n\n\
         Quality rules:\n\
         - A memory should be useful to an agent that has never seen this run.\n\
         - Be specific about *what* and *why*, not *where* (file paths) or *when* (timestamps).\n\
         - Extract 3-8 memories per run. Fewer high-quality memories beat many low-quality ones.\n\
         - Use 1-3 lowercase domain tags per memory. Prefer project module names \
(e.g., \"provider\", \"pipeline\", \"execution\", \"config\") or domain concepts \
(e.g., \"database\", \"auth\", \"perf\") that a future recall query would naturally contain.\n\n\
         Examples of GOOD memories:\n\
         - decision: \"Use connection pooling with max 20 connections for PostgreSQL\" reasoning: \"Single connections caused timeouts under load\"\n\
         - observation: \"The Gemini API returns 429 errors above 60 requests/minute\" reasoning: \"Discovered during load testing\"\n\
         - principle: \"Always validate pagination parameters before passing to SQL queries\" reasoning: \"Unbounded LIMIT/OFFSET caused full table scans\"\n\
         - summary: \"Implemented user authentication with OAuth2, replacing the legacy session-based system\" reasoning: \"Security audit required modern auth\"\n\n\
         Examples of BAD memories (do NOT produce these):\n\
         - \"Changed the database code\" (too vague, no actionable detail)\n\
         - \"Be careful with code\" (too generic, not actionable)\n\
         - \"The run completed successfully\" (trivial, not reusable)\n\
         - \"Modified line 42 in foo.rs\" (too specific to file location, ephemeral)\n\n\
         Return only the JSON array.\n\n",
    );

    // Part 2: Optional existing-memory section
    let memory_section_bytes = if !existing_memories.is_empty() {
        use crate::memory::recall::format_memory_entry_plain;
        use crate::post_run::EXTRACTION_MEMORY_MAX_BYTES;

        let mut section = String::from(
            "Existing memories for this project are listed below. \
             When extracting new memories:\n\
             - Focus on genuinely new decisions, observations, principles, \
               or summaries.\n\
             - If a new finding reinforces an existing principle, decision, \
               or observation, re-emit it using very similar wording to the \
               original — the system will recognize the duplicate and handle \
               it appropriately. Do NOT paraphrase or shorten; close-to-verbatim \
               wording is required for automatic dedup.\n\
             - Do NOT re-emit existing summaries — they are run-specific.\n\
             - Do NOT extract information that is already captured with no \
               new nuance or reinforcement value.\n\n\
             Existing memories:\n",
        );

        let header_len = section.len();
        let mut entry_bytes: usize = 0;

        for mem in existing_memories {
            let mut entry = String::new();
            format_memory_entry_plain(&mut entry, mem);
            if entry_bytes + entry.len() > EXTRACTION_MEMORY_MAX_BYTES.saturating_sub(header_len) {
                break;
            }
            entry_bytes += entry.len();
            section.push_str(&entry);
        }
        section.push('\n');
        let total = section.len();
        prompt.push_str(&section);
        total
    } else {
        0
    };

    // Part 3: Agent outputs marker
    prompt.push_str("Agent outputs:\n");

    let file_budget = EXTRACTION_MAX_INPUT_BYTES.saturating_sub(memory_section_bytes as u64);
    let mut budget = PostRunPromptBudget::with_limit(file_budget);
    let mut appended_any = false;
    let mut skipped_count: usize = 0;

    // Iterate in reverse so that finalization/consolidation outputs (appended
    // last by callers) get budget priority over earlier agent outputs.
    for (label, path) in files.iter().rev() {
        // Check file size before reading to avoid loading huge files into memory
        // only to discard them when the budget is exceeded. Skip (don't break) so
        // smaller files later in the list (e.g. finalization summaries) still get included.
        if let Ok(meta) = std::fs::metadata(path) {
            if budget.would_exceed(meta.len() as usize) {
                skipped_count += 1;
                continue;
            }
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if budget.add_text(&content, "Extraction input").is_err() {
            skipped_count += 1;
            continue; // Budget exceeded for this file — try remaining smaller ones
        }
        prompt.push_str(&format!("\n--- {label} ---\n{content}\n"));
        appended_any = true;
    }

    // Fallback: if every file exceeded the budget, truncate the first readable
    // file so that long single-agent runs still produce some memories.
    if !appended_any {
        let limit = file_budget as usize;
        for (label, path) in files.iter().rev() {
            let content = match std::fs::read_to_string(path) {
                Ok(c) if !c.is_empty() => c,
                _ => continue,
            };
            let truncated = if content.len() > limit {
                &content[..floor_char_boundary(&content, limit)]
            } else {
                &content
            };
            prompt.push_str(&format!(
                "\n--- {label} (truncated to fit extraction budget) ---\n{truncated}\n"
            ));
            appended_any = true;
            break;
        }
    }

    if !appended_any {
        return Err("No file content available for extraction".into());
    }

    Ok((prompt, skipped_count))
}

/// Maximum memories accepted from a single extraction. Prevents a chatty model
/// from flooding the database even when the prompt asks for 3-8.
const MAX_EXTRACTED_MEMORIES: usize = 10;

pub fn parse_extraction_response(response: &str) -> Vec<ExtractedMemory> {
    let trimmed = response.trim();

    let mut result = None;

    // Try 1: raw JSON array
    if result.is_none() {
        if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(trimmed) {
            result = Some(memories);
        }
    }

    // Try 2: extract from ```json ... ``` fence
    if result.is_none() {
        if let Some(json_str) = extract_fenced_json(trimmed) {
            if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
                result = Some(memories);
            }
        }
    }

    // Try 3: bare ``` ... ``` fence
    if result.is_none() {
        if let Some(start) = trimmed.find("```") {
            let after = &trimmed[start + 3..];
            // Skip optional language tag on same line
            let content_start = after.find('\n').map(|i| i + 1).unwrap_or(0);
            if let Some(end) = after[content_start..].find("```") {
                let json_str = after[content_start..content_start + end].trim();
                if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
                    result = Some(memories);
                }
            }
        }
    }

    // Try 4: {"memories": [...]} wrapper
    if result.is_none() {
        if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(trimmed) {
            if let Some(arr) = wrapper.get("memories").and_then(|v| v.as_array()) {
                if let Ok(memories) = serde_json::from_value::<Vec<ExtractedMemory>>(
                    serde_json::Value::Array(arr.clone()),
                ) {
                    result = Some(memories);
                }
            }
        }
    }

    // Apply hard cap to prevent DB flooding from chatty models
    let mut memories = result.unwrap_or_default();
    memories.truncate(MAX_EXTRACTED_MEMORIES);
    memories
}

/// Find the largest byte index <= `max` that is a valid char boundary.
pub(super) fn floor_char_boundary(s: &str, max: usize) -> usize {
    if max >= s.len() {
        return s.len();
    }
    let mut i = max;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn extract_fenced_json(s: &str) -> Option<&str> {
    let marker = "```json";
    let start = s.find(marker)?;
    let content_start = start + marker.len();
    let after = &s[content_start..];
    let newline = after.find('\n').map(|i| i + 1).unwrap_or(0);
    let end = after[newline..].find("```")?;
    Some(after[newline..newline + end].trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::types::MemoryKind;
    use tempfile::tempdir;

    #[test]
    fn build_extraction_prompt_inlines_content() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "Agent output content here").unwrap();

        let (prompt, skipped) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &[]).unwrap();
        assert!(prompt.contains("Agent output content here"));
        assert!(prompt.contains("--- Agent1 ---"));
        assert_eq!(skipped, 0);
    }

    #[test]
    fn build_extraction_prompt_budget_exceeded() {
        let dir = tempdir().unwrap();
        let mut files = Vec::new();
        // Create files that together exceed 100KB
        for i in 0..20 {
            let file = dir.path().join(format!("agent{i}.md"));
            std::fs::write(&file, "x".repeat(10 * 1024)).unwrap();
            files.push((format!("Agent{i}"), file));
        }

        let (prompt, _skipped) = build_extraction_prompt(&files, 90, 180, &[]).unwrap();
        // Should not fail, just truncate
        assert!(prompt.len() < 150 * 1024);
    }

    #[test]
    fn build_extraction_prompt_truncates_oversized_single_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("big.md");
        // Create a single file larger than the extraction budget
        std::fs::write(&file, "y".repeat(200 * 1024)).unwrap();

        let (prompt, _skipped) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &[]).unwrap();
        assert!(prompt.contains("truncated to fit extraction budget"));
        // Should be capped close to 100KB, not the full 200KB
        assert!(prompt.len() < 110 * 1024);
    }

    #[test]
    fn build_extraction_prompt_prioritizes_later_files() {
        let dir = tempdir().unwrap();
        let fin_file = dir.path().join("finalization.md");
        std::fs::write(&fin_file, "FINALIZATION_MARKER unique content").unwrap();
        let mut files = Vec::new();
        for i in 0..15 {
            let f = dir.path().join(format!("agent{i}.md"));
            std::fs::write(&f, "x".repeat(10 * 1024)).unwrap();
            files.push((format!("Agent{i}"), f));
        }
        files.push(("Finalization".into(), fin_file));

        let (prompt, skipped) = build_extraction_prompt(&files, 90, 180, &[]).unwrap();
        assert!(prompt.contains("FINALIZATION_MARKER"));
        assert!(skipped > 0);
    }

    #[test]
    fn parse_raw_json_array() {
        let response = r#"[{"kind":"decision","content":"Use X","reasoning":"Y","tags":["a"]}]"#;
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Decision);
    }

    #[test]
    fn parse_json_fence() {
        let response = "Here are the memories:\n```json\n[{\"kind\":\"observation\",\"content\":\"Found X\"}]\n```";
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Observation);
    }

    #[test]
    fn parse_bare_fence() {
        let response = "```\n[{\"kind\":\"summary\",\"content\":\"Did Y\"}]\n```";
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
    }

    #[test]
    fn parse_wrapper_object() {
        let response =
            r#"{"memories":[{"kind":"principle","content":"Always test","reasoning":"Quality"}]}"#;
        let memories = parse_extraction_response(response);
        assert_eq!(memories.len(), 1);
        assert_eq!(memories[0].kind, MemoryKind::Principle);
    }

    #[test]
    fn parse_garbage_returns_empty() {
        let memories = parse_extraction_response("this is not json at all");
        assert!(memories.is_empty());
    }

    #[test]
    fn parse_empty_returns_empty() {
        let memories = parse_extraction_response("");
        assert!(memories.is_empty());
    }

    #[test]
    fn parse_truncates_to_hard_cap() {
        // Generate 15 valid memories — should be capped to MAX_EXTRACTED_MEMORIES
        let items: Vec<String> = (0..15)
            .map(|i| {
                format!(
                    r#"{{"kind":"observation","content":"Finding number {i}","reasoning":"test"}}"#
                )
            })
            .collect();
        let response = format!("[{}]", items.join(","));
        let memories = parse_extraction_response(&response);
        assert_eq!(memories.len(), MAX_EXTRACTED_MEMORIES);
    }

    // ---- recall_for_extraction tests ----

    fn make_test_store_with_memory() -> (tempfile::TempDir, crate::memory::store::MemoryStore) {
        use crate::config::MemoryConfig;
        let dir = tempdir().unwrap();
        let store = crate::memory::store::MemoryStore::open(&dir.path().join("test.db")).unwrap();
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
        (dir, store)
    }

    #[test]
    fn recall_for_extraction_returns_memories() {
        let (_dir, store) = make_test_store_with_memory();
        let terms = vec!["postgresql".to_string(), "database".to_string()];
        let result = recall_for_extraction(&store, "proj1", &terms, 10, 0);
        assert!(!result.is_empty());
        assert!(result[0].content.contains("PostgreSQL"));
    }

    #[test]
    fn recall_for_extraction_empty_terms_returns_empty() {
        let (_dir, store) = make_test_store_with_memory();
        let result = recall_for_extraction(&store, "proj1", &[], 10, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn recall_for_extraction_empty_store_returns_empty() {
        let dir = tempdir().unwrap();
        let store = crate::memory::store::MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let terms = vec!["postgresql".to_string()];
        let result = recall_for_extraction(&store, "proj1", &terms, 10, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn recall_for_extraction_respects_max_recall() {
        use crate::config::MemoryConfig;
        let dir = tempdir().unwrap();
        let store = crate::memory::store::MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();
        // Insert multiple memories
        for i in 0..5 {
            store
                .insert(
                    "proj1",
                    &ExtractedMemory {
                        kind: MemoryKind::Observation,
                        content: format!("Database observation number {i}"),
                        reasoning: "testing".into(),
                        tags: vec!["database".into()],
                    },
                    "run1",
                    "Claude",
                    &cfg,
                )
                .unwrap();
        }
        let terms = vec!["database".to_string()];
        let result = recall_for_extraction(&store, "proj1", &terms, 2, 0);
        assert!(result.len() <= 2);
    }

    // ---- build_extraction_prompt with memories tests ----

    fn make_test_memory(content: &str, reasoning: &str) -> crate::memory::types::Memory {
        crate::memory::types::Memory {
            id: 1,
            project_id: "p".into(),
            kind: MemoryKind::Decision,
            content: content.into(),
            reasoning: reasoning.into(),
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
        }
    }

    #[test]
    fn build_extraction_prompt_includes_existing_memories() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "Agent output content here").unwrap();
        let memories = vec![make_test_memory("Use Redis for caching", "Low latency")];

        let (prompt, skipped) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &memories).unwrap();
        assert_eq!(skipped, 0);
        // Memory section is present
        assert!(prompt.contains("Existing memories"));
        assert!(prompt.contains("Use Redis for caching"));
        // Ordering: memories before agent outputs marker before file content
        let mem_pos = prompt.find("Use Redis for caching").unwrap();
        let marker_pos = prompt.find("Agent outputs:").unwrap();
        let content_pos = prompt.find("Agent output content here").unwrap();
        assert!(
            mem_pos < marker_pos,
            "memories should come before agent outputs marker"
        );
        assert!(
            marker_pos < content_pos,
            "marker should come before file content"
        );
    }

    #[test]
    fn build_extraction_prompt_memory_section_respects_budget() {
        use crate::post_run::EXTRACTION_MEMORY_MAX_BYTES;
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "Agent output").unwrap();

        // Create memories that together exceed EXTRACTION_MEMORY_MAX_BYTES
        let large_content = "x".repeat(2000);
        let memories: Vec<crate::memory::types::Memory> = (0..20)
            .map(|i| {
                let mut m = make_test_memory(&large_content, &format!("reason {i}"));
                m.id = i;
                m
            })
            .collect();

        let (prompt, _) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &memories).unwrap();
        // The memory section should be bounded near EXTRACTION_MEMORY_MAX_BYTES
        let mem_section_start = prompt.find("Existing memories").unwrap();
        let mem_section_end = prompt.find("Agent outputs:").unwrap();
        let mem_section_len = mem_section_end - mem_section_start;
        assert!(
            mem_section_len <= EXTRACTION_MEMORY_MAX_BYTES + 500,
            "memory section {} should be bounded near {}",
            mem_section_len,
            EXTRACTION_MEMORY_MAX_BYTES
        );
    }

    #[test]
    fn build_extraction_prompt_empty_memories_no_section() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "Agent output content here").unwrap();

        let (prompt, _) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &[]).unwrap();
        assert!(!prompt.contains("Existing memories"));
        // Agent outputs marker still present
        assert!(prompt.contains("Agent outputs:"));
    }

    #[test]
    fn build_extraction_prompt_memories_no_xml_escaping() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("test.md");
        std::fs::write(&file, "output").unwrap();
        let memories = vec![make_test_memory(
            "Use Vec<String> & HashMap<K, V>",
            "Because <generics>",
        )];

        let (prompt, _) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &memories).unwrap();
        // Raw characters preserved
        assert!(prompt.contains("Vec<String>"));
        assert!(prompt.contains("& HashMap<K, V>"));
        assert!(prompt.contains("<generics>"));
        // No XML entities
        assert!(!prompt.contains("&lt;"));
        assert!(!prompt.contains("&amp;"));
    }

    #[test]
    fn build_extraction_prompt_file_budget_reduced_by_memories() {
        let dir = tempdir().unwrap();
        let mut files = Vec::new();
        // Create files that together approach 100KB
        for i in 0..12 {
            let f = dir.path().join(format!("agent{i}.md"));
            std::fs::write(&f, "x".repeat(8 * 1024)).unwrap();
            files.push((format!("Agent{i}"), f));
        }

        // Without memories, all files should fit (12 * 8KB = 96KB < 100KB)
        let (_, skipped_without) = build_extraction_prompt(&files, 90, 180, &[]).unwrap();

        // With large memories consuming ~14KB, file budget shrinks to ~86KB
        let large_content = "y".repeat(1500);
        let memories: Vec<crate::memory::types::Memory> = (0..8)
            .map(|i| {
                let mut m = make_test_memory(&large_content, &format!("reason {i}"));
                m.id = i;
                m
            })
            .collect();
        let (_, skipped_with) = build_extraction_prompt(&files, 90, 180, &memories).unwrap();

        // More files should be skipped when memories consume budget
        assert!(
            skipped_with > skipped_without,
            "with memories: {} skipped vs without: {} skipped",
            skipped_with,
            skipped_without
        );
    }

    #[test]
    fn extraction_end_to_end_with_store_recall() {
        use crate::config::MemoryConfig;
        let dir = tempdir().unwrap();
        let store = crate::memory::store::MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = MemoryConfig::default();

        // Insert existing memory about PostgreSQL
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for persistence layer".into(),
                    reasoning: "ACID compliance needed".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();

        // Compute recall terms
        let terms = crate::memory::recall::compute_recall_terms(
            crate::execution::ExecutionMode::Relay,
            "Review database migration strategy for PostgreSQL upgrade",
            None,
        );

        // Recall existing memories
        let existing = recall_for_extraction(&store, "proj1", &terms, 10, 0);
        assert!(!existing.is_empty(), "should recall the PostgreSQL memory");

        // Build extraction prompt with existing memories
        let file = dir.path().join("output.md");
        std::fs::write(&file, "Agent analyzed database migration approach").unwrap();

        let (prompt, _) =
            build_extraction_prompt(&[("Agent1".into(), file)], 90, 180, &existing).unwrap();

        // Verify ordering: dedup instructions → existing memory → agent outputs marker → file content
        assert!(prompt.contains("Focus on genuinely new decisions"));
        assert!(prompt.contains("Use PostgreSQL for persistence layer"));
        assert!(prompt.contains("Agent outputs:"));
        assert!(prompt.contains("Agent analyzed database migration"));

        let dedup_pos = prompt.find("Focus on genuinely new").unwrap();
        let mem_pos = prompt.find("Use PostgreSQL").unwrap();
        let marker_pos = prompt.find("Agent outputs:").unwrap();
        let content_pos = prompt.find("Agent analyzed database").unwrap();
        assert!(dedup_pos < mem_pos);
        assert!(mem_pos < marker_pos);
        assert!(marker_pos < content_pos);
    }
}
