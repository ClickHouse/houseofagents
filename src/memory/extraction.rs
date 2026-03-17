use super::types::ExtractedMemory;
use crate::post_run::{PostRunPromptBudget, EXTRACTION_MAX_INPUT_BYTES};

pub fn build_extraction_prompt(files: &[(String, std::path::PathBuf)]) -> Result<String, String> {
    let mut prompt = String::from(
        "Extract reusable memories from these agent outputs. Return a JSON array.\n\n\
         Each object: {\"kind\": \"decision|observation|summary|principle\", \
         \"content\": \"...\", \"reasoning\": \"...\", \"tags\": [\"...\"]}\n\n\
         Rules:\n\
         - decision: a choice made and why (permanent)\n\
         - observation: a factual finding (temporary)\n\
         - summary: high-level run summary (temporary)\n\
         - principle: reusable rule/pattern (permanent, reinforced if repeated)\n\n\
         Return only the JSON array.\n\nAgent outputs:\n",
    );

    let mut budget = PostRunPromptBudget::with_limit(EXTRACTION_MAX_INPUT_BYTES);
    let mut appended_any = false;

    for (label, path) in files {
        // Check file size before reading to avoid loading huge files into memory
        // only to discard them when the budget is exceeded. Skip (don't break) so
        // smaller files later in the list (e.g. finalization summaries) still get included.
        if let Ok(meta) = std::fs::metadata(path) {
            if budget.would_exceed(meta.len() as usize) {
                continue;
            }
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        if budget.add_text(&content, "Extraction input").is_err() {
            continue; // Budget exceeded for this file — try remaining smaller ones
        }
        prompt.push_str(&format!("\n--- {label} ---\n{content}\n"));
        appended_any = true;
    }

    // Fallback: if every file exceeded the budget, truncate the first readable
    // file so that long single-agent runs still produce some memories.
    if !appended_any {
        let limit = EXTRACTION_MAX_INPUT_BYTES as usize;
        for (label, path) in files {
            let content = match std::fs::read_to_string(path) {
                Ok(c) if !c.is_empty() => c,
                _ => continue,
            };
            let truncated = if content.len() > limit {
                // Truncate to byte-safe boundary
                let end = floor_char_boundary(&content, limit);
                &content[..end]
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

    Ok(prompt)
}

pub fn parse_extraction_response(response: &str) -> Vec<ExtractedMemory> {
    let trimmed = response.trim();

    // Try 1: raw JSON array
    if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(trimmed) {
        return memories;
    }

    // Try 2: extract from ```json ... ``` fence
    if let Some(json_str) = extract_fenced_json(trimmed) {
        if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
            return memories;
        }
    }

    // Try 3: bare ``` ... ``` fence
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        // Skip optional language tag on same line
        let content_start = after.find('\n').map(|i| i + 1).unwrap_or(0);
        if let Some(end) = after[content_start..].find("```") {
            let json_str = after[content_start..content_start + end].trim();
            if let Ok(memories) = serde_json::from_str::<Vec<ExtractedMemory>>(json_str) {
                return memories;
            }
        }
    }

    // Try 4: {"memories": [...]} wrapper
    if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(arr) = wrapper.get("memories").and_then(|v| v.as_array()) {
            if let Ok(memories) = serde_json::from_value::<Vec<ExtractedMemory>>(
                serde_json::Value::Array(arr.clone()),
            ) {
                return memories;
            }
        }
    }

    // Give up gracefully
    Vec::new()
}

/// Find the largest byte index ≤ `max` that is a valid char boundary.
/// Equivalent to the nightly `str::floor_char_boundary`.
fn floor_char_boundary(s: &str, max: usize) -> usize {
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

        let prompt = build_extraction_prompt(&[("Agent1".into(), file)]).unwrap();
        assert!(prompt.contains("Agent output content here"));
        assert!(prompt.contains("--- Agent1 ---"));
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

        let prompt = build_extraction_prompt(&files).unwrap();
        // Should not fail, just truncate
        assert!(prompt.len() < 150 * 1024);
    }

    #[test]
    fn build_extraction_prompt_truncates_oversized_single_file() {
        let dir = tempdir().unwrap();
        let file = dir.path().join("big.md");
        // Create a single file larger than the extraction budget
        std::fs::write(&file, "y".repeat(200 * 1024)).unwrap();

        let prompt = build_extraction_prompt(&[("Agent1".into(), file)]).unwrap();
        assert!(prompt.contains("truncated to fit extraction budget"));
        // Should be capped close to 100KB, not the full 200KB
        assert!(prompt.len() < 110 * 1024);
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
}
