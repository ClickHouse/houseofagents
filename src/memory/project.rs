use std::process::Command;

pub fn detect_project_id(config_override: &str) -> String {
    if !config_override.is_empty() {
        return config_override.to_string();
    }
    // Try git remote origin URL
    if let Ok(output) = Command::new("git")
        .args(["remote", "get-url", "origin"])
        .output()
    {
        if output.status.success() {
            let url = String::from_utf8_lossy(&output.stdout);
            let url = url.trim();
            if !url.is_empty() {
                return format!("{:016x}", fnv1a_64(url.as_bytes()));
            }
        }
    }
    // Fallback: cwd path
    if let Ok(cwd) = std::env::current_dir() {
        let cwd_str = cwd.display().to_string();
        if !cwd_str.is_empty() {
            return format!("{:016x}", fnv1a_64(cwd_str.as_bytes()));
        }
    }
    "default".to_string()
}

fn fnv1a_64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv1a_deterministic() {
        let h1 = fnv1a_64(b"hello");
        let h2 = fnv1a_64(b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn fnv1a_different_inputs() {
        let h1 = fnv1a_64(b"hello");
        let h2 = fnv1a_64(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn config_override_used() {
        assert_eq!(detect_project_id("my-project"), "my-project");
    }

    #[test]
    fn empty_override_falls_through() {
        let id = detect_project_id("");
        assert!(!id.is_empty());
        // Should be either a hex hash or "default"
        assert!(id.len() >= 7);
    }
}
