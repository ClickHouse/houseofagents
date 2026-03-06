# Code Review Findings

## 1) High: `src/screen/pipeline.rs` is untracked but required by the build

- `pipeline` is imported and used by the screen registry:
  - `src/screen/mod.rs:3`
  - `src/screen/mod.rs:19`
- `git status` still shows `?? src/screen/pipeline.rs`.

Impact:
- If this file is not added before commit/push, downstream builds fail.

## 2) Medium: Pipeline event labels can be blank

- Pipeline execution now uses block name directly for labels:
  - `src/execution/pipeline.rs:400`
- `name` is serde-defaulted on load:
  - `src/execution/pipeline.rs:21`

Impact:
- Older pipeline files (or empty names) produce empty labels in progress/events.

Recommendation:
- Fallback to a generated label when `name.trim().is_empty()`, e.g. `Block {id} ({provider})`.

## 3) Medium: Auto-scroll “ensure visible” is based on hardcoded viewport size

- Auto-scroll logic uses fixed estimates:
  - `src/tui.rs:1003` (`visible_w = 78`)
  - `src/tui.rs:1004` (`visible_h = 12`)

Impact:
- On small or large terminals, selection can still be off-screen or scroll unexpectedly.

Recommendation:
- Use actual canvas inner dimensions from runtime layout instead of constants.

## 4) Low: Name/Session paste accepts raw multiline text

- Name paste path appends raw text:
  - `src/tui.rs:655`
- Session ID paste path appends raw text:
  - `src/tui.rs:667`

Impact:
- Multiline names/session IDs can be introduced via paste, creating awkward labels/keys.

Recommendation:
- Normalize pasted text for single-line fields (strip/replace newlines).

## Validation Note

- `cargo test -q` passes on this state (`235 passed`).
