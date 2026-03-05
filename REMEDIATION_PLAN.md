# Findings Remediation Plan

Source findings: `/home/antonio/houseofagents-output/20260305_211333_848_hoareview/consolidated_anthropic.md` (March 5, 2026).

## Scope and Priorities

1. Stop data loss and incorrect outputs (critical).
2. Remove crash paths and argument-contract drift (medium).
3. Fix async/runtime hygiene and low-risk correctness issues (secondary).
4. Improve dependency hygiene, CI guardrails, and targeted test gaps.

## Phase 1: Critical Fixes (P0)

### 1) Anthropic thinking-block parsing drops real output
- Severity: High
- Files: `src/provider/anthropic.rs` (reported around lines 69, 103)
- Plan:
1. Parse `content` as an array of typed blocks and select blocks by `type`.
2. Use text-bearing block(s) for assistant output instead of fixed index `content[0]["text"]`.
3. Add a fallback path for legacy responses and explicit error when no text block exists.
4. Add regression tests for:
   - thinking block first + text block second,
   - no thinking block,
   - malformed content array.
- Exit criteria:
1. Relay/swarm/solo flows return assistant text correctly with and without extended thinking.
2. New tests fail on old logic and pass on new logic.

### 2) Silent file-write failures
- Severity: High
- Files: `src/relay.rs`, `src/swarm.rs`, `src/solo.rs`, `src/tui.rs`
- Plan:
1. Replace `let _ = std::fs::write(...)` with explicit error handling (`?` where possible, otherwise surfaced UI/message error).
2. Ensure each command path returns non-zero/failure status if required artifact write fails.
3. Add contextual error messages (target path + operation).
4. Add tests for write-failure behavior (temp dir permissions / simulated IO error path).
- Exit criteria:
1. Write failures are user-visible and reflected in command result.
2. No path silently reports success on failed artifact write.

## Phase 2: Robustness Fixes (P1)

### 3) `extra_cli_args` contract mismatch
- Severity: Medium
- Files: `src/cli.rs`, `src/config.rs`, docs
- Plan:
1. Decide one contract and enforce it consistently:
   - Option A (recommended): treat config as full raw string passed verbatim.
   - Option B: treat config as list of tokens and update schema/docs.
2. Update parser implementation and docs in lockstep.
3. Add tests for quoted args, spaces, and escaped values.
- Exit criteria:
1. Behavior matches docs exactly.
2. Tests cover multi-word and quoted argument cases.

### 4) `expect()` panic on HTTP client construction
- Severity: Medium
- Files: `src/tui.rs` (reported around lines 995, 1323, 1834, 1975)
- Plan:
1. Replace `expect()` with recoverable error propagation.
2. Surface errors in TUI status/errors pane without terminating process.
3. Add tests for invalid client config path to verify non-panicking behavior.
- Exit criteria:
1. TUI does not panic on client-construction failure.
2. User sees actionable error and can continue or retry.

## Phase 3: Async and Correctness Hardening (P2)

### 5) Blocking terminal read in async worker
- Files: `src/event.rs`
- Plan:
1. Move blocking poll/read to dedicated thread or `spawn_blocking`.
2. Keep async channel boundary between event producer and async consumers.
3. Add shutdown signal and join behavior to close cleanly.

### 6) Sync fs in async tasks
- Files: `src/swarm.rs`, `src/solo.rs`, `src/tui.rs`, `src/cli.rs`
- Plan:
1. Replace blocking fs calls inside async contexts with `tokio::fs` (or isolate in blocking tasks).
2. Validate no hot-path blocking remains via code audit.

### 7-12) Low-severity cleanup batch
- Files: `src/tui.rs`, `src/home.rs`, `src/execution/mod.rs`, `src/cli.rs`, `src/relay.rs`, `src/event.rs`
- Plan:
1. Unify provider-kind mapping helper.
2. Optimize `truncate_chars` to single-pass or char-count-safe approach.
3. Replace `&key[..4]` with Unicode-safe prefix extraction.
4. Tighten session-id parser to expected path segment(s) only.
5. Fix relay resume condition (`iteration == 1`) to support mid-run restart.
6. Add explicit `EventHandler` shutdown.

## Phase 4: Build Hygiene and Delivery Gates (P2)

### Dependencies
1. Align `crossterm` to `0.29`.
2. Align `rand` to `0.9` where compatible.
3. Reduce `tokio` features to used subset.
4. Add `rust-version = "1.80"` to `Cargo.toml`.

### CI/CD baseline
1. Add CI job (GitHub Actions or existing system) with:
   - `cargo fmt --check`
   - `cargo clippy --all-targets --all-features -D warnings`
   - `cargo test --all-targets`
2. Add matrix for stable toolchain (and optional MSRV check at 1.80).

## Phase 5: Test Strategy (cross-cutting)

1. Highest-priority coverage expansion: `src/provider/cli.rs` (currently no tests).
2. Add provider parsing tests for Anthropic/OpenAI/Gemini response variants.
3. Consolidate duplicated `MockProvider` into shared test utility module.
4. Add regression test labels per finding ID (`F1`..`F12`) for traceability.

## Recommended Execution Order

1. P0 fixes + targeted tests (Findings 1-2).
2. P1 fixes + docs sync (Findings 3-4).
3. P2 async/correctness batch (Findings 5-12).
4. Dependency alignment + CI baseline.
5. Coverage expansion and test utility consolidation.

## Checkpoints and Definition of Done

### Checkpoint A (after P0)
1. No data-loss/silent-failure paths remain.
2. Regression tests merged for Findings 1-2.

### Checkpoint B (after P1)
1. No `expect()` crash points in TUI client creation paths.
2. `extra_cli_args` behavior and docs are fully aligned.

### Checkpoint C (after P2+P4+P5)
1. Async blocking risks are addressed or isolated.
2. CI enforces formatting, linting, and tests.
3. Added tests materially improve coverage in provider and CLI modules.
