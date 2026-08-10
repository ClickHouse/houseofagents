<div align="center">

# House of Agents

**Multi-agent prompt runner with a terminal UI**

Run Claude, OpenAI, Gemini, and OpenCode in collaborative execution modes and save all artifacts to disk.

[![Rust](https://img.shields.io/badge/Rust-1.88%2B-orange?logo=rust)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</div>

---

## Execution Modes

| Mode | Description |
|------|-------------|
| **Relay** | Sequential handoff — each agent builds on the previous agent's output |
| **Swarm** | Parallel rounds with cross-agent context injected between rounds |
| **Pipeline** | Custom DAG builder — wire arbitrary blocks of agents into a dependency graph, with optional finalization DAG for post-execution analysis |

## Supported Providers

| Provider | API | CLI |
|----------|:---:|:---:|
| **Anthropic** (Claude) | `api_key` | `claude` binary |
| **OpenAI** | `api_key` | `codex` binary |
| **Gemini** | `api_key` | `gemini` binary |
| **OpenCode** | -- | `opencode` binary |

Each agent can run in API mode or CLI mode (`use_cli = true`). Mix and match freely. OpenCode is CLI-only.

## Features

- **Terminal UI** — select agents, mode, prompt, iterations, run count, and concurrency from an interactive TUI
- **Named agents** — define multiple agents per provider with independent configs
- **Pipeline builder** — visual DAG editor for wiring arbitrary agent blocks with dependency-driven execution, independent per-connection routing, loop-back connections for iterative refinement of sub-DAGs, scatter connections for distributing work items across replicas, sub-pipeline blocks for encapsulating inner DAGs as opaque units, and an optional finalization DAG for post-execution analysis and summarization
- **Multiple runs** — launch N independent copies of the same setup in parallel with bounded concurrency
- **Resume runs** — pick up where you left off in relay or swarm sessions
- **Forward Prompt** — relay mode option to include the original prompt in every handoff
- **Keep Session** — toggle per-provider conversation history persistence across iterations (on by default; turn off to clear provider memory between iterations while preserving inter-agent handoff context). Pipeline mode has its own per-session configuration popup in the Builder screen (`s`)
- **Consolidation** — merge multi-agent output within a run or across multiple runs (any configured agent can consolidate)
- **Diagnostics** — optional post-run analysis pass that writes `errors.md`
- **Config editor** — add/remove/rename agents, edit settings, timeouts, and models live with a popup (`e`)
- **Model picker** — browse available models from the API directly inside the config editor (`l`)
- **Cross-run memory** — SQLite-backed memory system that recalls relevant context from previous runs and extracts new memories post-run (decision, observation, summary, principle)

## Requirements

- **Rust 1.88+** and Cargo
- A terminal with TUI support
- At least one agent configured via API key **or** locally-installed CLI with auth set up

## Install

```bash
git clone https://github.com/antonio2368/houseofagents.git
cd houseofagents
./install.sh
```

The install script runs `cargo install --path .` and writes a starter config via `houseofagents --init-config`.

<details>
<summary>Custom config path / overwrite existing</summary>

```bash
# Custom config location
./install.sh /absolute/path/config.toml

# Overwrite existing config
./install.sh /absolute/path/config.toml --force
```

</details>

## Quick Start

```bash
# 1. Initialize config (skip if you used install.sh)
houseofagents --init-config

# 2. Edit config — for at least one agent:
#    - API mode: set `api_key` and `use_cli = false`
#    - CLI mode: set `use_cli = true` and ensure the CLI is installed/authenticated
#    Default location: ~/.config/houseofagents/config.toml
#    Optional: set diagnostic_provider to an agent name to enable diagnostics

# 3. Launch
houseofagents
```

In the TUI: select agents with `Space`, pick a mode, enter your prompt, and hit `Enter` or `F5` to run.

## CLI Options

```
houseofagents [OPTIONS]

Options:
  -c, --config <PATH>   Config file path [default: ~/.config/houseofagents/config.toml]
      --init-config      Write a starter config file and exit
      --force            Overwrite config when used with --init-config
  -h, --help             Print help

Headless mode (noninteractive):
      --prompt <TEXT>              Prompt text (activates headless mode)
      --prompt-file <PATH>        Read prompt from file (activates headless mode)
      --pipeline <PATH>           Pipeline TOML file (activates headless mode)
      --mode <MODE>               Execution mode: relay, swarm, pipeline [default: swarm]
      --agents <A,B,...>          Comma-separated agent names
      --order <A,B,...>           Explicit relay order (defaults to --agents order)
      --iterations <N>            Number of iterations (relay/swarm only; ignored for pipeline)
      --runs <N>                  Independent runs [default: 1]
      --concurrency <N>           Batch concurrency, 0 = unlimited [default: 0]
      --session-name <NAME>       Output directory label
      --forward-prompt            Forward prompt to all relay agents
      --no-keep-session           Disable session history keeping
      --consolidate <AGENT>       Agent for post-run consolidation
      --consolidation-prompt <S>  Extra consolidation instructions
      --output-format <FMT>       Output format: text, json [default: text]
      --quiet                     Suppress stderr progress output
      --memory                    Enable cross-run memory
      --output-dir <DIR>          Override output directory
      --print-result              Print finalization/consolidation/sub-pipeline output to stdout
      --workdir <PATH>            Working directory for CLI agents (spawn cwd + allowed dir)
      --max-calls <N>             Hard budget: max agent invocations per pipeline run
                                  (code blocks are free); exceeding it cancels the run
      --allow-edits               Let agents create/edit files unattended (injects per-provider
                                  permission flags, e.g. claude --dangerously-skip-permissions,
                                  codex --skip-git-repo-check -s workspace-write)
```

### Editing code with CLI agents

By default CLI agents are told not to write files (they return their answer as
text / to their output file). To let a pipeline actually create and edit code:

```bash
houseofagents --pipeline fix.toml --prompt "fix the login bug" \
  --workdir /path/to/repo --allow-edits --output-format json
```

`--workdir` sets the spawned CLI's working directory and marks it as an allowed
(writable) directory; `--allow-edits` injects each provider's unattended-edit
permission flags and relaxes the "do not write files" instruction.

Plans are always auto-approved: with `--allow-edits`, claude runs with bypass
permissions (nothing prompts); without it, `ExitPlanMode` is still allowed, so a
planning step's plan is approved automatically instead of stalling headlessly. **`--allow-edits`
grants agents unattended file-write power in the working directory** — scope it to a
throwaway checkout or a git worktree (the Browser UI does this automatically per workflow).

## Noninteractive (Headless) Mode

Run agents from scripts and CI pipelines without the TUI. Activate headless mode by passing `--prompt`, `--prompt-file`, or `--pipeline`.

### Relay / Swarm Examples

```bash
# Swarm with two agents, 3 iterations
houseofagents --prompt "Analyze this codebase" --agents Claude,OpenAI --iterations 3

# Relay with explicit order
houseofagents --prompt "Review and fix" --mode relay --agents Claude,OpenAI --order OpenAI,Claude

# Batch: 5 independent runs, 3 at a time
houseofagents --prompt "Generate tests" --agents Claude --runs 5 --concurrency 3

# With consolidation
houseofagents --prompt "Debate pros and cons" --agents Claude,Gemini --iterations 2 \
  --consolidate Claude --consolidation-prompt "Summarize the debate"

# JSON output for scripting
houseofagents --prompt "Analyze" --agents Claude --output-format json --quiet
```

### Pipeline Examples

```bash
# Run a saved pipeline
houseofagents --pipeline my_pipeline.toml

# Batch: 3 independent runs
houseofagents --pipeline my_pipeline.toml --runs 3

# Pipeline with consolidation
houseofagents --pipeline my_pipeline.toml --consolidate Claude

# Print finalization output directly to stdout
houseofagents --pipeline my_pipeline.toml --print-result
```

### Output Behavior

- **stdout**: In text mode, prints the run directory path on success. In JSON mode, prints a structured result object. With `--print-result`, finalization or consolidation content is appended after the directory path (text) or included as a `"results"` array in the JSON object. Results are printed on both success and error paths (partial failures still produce usable artifacts) but NOT on cancellation. On error paths, the directory path is printed to stderr and result content appears directly on stdout without a preceding path line. For single-run pipelines without finalization or consolidation, sub-pipeline terminal outputs are used as a fallback. When results exceed the internal size budget (512 KB per file, 2 MB aggregate), a `"results_truncated": true` key is added to the JSON object and inline `[...truncated...]` markers appear in the content.
- **stderr**: Progress events (agent starts, iteration completions, errors). Suppressed with `--quiet`. In JSON output format, progress events are NDJSON on stderr.
  - `sub_block_started` — An inner block within a sub-pipeline has started. Fields: `parent_block_id`, `inner_block_id`, `inner_label`, `parent_label`, `iteration`, `loop_pass`, `inner_loop_pass`.
  - `sub_block_finished` — An inner block within a sub-pipeline has finished. Same fields as `sub_block_started`.
  - `sub_block_error` — An inner block within a sub-pipeline encountered an error or was skipped. Same fields as `sub_block_started` plus `error`, `details`, and `is_skip`. When `is_skip` is `true`, the event represents a forwarded `BlockSkipped` (dependency not met). Only non-skip errors feed the error ledger and may affect exit codes.
- Headless mode never enters alternate-screen mode — safe for piping and CI.

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Validation error (bad flags, missing agents, etc.) |
| `2` | Execution error (provider failure, partial failure) |
| `130` | Cancelled (Ctrl+C) |

### Limitations

- Resume is not supported in headless mode (planned for a future version).
- Prompt is required for relay/swarm (`--prompt` or `--prompt-file`); for pipeline, the pipeline TOML's `initial_prompt` is used if `--prompt` is not given.
- `--consolidate` is not supported for single-run relay (relay produces a single output chain with no branches to merge). Batch relay consolidation works normally.

## Browser UI (optional)

A browser **DAG workflow builder** lives in `web/` — build arbitrary agent pipelines visually, run many tasks/workflows in parallel, and (for code-editing workflows) review the resulting diffs. It's a thin bridge over headless mode.

```bash
cargo build --release          # the UI shells out to the built binary
python3 web/serve.py           # then open http://localhost:8765
```

The UI ships a clean light theme (white, near-black ink, yellow primary actions) with a dark theme behind the theme toggle in the app bar (also `?theme=dark`); the preference persists.

**Layout — three scoped zones:**
- **App bar:** app name · a **Build / Runs / Schedules** view switcher · **⚙** for global settings (parallelism, binary path).
- **Workflow bar:** current-workflow selector · **+ New** / **+ Example** · **🗑 Delete** · one primary **▶ Run** · **⏱ Schedule…** · **💾 Save** / **📂 Open**. Everything here acts on the *current* workflow.
- **Build pane:** a simple form — workflow settings, an ordered **step list**, and an optional **loop**.

**What you can build:**
- **A free canvas.** The Build pane is a drag-anywhere canvas: each step is a card you place freely; **drag from a step's ● port onto another step to wire them** (or 🔗 then click). An arrow = "runs after and receives the output"; **click any wire to remove it**; **Esc** cancels connecting, **Delete** removes the selected step, **🧹 Tidy** auto-arranges by execution order. Anything not connected runs in parallel; several arrows in = fan-in; several out = fan-out; **wiring backward onto something upstream creates a loop** (dashed). Click a step to edit its prompt/agent/model/contract/gate in the editor below — where the same wiring is also editable as "Depends on" checkboxes (cycle-safe by construction). Execution order is derived purely from the arrows; canvas position is just layout.
- **Multiple loops.** Any number of loop-backs, each with its own max passes and break agent/condition **or** deterministic break command. Create them by wiring backward on the canvas or with **+ Add loop**; they're listed with their settings under the canvas. (Loops must not overlap each other, and can't combine with human gates.)
- **JIRA-style run board.** Each run card in the Runs view shows a live board — **⛔ Blocked** (waiting on unfinished dependencies, listed by name) / **○ To do** (ready) / **⏳ In progress** (live elapsed time) / **✔ Done** (duration; click to read that step's output) / **✖ Failed**, plus a **⏸ Waiting for you** column while a human gate is pending. Loop passes are labeled; cards move as the graph executes — and while a run is live, the Build canvas glows the same states onto its nodes.
- **Runs are server-owned and reload-proof.** Launching a run returns immediately; the bridge executes it in the background and buffers its events. Reload the page, open another tab, close the browser entirely — **the fleet keeps running**, and reopening the UI reattaches to every live run with its full board, log, and pending gate rebuilt from the event history. The only thing that stops a run is **⏹ Stop**, which kills the engine and all its spawned agent CLIs (process-group kill) within seconds.
- **📁 Run history from disk.** The Runs view lists earlier run directories (with ok/failed status) — click one to read its outputs. The permanent record survives reloads and restarts.
- **Agents come from your config.** The UI loads agent names from `config.toml` via the bridge (`/agents`) — no more editing the HTML to match your setup. **⧉** duplicates a workflow; **⌘Enter** runs the current one.
- **Never lose work.** Workflows persist in the browser (reload-safe) with **⌘Z / ⌘⇧Z undo/redo** on canvas edits. **▶ Run preflights** the workflow first (empty prompts, invalid contracts, gate+loop conflicts, allow-edits without a workdir) and shows the problem instead of burning tokens. A live **"≈ N agent calls/task worst-case"** estimate sits next to the optimize buttons.
- **📚 Templates.** One-click starting graphs — Research desk (the article's diamond with contracts + skeptic verify), Review ↔ Fix loop, Bug hunt (loop-until-dry with adversarial verify), Launch kit (parallel research → ⏸ human-gated positioning doc → parallel copywriting).
- Human gates pause after the gated step's whole stage; downstream steps that depend on pre-gate steps receive their outputs (including your edited approval) automatically.
- **Per-step "Code step" — reduce with plain code, zero tokens.** Tick "Code step" and the prompt becomes a **shell command**: its dependencies' outputs are piped to stdin verbatim and stdout becomes the step's output (e.g. `sort -u`, `jq`, `python3 dedupe.py`). The diamond's reduce stage without burning a model call; output contracts still apply if set.
- **Max agent calls (hard budget).** A per-workflow cap on total agent invocations (code steps are free). Exceeding it **cancels the run** — the runaway-spend stop. Also available headless via `--max-calls N`.
- **Per-step model & effort.** Each step can override the model (free-text with suggestions, e.g. `claude-fable-5` to plan, `claude-opus-4-8` to implement, `claude-sonnet-5` for a small step) and the reasoning/thinking **effort** (`low`/`medium`/`high`/`xhigh`/`max`). Blank = inherit the agent's `config.toml` model/effort. (Anthropic `xhigh`/`max` require CLI mode; in API mode the override falls back to the configured effort.)
- **Per-step "Start fresh".** A step can ignore the previous steps' output — it still runs after them, but its message is built like a root (the task + its own prompt + working-directory access), not the upstream text. Use it for an unbiased review step that re-reads the actual code with fresh eyes.
- **Per-step "Run as command (raw)".** A step can send its prompt to the CLI **verbatim** so it invokes a custom slash command (e.g. `/goal`); the previous step's output is appended as the command's argument. No House of Agents wrapping is added, so the command is recognized. (Works with `claude`; codex is best-effort — depends on the CLI expanding custom commands non-interactively.)
- **✦ Optimize steps.** One agent call explores the working directory, **rewrites every step's prompt** to be precise and codebase-grounded, and **proposes an output contract per step** (where a fixed shape helps the next step consume it). Results land in the form for review, with ↩ Undo. Complements ✦ Rewrite task (same for the task text); every step also has its own ✦ for a single-step rewrite.
- **✦ Suggest contracts.** One agent call reasons over the **whole graph's wiring** and proposes a consistent **input + output contract for every step** — each step's input matches its dependencies' outputs along every edge. Output contracts are enforced by the engine (reject & retry); **input contracts are folded into the step's prompt at run time** so the agent knows exactly what shape arrives (saved TOMLs keep them as a separate `input_schema` field). Nested descriptor keys (`"bugs[].file"`) document item shapes for the model; only top-level fields are enforced. Both contracts are editable per step, with ↩ Undo.
- **Per-step "Human gate" — you stay the last yes.** Tick "Human gate" on a step and the run **pauses after it**: the Runs card shows that step's output in an editable box, and nothing downstream runs until you click **✔ Approve & continue** (your edits become the hand-off the next step receives) or **✖ Discard run**. E.g. *research → positioning doc → ⏸ gate → write the launch assets*. Gated workflows run one task at a time, can't contain a loop, and one git worktree spans all segments so approved edit runs still produce a single diff. Pending approvals persist to `gates.json` in the config dir, so they survive a `serve.py` restart (a gate whose worktree has been deleted is dropped).
- **Per-step output contract.** Give a step a JSON contract — a flat object of `"field": "type"` pairs (`string`/`number`/`boolean`/`array`/`object`/`any`, `"?"` suffix = optional, e.g. `{"price": "number", "plan": "string", "notes": "string?"}`). The engine **enforces** it: the step must return ONLY a JSON object matching the contract; free text or wrong types are **rejected and retried** (up to 2 retries, then the step errors). This is what makes steps mechanically wire-able — the next step reads structured data, not prose. (Not available on raw command steps.)

**Runs dashboard.** Each **▶ Run** launches an independent run that streams in the background — you can switch to **Build**, create/run another workflow, and both run **concurrently**. The **Runs** view lists every run with a status badge, a **live per-step timeline** (○ pending → ⏳ running → ✔ done / ✖ error, with loop passes), a log, code diffs, and — after completion — the **run directory** plus a **"view step outputs"** button that shows each step's output. A completed run stays completed; clicking Run again starts a new run (never re-executes a finished one). Runs are tracked for the browser session; the on-disk run directories are the permanent record.
- **Optional loop.** Tick **Enable loop** and choose *"after step X, loop back to step Y"*, a **break agent**, a break condition, and a max-passes cap — e.g. Review ↔ Fix until the reviewer says APPROVED. Or set a **break command** instead — a deterministic shell gate (exit 0 = stop, e.g. `cargo test`) that runs in the working directory after each pass and replaces the agent judge, so the loop ends on a machine-checked condition rather than a model's opinion.
- **Multiple workflows** — switch via the selector; **▶ Run** executes the **current** workflow, its tasks running under the global parallelism cap (⚙), with live per-workflow status in the **Runs** view and **in-browser toasts** as each agent starts/finishes/errors.
- **Tasks** — each task is one parallel run of the workflow, injected into step 1 as `{prompt}` + task text. Separate multiple tasks with a line containing only `---`, so a **single big multi-line prompt is one task**. Leave tasks empty to run once using only the step prompts (e.g. a one-step analysis job).
- **✦ Rewrite for LLM** — write the task in plain English, click the button, and an agent (the first step's agent) **explores the working directory** and rewrites the task into a precise, codebase-grounded prompt: real paths, real module names, clear success criteria — intent and scope preserved. The rewrite lands back in the task box for you to **review/edit before running** (↩ Undo restores your original). Multi-task inputs keep their `---` separators. The rewrite also returns an **execution plan** — best agent for the task, effort, model, and attack mode (`/goal` iterate-until-done, `/loop` sweep, plan-first, or "this wants a multi-step workflow"), plus a persona lens when one genuinely changes the output — auto-applied to single-step workflows (with Undo) and shown as a recommendation otherwise. Per-step ✦ and ✦ Optimize steps recommend and apply agent/effort per step the same way.
- **Save / Open** — persists workflows as `pipeline.toml` in the engine's `pipelines_dir`, shared with the terminal builder. (The step list maps to a linear pipeline of blocks + connections + an optional loop.)
- **Cross-run memory** — toggle per workflow; passes the engine's `--memory` flag so agents recall lessons extracted from previous runs and save new ones (the write-back step that makes repeated workflows improve over time).
- **Allow edits + auto worktrees** — toggle "Allow edits" per workflow. When the working directory is a git repo, **the whole workflow runs in one throwaway git worktree/branch** (its tasks share it and see each other's edits) so parallel workflows never collide; the committed diff streams back per workflow in the Runs view.

### Scheduled workflows (generic gate)

Run a workflow **repeatedly on a schedule until a condition holds** — fully generic, no built-in integrations. Click **⏱ Schedule…** on a workflow and set an interval, a max-runs cap, and an optional **gate command** (any shell command). Each tick the bridge runs the gate: **exit 0 ⇒ done (stop)**, non-zero ⇒ run the workflow, then wait for the next tick. The tool only ever checks the exit code — GitHub/CI/tests/lint are just gate commands *you* type, e.g.:

```
gate = gh pr checks 123      # stop when the PR's checks pass
gate = make test             # stop when tests pass
gate = test -f done.flag     # stop when a file appears
```

The gate can also be **an agent instead of a command**: pick "Agent judges" in the schedule form, choose an agent, and write the condition in plain language (e.g. *"the PR's CI is green and there are no unresolved review comments"*). Each tick that agent evaluates the condition in the working directory and replies DONE/CONTINUE — the fuzzy counterpart to the deterministic shell gate (mirrors the loop's break agent).

The **Schedules** view (app-bar switcher) shows each schedule's live state (`running` / `converged` / `capped` / `stopped`), run count, last gate exit code, and log, with a **Stop** button. Each tick launches as a normal run — it appears live in the **Runs** view (named `⏱ <schedule>`) with the full pipeline strip, step results, and its own Stop. Scheduled runs edit the working directory directly (not a worktree) so an agent step can commit/push and let the gate converge. Schedules are persisted to `schedules.json` in the config dir — restarting `serve.py` restores them, and ones that were still running resume ticking.

**How it maps:** the bridge (`web/serve.py`, Python stdlib only — no pip installs) generates one `pipeline.toml` per workflow (blocks + connections + loop_connections), creates a per-workflow git worktree when editing, then spawns `houseofagents --pipeline … [--prompt "<task>"] [--workdir …] [--allow-edits] --output-format json --print-result` and streams JSON events to the browser as live per-task cards.

**Notes:**
- Edit the agent list in `web/index.html` (`const AGENTS`) to match the agent names in your `config.toml`.
- Binary auto-detected at `target/release` then `target/debug`; override with `HOA_BIN=/path/to/houseofagents` or the "Binary" field.
- The canvas authors blocks + connections + loops + scatter. Sub-pipelines and the finalization DAG are not yet drawable in the browser (the engine still supports them via saved TOML).

## Configuration

The starter config lives at `~/.config/houseofagents/config.toml`:

```toml
output_dir = "~/houseofagents-output"
default_max_tokens = 4096
# For values 4+, providers keep the first exchange plus the newest messages.
# For values 1-3, providers keep only the most recent messages up to the cap.
max_history_messages = 50
http_timeout_seconds = 120
model_fetch_timeout_seconds = 30
cli_timeout_seconds = 600
max_history_bytes = 102400
# pipeline_block_concurrency = 0

# Optional: set to an agent name to enable diagnostics
# diagnostic_provider = "Claude"

# Named agents — you can have multiple agents per provider
[[agents]]
name = "OpenAI"
provider = "openai"
api_key = ""
model = "gpt-5.3-codex"
reasoning_effort = "high"
use_cli = true
extra_cli_args = ""

[[agents]]
name = "Claude"
provider = "anthropic"
api_key = ""
model = "claude-opus-4-6"
thinking_effort = "high"
use_cli = true
extra_cli_args = ""

[[agents]]
name = "Gemini"
provider = "gemini"
api_key = ""
model = "gemini-2.5-pro"
thinking_effort = "medium"
use_cli = true
extra_cli_args = ""

[[agents]]
name = "OpenCode"
provider = "opencode"
api_key = ""
model = "anthropic/claude-sonnet-4-5"
use_cli = true
extra_cli_args = ""
```

### Config Reference

**General settings** (top-level):

| Field | Description |
|-------|-------------|
| `output_dir` | Base directory for run output folders |
| `default_max_tokens` | Token budget sent to providers |
| `max_history_messages` | Max chat history kept per provider session. Values `4+` preserve the first exchange plus newer messages; values `1-3` keep only the most recent messages. |
| `http_timeout_seconds` | Timeout for API calls (`use_cli = false`) |
| `model_fetch_timeout_seconds` | Timeout for model list fetch in config editor |
| `cli_timeout_seconds` | Timeout for CLI calls (`use_cli = true`) |
| `max_history_bytes` | Max total bytes of conversation history per provider session (default 100 KB). Applied after message-count pruning. |
| `pipeline_block_concurrency` | Max concurrent pipeline blocks. `0` = unlimited (default). |
| `diagnostic_provider` | Agent name for the automatic diagnostics pass (disabled when unset) |

**Memory settings** (`[memory]`):

| Field | Description |
|-------|-------------|
| `enabled` | Enable cross-run memory (`false` by default) |
| `db_path` | Custom SQLite database path (default: `{output_dir}/memory.db`) |
| `project_id` | Override automatic project detection (default: derived from git remote or cwd) |
| `max_recall` | Max memories to recall per run (default: 10) |
| `max_recall_bytes` | Byte budget for recalled memory context (default: 8192) |
| `max_summary_recall` | Max summary-kind memories per recall, 0 = unlimited (default: 2) |
| `extraction_agent` | Agent to use for post-run memory extraction (default: first participating agent, then first configured). Stronger models produce higher-quality memories. |
| `disable_extraction` | Disable post-run memory extraction (default: false) |
| `observation_ttl_days` | Days before observations expire (default: 120) |
| `summary_ttl_days` | Days before summaries expire (default: 180) |
| `stale_permanent_days` | Archive permanent memories (decisions, principles) after N days without recall (default: 365, 0 to disable) |

**Agent settings** (`[[agents]]`):

| Field | Description |
|-------|-------------|
| `name` | Display name for the agent (must be unique) |
| `provider` | Provider type — `anthropic`, `openai`, `gemini`, or `opencode` |
| `api_key` | API key (required when `use_cli = false`, leave empty for CLI mode) |
| `model` | Model identifier to use |
| `use_cli` | Use local CLI binary instead of HTTP API (always `true` for OpenCode) |
| `extra_cli_args` | Shell-style extra CLI args parsed at runtime, for example `--sandbox workspace-write --profile "fast mode"` |
| `reasoning_effort` | OpenAI effort setting — `low` / `medium` / `high` / `xhigh` |
| `thinking_effort` | Anthropic & Gemini effort setting — `low` / `medium` / `high`; Anthropic CLI also supports `xhigh` and `max` (`max` for `claude-opus-4-6`) |

Anthropic `thinking_effort = "xhigh"` and `thinking_effort = "max"` are rejected in API mode. In CLI mode, House of Agents passes them through and lets the `claude` CLI report any model-specific incompatibility.

OpenCode is a CLI-only provider; it has no API mode. Loaded OpenCode agents are normalized to `use_cli = true`, and runtime validation still rejects any invalid in-memory OpenCode API-mode state. The `model` field uses `provider/model` format (e.g. `anthropic/claude-sonnet-4-5`, `openai/gpt-4o`). Run `opencode models` to view or configure models, then set the model manually. OpenCode effort is delegated to the OpenCode/model configuration. The `opencode` binary must be installed and authenticated. OpenCode agents always pass `--dangerously-skip-permissions` for non-interactive execution.

OpenCode `extra_cli_args` cannot include `--format`, `--dir`, `--model`, or `-m` forms because House of Agents owns those flags. OpenCode prompts are passed to `opencode run` as a positional argument and preflighted for embedded NUL bytes and a conservative byte limit before spawn. The default `max_history_bytes` can still allow a final OpenCode argv prompt above this conservative cap once prompt wrappers are added. If a prompt-limit error appears, reduce prompt/history size or lower `max_history_bytes`.

## Keyboard Shortcuts

### Home Screen

| Key | Action |
|-----|--------|
| `j` / `k` / `Up` / `Down` | Navigate agents and modes |
| `Space` | Toggle agent / mode selection |
| `Tab` | Switch panels |
| `e` | Open config editor |
| `M` | Open memory management (when memory enabled) |
| `?` | Open help popup |
| `Enter` | Continue to prompt |
| `q` | Quit |

### Memory Management

| Key | Action |
|-----|--------|
| `j` / `k` / `Up` / `Down` | Navigate memories |
| `d` | Delete selected memory |
| `D` | Bulk delete all visible memories (press twice to confirm) |
| `f` | Cycle kind filter (all → decision → observation → summary → principle) |
| `r` | Toggle "never recalled" filter |
| `a` | Toggle archived view |
| `u` | Unarchive selected memory (in archived view) |
| `q` / `Esc` | Back to home |

### Config Editor

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate agents, timeouts, or memory settings |
| `Tab` / `Shift+Tab` | Switch section (Agents / Timeouts / Memory) |
| `n` | Add new agent |
| `Del` / `Backspace` | Remove agent |
| `r` | Rename agent |
| `p` | Cycle provider (Anthropic / OpenAI / Gemini / OpenCode) |
| `c` | Toggle CLI / API mode |
| `a` | Edit API key |
| `m` | Edit model |
| `l` | Open model picker |
| `t` | Cycle thinking / reasoning effort where supported |
| `x` | Edit extra CLI args |
| `d` | Toggle diagnostic agent |
| `o` | Edit output directory |
| `e` / `Enter` | Edit selected value (Timeouts / Memory sections) |
| `Space` | Toggle boolean setting (Memory section) |
| `s` | Save config to disk |
| `Esc` | Close (keep changes for session) |

### Prompt Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle input fields forward / backward |
| `Space` | Toggle focused option (Resume / Forward Prompt / Keep Session) |
| `Enter` / `F5` | Start run |
| `Ctrl+E` | Analyze setup — sends current configuration to `diagnostic_provider` for a plain-language explanation |
| `?` | Open help (unavailable while editing text fields: prompt, session name) |
| `Esc` | Back |

Fields vary by mode for options, but every prompt flow includes Prompt, Session Name, Runs, and Concurrency. Relay and swarm modes also include Iterations.

### Pipeline Builder Screen

| Key | Action |
|-----|--------|
| `Tab` / `Shift+Tab` | Cycle focus: Initial Prompt → Session Name → Runs → Concurrency → Builder |
| `a` | Add a new block |
| `d` | Delete selected block |
| `e` | Edit selected block (name, agents, prompt, session ID, replicas); for sub-pipelines edits name + replicas |
| `Enter` | Edit selected block (same as `e`); for sub-pipelines drills into the inner DAG |
| `c` | Enter connect mode — select a second block to create a connection |
| `x` | Enter connection-action mode — pick a connection to delete (`Enter`) or toggle scatter (`s`) |
| `o` | Create loop-back connection — press on the downstream feedback block, then select the upstream restart target; set count and prompt; press on existing loop to edit |
| `p` | Add a sub-pipeline block — contains a full inner DAG that executes as one opaque unit |
| `A` | Add a finalization block (placed below the separator in the finalization region) |
| `f` | Create or edit data feed — on an execution block, enters feed-connect mode (navigate to a finalization block and press Enter to create the feed); on a finalization block with multiple feeds, opens a feed list picker for selecting and editing individual feeds (opens edit directly if only one feed) |
| `F` | Remove a data feed — removes directly if only one feed on the block; on a finalization block with multiple feeds, opens the feed list picker to select which one; on an execution block with multiple feeds, prompts to use the finalization block instead |
| `s` | Open session configuration popup — toggle per-session Loop history persistence |
| `Arrow keys` / `h j k l` | Navigate/select blocks spatially without moving |
| `Shift+Arrow keys` / `Shift+H J K L` | Move selected block (swap with occupied target cell, otherwise move) |
| `Ctrl+Arrow keys` | Scroll the builder canvas |
| `↑`/`+` `↓`/`-` | Increment / decrement runs or concurrency on the focused numeric field |
| `Ctrl+S` | Save pipeline (always prompts for filename, prefills current name) |
| `Ctrl+L` | Load pipeline from file (type to search, Tab toggles search/list focus, j/k navigates list) |
| `Ctrl+E` | Analyze setup — sends current pipeline to `diagnostic_provider` for a plain-language explanation |
| `F5` | Validate and run the pipeline |
| `?` | Open help popup (8 tabbed sections; Tab/Shift+Tab to cycle). Only when focus is not on a text field (initial prompt / session name). |
| `Esc` | Cancel current action / back to home |

Inside the **edit popup**: `Tab` cycles between Name, Agents (multiselect list — `Up`/`Down` to navigate, `Space` to toggle), Profiles (multiselect list of reusable instruction files), Prompt (text area), Session ID, and Replicas fields. `Esc` closes the popup. Each block can have one or more agents selected. Setting Replicas > 1 spawns that many copies per agent. Total tasks per block = agents × replicas (max 32).

**Sub-pipeline blocks**: Press `p` to create a sub-pipeline block. Press `Enter` to drill into it and edit its inner DAG. Press `e` to edit its name and replica count. Press `Esc` to pop back to the parent. Sub-pipelines execute as opaque units. The parent block can have replicas > 1, in which case each replica runs an independent copy of the inner DAG with its own sub-run directory. The inner DAG's terminal finalization block output becomes the sub-pipeline's output to the parent pipeline. Limitations: one level of nesting only (sub-pipelines cannot contain sub-pipelines), the inner pipeline must have exactly one finalization leaf with a single agent and `replicas = 1`. Save (Ctrl+S) and Run (F5) are disabled while inside a sub-pipeline. Tab cycles between the initial prompt and the builder canvas.

**Scatter connections**: A scatter connection splits an upstream block's output into discrete work items using a configurable delimiter (default `===SCATTER_ITEM===`) and distributes them across replicas of the downstream block. Source blocks automatically receive a prompt instruction telling them to format their output with the scatter delimiter — no manual prompt editing needed. To toggle scatter: press `x` to enter connection-action mode, navigate to the connection, and press `s`. Sub-pipeline scatter targets auto-consume: each replica loops internally until the shared queue is drained, so all items are processed even when items outnumber replicas. Faster replicas naturally consume more items. Because sub-pipeline targets drain the queue in a single pass, loop connections are redundant for sub-pipeline scatter targets (the loop will terminate after the first pass since the queue is already empty). For regular (non-sub-pipeline) scatter targets, each replica pops one item at a time from the queue. Combine with a loop connection for full queue drain — replicas keep processing items until the queue is empty, at which point the loop terminates automatically (loop count acts as a safety cap). The scatter target must be the loop's restart block (the block the loop feeds back into). Sub-pipeline blocks can be scatter targets (each replica auto-consumes multiple work items) or scatter sources (the finalization leaf receives the delimiter instruction). Scatter wires render as dashed lines (`╌╎`) in cyan. Constraints: source must have exactly 1 logical task (1 agent × 1 replica, or a single-replica sub-pipeline), max 1 scatter input per block, all scatter edges from the same source must use the same delimiter.

### Order Screen (relay with 2+ agents)

| Key | Action |
|-----|--------|
| `j` / `k` | Move cursor |
| `Space` | Grab / reorder agent |
| `Enter` | Confirm and start |
| `Ctrl+E` | Analyze setup — sends current configuration to `diagnostic_provider` for a plain-language explanation |
| `?` | Open help |
| `Esc` | Back to prompt |

### Running Screen

| Key | Action |
|-----|--------|
| `j` / `k` | Select run row in batch mode |
| `Esc` | Cancel in-flight run or batch |
| `Enter` | Open results after completion |
| `q` | Quit after run completes |
| `l` | Toggle activity log detail panel |
| `p` | Toggle stream preview on/off |
| `Tab` / `Shift+Tab` | Cycle preview target between agents/blocks |

### Results Screen

| Key | Action |
|-----|--------|
| `j` / `k` | Navigate files |
| `Enter` / `l` | Expand/collapse run groups in batch results |
| `Esc` | Return to home |
| `q` | Quit |

> `Ctrl+C` exits from any screen (cancels an active run first).

## Run Artifacts

Each run creates a directory inside `output_dir`:

```
output_dir/
  latest -> 2026-03-05/my_session  # symlink to most recent run (unix)
  runs.toml                        # append-only run index
  2026-03-05/
    my_session/                    # user-defined session name
      prompt.md                    # Original prompt
      session.toml                 # Run metadata
      Claude_iter1.md              # Agent output per iteration
      OpenAI_iter2.md
      consolidated_Claude.md       # Optional: merged output
      errors.md                    # Optional: diagnostics report
      _memories.json               # Optional: extracted memories from this run
      _errors.log                  # Application-level error log
      _sessions.toml               # CLI provider session ID mapping (block/agent → session ID)
    swift-falcon/                  # auto-generated name (no session provided)
      ...
```

Pipeline runs produce per-block output files named using the block's name (sanitized) and a unique block id suffix. Blocks with multiple agents produce one file per agent. When `replicas > 1`, each replica gets an `_rN` suffix. Total runtime tasks per block = agents × replicas:

```
output_dir/
  2026-03-05/
    my_session/
      session.toml                         # mode = "pipeline", block/connection counts, total_runtime_tasks
      prompt.md                            # Pipeline-level prompt (shared across blocks)
      pipeline.toml                        # Pipeline definition snapshot (may include [[session_configs]])
      Analyzer_b1_Claude.md                # Block "Analyzer" (id 1), agent Claude
      Reviewer_b2_Gemini.md                # Block "Reviewer" (id 2), single agent, replicas=1
      Critic_b4_Claude.md                  # Block "Critic" (id 4), agents=[Claude, GPT] — one file per agent
      Critic_b4_GPT.md
      Worker_b3_Claude_r1.md               # Block "Worker" (id 3), replicas=3, replica 1
      Worker_b3_Claude_r2.md               # replica 2
      Worker_b3_Claude_r3.md               # replica 3
      Analyzer_b1_Claude_loop1.md          # loop pass 1 (from loop connection)
      Analyzer_b1_Claude_loop2.md          # loop pass 2
      Fixer_b5_Claude_r1_item0.md          # scatter: replica 1, item 0 (pass 0)
      Fixer_b5_Claude_r2_item1.md          # scatter: replica 2, item 1 (pass 0)
      Fixer_b5_Claude_r1_item2_loop1.md    # scatter: replica 1, item 2 (loop pass 1)
      _errors.log
      _sessions.toml                      # CLI provider session ID mapping
```

Sub-pipeline blocks create a `sub_{name}_b{id}/` subdirectory containing the inner pipeline's outputs and finalization:

```
my_session/
  Analyzer_b1_Claude.md
  sub_Debugger_b2_pipeline.md         # Parent-level output (consumed by downstream blocks/feeds)
  sub_Debugger_b2/                    # Sub-pipeline block inner artifacts
    InnerBlock_b100_Gemini.md         # Inner execution block
    finalization/
      Consolidate_b200_Claude.md      # Terminal output → parent pipeline
```

With `replicas > 1`, each replica creates its own sub-directory and parent-level output:

```
my_session/
  Analyzer_b1_Claude.md
  sub_Debugger_b2_pipeline_r1.md      # Replica 1 parent-level output
  sub_Debugger_b2_pipeline_r2.md      # Replica 2 parent-level output
  sub_Debugger_b2_r1/                 # Replica 1 inner artifacts
    InnerBlock_b100_Gemini.md
    finalization/
      Consolidate_b200_Claude.md
  sub_Debugger_b2_r2/                 # Replica 2 inner artifacts
    InnerBlock_b100_Gemini.md
    finalization/
      Consolidate_b200_Claude.md
```

With scatter connections to a sub-pipeline, each replica auto-consumes items from the shared queue. Each processed scatter item gets its own sub-directory and parent-level output file:

```
my_session/
  Source_b1_Claude.md
  sub_Debugger_b2_pipeline_r1_item0.md   # Replica 1, scatter item 0
  sub_Debugger_b2_pipeline_r1_item3.md   # Replica 1, scatter item 3 (auto-consumed)
  sub_Debugger_b2_pipeline_r2_item1.md   # Replica 2, scatter item 1
  sub_Debugger_b2_pipeline_r2_item2.md   # Replica 2, scatter item 2
  sub_Debugger_b2_r1_i0/                 # Item 0 inner artifacts
    ...
  sub_Debugger_b2_r1_i3/                 # Item 3 inner artifacts (auto-consumed)
    ...
  sub_Debugger_b2_r2_i1/
    ...
  sub_Debugger_b2_r2_i2/
    ...
```

Loop-back connections create iterative refinement cycles. `from` is the downstream feedback source and `to` is the upstream restart target. All blocks on regular-graph paths between the two endpoints form the loop sub-DAG and re-run on each pass. In saved pipeline TOML files they appear as:

```toml
[[loop_connections]]
from = 2
to = 1
count = 3
prompt = "Refine based on feedback"
break_agent = "Claude"
break_condition = "Stop when the output is stable and no new improvements are suggested"
```

`count` is the number of additional passes beyond the initial run. Each block in the sub-DAG runs `count + 1` times total. Loop wires are drawn as double-line in yellow on the canvas. Optionally set `break_agent` and `break_condition` to enable early termination — the agent evaluates outputs after each pass and can stop the loop before reaching `count`.

Alternatively set `break_command` — a **deterministic** break gate. After each pass the shell command runs (in `--workdir` if given, else the current directory); **exit 0 means the condition is met and the loop stops**. Prefer it over the agent judge whenever the goal is machine-checkable — the model never gets to grade its own homework:

```toml
[[loop_connections]]
from = 2
to = 1
count = 5
break_command = "cargo test"   # loop Fix ↔ Review until the tests actually pass
```

`break_command` and `break_agent` are mutually exclusive. The command is best-effort like the agent evaluator: spawn failure or a 300s timeout counts as CONTINUE.

### Code (reduce) blocks

A block can run a **shell command instead of an agent** — the article's "reduce with plain code, no model, no tokens" node:

```toml
[[blocks]]
id = 3
name = "Dedupe findings"
command = "sort -u"        # upstream outputs piped to stdin; stdout = block output
position = [2, 0]
```

Code blocks take their dependencies' outputs verbatim on stdin (the initial prompt if they're roots), run in `--workdir` (else the current directory, 600s timeout), and their stdout feeds downstream blocks like any agent output. `schema` is validated once (no retry — code is deterministic). They cost zero agent calls, so they're exempt from `--max-calls`. Mutually exclusive with `agents`, `raw`, and `sub_pipeline`.

### Block output contracts

Any (non-raw) block can declare an enforced output contract in its TOML:

```toml
[[blocks]]
id = 2
name = "Research pricing"
agents = ["Claude"]
prompt = "Research this competitor's current pricing."
position = [1, 0]
schema = '{"price": "number", "plan": "string", "source": "string", "date": "string", "notes": "string?"}'
```

The contract is a flat JSON object mapping field names to types (`string`, `number`, `boolean`, `array`, `object`, `any`; a `"?"` suffix marks a field optional; extra fields are allowed). The block's prompt is augmented with the contract, and after each response the engine validates the output: free text, invalid JSON, missing required fields, or wrong types are **rejected** — the same agent (with its session context) is told why and asked to retry, up to 2 retries, after which the block fails. Downstream blocks receive the validated JSON, so they can consume it without a human in the middle.

### Profiles

Profiles are reusable system instruction files (Markdown) stored in
`~/.config/houseofagents/profiles/`. Assign them to pipeline blocks to
inject instructions into every message sent by that block.

**Create a profile:**
```bash
mkdir -p ~/.config/houseofagents/profiles
cat > ~/.config/houseofagents/profiles/reviewer.md << 'EOF'
You are a senior code reviewer. Focus on:
- Security vulnerabilities
- Performance implications
- API contract violations
EOF
```

**Assign profiles:** Open the pipeline builder, select a block, press `e` to edit,
Tab to the Profiles field, and Space to toggle profiles on/off.

**CLI vs API behavior:**
- **API agents:** Profile content is read and inlined into the message.
- **CLI agents:** Profile file paths are passed (the CLI tool reads them).

Profiles persist in the pipeline TOML under each block's `profiles` key.
Values are file stems without the `.md` extension:
```toml
[[blocks]]
id = 1
agents = ["Claude"]
profiles = ["reviewer", "security"]
prompt = "Review this PR"
position = [0, 0]
```

Finalization blocks support profiles too since they share the same block model.

**Missing profiles:** If a profile file is deleted or renamed after assignment, the
edit dialog shows it in yellow with `[!]` and setup analysis marks it `[missing]`.
Missing profiles are silently skipped at runtime — no instructions are injected for them.

### Finalization DAG

Pipeline mode supports an optional **finalization phase** that runs after the execution DAG completes. Finalization blocks receive execution outputs via **data feeds** and can be wired into their own dependency DAG via finalization connections. When finalization is defined, it replaces consolidation for pipeline mode.

**Data feeds** connect execution blocks to finalization blocks with two configurable dimensions:

| Setting | Options | Description |
|---------|---------|-------------|
| **Collection** | `last_pass` (default), `all_passes` | `last_pass` deduplicates loop passes, keeping only the latest output per block. `all_passes` feeds every output including all loop pass variants. |
| **Granularity** | `per_run` (default), `all_runs` | Whether the finalization block runs once per successful run or once across all runs |

A **per-run** finalization block runs independently for each successful run, receiving only that run's execution outputs. An **all-runs** finalization block runs once, receiving outputs from all successful runs. Per-run blocks can feed into all-runs blocks via finalization connections, enabling patterns like "summarize each run, then synthesize across summaries."

Wildcard feeds (`from = 0`) collect outputs from all execution blocks. Block-specific feeds target a single execution block.

Single-run finalization outputs are stored in `run_dir/finalization/`:

```
my_session/
  session.toml
  pipeline.toml
  Analyzer_b1_Claude.md
  Reviewer_b2_Gemini.md
  finalization/
    finalization.toml
    Summary_b3_Claude.md
    Report_b4_GPT.md
```

Batch finalization outputs are stored in `batch_root/finalization/`:

```
my_session/
  batch.toml
  run_1/
    ...
  run_3/
    ...
  finalization/
    finalization.toml
    per_run_summary_Claude_run1.md
    per_run_summary_Claude_run3.md
    meta_report_Claude.md
```

Per-run finalization filenames include the real run ID (only successful runs). The `finalization.toml` metadata file records the successful run IDs, finalization block count, and feed count.

Example pipeline TOML with finalization:

```toml
initial_prompt = "Analyze the codebase"

[[blocks]]
id = 1
name = "Analyzer"
agents = ["Claude"]
prompt = "Analyze the architecture"
row = 0
col = 0

[[blocks]]
id = 2
name = "Reviewer"
agents = ["Gemini"]
prompt = "Review the analysis"
row = 0
col = 1

[[connections]]
from = 1
to = 2

[[finalization_blocks]]
id = 3
name = "Summary"
agents = ["Claude"]
prompt = "Synthesize all findings into a final report"
row = 0
col = 0

[[data_feeds]]
from = 0
to = 3
collection = "last_pass"
granularity = "per_run"
```

Example pipeline TOML with a sub-pipeline block:

```toml
initial_prompt = "Analyze the codebase"

[[blocks]]
id = 1
name = "Prep"
agents = ["Claude"]
prompt = "Gather context"
position = [0, 0]

[[blocks]]
id = 2
name = "Deep Analysis"
position = [1, 0]

[blocks.sub_pipeline]
initial_prompt = "Perform deep analysis"

[[blocks.sub_pipeline.blocks]]
id = 100
name = "Researcher"
agents = ["Gemini"]
prompt = "Research thoroughly"
position = [0, 0]

[[blocks.sub_pipeline.finalization_blocks]]
id = 200
name = "Synthesizer"
agents = ["Claude"]
prompt = "Synthesize findings"
position = [0, 1]

[[blocks.sub_pipeline.data_feeds]]
from = 0
to = 200
collection = "last_pass"
granularity = "per_run"

[[connections]]
from = 1
to = 2
```

Runs are grouped by date: `YYYY-MM-DD/<session_name>`. When no session name is provided, a random two-word name (`adjective-noun`) is generated. Duplicate user-defined session names within the same date are rejected.

When `runs > 1`, House of Agents creates a batch root and one subdirectory per independent run:

```
output_dir/
  2026-03-08/
    my_session/
      batch.toml
      cross_run_consolidation.md   # Optional: synthesis across successful runs
      run_1/
        prompt.md
        session.toml
        Claude_iter1.md
        consolidation.md           # Optional: per-run synthesis
      run_2/
        ...
```

Legacy directories (`YYYYMMDD_HHMMSS_NNN[_session]` and `YYYY-MM-DD/HH-MM-SS[_session]`) from older versions are preserved and remain searchable.

## Resume, Consolidation & Diagnostics

- **Resume** (toggle with `Space` on Prompt screen) — available for relay and swarm modes
  - With a session name: resolves the latest run with that name, then validates it against the current run configuration before resuming
  - Without: resumes the latest compatible run with an exact mode match
  - Relay resume requires the exact same agent order
  - Swarm resume requires the exact same agent set
  - Resume also requires the same Keep Session setting — a run started with `keep_session = true` cannot be resumed with it off, and vice versa
  - Batch roots are excluded from resume lookup; resume is currently single-run only
  - Note: resuming a `keep_session = true` run across app restarts does not restore provider conversation history — providers are recreated fresh, though inter-agent handoff context is preserved
- **Forward Prompt** (toggle with `Space` on Prompt screen) — relay mode only; when enabled, downstream agents receive the original prompt alongside the previous agent's output, preventing context loss in the handoff chain
- **Keep Session** (toggle with `Space` on Prompt screen) — on by default; controls whether providers retain their conversation history across iterations. When turned off, each provider's history is cleared before every iteration after the first, so agents treat each round as a fresh conversation. Inter-agent handoff context (relay's previous output, swarm's round outputs) is always preserved regardless of this setting. Pipeline mode has its own per-session session configuration popup accessible via `s` in the Builder screen — each effective session (shared or isolated) can toggle **Loop** (keep across loop passes, default on). When Loop is off, provider history is cleared between loop pass advances. Use `Space` to toggle. Non-default settings are stored in `pipeline.toml` as `[[session_configs]]` entries
- **Consolidation**
  - Single-run: offered after non-cancelled swarm/pipeline runs with 2+ final outputs
  - Batch: first offers per-run consolidation, then optional cross-run consolidation across successful runs
  - Skipped automatically for pipeline runs that have finalization defined (finalization replaces consolidation)
- **Setup Analysis** — press `Ctrl+E` on the Prompt, Order, or Pipeline screen to send the current run configuration to the `diagnostic_provider` for a plain-language explanation of what the run will do. Requires `diagnostic_provider` to be set in config. The popup shows loading state, then a scrollable analysis result (scroll with `j`/`k`/arrows/PgUp/PgDn, close with `Esc`/`q`). Pre-flight checks catch invalid setups locally before making the provider call. Errors are shown inside the popup itself.
- **Diagnostics** — when `diagnostic_provider` is set to an agent name, a final analysis pass writes `errors.md`
