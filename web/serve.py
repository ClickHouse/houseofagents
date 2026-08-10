#!/usr/bin/env python3
"""Browser-UI bridge for House of Agents.

Serves web/index.html (a DAG workflow builder). On POST /run it accepts a list
of workflows — each an arbitrary DAG of agent blocks + connections + loop-backs
— generates one pipeline.toml per workflow, and spawns
`houseofagents --pipeline ... [--prompt <task>] [--workdir ...] [--allow-edits]`
for every task across every workflow, bounded by a global concurrency cap. When
a workflow may edit code, the whole workflow runs in one throwaway git worktree
(tasks share it) so parallel workflows don't collide; the workflow's combined
diff is streamed back when its last task finishes.

Also exposes save/load of workflows to the engine's pipelines_dir so the TUI and
browser share saved pipelines.

Stdlib only. No pip installs.

    python3 web/serve.py            # then open http://localhost:8765
    HOA_BIN=/path/to/houseofagents python3 web/serve.py --port 9000
"""

import argparse
import json
import os
import queue
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import uuid
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parent.parent
HERE = Path(__file__).resolve().parent


def find_binary():
    env = os.environ.get("HOA_BIN")
    if env:
        return env
    for rel in ("target/release/houseofagents", "target/debug/houseofagents"):
        p = ROOT / rel
        if p.is_file():
            return str(p)
    return "houseofagents"


def binary_stale(binary):
    """True if any .rs source is newer than the binary — new per-step features
    (model/effort/raw/fresh) would be silently ignored by a stale binary."""
    try:
        bt = Path(binary).stat().st_mtime
        return any(p.stat().st_mtime > bt for p in (ROOT / "src").rglob("*.rs"))
    except OSError:
        return False


STALE_MSG = ("houseofagents binary is older than src/ — run `cargo build --release`; "
             "per-step model/effort/raw/fresh are silently ignored by a stale binary")


def config_dir():
    """Mirror the Rust `dirs::config_dir()` used by pipelines_dir()."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"
    return Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config"))


def pipelines_dir():
    d = config_dir() / "houseofagents" / "pipelines"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_config():
    try:
        return tomllib.loads((config_dir() / "houseofagents" / "config.toml").read_text())
    except (OSError, tomllib.TOMLDecodeError):
        return {}


def agent_names():
    """Agent names straight from config.toml — the UI must never hardcode them."""
    return [a.get("name") for a in load_config().get("agents", []) if a.get("name")]


_PROVIDER_CLI = {"anthropic": "claude", "openai": "codex", "gemini": "gemini", "opencode": "opencode"}


def usable_agent_names():
    """Agents that can actually run on THIS machine: CLI-mode needs its binary
    on PATH, API-mode needs a key. Rewrites must never recommend a ghost."""
    out = []
    for a in load_config().get("agents", []):
        if not a.get("name"):
            continue
        if a.get("use_cli"):
            cli = _PROVIDER_CLI.get((a.get("provider") or "").lower())
            if cli and not shutil.which(cli):
                continue
        elif not (a.get("api_key") or "").strip():
            continue
        out.append(a["name"])
    return out


def output_root():
    d = load_config().get("output_dir")
    return Path(d).expanduser() if d else Path.home() / "houseofagents-output"


def list_run_history(limit=40):
    """Recent run directories from disk — the permanent record the UI can
    reopen after a reload."""
    root = output_root()
    if not root.is_dir():
        return []
    runs = []
    for day in sorted((p for p in root.iterdir() if p.is_dir()), reverse=True):
        for rd in day.iterdir():
            if not rd.is_dir():
                continue
            if not ((rd / "session.toml").exists() or (rd / "pipeline.toml").exists()):
                continue
            try:
                mtime = rd.stat().st_mtime
            except OSError:
                continue
            err = (rd / "_errors.log")
            failed = err.exists() and err.stat().st_size > 0
            runs.append({"name": rd.name, "date": day.name, "path": str(rd),
                         "status": "failed" if failed else "ok", "mtime": mtime})
        if len(runs) >= limit * 2:
            break
    runs.sort(key=lambda r: -r["mtime"])
    return runs[:limit]


# --------------------------------------------------------------------------- #
# TOML generation (graph -> PipelineDefinition)
# --------------------------------------------------------------------------- #

def toml_str(s):
    out = (
        (s or "")
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
    )
    return '"' + out + '"'


def build_pipeline_toml(graph, fold_input=False):
    """Emit a full PipelineDefinition TOML from a canvas graph.

    graph = {
      nodes: [{id:int, name, agents:[str], prompt, replicas?, pos?:[col,row]}],
      edges: [{from:int, to:int, scatter?:bool, delimiter?:str}],
      loops: [{from:int, to:int, count:int, breakAgent:str, breakCondition:str}],
    }
    Block ids are the node ids (positive ints; 0 is reserved by the engine).
    """
    nodes = graph.get("nodes", [])
    lines = ['initial_prompt = ""  # overridden per task via --prompt', ""]
    for i, n in enumerate(nodes):
        pos = n.get("pos") or [i, 0]
        name = n.get("name") or f"Block {n['id']}"
        lines += [
            "[[blocks]]",
            f"id = {int(n['id'])}",
            f"name = {toml_str(name)}",
        ]
        if (n.get("command") or "").strip():
            # Code block: shell command, no agents.
            lines.append(f"command = {toml_str(n['command'].strip())}")
        else:
            agents = n.get("agents") or [n.get("agent", "Claude")]
            agents_toml = ", ".join(toml_str(a) for a in agents)
            lines.append(f"agents = [{agents_toml}]")
        prompt = n.get("prompt", "")
        inp = (n.get("input_schema") or "").strip()
        if fold_input and inp and not n.get("raw"):
            # Run-time only: tell the agent the shape of what it receives.
            prompt = (f"INPUT CONTRACT: the upstream output you receive conforms to this JSON shape "
                      f"(name: type, \"?\" = optional): {inp}\n\n{prompt}")
        lines += [
            f"prompt = {toml_str(prompt)}",
            f"position = [{int(pos[0])}, {int(pos[1])}]",
        ]
        if inp and not fold_input:
            lines.append(f"input_schema = {toml_str(inp)}  # advisory; folded into the prompt at run time")
        if int(n.get("replicas", 1)) > 1:
            lines.append(f"replicas = {int(n['replicas'])}")
        if (n.get("model") or "").strip():
            lines.append(f"model = {toml_str(n['model'].strip())}")
        if (n.get("effort") or "").strip():
            lines.append(f"effort = {toml_str(n['effort'].strip())}")
        if n.get("fresh"):
            lines.append("fresh = true")
        if n.get("raw"):
            lines.append("raw = true")
        if (n.get("schema") or "").strip():
            lines.append(f"schema = {toml_str(n['schema'].strip())}")
        if n.get("gate"):
            lines.append("gate = true  # browser-UI human gate; ignored by the engine")
        lines.append("")
    for e in graph.get("edges", []):
        lines += ["[[connections]]", f"from = {int(e['from'])}", f"to = {int(e['to'])}"]
        if e.get("scatter"):
            lines.append("scatter = true")
            if e.get("delimiter"):
                lines.append(f"scatter_delimiter = {toml_str(e['delimiter'])}")
        lines.append("")
    for lp in graph.get("loops", []):
        lines += [
            "[[loop_connections]]",
            f"from = {int(lp['from'])}",
            f"to = {int(lp['to'])}",
            f"count = {int(lp.get('count', 8))}",
        ]
        if lp.get("breakCommand"):
            lines.append(f"break_command = {toml_str(lp['breakCommand'])}")
        elif lp.get("breakAgent"):
            lines.append(f"break_agent = {toml_str(lp['breakAgent'])}")
            if lp.get("breakCondition"):
                lines.append(f"break_condition = {toml_str(lp['breakCondition'])}")
        lines.append("")
    return "\n".join(lines)


def toml_to_graph(doc):
    """Parse a loaded pipeline.toml (dict) back into a canvas graph."""
    nodes = []
    for b in doc.get("blocks", []):
        pos = b.get("position", [0, 0])
        nodes.append({
            "id": b["id"],
            "name": b.get("name", f"Block {b['id']}"),
            "agents": b.get("agents") or ([b["agent"]] if b.get("agent") else ["Claude"]),
            "prompt": b.get("prompt", ""),
            "replicas": b.get("replicas", 1),
            "pos": [pos[0], pos[1]] if len(pos) >= 2 else [0, 0],
            "model": b.get("model", ""),
            "effort": b.get("effort", ""),
            "fresh": bool(b.get("fresh", False)),
            "raw": bool(b.get("raw", False)),
            "schema": b.get("schema", ""), "input_schema": b.get("input_schema", ""),
            "gate": bool(b.get("gate", False)),
            "command": b.get("command", ""),
        })
    edges = [{"from": c["from"], "to": c["to"], "scatter": c.get("scatter", False),
              "delimiter": c.get("scatter_delimiter", "")} for c in doc.get("connections", [])]
    loops = [{"from": l["from"], "to": l["to"], "count": l.get("count", 8),
              "breakAgent": l.get("break_agent", ""), "breakCondition": l.get("break_condition", ""),
              "breakCommand": l.get("break_command", "")}
             for l in doc.get("loop_connections", [])]
    return {"nodes": nodes, "edges": edges, "loops": loops}


def split_tasks(text):
    if not text or not text.strip():
        return []
    chunks, cur = [], []
    for line in text.splitlines():
        if line.strip() == "---":
            chunks.append("\n".join(cur).strip())
            cur = []
        else:
            cur.append(line)
    chunks.append("\n".join(cur).strip())
    return [c for c in chunks if c]


def slugify(s, n=32):
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s or "").strip("_").lower()
    return s[:n] or "task"


# --------------------------------------------------------------------------- #
# Git worktree isolation
# --------------------------------------------------------------------------- #

def _git(args, cwd=None):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def is_git_repo(path):
    if not path:
        return False
    r = _git(["rev-parse", "--is-inside-work-tree"], cwd=path)
    return r.returncode == 0 and r.stdout.strip() == "true"


def make_worktree(repo, slug):
    """Create an isolated worktree on a fresh branch. Returns (path, branch)."""
    branch = f"hoa/{slug}-{uuid.uuid4().hex[:8]}"
    tmp = tempfile.mkdtemp(prefix="hoa_wt_")
    r = _git(["worktree", "add", "-b", branch, tmp, "HEAD"], cwd=repo)
    if r.returncode != 0:
        shutil.rmtree(tmp, ignore_errors=True)
        raise RuntimeError(f"git worktree add failed: {r.stderr.strip()}")
    return tmp, branch


def finalize_worktree(repo, tmp, branch, task):
    """Commit any edits (so they survive on the branch), capture the diff, and
    remove the worktree. Returns the unified diff (str, possibly empty)."""
    _git(["add", "-A"], cwd=tmp)
    diff = _git(["diff", "--cached"], cwd=tmp).stdout
    if diff.strip():
        _git(["-c", "user.email=hoa@localhost", "-c", "user.name=HouseOfAgents",
              "commit", "-m", f"hoa: {task[:60]}"], cwd=tmp)
    _git(["worktree", "remove", "--force", tmp], cwd=repo)
    if not diff.strip():
        # nothing was committed — don't litter the repo with empty branches
        _git(["branch", "-D", branch], cwd=repo)
    return diff


# --------------------------------------------------------------------------- #
# Job execution
# --------------------------------------------------------------------------- #

def make_workflow_worktree(repo, slug, njobs):
    """One shared worktree for a whole workflow; the last job to finish
    finalizes it (commit + diff + remove)."""
    tmp, branch = make_worktree(repo, slug)
    return {"repo": repo, "tmp": tmp, "branch": branch, "label": slug,
            "remaining": njobs, "lock": threading.Lock()}


def _wt_job_done(job, emit):
    """Decrement the shared worktree's job count; finalize on the last one."""
    wt = job.get("wt")
    if not wt:
        return
    with wt["lock"]:
        wt["remaining"] -= 1
        last = wt["remaining"] == 0
    if last:
        try:
            diff = finalize_worktree(wt["repo"], wt["tmp"], wt["branch"], wt["label"])
            emit({"event": "diff", "branch": wt["branch"], "diff": diff[:200_000],
                  "changed": bool(diff.strip())})
        except Exception as e:  # best-effort cleanup/diff
            emit({"event": "log", "message": f"worktree finalize failed: {e}"})


def run_job(job, out_q, sem, cancel):
    """Run one CLI process (in the workflow's shared worktree, if any);
    forward events. Scheduled runs have no worktree — they edit the real
    checkout so an agent can commit/push and let the gate converge."""
    jid = job["id"]

    def emit(obj):
        obj["id"] = jid
        out_q.put(json.dumps(obj))

    with sem:
        try:
            if cancel.is_set():
                emit({"event": "task_skipped"})
                return

            wt = job.get("wt")
            run_dir = wt["tmp"] if wt else (job["workdir"] or None)

            emit({"event": "task_started"})
            cmd = [job["binary"], "--pipeline", job["toml"], "--output-format", "json",
                   "--print-result", "--session-name", job["session"]]
            if job["task"] is not None:
                cmd += ["--prompt", job["task"]]
            if run_dir:
                cmd += ["--workdir", run_dir]
            if job["allow_edits"]:
                cmd += ["--allow-edits"]
            if job.get("memory"):
                cmd += ["--memory"]
            if job.get("max_calls"):
                cmd += ["--max-calls", str(int(job["max_calls"]))]

            try:
                proc = subprocess.Popen(
                    cmd, cwd=run_dir or None, stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True, bufsize=1,
                    preexec_fn=os.setsid,
                )
            except FileNotFoundError:
                emit({"event": "task_error", "error": f"binary not found: {job['binary']}"})
                return

            # Cancellation must actually stop the fleet: when the browser
            # aborts (or a Stop is requested), kill the whole process group —
            # the engine AND its spawned agent CLIs.
            def reaper():
                while proc.poll() is None:
                    if cancel.is_set():
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except (OSError, ProcessLookupError):
                            pass
                        return
                    time.sleep(0.5)
            threading.Thread(target=reaper, daemon=True).start()

            def pump(stream):
                for line in stream:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        emit(json.loads(line))
                    except json.JSONDecodeError:
                        emit({"event": "log", "message": line})

            t_err = threading.Thread(target=pump, args=(proc.stderr,), daemon=True)
            t_err.start()
            pump(proc.stdout)
            t_err.join()
            code = proc.wait()
            emit({"event": "task_exit", "code": code})
        finally:
            _wt_job_done(job, emit)


# --------------------------------------------------------------------------- #
# Workflow -> jobs (shared by manual runs and schedules)
# --------------------------------------------------------------------------- #

def expand_workflow(wf, wi, binary, jid_offset):
    """Validate a workflow, write its pipeline.toml, and build its jobs.
    Returns (toml_path, jobs, plan_entries). Raises ValueError on bad input."""
    graph = wf.get("graph", {})
    if not graph.get("nodes"):
        raise ValueError(f"workflow '{wf.get('name')}' has no blocks")
    workdir = (wf.get("workdir") or "").strip()
    if workdir and not Path(workdir).is_dir():
        raise ValueError(f"workdir not a directory: {workdir}")
    allow_edits = bool(wf.get("allowEdits"))
    tf = tempfile.NamedTemporaryFile("w", suffix=".toml", prefix="hoa_wf_", delete=False)
    tf.write(build_pipeline_toml(graph, fold_input=True))
    tf.close()
    tasks = split_tasks(wf.get("tasks", "")) or [None]
    wfname = wf.get("name") or f"Workflow {wi + 1}"
    names = [n.get("name") or f"Block {n['id']}" for n in graph["nodes"]]
    jobs, plan = [], []
    for task in tasks:
        jid = jid_offset + len(jobs)
        jobs.append({
            "id": jid, "binary": binary, "toml": tf.name, "task": task,
            "session": f"{slugify(wfname)}_{slugify(task or 'run')}_{jid}_{uuid.uuid4().hex[:6]}",
            "workdir": workdir, "allow_edits": allow_edits,
            "memory": bool(wf.get("memory")),
            "max_calls": wf.get("maxCalls"),
        })
        plan.append({
            "id": jid, "wf": wi, "wfName": wfname, "steps": names,
            "task": (task or "").split("\n")[0][:80] if task else "—",
            "edits": allow_edits,
        })
    return tf.name, jobs, plan


# --------------------------------------------------------------------------- #
# Server-owned runs: execution happens in background threads and events are
# buffered per run, so the browser can attach/detach/reload freely. A run only
# stops when YOU stop it (or it finishes) — never because a tab closed.
# --------------------------------------------------------------------------- #

RUNS = {}
RUNS_LOCK = threading.Lock()
_run_seq = 0
MAX_RUN_EVENTS = 12000


def _new_run(wfname, task, edits, steps):
    global _run_seq
    with RUNS_LOCK:
        _run_seq += 1
        rid = f"r{_run_seq}"
        RUNS[rid] = {
            "id": rid, "wfName": wfname, "task": task, "edits": edits,
            "steps": steps, "state": "running", "events": [],
            "cancel": threading.Event(), "created": time.time(), "gate": None,
        }
        # keep memory bounded: drop oldest finished runs beyond 20
        done = [r for r in RUNS.values() if r["state"] not in ("running", "gated")]
        for old in sorted(done, key=lambda r: r["created"])[:-20]:
            RUNS.pop(old["id"], None)
        return RUNS[rid]


def _push(run, ev):
    run["events"].append(ev)
    if len(run["events"]) > MAX_RUN_EVENTS:
        # keep the head (plan/worktree) + tail; drop middle logs
        run["events"] = run["events"][:50] + [{"event": "log", "message": "…older events trimmed…"}] + run["events"][-(MAX_RUN_EVENTS - 100):]


def _finish_state(run):
    if run["cancel"].is_set():
        return "stopped"
    for e in run["events"]:
        if e.get("event") == "task_error":
            return "failed"
        if e.get("event") == "result" and e.get("status") not in (None, "ok"):
            return "failed"
    return "done"


def _bg_jobs(run, jobs, wts, parallel, tmp_files):
    """Run plain (non-gated) jobs in the background, buffering events."""
    out_q = queue.Queue()
    for wt in wts:
        out_q.put(json.dumps({"event": "worktree", "branch": wt["branch"]}))
    sem = threading.Semaphore(parallel)
    workers = [threading.Thread(target=run_job, args=(j, out_q, sem, run["cancel"]), daemon=True) for j in jobs]
    for wk in workers:
        wk.start()
    threading.Thread(target=lambda: ([wk.join() for wk in workers], out_q.put(None)), daemon=True).start()
    while True:
        item = out_q.get()
        if item is None:
            break
        try:
            _push(run, json.loads(item))
        except json.JSONDecodeError:
            pass
    _push(run, {"event": "all_tasks_done"})
    run["state"] = _finish_state(run)
    for f in tmp_files:
        try:
            os.unlink(f)
        except OSError:
            pass


def _bg_segments(run, state):
    """Run a gated workflow's segments in the background; pauses at gates."""
    while state["segments"]:
        if run["cancel"].is_set():
            run["state"] = "stopped"
            _push(run, {"event": "all_tasks_done"})
            return
        seg = state["segments"].pop(0)
        state["seg_no"] += 1
        graph = segment_graph(seg, state["deps"], state["outputs"], state["node_names"])
        tf = tempfile.NamedTemporaryFile("w", suffix=".toml", prefix="hoa_seg_", delete=False)
        tf.write(build_pipeline_toml(graph, fold_input=True)); tf.close()
        job = {
            "id": 0, "binary": state["binary"], "toml": tf.name, "task": state["task"],
            "session": f"{slugify(state['wfname'])}_seg{state['seg_no']}_{uuid.uuid4().hex[:6]}",
            "workdir": state["workdir"], "allow_edits": state["allow_edits"],
            "memory": state["memory"], "max_calls": state.get("max_calls"),
        }
        if state.get("wt"):
            job["wt"] = state["wt"]
        out_q = queue.Queue()
        cancel = run["cancel"]
        wk = threading.Thread(target=run_job, args=(job, out_q, threading.Semaphore(1), cancel), daemon=True)
        wk.start()
        threading.Thread(target=lambda: (wk.join(), out_q.put(None)), daemon=True).start()
        run_dir = None
        while True:
            item = out_q.get()
            if item is None:
                break
            try:
                e = json.loads(item)
            except json.JSONDecodeError:
                continue
            if e.get("event") in ("result", "run_dir"):
                run_dir = e.get("run_dir") or e.get("path") or run_dir
            _push(run, e)
        try:
            os.unlink(tf.name)
        except OSError:
            pass
        state["outputs"].update(read_segment_outputs(run_dir, seg))
        if state["segments"] and not run["cancel"].is_set():
            gated = next((x for x in reversed(seg) if x.get("gate")), seg[-1])
            state["gated_id"] = str(gated["id"])
            state["run_id"] = run["id"]
            gid = _new_gate_id()
            HUMAN_GATES[gid] = state
            run["gate"] = gid
            run["state"] = "gated"
            save_gates()
            nxt = [s.get("name") or f"Block {s['id']}" for s in state["segments"][0]]
            _push(run, {"event": "human_gate", "gate": gid,
                        "output": state["outputs"].get(state["gated_id"], "")[:100_000],
                        "next_steps": nxt, "id": 0})
            return
    _push(run, {"event": "all_tasks_done"})
    run["state"] = _finish_state(run)


def run_view(r):
    return {"id": r["id"], "wfName": r["wfName"], "task": r["task"], "edits": r["edits"],
            "steps": r["steps"], "state": r["state"], "created": r["created"], "gate": r["gate"]}


# --------------------------------------------------------------------------- #
# Human gates: a step can pause the workflow until the human approves.
# The workflow is split into segments at gate steps; each approval (optionally
# with an edited hand-off text) launches the next segment. One shared worktree
# spans all segments of an editing workflow.
# --------------------------------------------------------------------------- #

HUMAN_GATES = {}  # gate_id -> continuation state
GATE_LOCK = threading.Lock()
_gate_seq = 0

GATES_FILE = config_dir() / "houseofagents" / "gates.json"


def save_gates():
    """Persist pending gate approvals so they survive serve.py restarts.
    The worktree lock is recreated on load."""
    try:
        GATES_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for gid, s in HUMAN_GATES.items():
            s2 = dict(s)
            if s2.get("wt"):
                s2["wt"] = {k: v for k, v in s2["wt"].items() if k != "lock"}
            data[gid] = s2
        GATES_FILE.write_text(json.dumps(data))
    except (OSError, TypeError):
        pass


def load_gates():
    global _gate_seq
    try:
        data = json.loads(GATES_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return
    for gid, s in data.items():
        if s.get("wt"):
            if not Path(s["wt"].get("tmp", "")).is_dir():
                continue  # worktree gone — stale gate, drop it
            s["wt"]["lock"] = threading.Lock()
        HUMAN_GATES[gid] = s
        try:
            _gate_seq = max(_gate_seq, int(gid.lstrip("g")))
        except ValueError:
            pass


def _new_gate_id():
    global _gate_seq
    with GATE_LOCK:
        _gate_seq += 1
        return f"g{_gate_seq}"


def split_at_gates(nodes):
    """Split nodes into segments, cutting after each LEVEL that contains a
    gated node (parallel siblings of the gated step finish before the pause).
    Nodes without a level fall back to one level per node (legacy chains)."""
    levels = {}
    for i, n in enumerate(nodes):
        levels.setdefault(n.get("level", i), []).append(n)
    segments, cur = [], []
    for lv in sorted(levels):
        cur += levels[lv]
        if any(x.get("gate") for x in levels[lv]):
            segments.append(cur)
            cur = []
    if cur:
        segments.append(cur)
    return segments or [[]]


def segment_graph(seg_nodes, deps, outputs, node_names):
    """Build one segment's graph. Dependencies on nodes from earlier segments
    are satisfied by injecting those nodes' (possibly human-edited) outputs
    into the prompt; dependencies within the segment become real edges."""
    in_seg = {n["id"] for n in seg_nodes}
    nodes, edges = [], []
    for s in seg_nodes:
        n = dict(s)
        n.pop("gate", None)
        n.pop("indep", None)
        n.pop("level", None)
        injected = []
        for dep in deps.get(str(s["id"]), []):
            if dep in in_seg:
                edges.append({"from": dep, "to": s["id"]})
            else:
                text = outputs.get(str(dep), "")
                if text:
                    injected.append((node_names.get(str(dep), dep), text))
        if (n.get("command") or "").strip():
            # Code step: injected text must arrive on STDIN, not in the prompt
            # (code blocks ignore prompts). ponytail: printf|pipe hits ARG_MAX
            # around ~1MB of hand-off text; switch to a temp file if that lands.
            if injected:
                blob = "\n\n".join(t for _, t in injected)
                n["command"] = f"printf '%s' {shlex.quote(blob)} | ({n['command'].strip()})"
        else:
            for name, text in injected:
                n["prompt"] = (n.get("prompt") or "") + \
                    f"\n\n--- Output from {name} (previous stage) ---\n{text}"
        nodes.append(n)
    return {"nodes": nodes, "edges": edges, "loops": []}


def read_segment_outputs(run_dir, seg_nodes):
    """Read each segment node's output file: {str(node_id): text}."""
    out = {}
    if not run_dir or not Path(run_dir).is_dir():
        return out
    files = [f for f in sorted(Path(run_dir).glob("*.md"))
             if not f.name.startswith("_") and f.name != "prompt.md"]
    for n in seg_nodes:
        want = slugify(n.get("name") or f"Block {n['id']}", 64)
        for f in files:
            if slugify(f.stem, 64).startswith(want):
                try:
                    out[str(n["id"])] = f.read_text(errors="replace")
                except OSError:
                    pass
                break
    return out



def render_claude_transcript(path, cap=250_000):
    """Flatten a Claude Code session .jsonl into a readable conversation:
    what was sent, what the agent thought, every tool call and result."""
    out, total = [], 0
    def push(s):
        nonlocal total
        s = s[:2500]
        total += len(s)
        if total < cap:
            out.append(s)
    for line in path.read_text(errors="replace").splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg = e.get("message") or {}
        content = msg.get("content")
        if e.get("type") == "user":
            if isinstance(content, str):
                push(f"◆ USER →\n{content}")
            elif isinstance(content, list):
                for c in content:
                    if c.get("type") == "text":
                        push(f"◆ USER →\n{c.get('text', '')}")
                    elif c.get("type") == "tool_result":
                        body = c.get("content")
                        if isinstance(body, list):
                            body = " ".join(x.get("text", "") for x in body if isinstance(x, dict))
                        push(f"  ↳ tool result: {str(body)[:800]}")
        elif e.get("type") == "assistant" and isinstance(content, list):
            for c in content:
                if c.get("type") == "thinking":
                    push(f"○ THINKING\n{c.get('thinking', '')}")
                elif c.get("type") == "text":
                    push(f"● AGENT\n{c.get('text', '')}")
                elif c.get("type") == "tool_use":
                    push(f"▸ TOOL {c.get('name')}({json.dumps(c.get('input', {}))[:400]})")
    if total >= cap:
        out.append("…transcript trimmed…")
    return "\n\n".join(out)


# --------------------------------------------------------------------------- #
# AI assist tasks (rewrite / optimize / contracts): run in the background with
# a live log so the UI can show real progress and cancel — never a dead "…".
# --------------------------------------------------------------------------- #

AI_TASKS = {}
AI_LOCK = threading.Lock()
_ai_seq = 0


def _require_workdir(body):
    workdir = (body.get("workdir") or "").strip()
    if workdir and not Path(workdir).is_dir():
        raise ValueError(f"workdir not a directory: {workdir}")
    return workdir


def _contract_str(v):
    """Models return contracts as strings OR inline JSON objects — accept both."""
    if isinstance(v, dict):
        return json.dumps(v) if v else ""
    if isinstance(v, str):
        v = v.strip()
        if v:
            try:
                if not isinstance(json.loads(v), dict):
                    return ""
            except json.JSONDecodeError:
                return ""
        return v
    return ""


def _strip_fences(text):
    return text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()


def ai_rewrite(body, log, cancel):
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("empty prompt")
    workdir = _require_workdir(body)
    agent = (body.get("agent") or "Claude").strip()
    binary = (body.get("binary") or "").strip() or find_binary()
    meta = REWRITE_PROMPT.replace("{AGENTS}", ", ".join(usable_agent_names()))
    text = run_oneshot_agent(binary, agent, meta + prompt, workdir, "rewrite", log, cancel).strip()
    text = re.sub(r"\A\s*---\s*\n", "", re.sub(r"\n\s*---\s*\Z", "", text)).strip()
    if not text:
        raise RuntimeError("rewrite agent produced no output")
    try:
        d = json.loads(_strip_fences(text))
        rw = (d.get("prompt") or "").strip()
        if not rw:
            raise ValueError("no prompt field")
    except (json.JSONDecodeError, ValueError, AttributeError, TypeError):
        # Older/misbehaving agent replied with plain text — still a valid rewrite.
        return {"rewritten": text, "mode": "direct", "why": ""}
    eff = (d.get("effort") or "").strip().lower()
    rec_agent = (d.get("agent") or "").strip()
    return {"rewritten": rw,
            "agent": rec_agent if rec_agent in usable_agent_names() else "",
            "model": (d.get("model") or "").strip(),
            "effort": eff if eff in ("low", "medium", "high", "xhigh") else "",
            "raw": bool(d.get("raw")) or rw.lstrip().startswith("/"),
            "mode": (d.get("mode") or "direct").strip(),
            "why": (d.get("why") or "").strip()}


def ai_optimize(body, log, cancel):
    steps = [s for s in (body.get("steps") or []) if not (s.get("code") or s.get("raw"))]
    if not steps:
        raise ValueError("no optimizable steps (code/raw steps are left as-is)")
    workdir = _require_workdir(body)
    agent = (body.get("agent") or "Claude").strip()
    binary = (body.get("binary") or "").strip() or find_binary()
    payload = json.dumps([{"id": s["id"], "name": s.get("name", ""),
                           "prompt": s.get("prompt", ""),
                           "deps": s.get("deps") or [],
                           "current_input": (s.get("input") or "")[:600],
                           "current_output": (s.get("output") or "")[:600]} for s in steps], indent=1)
    prompt = OPTIMIZE_PROMPT.replace("{AGENTS}", ", ".join(usable_agent_names())) + payload
    context = body.get("context") or []
    if context:
        ctx = json.dumps([{"name": c.get("name", ""), "relation": c.get("relation", "other"),
                           "prompt": (c.get("prompt") or "")[:500],
                           "output": (c.get("output") or "")[:400],
                           "input": (c.get("input") or "")[:400]}
                          for c in context], indent=1)
        prompt += ("\n\n=== Connected steps (context only — do NOT include them in your output; "
                   "'parent' steps feed the step you are rewriting, 'child' steps consume it) ===\n" + ctx)
    task = (body.get("task") or "").strip()
    if task:
        prompt += f"\n\n=== The task these steps will run on ===\n{task[:4000]}"
    text = run_oneshot_agent(binary, agent, prompt, workdir, "optimize", log, cancel).strip()
    if not text:
        raise RuntimeError(f"the {agent} agent produced no output — check the log above; is its CLI installed, authenticated, and working?")
    try:
        data = json.loads(_strip_fences(text))
        names = usable_agent_names()
        out = {int(s["id"]): {"prompt": s.get("prompt", ""),
                              "output": _contract_str(s.get("output", s.get("schema", ""))),
                              "input": _contract_str(s.get("input", "")),
                              "agent": s.get("agent", "") if s.get("agent", "") in names else "",
                              "effort": s.get("effort", "") if s.get("effort", "") in ("low", "medium", "high", "xhigh") else "",
                              "raw": bool(s.get("raw")) or (s.get("prompt") or "").lstrip().startswith("/"),
                              "mode": s.get("mode", "direct"),
                              "why": s.get("why", "")}
               for s in data["steps"]}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        raise RuntimeError(f"optimizer returned unparseable output: {text[:400]}")
    for s in steps:
        if int(s["id"]) not in out:
            raise RuntimeError(f"optimizer dropped step {s['id']}")
    return {"steps": {str(k): v for k, v in out.items()}}


def ai_contracts(body, log, cancel):
    steps = body.get("steps") or []
    if not steps:
        raise ValueError("no steps")
    workdir = _require_workdir(body)
    agent = (body.get("agent") or "Claude").strip()
    binary = (body.get("binary") or "").strip() or find_binary()
    payload = json.dumps([{"id": s["id"], "name": s.get("name", ""),
                           "prompt": (s.get("prompt") or "")[:1500],
                           "deps": s.get("deps") or [],
                           "current_input": (s.get("input") or "")[:600],
                           "current_output": (s.get("output") or "")[:600]} for s in steps], indent=1)
    prompt = CONTRACTS_PROMPT + payload
    task = (body.get("task") or "").strip()
    if task:
        prompt += f"\n\n=== The task the workflow runs on ===\n{task[:3000]}"
    text = run_oneshot_agent(binary, agent, prompt, workdir, "contracts", log, cancel).strip()
    if not text:
        raise RuntimeError(f"the {agent} agent produced no output — check the log above; is its CLI installed, authenticated, and working?")
    try:
        data = json.loads(_strip_fences(text))
        out = {}
        for s in data["steps"]:
            entry = {}
            for kind in ("input", "output"):
                v = _contract_str(s.get(kind))
                if v:
                    entry[kind] = v
            out[str(int(s["id"]))] = entry
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        raise RuntimeError(f"contract agent returned unparseable output: {text[:400]}")
    return {"steps": out}



PLAN_PROMPT = """You design multi-step agent workflows (DAGs) for a workflow runner.

Given the user's task below, design the minimal workflow that genuinely helps: each step is ONE bounded job with a precise, self-contained prompt; steps are wired by dependencies; add a review/fix loop only when iteration clearly helps; add contracts where a fixed hand-off shape helps the next step.

Rules:
- Fewest steps that add real value. One step is a valid answer. Use parallel roots only for genuinely independent subtasks.
- Step prompts follow these hard rules: never mention pipelines/steps/orchestration; reference the working directory only if the task is about that codebase; state goal, requirements, and success criteria.
- "deps" lists the ids a step waits for (and whose output it receives). No cycles.
- A loop goes from a downstream step back to one of its ancestors, with a clear break_condition.
- "gate": true on a step pauses the run after it — the human reviews/edits that step's output before anything downstream runs. Use it where a human decision genuinely matters (approving a plan/positioning doc before expensive downstream work, sign-off before anything ships). Gates and loops cannot coexist in one workflow — pick whichever serves the task better.
- "input"/"output" contracts: flat JSON objects mapping field -> type ("string"|"number"|"boolean"|"array"|"object", "?" = optional), or "" for free text.

Respond with ONLY this JSON shape (no prose, no fences):
{"steps": [{"id": 1, "name": "...", "prompt": "...", "deps": [], "gate": false, "input": "", "output": ""}],
 "loops": [{"from": 2, "to": 1, "max_passes": 4, "break_condition": "..."}]}

=== The task ===
"""


def ai_plan(body, log, cancel):
    task = (body.get("task") or "").strip()
    if not task:
        raise ValueError("write the Task first — the draft is designed from it")
    workdir = _require_workdir(body)
    agent = (body.get("agent") or "Claude").strip()
    binary = (body.get("binary") or "").strip() or find_binary()
    text = run_oneshot_agent(binary, agent, PLAN_PROMPT + task[:6000], workdir, "plan", log, cancel).strip()
    if not text:
        raise RuntimeError(f"the {agent} agent produced no output — check the log above; is its CLI installed, authenticated, and working?")
    try:
        data = json.loads(_strip_fences(text))
        steps = data["steps"]
        assert isinstance(steps, list) and steps
        ids = set()
        for s in steps:
            s["id"] = int(s["id"]); ids.add(s["id"])
        for s in steps:
            s["deps"] = [int(d) for d in (s.get("deps") or []) if int(d) in ids and int(d) != s["id"]]
            s["gate"] = bool(s.get("gate"))
            for kind in ("input", "output"):
                s[kind] = _contract_str(s.get(kind))
        # loops must ride a real dependency path (downstream -> ancestor)
        depmap = {s["id"]: set(s["deps"]) for s in steps}
        def reaches(a, b, seen=None):
            seen = seen or set()
            if a == b: return True
            if a in seen: return False
            seen.add(a)
            return any(reaches(d, b, seen) for d in depmap.get(a, ()))
        loops = []
        for l in (data.get("loops") or []):
            f, t = int(l["from"]), int(l["to"])
            if f in ids and t in ids and f != t and reaches(f, t):
                loops.append({"from": f, "to": t, "max_passes": max(1, int(l.get("max_passes", 4))),
                              "break_condition": (l.get("break_condition") or "").strip()})
        if any(s.get("gate") for s in steps) and loops:
            loops = []
        return {"steps": steps, "loops": loops}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError, AssertionError):
        raise RuntimeError(f"planner returned unparseable output: {text[:400]}")



BREAKS_PROMPT = """You design the STOP condition for an iterative loop inside an agent workflow.

You are given the loop's literal sub-graph: "steps_re_run_each_pass" lists every step inside the loop in execution order; each step's "waits_for" names the in-loop steps it depends on. Each pass runs from "restart_target" down through the graph to "feedback_source", whose output is fed back to the restart target for the next pass. "frozen_inputs" are upstream steps OUTSIDE the loop whose first-pass output stays fixed. The loop stops when the break holds, or at the pass cap.

The break judge can read EVERY in-loop step's output from every pass — so the condition may reference whichever step's output actually signals completion (usually the feedback source, e.g. a reviewer, but not necessarily). Reason over the step prompts to find which output carries the done-signal and what exact text/shape marks it.

Propose exactly ONE of (the other must be ""):
- "break_command": a shell command run in the working directory after each pass; exit 0 = stop. STRONGLY preferred whenever the loop's goal is mechanically checkable — tests passing, lint clean, a build succeeding, a file existing. A model judging its own work is the weakest gate.
- "break_condition": a precise plain-language condition a judge agent evaluates by reading the loop's pass outputs, e.g. "the reviewer's latest output lists zero remaining issues or is exactly APPROVED". Use when no deterministic check exists. Make it decidable from the outputs alone — no vague 'good enough'.

If the loop carries "current_command" or "current_condition", that is the user's stated intent: REWRITE it to be precise and mechanically evaluable rather than inventing something different — fix vagueness ("looks good" → a decidable check), correct shell mistakes, and only switch kind (condition ↔ command) when the goal is clearly machine-checkable.

Respond with ONLY this JSON (no prose, no fences):
{"break_command": "<shell or empty>", "break_condition": "<condition or empty>"}

=== The loop ===
"""


def ai_breaks(body, log, cancel):
    steps = body.get("steps") or []
    if not steps:
        raise ValueError("no loop steps")
    workdir = _require_workdir(body)
    agent = (body.get("agent") or "Claude").strip()
    binary = (body.get("binary") or "").strip() or find_binary()
    payload = json.dumps({
        "loop": body.get("loop") or {},
        "steps_re_run_each_pass": [{"name": s.get("name", ""),
                                    "waits_for": s.get("waits_for") or [],
                                    "prompt": (s.get("prompt") or "")[:1200],
                                    "output_contract": (s.get("output") or "")[:400]} for s in steps],
        "frozen_inputs": body.get("frozen_inputs") or [],
        "task": (body.get("task") or "")[:2000],
    }, indent=1)
    text = run_oneshot_agent(binary, agent, BREAKS_PROMPT + payload, workdir, "breaks", log, cancel).strip()
    if not text:
        raise RuntimeError(f"the {agent} agent produced no output — check the log above; is its CLI installed, authenticated, and working?")
    try:
        data = json.loads(_strip_fences(text))
        cmd = (data.get("break_command") or "").strip()
        cond = (data.get("break_condition") or "").strip()
        if cmd:
            cond = ""   # engine: command and judge are mutually exclusive
        if not cmd and not cond:
            raise ValueError
        return {"break_command": cmd, "break_condition": cond}
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        raise RuntimeError(f"break designer returned unparseable output: {text[:400]}")


AI_KINDS = {"rewrite": ai_rewrite, "optimize": ai_optimize, "contracts": ai_contracts, "plan": ai_plan, "breaks": ai_breaks}


def start_ai_task(kind, body):
    global _ai_seq
    with AI_LOCK:
        _ai_seq += 1
        t = {"id": f"a{_ai_seq}-{uuid.uuid4().hex[:6]}", "kind": kind, "state": "running", "log": [],
             "t0": time.time(), "cancel": threading.Event(), "result": None, "error": None}
        AI_TASKS[t["id"]] = t
        done = [x for x in AI_TASKS.values() if x["state"] != "running"]
        for old in sorted(done, key=lambda x: x["t0"])[:-10]:
            AI_TASKS.pop(old["id"], None)

    def log(msg):
        t["log"].append(msg)
        del t["log"][:-40]

    def runner():
        try:
            t["result"] = AI_KINDS[kind](body, log, t["cancel"])
            t["state"] = "cancelled" if t["cancel"].is_set() else "done"
        except (ValueError, RuntimeError) as e:
            t["error"] = str(e)
            t["state"] = "cancelled" if t["cancel"].is_set() else "error"
        except Exception as e:  # noqa: BLE001 — a dead thread must never leave state "running"
            import traceback
            t["error"] = f"internal error: {e!r}"
            log("TRACE: " + traceback.format_exc(limit=4).strip().replace("\n", " | "))
            t["state"] = "error"
    threading.Thread(target=runner, daemon=True).start()
    return t["id"]


# --------------------------------------------------------------------------- #
# Generic scheduled workflows: schedule + optional shell gate (exit code).
# Nothing here knows about GitHub/CI/git — only exit codes.
# --------------------------------------------------------------------------- #

SCHEDULES = {}
SCHED_LOCK = threading.Lock()
_sched_seq = 0

SCHED_FILE = config_dir() / "houseofagents" / "schedules.json"


def save_schedules():
    """Persist schedule state so it survives serve.py restarts."""
    try:
        SCHED_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = [{k: v for k, v in s.items() if k != "stop"} for s in SCHEDULES.values()]
        SCHED_FILE.write_text(json.dumps(data))
    except OSError:
        pass


def load_schedules():
    """Reload persisted schedules; resume the ones that were still running."""
    global _sched_seq
    try:
        data = json.loads(SCHED_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return
    for s in data:
        s["stop"] = threading.Event()
        SCHEDULES[s["id"]] = s
        try:
            _sched_seq = max(_sched_seq, int(s["id"].lstrip("s")))
        except ValueError:
            pass
        if s.get("state") in ("pending", "running"):
            _sched_log(s, "resumed after serve.py restart")
            threading.Thread(target=run_schedule, args=(s,), daemon=True).start()


def _new_sched_id():
    global _sched_seq
    with SCHED_LOCK:
        _sched_seq += 1
        return f"s{_sched_seq}"


def _sched_log(s, msg):
    s["log"].append(msg)
    del s["log"][:-100]  # keep last 100 lines
    save_schedules()


def _run_workflow_once(s):
    """Run the schedule's workflow one tick as a real registered run — it shows
    live in the Runs view like any manual run; the outcome is summarized back
    into the schedule log. Blocks until the tick finishes (the gate check runs
    after)."""
    try:
        tmp, jobs, plan = expand_workflow(s["wf"], 0, s["binary"], 0)
    except ValueError as e:
        s["state"] = "error"; _sched_log(s, f"error: {e}"); return False
    nodes = s["wf"].get("graph", {}).get("nodes", [])
    edges = s["wf"].get("graph", {}).get("edges", [])
    steps = [{"id": n["id"], "name": n.get("name") or f"Block {n['id']}",
              "deps": [e["from"] for e in edges if e["to"] == n["id"]]} for n in nodes]
    run = _new_run(f"⏱ {s['name']}", plan[0]["task"] if plan else "",
                   bool(s["wf"].get("allowEdits")), steps)
    _push(run, {"event": "plan", "jobs": plan})
    _sched_log(s, f"  ▶ live in Runs as {run['id']}")
    _bg_jobs(run, jobs, [], 2, [tmp])
    for e in run["events"]:
        evt = e.get("event")
        if evt == "result":
            _sched_log(s, f"  result: {e.get('status')}")
        elif evt == "block_error":
            _sched_log(s, f"  ✖ {e.get('label')}: {e.get('details') or e.get('error')}")
        elif evt == "task_error":
            _sched_log(s, f"  ✖ {e.get('error')}")
    if run["state"] == "failed":
        _sched_log(s, "  tick failed — schedule continues (the gate decides when to stop)")
    return True


def run_oneshot_agent(binary, agent, prompt, workdir, session, on_log=None, cancel=None):
    """Run a single agent once (read-only, in workdir if given) and return its
    output text ("" on failure). `on_log` receives live progress lines; a set
    `cancel` event kills the whole process group. Raises RuntimeError on
    timeout/spawn failure/cancel so callers can surface it."""
    # ponytail: effort=low — one-shots (gate judge, prompt rewrite) are latency-
    # sensitive and don't need deep reasoning; raise here if quality disappoints
    graph = {"nodes": [{"id": 1, "name": session.capitalize(), "agents": [agent],
                        "prompt": prompt, "effort": "low", "pos": [0, 0]}], "edges": [], "loops": []}
    tf = tempfile.NamedTemporaryFile("w", suffix=".toml", prefix=f"hoa_{session}_", delete=False)
    tf.write(build_pipeline_toml(graph, fold_input=True)); tf.close()
    # Unique session per invocation — the engine rejects a repeated session
    # name on the same day, which would silently break every gate/rewrite
    # after the first. The throwaway run dir is removed after reading.
    cmd = [binary, "--pipeline", tf.name, "--output-format", "json",
           "--session-name", f"{session}-{uuid.uuid4().hex[:8]}"]
    if workdir:
        cmd += ["--workdir", workdir]
    run_dir = None
    log = on_log or (lambda m: None)
    try:
        # Own process group so a timeout/cancel kills the engine AND its CLI
        # children (SIGKILL on the engine alone orphans the spawned claude).
        proc = subprocess.Popen(cmd, cwd=workdir or None, stdout=subprocess.PIPE,                                stderr=subprocess.STDOUT, text=True, bufsize=1,
                                preexec_fn=os.setsid)

        def reaper():
            deadline = time.time() + 600
            while proc.poll() is None:
                if (cancel and cancel.is_set()) or time.time() > deadline:
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    except (OSError, ProcessLookupError):
                        pass
                    return
                time.sleep(0.5)
        threading.Thread(target=reaper, daemon=True).start()

        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            ev = e.get("event")
            if ev in ("result", "run_dir") and (e.get("run_dir") or e.get("path")):
                run_dir = e.get("run_dir") or e.get("path")
            if ev == "block_started":
                log(f"agent started ({e.get('agent')})")
            elif ev == "block_log" and e.get("message"):
                log(e["message"])
            elif ev == "block_error":
                log(f"error: {e.get('details') or e.get('error')}")
        proc.wait()
        if cancel and cancel.is_set():
            if run_dir:
                shutil.rmtree(run_dir, ignore_errors=True)  # don't leak the aborted run dir
            raise RuntimeError("cancelled")
        if proc.returncode not in (0, None) and not run_dir:
            raise RuntimeError(f"{session} agent exited with {proc.returncode}")
    except OSError as e:
        raise RuntimeError(f"{session} agent error: {e}")
    finally:
        try:
            os.unlink(tf.name)
        except OSError:
            pass
    text = ""
    if run_dir and Path(run_dir).is_dir():
        for f in sorted(Path(run_dir).glob("*.md")):
            if not f.name.startswith("_") and f.name != "prompt.md":
                try:
                    text = f.read_text(errors="replace")
                except OSError:
                    pass
        shutil.rmtree(run_dir, ignore_errors=True)  # ephemeral utility run
    return text


def _eval_agent_gate(s, workdir):
    """Run the gate agent with the gate prompt; it replies DONE/CONTINUE.
    Returns True if the agent says the goal is met (stop). Read-only."""
    prompt = s["gate"] + (
        "\n\nEvaluate whether the above condition/goal is now satisfied. "
        "Reply with ONLY one word: DONE if it is satisfied and the schedule "
        "should stop, otherwise CONTINUE."
    )
    try:
        text = run_oneshot_agent(s["binary"], s["gate_agent"], prompt, workdir, "gate")
    except RuntimeError as e:
        _sched_log(s, str(e))
        text = ""
    up = text.upper()
    met = "DONE" in up and "CONTINUE" not in up
    _sched_log(s, f"gate agent [{s['gate_agent']}]: {'DONE' if met else 'CONTINUE'}")
    return met


PROMPT_ECONOMY = """Optimize the prompt for ROI, not brevity — there is no length cap. A token earns its place by either changing what the executing agent does or saving it work; cut every token that does neither:
- Courtesies, role-play ("You are an expert…"), motivation, and generic best-practice reminders ("write clean code") have zero effect on output — always cut.
- Never restate what the agent can trivially discover itself (it can read the repo).
- Terse imperatives and bullets over prose; no headings unless they disambiguate.
Spend freely on what pays: exact paths/names/commands that skip an exploration phase, decisions already made, non-obvious constraints and edge cases, and precise acceptance criteria (how the agent knows it is done). When in doubt: a token that removes ambiguity is cheap; one that adds none is waste."""

EXEC_GUIDANCE = """Beyond the prompt text, decide HOW the task should execute — always pick the cheapest setup that nails it:

Agent (recommend ONLY from the available list you are given — it is exhaustive; an agent not on it does not exist on this machine, whatever its merits):
- Claude: deepest multi-file coding, debugging, refactors; the only agent with slash skills (/goal, /loop) and plan discipline.
- OpenAI: fast and strong for focused code edits, reviews, and well-scoped implementation.
- Gemini: very large context — long-document reading, research, summarization, cross-repo audits.
- OpenCode: lightweight general CLI coding agent for straightforward code tasks.

Effort — match difficulty, never default to high: "low" = mechanical/small edits, extraction, formatting; "medium" = a typical feature or bugfix; "high" = cross-cutting changes, tricky debugging, design; "xhigh" (Claude only) = hardest correctness-critical work. Model: use "" (agent default) unless the task truly needs a specific model.

Mode — how the agent should attack the task:
- "direct": one straight pass. The default.
- "plan-first": risky/architectural/multi-file work — the prompt must OPEN with an instruction to produce a complete plan (files, ordered changes, risks) before any edit, then implement it.
- "goal" (Claude only): the outcome is mechanically verifiable (tests pass, build green) and wants autonomous iterate-until-done — the prompt MUST then start with "/goal " and "raw" must be true.
- "loop" (Claude only): a repetitive sweep over many similar items (fix all X, migrate every Y) — the prompt MUST then start with "/loop " and "raw" must be true.
- "workflow": the task naturally decomposes into stages wanting different agents/personas or human sign-off — say so in "why" (the user can auto-draft a workflow); still return the best single-prompt version.

Persona — open the prompt with "Act as <persona>: <what that lens prioritizes>" ONLY when a professional lens genuinely changes the output (product manager for requirements, UX engineer for interface work, security reviewer for an audit). "You are an expert engineer" is flattery, not a lens — never emit it."""

REWRITE_PROMPT = """You are a prompt engineer preparing a task prompt for an LLM coding agent.

The user wrote the task below in plain English. Rewrite it to be maximally effective for an autonomous LLM agent that will execute it in the working directory you have access to.

First, briefly explore the working directory (layout, languages, build system, relevant modules and naming conventions) so the rewrite is grounded in the real codebase — reference actual paths, real module/class names, and existing conventions where relevant. Do not invent files or APIs; if something is uncertain, phrase it as something for the agent to locate.

Rewrite rules:
- Preserve the user's intent and scope exactly — do not add features, tasks, or constraints they did not ask for.
- Reference the working directory ONLY if the task is actually about that codebase; if the task concerns another system or domain, do not inject this repository's structure or paths.
- Never mention House of Agents, pipelines, steps, or orchestration — the executing model needs the task, not the machinery.
- Make it precise and self-contained: state the goal, the relevant starting points in the codebase, concrete steps or areas to investigate, and clear success criteria (how the agent knows it is done, e.g. which tests/build must pass).
- No preamble, no meta-commentary.

{PROMPT_ECONOMY}
- If the input contains several tasks separated by a line with only `---`, rewrite each task and keep the same `---` separators.

{EXEC_GUIDANCE}

Available agents on this machine: {AGENTS}

Respond with ONLY a JSON object (no prose, no code fences):
{"prompt": "<rewritten prompt>", "agent": "<name from the available list>", "model": "", "effort": "low|medium|high|xhigh", "raw": <true ONLY when prompt starts with a slash command>, "mode": "direct|plan-first|goal|loop|workflow", "why": "<one short line: why this agent/effort/mode>"}
If the input contains several tasks separated by `---`, rewrite each and join them with the same separators inside "prompt". If you are asked to write your response to a file but cannot, print the JSON as your entire response — never mention tooling, files, or permission problems.

=== User's task ===
"""
REWRITE_PROMPT = REWRITE_PROMPT.replace("{PROMPT_ECONOMY}", PROMPT_ECONOMY).replace("{EXEC_GUIDANCE}", EXEC_GUIDANCE)


OPTIMIZE_PROMPT = """You are a prompt engineer preparing a multi-step agent workflow for execution.

Below is a JSON list of the workflow's steps (each runs as one autonomous LLM agent; steps may feed their output to later steps). Rewrite EVERY step's prompt to be maximally effective, and propose an output contract per step.

First, briefly explore the working directory you have access to (layout, languages, build system, relevant modules and conventions) so the rewrites are grounded in the real codebase — reference actual paths, real module names, existing conventions. Do not invent files or APIs.

For each step:
- Rewrite "prompt": precise, self-contained for that step's single job, preserving the user's intent and scope exactly. State the goal, the relevant starting points, and clear success criteria.
- Propose "output": a flat JSON object mapping field name to type ("string" | "number" | "boolean" | "array" | "object"; "?" suffix = optional) IF the step's output feeds a later step and a fixed shape helps it be consumed mechanically. Use "" when free text is the right output (final human-facing reports, code-writing steps). Must itself be valid JSON.
- Propose "input": likewise, the shape of what the step RECEIVES — it must be consistent with its PARENT steps' outputs (parents are tagged in the context). Use "" for root steps or free-text input.
- Steps may carry "current_input"/"current_output": treat them as the baseline; keep them unless the prompts or wiring contradict them.


Hard rules for every rewritten prompt:
- NEVER mention House of Agents, pipelines, steps, agents, orchestration, or that this is part of a workflow. The executing model needs the job, not the machinery. No "You are Step N..." framing.
- Reference the working directory ONLY when the step's task is actually about that codebase. If the task concerns a different system or domain (e.g. another product, external service, general research), do NOT inject this repository's structure, file paths, or conventions — that is wasted, misleading context.
- Spend the prompt's length budget on the task itself: goal, inputs, concrete requirements, output shape, success criteria.

{PROMPT_ECONOMY}

{EXEC_GUIDANCE}

Available agents on this machine: {AGENTS}. Per step also recommend "agent" (from that list; "" = keep the user's choice), "effort" ("" = agent default), and "mode" as defined above ("workflow" is not valid per-step). When mode is "goal" or "loop" the step's prompt MUST start with the slash command and "raw" must be true.

Respond with ONLY a JSON object of this exact shape (no prose, no code fences):
{"steps": [{"id": <same id>, "prompt": "<rewritten prompt>", "input": "<contract JSON or empty string>", "output": "<contract JSON or empty string>", "agent": "", "effort": "", "raw": false, "mode": "direct", "why": "<one short line>"}]}
Include every input step id exactly once. Do not rename, add, or remove steps.

=== Workflow steps ===
"""
OPTIMIZE_PROMPT = OPTIMIZE_PROMPT.replace("{PROMPT_ECONOMY}", PROMPT_ECONOMY).replace("{EXEC_GUIDANCE}", EXEC_GUIDANCE)


CONTRACTS_PROMPT = """You are designing data contracts for a multi-step agent workflow (a DAG).

Below is a JSON list of the workflow's steps (id, name, prompt) and its wiring: "deps" lists which steps' outputs each step receives as its input.

For EVERY step propose two contracts:
- "output": a flat JSON object mapping field name to type ("string" | "number" | "boolean" | "array" | "object"; "?" suffix = optional) IF a fixed shape makes this step's output mechanically consumable by the steps that depend on it. Use "" (empty string) when free text is the right output (final human-facing reports, code-writing steps).
- "input": likewise, the shape of what the step RECEIVES — it must be consistent with the union of its dependencies' output contracts. Use "" for root steps (they receive the task text) or when upstream output is free text.

Steps may carry "current_input"/"current_output" — contracts that already exist (possibly hand-edited). Treat them as the baseline: KEEP them unless the prompts or the wiring contradict them, and repair inconsistencies rather than inventing new shapes from scratch.

Contracts must be CONSISTENT along every edge: if step A's output contract has fields, every step depending on A should have an input contract containing those fields. If the working directory is available, explore briefly to ground field names in reality.

Respond with ONLY a JSON object of this exact shape (no prose, no code fences):
{"steps": [{"id": <same id>, "input": "<contract JSON or empty string>", "output": "<contract JSON or empty string>"}]}
Include every input step id exactly once.

=== Workflow steps & wiring ===
"""


def run_schedule(s):
    """Gate-first loop: gate satisfied => converged (stop); else run workflow.
    Gate is a shell command (exit 0 = satisfied) or an agent prompt (DONE)."""
    s["state"] = "running"
    workdir = (s["wf"].get("workdir") or "").strip() or None
    while not s["stop"].is_set():
        if s["gate"]:
            if s.get("gate_kind") == "agent":
                _sched_log(s, f"gate (agent): {s['gate']}")
                if _eval_agent_gate(s, workdir):
                    s["state"] = "converged"; _sched_log(s, "gate agent said DONE — stopping"); break
            else:
                _sched_log(s, f"gate: {s['gate']}")
                try:
                    rc = subprocess.run(["/bin/sh", "-c", s["gate"]], cwd=workdir,
                                        capture_output=True, text=True, timeout=300).returncode
                except subprocess.TimeoutExpired:
                    rc = 124; _sched_log(s, "gate timed out")
                s["last_gate_exit"] = rc
                _sched_log(s, f"gate exit {rc}")
                if rc == 0:
                    s["state"] = "converged"; _sched_log(s, "gate passed — stopping"); break
        s["runs_done"] += 1
        _sched_log(s, f"run {s['runs_done']}/{s['max_runs']} started")
        if not _run_workflow_once(s):
            break  # error state already set
        if s["runs_done"] >= s["max_runs"]:
            s["state"] = "capped"; _sched_log(s, f"reached max_runs {s['max_runs']} — stopping"); break
        s["next_tick_at"] = time.time() + s["interval_sec"]
        waited = 0
        while waited < s["interval_sec"] and not s["stop"].is_set():
            time.sleep(1); waited += 1
        s["next_tick_at"] = 0
    if s["stop"].is_set() and s["state"] not in ("converged", "capped", "error"):
        s["state"] = "stopped"; _sched_log(s, "stopped")
    save_schedules()


def start_schedule(body):
    """Create + launch a schedule from a request body. Returns (id) or raises ValueError."""
    wf = body.get("workflow") or {}
    if not wf.get("graph", {}).get("nodes"):
        raise ValueError("workflow has no blocks")
    workdir = (wf.get("workdir") or "").strip()
    if workdir and not Path(workdir).is_dir():
        raise ValueError(f"workdir not a directory: {workdir}")
    s = {
        "id": _new_sched_id(),
        "name": wf.get("name") or "Workflow",
        "wf": wf,
        "binary": (body.get("binary") or "").strip() or find_binary(),
        "interval_sec": max(5, int(body.get("interval_sec", 300))),
        "gate": (body.get("gate") or "").strip(),
        "gate_kind": "agent" if body.get("gate_kind") == "agent" else "command",
        "gate_agent": (body.get("gate_agent") or "Claude").strip(),
        "max_runs": max(1, int(body.get("max_runs", 10))),
        "runs_done": 0,
        "state": "pending",
        "created": time.time(),
        "next_tick_at": 0,
        "last_gate_exit": None,
        "log": [],
        "stop": threading.Event(),
    }
    SCHEDULES[s["id"]] = s
    save_schedules()
    threading.Thread(target=run_schedule, args=(s,), daemon=True).start()
    return s["id"]


def schedule_view(s):
    nt = s.get("next_tick_at") or 0
    return {
        "id": s["id"], "name": s["name"], "state": s["state"],
        "runs_done": s["runs_done"], "max_runs": s["max_runs"],
        "interval_sec": s["interval_sec"], "gate": s["gate"], "gate_kind": s["gate_kind"],
        "gate_agent": s.get("gate_agent", ""),
        "last_gate_exit": s["last_gate_exit"], "log": s["log"][-25:],
        "created": s.get("created", 0),
        "next_in": max(0, round(nt - time.time())) if nt else None,
        "steps": [n.get("name") or f"Block {n['id']}" for n in (s.get("wf") or {}).get("graph", {}).get("nodes", [])],
        "workdir": (s.get("wf") or {}).get("workdir", ""),
    }


# --------------------------------------------------------------------------- #
# HTTP handler
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, *a):
        pass

    def _send(self, code, ctype, body):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        # Never cache — the UI must always match the running bridge.
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code, obj):
        self._send(code, "application/json", json.dumps(obj))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", (HERE / "index.html").read_bytes())
        elif parsed.path == "/pipelines":
            names = sorted(p.stem for p in pipelines_dir().glob("*.toml"))
            self._json(200, {"pipelines": names})
        elif parsed.path == "/pipeline":
            name = (parse_qs(parsed.query).get("name") or [""])[0]
            safe = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
            path = pipelines_dir() / f"{safe}.toml"
            if not safe or not path.is_file():
                self._json(404, {"error": "not found"})
                return
            try:
                doc = tomllib.loads(path.read_text())
                self._json(200, {"name": safe, "graph": toml_to_graph(doc)})
            except Exception as e:
                self._json(400, {"error": f"parse failed: {e}"})
        elif parsed.path == "/health":
            b = find_binary()
            self._json(200, {
                "binary": b,
                "binary_found": Path(b).is_file() or shutil.which(b) is not None,
                "stale": binary_stale(b),
                "agents": agent_names(),
                "config_found": bool(load_config()),
            })
        elif parsed.path == "/agents":
            self._json(200, {"agents": agent_names()})
        elif parsed.path == "/runs":
            self._json(200, {"runs": [run_view(r) for r in sorted(RUNS.values(), key=lambda r: r["created"])]})
        elif re.fullmatch(r"/ai-tasks/[^/]+", parsed.path):
            t = AI_TASKS.get(parsed.path.split("/")[2])
            if not t:
                self._json(404, {"error": "no such task"})
            else:
                self._json(200, {"state": t["state"], "elapsed": round(time.time() - t["t0"]),
                                 "log": t["log"][-12:], "result": t["result"], "error": t["error"]})
        elif re.fullmatch(r"/runs/[^/]+/events", parsed.path):
            self._run_events(parsed.path.split("/")[2],
                             int((parse_qs(parsed.query).get("since") or ["0"])[0]))
        elif parsed.path == "/step-transcript":
            sid = (parse_qs(parsed.query).get("session") or [""])[0]
            if not re.fullmatch(r"[0-9a-fA-F-]{8,64}", sid):
                self._json(400, {"error": "bad session id"})
                return
            hit = next((Path.home() / ".claude" / "projects").glob(f"*/{sid}.jsonl"), None)
            if not hit:
                self._json(404, {"error": "no transcript found for this session (claude sessions only)"})
                return
            self._json(200, {"transcript": render_claude_transcript(hit)})
        elif parsed.path == "/runs-history":
            self._json(200, {"runs": list_run_history()})
        elif parsed.path == "/schedules":
            self._json(200, {"schedules": [schedule_view(s) for s in SCHEDULES.values()]})
        elif parsed.path == "/run-artifacts":
            self._run_artifacts((parse_qs(parsed.query).get("dir") or [""])[0])
        else:
            self._send(404, "text/plain", "not found")

    def _run_artifacts(self, d):
        # Read-only listing of a run directory's markdown outputs. Sandboxed:
        # the path must resolve to a real dir that looks like a HoA run dir.
        try:
            p = Path(d).resolve()
        except (OSError, ValueError):
            self._json(400, {"error": "bad path"})
            return
        if not p.is_dir() or not ((p / "session.toml").exists() or (p / "pipeline.toml").exists()):
            self._json(404, {"error": "not a run directory"})
            return
        cap = 200_000
        files, inputs = [], []
        def add(coll, name, fp):
            try:
                coll.append({"name": name, "content": fp.read_text(errors="replace")[:cap]})
            except OSError:
                pass
        for f in sorted(p.glob("*.md")):
            if f.name.startswith("_input_"):
                add(inputs, f.name.removeprefix("_input_"), f)
            elif not f.name.startswith("_"):
                add(files, f.name, f)
        fin = p / "finalization"
        if fin.is_dir():
            for f in sorted(fin.glob("*.md")):
                add(files, "finalization/" + f.name, f)
        self._json(200, {"files": files, "inputs": inputs})

    def do_POST(self):
        if self.path == "/pipeline":
            self._save_pipeline()
        elif self.path == "/run":
            self._run()
        elif re.fullmatch(r"/ai-tasks/(rewrite|optimize|contracts|plan|breaks)", self.path):
            kind = self.path.split("/")[2]
            try:
                self._json(200, {"task_id": start_ai_task(kind, self._read_json())})
            except json.JSONDecodeError:
                self._json(400, {"error": "bad json"})
        elif re.fullmatch(r"/ai-tasks/[^/]+/cancel", self.path):
            t = AI_TASKS.get(self.path.split("/")[2])
            if t:
                t["cancel"].set()
            self._json(200 if t else 404, {"cancelled": bool(t)})
        elif re.fullmatch(r"/human-gates/[^/]+/continue", self.path):
            self._gate_continue(self.path.split("/")[2])
        elif re.fullmatch(r"/human-gates/[^/]+/discard", self.path):
            self._gate_discard(self.path.split("/")[2])
        elif re.fullmatch(r"/runs/[^/]+/stop", self.path):
            self._run_stop(self.path.split("/")[2])
        elif self.path == "/schedules":
            try:
                sid = start_schedule(self._read_json())
                self._json(200, {"id": sid})
            except (ValueError, json.JSONDecodeError) as e:
                self._json(400, {"error": str(e)})
        elif re.fullmatch(r"/schedules/[^/]+/stop", self.path):
            sid = self.path.split("/")[2]
            s = SCHEDULES.get(sid)
            if not s:
                self._json(404, {"error": "no such schedule"})
            else:
                s["stop"].set()
                self._json(200, {"stopped": sid})
        elif re.fullmatch(r"/schedules/[^/]+/delete", self.path):
            sid = self.path.split("/")[2]
            s = SCHEDULES.pop(sid, None)
            if s:
                s["stop"].set()
            save_schedules()
            self._json(200 if s else 404, {"deleted": sid} if s else {"error": "no such schedule"})
        elif re.fullmatch(r"/schedules/[^/]+/run-now", self.path):
            sid = self.path.split("/")[2]
            s = SCHEDULES.get(sid)
            if not s:
                self._json(404, {"error": "no such schedule"})
                return
            # Launch the schedule's workflow once as a normal detached run —
            # it shows up in the Runs view like any manual run.
            try:
                tmp, jobs, plan = expand_workflow(s["wf"], 0, s["binary"], 0)
            except ValueError as e:
                self._json(400, {"error": str(e)})
                return
            nodes = s["wf"].get("graph", {}).get("nodes", [])
            edges = s["wf"].get("graph", {}).get("edges", [])
            steps = [{"id": n["id"], "name": n.get("name") or f"Block {n['id']}",
                      "deps": [e["from"] for e in edges if e["to"] == n["id"]]} for n in nodes]
            run = _new_run(s["name"] + " (manual)", plan[0]["task"] if plan else "",
                           bool(s["wf"].get("allowEdits")), steps)
            _push(run, {"event": "plan", "jobs": plan})
            threading.Thread(target=_bg_jobs, args=(run, jobs, [], 2, [tmp]), daemon=True).start()
            self._json(200, {"run_id": run["id"]})
        else:
            self._send(404, "text/plain", "not found")

    def _run_gated(self, wf, binary):
        graph = wf.get("graph", {})
        if not graph.get("nodes"):
            self._send(400, "text/plain", "workflow has no blocks")
            return
        if graph.get("loops"):
            self._send(400, "text/plain", "human gates cannot be combined with a loop (the gate would break the cycle)")
            return
        tasks = split_tasks(wf.get("tasks", "")) or [None]
        if len(tasks) > 1:
            self._send(400, "text/plain", "human-gated workflows run one task at a time (approvals are per run)")
            return
        workdir = (wf.get("workdir") or "").strip()
        if workdir and not Path(workdir).is_dir():
            self._send(400, "text/plain", f"workdir not a directory: {workdir}")
            return
        nodes = [dict(n) for n in graph.get("nodes", [])]
        deps = {}
        for e in graph.get("edges", []):
            deps.setdefault(str(e["to"]), []).append(e["from"])
        segments = split_at_gates(nodes)
        wfname = wf.get("name") or "Workflow"
        state = {
            "wfname": wfname, "binary": binary, "task": tasks[0],
            "workdir": workdir, "allow_edits": bool(wf.get("allowEdits")),
            "memory": bool(wf.get("memory")), "max_calls": wf.get("maxCalls"),
            "segments": segments, "seg_no": 0, "wt": None,
            "deps": deps, "outputs": {},
            "node_names": {str(n["id"]): (n.get("name") or f"Block {n['id']}") for n in nodes},
        }
        pre = [{"event": "plan", "jobs": [{
            "id": 0, "wf": 0, "wfName": wfname,
            "steps": [s.get("name") or f"Block {s['id']}" for s in nodes],
            "task": (tasks[0] or "").split("\n")[0][:80] if tasks[0] else "—",
            "edits": bool(wf.get("allowEdits")), "gated": True}]}]
        if state["allow_edits"] and is_git_repo(workdir):
            try:
                # One worktree spans all segments; finalized after the last one.
                state["wt"] = make_workflow_worktree(workdir, slugify(wfname), len(segments))
                pre.append({"event": "worktree", "branch": state["wt"]["branch"]})
            except RuntimeError as e:
                self._send(400, "text/plain", str(e))
                return
        run_steps = [{"id": n["id"], "name": n.get("name") or f"Block {n['id']}",
                      "deps": deps.get(str(n["id"]), [])} for n in nodes]
        run = _new_run(wfname, (tasks[0] or "").split("\n")[0][:80] if tasks[0] else "—",
                       bool(wf.get("allowEdits")), run_steps)
        for ev in pre:
            _push(run, ev)
        threading.Thread(target=_bg_segments, args=(run, state), daemon=True).start()
        self._json(200, {"run_id": run["id"]})

    def _gate_continue(self, gid):
        state = HUMAN_GATES.pop(gid, None)
        if not state:
            self._json(404, {"error": "no such gate (serve.py restarted?)"})
            return
        save_gates()
        try:
            body = self._read_json()
        except json.JSONDecodeError:
            body = {}
        approved = (body.get("output") or "").strip()
        if approved and state.get("gated_id"):
            # The human-edited text replaces the gated step's output.
            state["outputs"][state["gated_id"]] = approved
        run = RUNS.get(state.get("run_id") or "")
        if not run:
            # serve.py restarted since the gate was created — new run entry
            run = _new_run(state["wfname"], state.get("task") or "", state["allow_edits"],
                           [{"id": n["id"], "name": n.get("name") or f"Block {n['id']}",
                             "deps": state["deps"].get(str(n["id"]), [])}
                            for seg in state["segments"] for n in seg])
        run["gate"] = None
        run["state"] = "running"
        _push(run, {"event": "log", "message": "✔ gate approved — continuing"})
        threading.Thread(target=_bg_segments, args=(run, state), daemon=True).start()
        self._json(200, {"run_id": run["id"]})

    def _run_events(self, rid, since):
        """Long-poll a run's buffered events from `since`."""
        r = RUNS.get(rid)
        if not r:
            self._json(404, {"error": "no such run (bridge restarted?)"})
            return
        deadline = time.time() + 25
        while time.time() < deadline and len(r["events"]) <= since and r["state"] in ("running", "gated"):
            time.sleep(0.3)
        evs = r["events"][since:since + 500]
        self._json(200, {"events": evs, "next": since + len(evs),
                         "state": r["state"], "total": len(r["events"])})

    def _run_stop(self, rid):
        r = RUNS.get(rid)
        if not r:
            self._json(404, {"error": "no such run"})
            return
        r["cancel"].set()
        if r.get("gate"):
            state = HUMAN_GATES.pop(r["gate"], None)
            save_gates()
            if state and state.get("wt"):
                _git(["worktree", "remove", "--force", state["wt"]["tmp"]], cwd=state["wt"]["repo"])
            r["gate"] = None
        r["state"] = "stopped"
        _push(r, {"event": "log", "message": "⏹ stopped by you"})
        _push(r, {"event": "all_tasks_done"})
        self._json(200, {"stopped": rid})

    def _gate_discard(self, gid):
        state = HUMAN_GATES.pop(gid, None)
        save_gates()
        if not state:
            self._json(404, {"error": "no such gate"})
            return
        wt = state.get("wt")
        if wt:
            _git(["worktree", "remove", "--force", wt["tmp"]], cwd=wt["repo"])
        run = RUNS.get(state.get("run_id") or "")
        if run:
            run["gate"] = None
            run["state"] = "stopped"
            _push(run, {"event": "log", "message": "✖ discarded at human gate"})
            _push(run, {"event": "all_tasks_done"})
        self._json(200, {"discarded": gid})

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def _save_pipeline(self):
        try:
            body = self._read_json()
        except json.JSONDecodeError:
            self._json(400, {"error": "bad json"})
            return
        name = body.get("name", "").strip()
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
        if not safe:
            self._json(400, {"error": "invalid name"})
            return
        toml_text = build_pipeline_toml(body.get("graph", {}))
        (pipelines_dir() / f"{safe}.toml").write_text(toml_text)
        self._json(200, {"saved": safe})

    def _run(self):
        try:
            cfg = self._read_json()
        except json.JSONDecodeError:
            self._send(400, "text/plain", "bad json")
            return

        parallel = max(1, int(cfg.get("parallel", 2)))
        binary = cfg.get("binary", "").strip() or find_binary()
        workflows = cfg.get("workflows", [])
        if not workflows:
            self._send(400, "text/plain", "no workflows")
            return

        # Human-gated workflow: split at gates and run segment by segment.
        if len(workflows) == 1 and any(n.get("gate") for n in workflows[0].get("graph", {}).get("nodes", [])):
            self._run_gated(workflows[0], binary)
            return

        tmp_files, jobs, plan, wts = [], [], [], []

        def fail(msg):
            for w in wts:  # undo worktrees created before the failure
                _git(["worktree", "remove", "--force", w["tmp"]], cwd=w["repo"])
            self._send(400, "text/plain", msg)

        for wi, wf in enumerate(workflows):
            try:
                tp, js, pl = expand_workflow(wf, wi, binary, len(jobs))
            except ValueError as e:
                fail(str(e))
                return
            tmp_files.append(tp)
            # One worktree per workflow: its tasks share it (and see each
            # other's edits); separate workflows stay isolated.
            workdir = (wf.get("workdir") or "").strip()
            if wf.get("allowEdits") and is_git_repo(workdir):
                try:
                    wt = make_workflow_worktree(workdir, slugify(wf.get("name") or f"workflow_{wi + 1}"), len(js))
                except RuntimeError as e:
                    fail(str(e))
                    return
                wts.append(wt)
                for j in js:
                    j["wt"] = wt
            jobs += js; plan += pl

        wf0 = workflows[0]
        run_steps = [{"id": n["id"], "name": n.get("name") or f"Block {n['id']}",
                      "deps": [e["from"] for e in wf0.get("graph", {}).get("edges", []) if e["to"] == n["id"]]}
                     for n in wf0.get("graph", {}).get("nodes", [])]
        run = _new_run(wf0.get("name") or "workflow",
                       (plan[0]["task"] if plan else ""), bool(wf0.get("allowEdits")), run_steps)
        _push(run, {"event": "plan", "jobs": plan})
        if binary_stale(binary):
            _push(run, {"event": "warning", "message": STALE_MSG})
        threading.Thread(target=_bg_jobs, args=(run, jobs, wts, parallel, tmp_files), daemon=True).start()
        self._json(200, {"run_id": run["id"]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    load_schedules()
    load_gates()
    srv = ThreadingHTTPServer((args.host, args.port), partial(Handler))
    print(f"House of Agents web UI  ->  http://{args.host}:{args.port}", flush=True)
    print(f"Using binary: {find_binary()}", flush=True)
    if binary_stale(find_binary()):
        print(f"WARNING: {STALE_MSG}", flush=True)
    print(f"Saved pipelines: {pipelines_dir()}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
