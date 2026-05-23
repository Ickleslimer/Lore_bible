use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;

#[cfg(windows)]
use std::os::windows::process::CommandExt;

const CREATE_NO_WINDOW: u32 = 0x08000000;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x00000008;

#[derive(Default)]
struct PipelineProcessState {
    status: String,
    message: String,
    logs: Vec<String>,
    child_pid: Option<u32>,
    artifacts_root: Option<String>,
    started_at: Option<String>,
    finished_at: Option<String>,
    last_exit_code: Option<i32>,
}

type SharedPipelineState = Arc<Mutex<PipelineProcessState>>;

#[derive(Debug, Serialize, Deserialize)]
struct BridgeEnvelope {
    ok: bool,
    result: Option<Value>,
    error: Option<String>,
}

fn looks_like_repo_root(path: &Path) -> bool {
    path.join("config").join("pipeline_config.json").exists()
        && path.join("pipeline").join("run_pipeline.py").exists()
}

fn find_repo_root(explicit: Option<String>) -> Result<PathBuf, String> {
    if let Some(raw) = explicit {
        if !raw.trim().is_empty() {
            let path = PathBuf::from(raw);
            if path.exists() {
                return path.canonicalize().map_err(|err| err.to_string());
            }
        }
    }
    if let Ok(env_root) = std::env::var("THERIAC_LORE_ROOT") {
        let path = PathBuf::from(env_root);
        if path.exists() {
            return path.canonicalize().map_err(|err| err.to_string());
        }
    }
    let mut starts = Vec::new();
    if let Ok(current) = std::env::current_dir() {
        starts.push(current);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            starts.push(parent.to_path_buf());
        }
    }
    for start in starts {
        for candidate in start.ancestors() {
            if looks_like_repo_root(candidate) {
                return candidate.canonicalize().map_err(|err| err.to_string());
            }
        }
    }
    std::env::current_dir().map_err(|err| err.to_string())
}

fn python_executable() -> String {
    std::env::var("THERIAC_PYTHON").unwrap_or_else(|_| "python".to_string())
}

fn no_window(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }
}

fn detached_worker(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
    }
}

fn now_string() -> String {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => format!("{}", duration.as_secs()),
        Err(_) => String::new(),
    }
}

fn write_diagnostic(root: Option<&Path>, message: &str) {
    let base = root
        .map(Path::to_path_buf)
        .or_else(|| find_repo_root(None).ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let path = base.join("artifacts").join("tauri_diagnostics.log");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} | {}", now_string(), message);
    }
}

fn worker_log_path(artifacts: &Path) -> PathBuf {
    artifacts.join("tauri_pipeline_worker.log")
}

fn append_worker_log(artifacts: &Path, line: &str) {
    let path = worker_log_path(artifacts);
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{}", line);
    }
}

fn read_log_tail(path: &Path, max_lines: usize) -> Vec<String> {
    let Ok(file) = std::fs::File::open(path) else {
        return Vec::new();
    };
    let reader = BufReader::new(file);
    let mut lines = Vec::new();
    for line in reader.lines().map_while(Result::ok) {
        lines.push(line);
        let extra = lines.len().saturating_sub(max_lines);
        if extra > 0 {
            lines.drain(0..extra);
        }
    }
    lines
}

fn bounded_log_preview(
    lines: Vec<String>,
    max_lines: usize,
    max_chars_per_line: usize,
) -> Vec<String> {
    let start = lines.len().saturating_sub(max_lines);
    lines
        .into_iter()
        .skip(start)
        .map(|line| {
            if line.chars().count() <= max_chars_per_line {
                line
            } else {
                let mut truncated: String = line.chars().take(max_chars_per_line).collect();
                truncated.push_str("...");
                truncated
            }
        })
        .collect()
}

fn is_progress_log_line(line: &str) -> bool {
    let lower = line.to_lowercase();
    line.contains("] START ")
        || line.contains("] DONE ")
        || line.contains("] SKIP ")
        || lower.contains("desktop:")
        || lower.contains("progress:")
        || lower.contains("batch progress")
        || lower.contains("model call")
        || lower.contains("model window")
        || lower.contains("requesting model")
        || (lower.contains("sending ") && lower.contains("prompt"))
        || lower.contains("paused for review")
        || lower.contains("requiring review")
        || lower.contains("complete:")
        || lower.contains("runtimeerror")
        || lower.contains("traceback")
}

fn progress_log_preview(
    lines: &[String],
    max_lines: usize,
    max_chars_per_line: usize,
) -> Vec<String> {
    let filtered = lines
        .iter()
        .filter(|line| is_progress_log_line(line))
        .cloned()
        .collect();
    bounded_log_preview(filtered, max_lines, max_chars_per_line)
}

fn pipeline_snapshot(state: &SharedPipelineState, include_logs: bool) -> Value {
    let guard = state.lock().expect("pipeline state poisoned");
    json!({
        "status": guard.status,
        "message": guard.message,
        "logs": if include_logs { guard.logs.clone() } else { Vec::<String>::new() },
        "child_pid": guard.child_pid,
        "artifacts_root": guard.artifacts_root,
        "started_at": guard.started_at,
        "finished_at": guard.finished_at,
        "last_exit_code": guard.last_exit_code,
    })
}

#[tauri::command]
async fn python_bridge(
    repo_root: Option<String>,
    command: String,
    payload: Value,
) -> Result<BridgeEnvelope, String> {
    let root = find_repo_root(repo_root)?;
    tauri::async_runtime::spawn_blocking(move || run_python_bridge_request(root, command, payload))
        .await
        .map_err(|err| format!("Python bridge task failed: {err}"))?
}

fn run_python_bridge_request(
    root: PathBuf,
    bridge_command: String,
    payload: Value,
) -> Result<BridgeEnvelope, String> {
    let request = json!({
        "repo_root": root,
        "command": bridge_command,
        "payload": payload,
    });
    let mut command = Command::new(python_executable());
    command
        .args(["-m", "pipeline.tauri_bridge"])
        .current_dir(&root)
        .env("PYTHONIOENCODING", "utf-8")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    no_window(&mut command);
    let mut child = command
        .spawn()
        .map_err(|err| format!("Could not start Python bridge: {err}"))?;

    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(request.to_string().as_bytes())
            .map_err(|err| format!("Could not write bridge request: {err}"))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("Python bridge failed: {err}"))?;
    if !output.status.success() {
        return Ok(BridgeEnvelope {
            ok: false,
            result: None,
            error: Some(String::from_utf8_lossy(&output.stderr).trim().to_string()),
        });
    }
    serde_json::from_slice::<BridgeEnvelope>(&output.stdout)
        .map_err(|err| format!("Could not parse Python bridge response: {err}"))
}

#[tauri::command]
fn pipeline_status(state: tauri::State<'_, SharedPipelineState>) -> Value {
    pipeline_snapshot(&state, false)
}

#[tauri::command]
fn pipeline_log_tail(
    state: tauri::State<'_, SharedPipelineState>,
    artifacts_root: Option<String>,
    max_lines: Option<usize>,
) -> Value {
    write_diagnostic(None, "pipeline_log_tail invoked");
    let root = artifacts_root.or_else(|| {
        state
            .lock()
            .ok()
            .and_then(|guard| guard.artifacts_root.clone())
    });
    let lines = root
        .map(|raw| {
            read_log_tail(
                &worker_log_path(&PathBuf::from(raw)),
                max_lines.unwrap_or(250),
            )
        })
        .unwrap_or_default();
    let total_lines = lines.len();
    let preview = bounded_log_preview(lines, 30, 360);
    write_diagnostic(
        None,
        &format!("pipeline_log_tail returned {} line(s)", preview.len()),
    );
    json!({ "logs": preview, "total_lines": total_lines })
}

#[tauri::command]
fn pipeline_progress_tail(
    state: tauri::State<'_, SharedPipelineState>,
    artifacts_root: Option<String>,
    max_lines: Option<usize>,
) -> Value {
    let root = artifacts_root.or_else(|| {
        state
            .lock()
            .ok()
            .and_then(|guard| guard.artifacts_root.clone())
    });
    let lines = root
        .map(|raw| {
            read_log_tail(
                &worker_log_path(&PathBuf::from(raw)),
                max_lines.unwrap_or(120),
            )
        })
        .unwrap_or_default();
    let latest_line = lines.last().cloned().unwrap_or_default();
    let latest_preview = bounded_log_preview(vec![latest_line], 1, 260)
        .pop()
        .unwrap_or_default();
    let progress = progress_log_preview(&lines, 8, 260);
    let latest_progress_line = progress.last().cloned().unwrap_or_default();
    json!({
        "latest_line": latest_preview,
        "latest_progress_line": latest_progress_line,
        "lines": progress,
        "total_scanned": lines.len(),
        "updated_at_epoch": now_string(),
    })
}

#[tauri::command]
fn pipeline_start(
    state: tauri::State<'_, SharedPipelineState>,
    repo_root: Option<String>,
    artifacts_root: String,
    resume: bool,
    ignore_pending: bool,
    start_stage: Option<i32>,
) -> Result<Value, String> {
    let root = find_repo_root(repo_root)?;
    let artifacts = PathBuf::from(&artifacts_root);
    let artifacts = if artifacts.is_absolute() {
        artifacts
    } else {
        root.join(artifacts)
    };
    write_diagnostic(
        Some(&root),
        &format!(
            "pipeline_start invoked artifacts={} resume={} ignore_pending={} start_stage={:?}",
            artifacts.display(),
            resume,
            ignore_pending,
            start_stage
        ),
    );
    {
        let mut guard = state.lock().map_err(|err| err.to_string())?;
        if guard.child_pid.is_some() && guard.status == "running" {
            return Err("Pipeline is already running.".to_string());
        }
        guard.status = "starting".to_string();
        guard.message = if resume {
            "Pipeline resume is starting.".to_string()
        } else {
            "Full pipeline is starting.".to_string()
        };
        guard.logs.clear();
        guard.logs.push("Queued pipeline worker start.".to_string());
        guard.child_pid = None;
        guard.artifacts_root = Some(artifacts.to_string_lossy().to_string());
        guard.started_at = Some(now_string());
        guard.finished_at = None;
        guard.last_exit_code = None;
    }

    let worker_state = state.inner().clone();
    thread::spawn(move || {
        run_pipeline_worker(worker_state, root, artifacts, resume, ignore_pending, start_stage);
    });
    write_diagnostic(
        None,
        "pipeline_start returned to UI after background thread dispatch",
    );
    Ok(pipeline_snapshot(&state, true))
}

fn configured_path(root: &Path, raw: Option<String>, fallback: &str) -> PathBuf {
    let value = raw
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| fallback.to_string());
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

fn configured_pipeline_inputs(root: &Path) -> (PathBuf, PathBuf) {
    let config_path = root.join("config").join("pipeline_config.json");
    let mut docx: Option<String> = None;
    let mut conversations: Option<String> = None;
    if let Ok(text) = std::fs::read_to_string(config_path) {
        if let Ok(config) = serde_json::from_str::<Value>(&text) {
            if let Some(paths) = config.get("paths").and_then(|value| value.as_object()) {
                docx = paths
                    .get("docx_lore_bible")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
                conversations = paths
                    .get("discord_conversations_root")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string());
            }
        }
    }
    (
        configured_path(root, docx, "theriac-coda---lore-bible.docx"),
        configured_path(root, conversations, "discord_conversations"),
    )
}

fn run_pipeline_worker(
    state: SharedPipelineState,
    root: PathBuf,
    artifacts: PathBuf,
    resume: bool,
    ignore_pending: bool,
    start_stage: Option<i32>,
) {
    write_diagnostic(Some(&root), "background worker preparing Python command");
    let (docx, conversations) = configured_pipeline_inputs(&root);
    if !docx.exists() {
        let mut guard = state.lock().expect("pipeline state poisoned");
        guard.status = "failed".to_string();
        guard.message = format!("Lore bible DOCX not found: {}", docx.display());
        guard.finished_at = Some(now_string());
        return;
    }
    if !conversations.exists() {
        let mut guard = state.lock().expect("pipeline state poisoned");
        guard.status = "failed".to_string();
        guard.message = format!(
            "Conversations folder not found: {}",
            conversations.display()
        );
        guard.finished_at = Some(now_string());
        return;
    }

    let mut args = vec![
        "-u".to_string(),
        "-m".to_string(),
        "pipeline.run_pipeline".to_string(),
        "--docx".to_string(),
        docx.to_string_lossy().to_string(),
        "--conversations-root".to_string(),
        conversations.to_string_lossy().to_string(),
        "--artifacts-root".to_string(),
        artifacts.to_string_lossy().to_string(),
        "--log-level".to_string(),
        "INFO".to_string(),
    ];
    if resume {
        args.push("--resume".to_string());
    }
    if ignore_pending {
        args.push("--ignore-pending".to_string());
    }
    if let Some(stage) = start_stage {
        if stage >= 1 && stage <= 12 {
            args.push("--start-stage".to_string());
            args.push(stage.to_string());
        }
    }

    let log_path = worker_log_path(&artifacts);
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    append_worker_log(
        &artifacts,
        &format!(
            "{} | desktop: starting pipeline worker resume={} ignore_pending={}",
            now_string(),
            resume,
            ignore_pending
        ),
    );
    let stdout_file = match OpenOptions::new().create(true).append(true).open(&log_path) {
        Ok(file) => file,
        Err(err) => {
            let mut guard = state.lock().expect("pipeline state poisoned");
            guard.status = "failed".to_string();
            guard.message = format!("Could not open worker log file: {err}");
            guard.finished_at = Some(now_string());
            return;
        }
    };
    let stderr_file = match stdout_file.try_clone() {
        Ok(file) => file,
        Err(err) => {
            let mut guard = state.lock().expect("pipeline state poisoned");
            guard.status = "failed".to_string();
            guard.message = format!("Could not attach worker stderr log: {err}");
            guard.finished_at = Some(now_string());
            return;
        }
    };

    let mut command = Command::new(python_executable());
    command
        .args(&args)
        .current_dir(&root)
        .env("PYTHONIOENCODING", "utf-8")
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    detached_worker(&mut command);
    let mut child = command
        .spawn()
        .map_err(|err| {
            let mut guard = state.lock().expect("pipeline state poisoned");
            guard.status = "failed".to_string();
            guard.message = format!("Could not start pipeline worker: {err}");
            guard.finished_at = Some(now_string());
            write_diagnostic(Some(&root), &format!("pipeline worker spawn failed: {err}"));
        })
        .ok();
    let Some(mut child) = child.take() else {
        return;
    };
    let pid = child.id();

    {
        let mut guard = state.lock().expect("pipeline state poisoned");
        guard.status = "running".to_string();
        guard.message = if resume {
            "Pipeline resume started.".to_string()
        } else {
            "Full pipeline started.".to_string()
        };
        guard.logs.clear();
        guard.logs.push(format!("Started pipeline process {pid}."));
        guard.child_pid = Some(pid);
        guard.artifacts_root = Some(artifacts.to_string_lossy().to_string());
        guard.started_at = Some(now_string());
        guard.finished_at = None;
        guard.last_exit_code = None;
    }
    write_diagnostic(Some(&root), &format!("pipeline worker spawned pid={pid}"));
    let result = child.wait();
    let mut guard = state.lock().expect("pipeline state poisoned");
    guard.child_pid = None;
    guard.finished_at = Some(now_string());
    match result {
        Ok(status) => {
            let code = status.code();
            guard.last_exit_code = code;
            if status.success() {
                guard.status = "succeeded".to_string();
                guard.message = "Pipeline completed.".to_string();
            } else {
                guard.status = "failed".to_string();
                guard.message = format!("Pipeline stopped with exit code {}.", code.unwrap_or(-1));
            }
        }
        Err(err) => {
            guard.status = "failed".to_string();
            guard.message = format!("Pipeline wait failed: {err}");
        }
    }
    append_worker_log(
        &artifacts,
        &format!("{} | desktop: {}", now_string(), guard.message),
    );
    write_diagnostic(
        Some(&root),
        &format!("pipeline worker finished: {}", guard.message),
    );
}

#[tauri::command]
fn pipeline_cancel(state: tauri::State<'_, SharedPipelineState>) -> Result<Value, String> {
    let pid = {
        let guard = state.lock().map_err(|err| err.to_string())?;
        guard.child_pid
    };
    if let Some(pid) = pid {
        let mut command = Command::new("taskkill");
        command
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        no_window(&mut command);
        let output = command
            .output()
            .map_err(|err| format!("Could not cancel pipeline process: {err}"))?;
        let mut guard = state.lock().map_err(|err| err.to_string())?;
        guard.status = "cancelled".to_string();
        guard.message = "Pipeline cancellation requested.".to_string();
        guard.child_pid = None;
        guard.finished_at = Some(now_string());
        if !output.stdout.is_empty() {
            guard
                .logs
                .push(String::from_utf8_lossy(&output.stdout).trim().to_string());
        }
        if !output.stderr.is_empty() {
            guard
                .logs
                .push(String::from_utf8_lossy(&output.stderr).trim().to_string());
        }
    }
    Ok(pipeline_snapshot(&state, true))
}

pub fn run() {
    let pipeline_state = Arc::new(Mutex::new(PipelineProcessState {
        status: "idle".to_string(),
        ..PipelineProcessState::default()
    }));
    tauri::Builder::default()
        .manage(pipeline_state)
        .invoke_handler(tauri::generate_handler![
            python_bridge,
            pipeline_start,
            pipeline_status,
            pipeline_log_tail,
            pipeline_progress_tail,
            pipeline_cancel
        ])
        .run(tauri::generate_context!())
        .expect("error while running Tauri application");
}

#[cfg(test)]
mod tests {
    use super::{bounded_log_preview, progress_log_preview, read_log_tail, worker_log_path};
    use std::fs::OpenOptions;
    use std::io::Write;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "theriac_tauri_{label}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock before epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn log_tail_missing_file_returns_empty_lines() {
        let root = temp_dir("missing_log");
        let path = worker_log_path(&root);
        let lines = read_log_tail(&path, 300);
        assert!(lines.is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn log_tail_returns_only_requested_tail() {
        let root = temp_dir("tail_limit");
        let path = worker_log_path(&root);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open log");
        for index in 0..12 {
            writeln!(file, "line {index}").expect("write log line");
        }
        drop(file);

        let lines = read_log_tail(&path, 5);

        assert_eq!(
            lines,
            vec!["line 7", "line 8", "line 9", "line 10", "line 11"]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn log_tail_reads_while_log_is_open_for_append() {
        let root = temp_dir("open_writer");
        let path = worker_log_path(&root);
        let mut writer = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open writer");
        writeln!(writer, "worker started").expect("write first line");
        writer.flush().expect("flush writer");

        let lines = read_log_tail(&path, 10);

        assert_eq!(lines, vec!["worker started"]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn log_preview_bounds_lines_and_characters_for_webview() {
        let lines = vec![
            "short 1".to_string(),
            "short 2".to_string(),
            "x".repeat(20),
            "final".to_string(),
        ];

        let preview = bounded_log_preview(lines, 3, 8);

        assert_eq!(preview, vec!["short 2", "xxxxxxxx...", "final"]);
    }

    #[test]
    fn progress_preview_filters_to_pipeline_and_model_heartbeats() {
        let lines = vec![
            "ordinary debug noise".to_string(),
            "08:12:00 | INFO | pipeline.run_pipeline | [7/12] START Stage 07 Entity Resolution".to_string(),
            "08:12:01 | INFO | pipeline.stage_10_identity_merge | Sending identity merge prompt 1/3".to_string(),
            "another ordinary line".to_string(),
            "08:12:03 | INFO | pipeline.run_pipeline | [7/12] DONE  Stage 07 Entity Resolution (2.0s)".to_string(),
        ];

        let preview = progress_log_preview(&lines, 8, 260);

        assert_eq!(preview.len(), 3);
        assert!(preview[0].contains("START Stage 07 Entity Resolution"));
        assert!(preview[1].contains("Sending identity merge prompt"));
        assert!(preview[2].contains("DONE  Stage 07 Entity Resolution"));
    }

    #[test]
    fn progress_preview_bounds_line_count_and_length() {
        let lines = vec![
            "Stage 04 batch progress: 1/4".to_string(),
            "Stage 04 batch progress: 2/4".to_string(),
            format!("Stage 04 model call {}", "x".repeat(40)),
        ];

        let preview = progress_log_preview(&lines, 2, 24);

        assert_eq!(preview.len(), 2);
        assert_eq!(preview[0], "Stage 04 batch progress:...");
        assert_eq!(preview[1], "Stage 04 model call xxxx...");
    }
}
