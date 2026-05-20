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
fn python_bridge(
    repo_root: Option<String>,
    command: String,
    payload: Value,
) -> Result<BridgeEnvelope, String> {
    let root = find_repo_root(repo_root)?;
    let request = json!({
        "repo_root": root,
        "command": command,
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
    let root = artifacts_root.or_else(|| {
        state
            .lock()
            .ok()
            .and_then(|guard| guard.artifacts_root.clone())
    });
    let lines = root
        .map(|raw| read_log_tail(&worker_log_path(&PathBuf::from(raw)), max_lines.unwrap_or(250)))
        .unwrap_or_default();
    json!({ "logs": lines })
}

#[tauri::command]
fn pipeline_start(
    state: tauri::State<'_, SharedPipelineState>,
    repo_root: Option<String>,
    artifacts_root: String,
    resume: bool,
    ignore_pending: bool,
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
            "pipeline_start invoked artifacts={} resume={} ignore_pending={}",
            artifacts.display(),
            resume,
            ignore_pending
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
        run_pipeline_worker(worker_state, root, artifacts, resume, ignore_pending);
    });
    write_diagnostic(None, "pipeline_start returned to UI after background thread dispatch");
    Ok(pipeline_snapshot(&state, true))
}

fn run_pipeline_worker(
    state: SharedPipelineState,
    root: PathBuf,
    artifacts: PathBuf,
    resume: bool,
    ignore_pending: bool,
) {
    write_diagnostic(Some(&root), "background worker preparing Python command");
    let docx = root.join("theriac-coda---lore-bible.docx");
    let conversations = root.join("discord_conversations");
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
        guard.message = format!("Conversations folder not found: {}", conversations.display());
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
    no_window(&mut command);
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
    write_diagnostic(Some(&root), &format!("pipeline worker finished: {}", guard.message));
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
            guard.logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
        }
        if !output.stderr.is_empty() {
            guard.logs.push(String::from_utf8_lossy(&output.stderr).trim().to_string());
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
            pipeline_cancel
        ])
        .run(tauri::generate_context!())
        .expect("error while running Tauri application");
}
