<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from "svelte";
  import { Play, RotateCcw, Square, Sparkles } from "lucide-svelte";
  import {
    cancelPipeline,
    createRun,
    pipelineLogTail,
    pipelineProgressTail,
    pipelineStatus,
    startPipeline,
  } from "../lib/api";
  import type { AppState, PipelineRuntimeStatus } from "../lib/types";

  export let state: AppState;
  export let disabled = false;

  const dispatch = createEventDispatcher<{ stateChanged: AppState; refresh: void }>();
  let runtime: PipelineRuntimeStatus = { status: "idle", message: "", logs: [] };
  let ignorePending = false;
  let busy = false;
  let error = "";
  let pollTimer: number | undefined;
  let progressTimer: number | undefined;
  let polling = false;
  let progressPolling = false;
  let loadingLogs = false;
  let logLines: string[] = [];
  let logRequestId = 0;
  let logSummary = "";
  let progressLines: string[] = [];
  let progressLatest = "";
  let progressUpdated = "";
  let statusWarning = "";

  function withTimeout<T>(promise: Promise<T>, milliseconds: number, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        window.setTimeout(() => reject(new Error(`${label} timed out.`)), milliseconds);
      }),
    ]);
  }

  $: running = runtime.status === "running" || runtime.status === "starting";
  $: pendingBypassBlocked = ignorePending && state.pending_total > 0;
  $: startDisabled = disabled || busy || running;
  $: logText = logLines.slice(-30).join("\n");
  $: failureDetected =
    runtime.status === "failed" ||
    (runtime.last_exit_code !== null && runtime.last_exit_code !== undefined && runtime.last_exit_code !== 0) ||
    /stopped with exit code\s+(-?[1-9]\d*)/i.test(progressLatest);
  $: reviewPaused = !failureDetected && Boolean(state.progress?.review_gate);
  $: statusTone = failureDetected ? "failed" : running ? "running" : reviewPaused ? "attention" : "ready";
  $: statusLabel = failureDetected
    ? "Last Run Failed"
    : running
      ? "Running"
      : reviewPaused
        ? "Paused For Review"
        : "Ready";
  $: statusDetail = failureDetected
    ? cleanLogLine(progressLatest || runtime.message || state.progress?.summary || "Pipeline stopped before completion.")
    : runtime.message || state.progress?.summary || "Choose how to run the selected artifact folder.";
  $: activeRunName = displayRunName(state.active_label);

  async function refreshRuntime() {
    if (polling) return;
    polling = true;
    try {
      runtime = await withTimeout(pipelineStatus(), 2500, "Pipeline status refresh");
      statusWarning = "";
    } catch (err) {
      statusWarning = err instanceof Error ? err.message : String(err);
    } finally {
      polling = false;
    }
  }

  async function refreshLogs() {
    const requestId = ++logRequestId;
    loadingLogs = true;
    error = "";
    try {
      const result = await withTimeout(pipelineLogTail(state.active_root, 300), 2500, "Worker log refresh");
      if (requestId !== logRequestId) return;
      const rawLines = result.logs ?? [];
      logSummary = `${rawLines.length} log line(s) loaded. Showing the latest ${Math.min(rawLines.length, 30)}.`;
      logLines = rawLines.slice(-30).map((line) => (line.length > 360 ? `${line.slice(0, 360)}...` : line));
    } catch (err) {
      if (requestId !== logRequestId) return;
      error = err instanceof Error ? err.message : String(err);
    } finally {
      if (requestId === logRequestId) {
        loadingLogs = false;
      }
    }
  }

  async function refreshProgress() {
    if (progressPolling || !state.active_root) return;
    progressPolling = true;
    try {
      const result = await withTimeout(pipelineProgressTail(state.active_root, 120), 1800, "Progress refresh");
      progressLines = (result.lines ?? []).map(cleanLogLine);
      progressLatest = cleanLogLine(result.latest_progress_line || result.latest_line || "");
      const epoch = Number(result.updated_at_epoch || 0);
      progressUpdated = epoch > 0 ? new Date(epoch * 1000).toLocaleTimeString() : "";
    } catch {
      // Keep this deliberately quiet; progress polling should never interrupt review work.
    } finally {
      progressPolling = false;
    }
  }

  function optimisticStart(resume: boolean, targetRoot: string) {
    runtime = {
      status: "starting",
      message: resume ? "Pipeline resume is starting." : "Full pipeline is starting.",
      logs: [],
      child_pid: null,
      artifacts_root: targetRoot,
      started_at: null,
      finished_at: null,
      last_exit_code: null,
    };
    logLines = ["Start request sent to Tauri. Worker output is written to the run log."];
    logSummary = "";
    progressLines = [];
    progressLatest = "Start request sent.";
    progressUpdated = new Date().toLocaleTimeString();
  }

  function displayRunName(path: string | undefined) {
    if (!path) return "Selected run";
    return path.split(/[\\/]/).filter(Boolean).pop() ?? path;
  }

  function cleanLogLine(line: string) {
    let text = String(line || "").trim();
    text = text.replace(/^(\d{10})\s+\|\s*/, (_match, epoch) => {
      const seconds = Number(epoch);
      return Number.isFinite(seconds) ? `${new Date(seconds * 1000).toLocaleTimeString()} | ` : "";
    });
    text = text.replace(/pipeline\.stage_g_merge_engine/g, "Stage 10 Identity Merge");
    text = text.replace(/pipeline\.stage_10_identity_merge/g, "Stage 10 Identity Merge");
    text = text.replace(/pipeline\.stage_11_card_synthesis/g, "Stage 11 Card Synthesis");
    text = text.replace(/pipeline\.stage_09_claim_drafting/g, "Stage 09 Claim Drafting");
    text = text.replace(/\s+/g, " ");
    return text;
  }

  function start(resume: boolean) {
    if (
      pendingBypassBlocked &&
      !window.confirm(
        `Force past ${state.pending_total} pending review item(s)? This can spend model calls and may skip human review gates.`,
      )
    ) {
      return;
    }
    busy = true;
    error = "";
    optimisticStart(resume, state.active_root);
    withTimeout(startPipeline(state.active_root, { resume, ignorePending }), 3500, "Pipeline start")
      .then((status) => {
        runtime = status;
        logLines = status.logs?.length ? status.logs : logLines;
      })
      .catch((err) => {
      error = err instanceof Error ? err.message : String(err);
      })
      .finally(() => {
      busy = false;
      });
    window.setTimeout(() => {
      busy = false;
      refreshRuntime().catch(() => undefined);
      refreshProgress().catch(() => undefined);
      dispatch("refresh");
    }, 1200);
  }

  async function newRunAndStart() {
    busy = true;
    error = "";
    try {
      const newState = await createRun();
      dispatch("stateChanged", newState);
      optimisticStart(false, newState.active_root);
      withTimeout(startPipeline(newState.active_root, { resume: false, ignorePending }), 3500, "Pipeline start")
        .then((status) => {
          runtime = status;
          logLines = status.logs?.length ? status.logs : logLines;
        })
        .catch((err) => {
          error = err instanceof Error ? err.message : String(err);
        });
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
      window.setTimeout(() => {
        refreshRuntime().catch(() => undefined);
        refreshProgress().catch(() => undefined);
        dispatch("refresh");
      }, 1200);
    }
  }

  async function cancel() {
    busy = true;
    error = "";
    try {
      runtime = await cancelPipeline();
      logLines = runtime.logs?.length ? runtime.logs : logLines;
      dispatch("refresh");
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
    }
  }

  onMount(() => {
    refreshRuntime().catch((err) => (error = err instanceof Error ? err.message : String(err)));
    refreshProgress().catch(() => undefined);
    pollTimer = window.setInterval(() => {
      refreshRuntime().catch(() => undefined);
    }, 6000);
    progressTimer = window.setInterval(() => {
      refreshProgress().catch(() => undefined);
    }, 3500);
  });

  onDestroy(() => {
    if (pollTimer !== undefined) window.clearInterval(pollTimer);
    if (progressTimer !== undefined) window.clearInterval(progressTimer);
  });
</script>

<section class="pipeline-control">
  <div class="control-grid">
    <article class={`control-card primary-control status-${statusTone}`}>
      <span class="caption">Pipeline Controls</span>
      <h3>{statusLabel}</h3>
      <p>{statusDetail}</p>

      <div class="run-actions">
        <button disabled={startDisabled} on:click={() => start(true)}>
          <RotateCcw size={16} /> Resume Pipeline
        </button>
        <button class="secondary" disabled={startDisabled} on:click={() => start(false)}>
          <Play size={16} /> Run Active From Start
        </button>
        <button class="secondary" disabled={startDisabled} on:click={newRunAndStart}>
          <Sparkles size={16} /> New Run + Start
        </button>
        <button class="danger" disabled={disabled || busy || !running} on:click={cancel}>
          <Square size={16} /> Cancel Run
        </button>
      </div>

      <label class="check-line">
        <input type="checkbox" bind:checked={ignorePending} />
        <span>Force past pending review gates</span>
      </label>
      {#if pendingBypassBlocked}
        <p class="soft-warning">
          This will bypass {state.pending_total} pending review item(s) after confirmation.
        </p>
      {/if}
    </article>

    <article class="control-card">
      <span class="caption">Selected Run</span>
      <h3 class="selected-run-title">{activeRunName}</h3>
      <p class="selected-run-path" title={state.active_root}>{state.active_label}</p>
      <p>{state.pending_summary}</p>
      <dl>
        <div><dt>Pending</dt><dd>{state.pending_total}</dd></div>
        <div><dt>Process</dt><dd>{runtime.child_pid ?? "none"}</dd></div>
        <div><dt>Exit</dt><dd>{runtime.last_exit_code ?? "n/a"}</dd></div>
      </dl>
    </article>
  </div>

  {#if error}
    <div class="error-banner">{error}</div>
  {/if}
  {#if statusWarning}
    <div class="warning-banner">
      <span>{statusWarning}. The run log preview may still be current.</span>
      <button class="ghost-button compact" disabled={polling} on:click={refreshRuntime}>Retry Status</button>
    </div>
  {/if}

  <section class="progress-feed">
    <div class="panel-heading">
      <span class="caption">Live Progress</span>
      {#if progressUpdated}
        <span class="quiet-meta">Updated {progressUpdated}</span>
      {/if}
    </div>
    <p>{progressLatest || runtime.message || "Waiting for pipeline progress."}</p>
    {#if progressLines.length}
      <div class="progress-lines" aria-live="polite">
        {#each progressLines as line}
          <code>{line}</code>
        {/each}
      </div>
    {/if}
  </section>

  <section class="log-panel">
    <div class="panel-heading">
      <span class="caption">Worker Output</span>
      <button class="ghost-button" disabled={loadingLogs} on:click={refreshLogs}>
        {loadingLogs ? "Loading..." : "Refresh Logs"}
      </button>
    </div>
    {#if logSummary}
      <p class="log-summary">{logSummary}</p>
    {/if}
    <pre>{logText || "No worker output loaded. Click Refresh Logs to show a bounded preview."}</pre>
  </section>
</section>
