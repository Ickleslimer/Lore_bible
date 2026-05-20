<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount, tick } from "svelte";
  import { Play, RotateCcw, Square, Sparkles } from "lucide-svelte";
  import { cancelPipeline, createRun, pipelineLogTail, pipelineStatus, startPipeline } from "../lib/api";
  import type { AppState, PipelineRuntimeStatus } from "../lib/types";

  export let state: AppState;
  export let disabled = false;

  const dispatch = createEventDispatcher<{ stateChanged: AppState; refresh: void }>();
  let runtime: PipelineRuntimeStatus = { status: "idle", message: "", logs: [] };
  let ignorePending = false;
  let busy = false;
  let error = "";
  let pollTimer: number | undefined;
  let logEl: HTMLPreElement | null = null;
  let polling = false;
  let loadingLogs = false;
  let logLines: string[] = [];

  $: running = runtime.status === "running" || runtime.status === "starting";
  $: identityBypassBlocked = ignorePending && (state.counts.identity_merges ?? 0) > 0;
  $: startDisabled = disabled || busy || running || identityBypassBlocked;
  $: logText = logLines.join("\n");

  async function refreshRuntime() {
    if (polling) return;
    polling = true;
    try {
      runtime = await pipelineStatus();
    } finally {
      polling = false;
    }
  }

  async function refreshLogs() {
    loadingLogs = true;
    try {
      const result = await pipelineLogTail(state.active_root, 300);
      logLines = result.logs ?? [];
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      loadingLogs = false;
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
  }

  function start(resume: boolean) {
    busy = true;
    error = "";
    optimisticStart(resume, state.active_root);
    startPipeline(state.active_root, { resume, ignorePending })
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
    }, 1200);
  }

  async function newRunAndStart() {
    busy = true;
    error = "";
    try {
      const newState = await createRun();
      dispatch("stateChanged", newState);
      optimisticStart(false, newState.active_root);
      startPipeline(newState.active_root, { resume: false, ignorePending })
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
      window.setTimeout(() => refreshRuntime().catch(() => undefined), 1200);
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

  $: if (logEl && logText) {
    tick().then(() => {
      if (logEl) logEl.scrollTop = logEl.scrollHeight;
    });
  }

  onMount(() => {
    refreshRuntime().catch((err) => (error = err instanceof Error ? err.message : String(err)));
    refreshLogs().catch(() => undefined);
    pollTimer = window.setInterval(() => {
      refreshRuntime().catch(() => undefined);
    }, 6000);
  });

  onDestroy(() => {
    if (pollTimer !== undefined) window.clearInterval(pollTimer);
  });
</script>

<section class="pipeline-control">
  <div class="control-grid">
    <article class="control-card primary-control">
      <span class="caption">Pipeline Controls</span>
      <h3>{runtime.status === "idle" ? "Ready" : runtime.status}</h3>
      <p>{runtime.message || "Choose how to run the selected artifact folder."}</p>

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
        <span>Ignore pending review gates</span>
      </label>
      {#if identityBypassBlocked}
        <p class="soft-warning">
          Identity clusters need explicit review before this gate can be ignored. Refresh after approving or rejecting them.
        </p>
      {/if}
    </article>

    <article class="control-card">
      <span class="caption">Selected Run</span>
      <h3>{state.active_label}</h3>
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

  <section class="log-panel">
    <div class="panel-heading">
      <span class="caption">Worker Output</span>
      <button class="ghost-button" disabled={loadingLogs} on:click={refreshLogs}>
        {loadingLogs ? "Loading..." : "Refresh Logs"}
      </button>
    </div>
    <pre bind:this={logEl}>{logText || "No worker output yet."}</pre>
  </section>
</section>
