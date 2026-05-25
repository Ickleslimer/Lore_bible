<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from "svelte";
  import { AlertTriangle, CheckCircle2, CircleDashed, Clock3, Play, RefreshCcw, Sparkles } from "lucide-svelte";
  import { approveThemeRescue, loadThemeRescue, pipelineProgressTail, startPipeline } from "../lib/api";
  import type { ThemeRescueProcess, ThemeRescueResponse } from "../lib/types";

  export let artifactsRoot = "";
  export let disabled = false;
  export let initialStatus: ThemeRescueResponse | null = null;

  const dispatch = createEventDispatcher<{ changed: void }>();

  let status: ThemeRescueResponse | null = initialStatus;
  let loading = false;
  let busy = false;
  let error = "";
  let progressLatest = "";
  let progressTimer: number | undefined;

  $: processes = status?.processes ?? [];
  $: prompt = status?.prompt;
  $: showPrompt = Boolean(prompt?.show);
  $: canApproveRescue = showPrompt && !disabled && !busy;
  $: canRunRescue =
    Boolean(
      status?.enabled &&
        status?.approved &&
        status?.rescue_pending &&
        !disabled &&
        !busy,
    );
  $: canResumeRescue = canRunRescue;

  function stateLabel(state: string): string {
    switch (state) {
      case "done":
        return "Complete";
      case "stale":
        return "Needs refresh";
      case "ready":
        return "Ready";
      case "waiting":
        return "Waiting";
      case "skipped":
        return "Disabled";
      default:
        return state;
    }
  }

  function stateIcon(state: string) {
    if (state === "done") return CheckCircle2;
    if (state === "stale") return AlertTriangle;
    if (state === "ready") return Sparkles;
    return CircleDashed;
  }

  function metricLines(process: ThemeRescueProcess): string[] {
    const summary = process.summary ?? {};
    if (process.id === "04R") {
      return [
        `Candidates ${summary.candidate_window_count ?? 0}`,
        `Rescued conversations ${summary.rescued_conversation_count ?? 0}`,
        `Rescued messages ${summary.rescued_message_count ?? 0}`,
        `Failures ${summary.failure_count ?? 0}`,
      ];
    }
    return [
      `Rescue snippets ${summary.rescue_snippet_count ?? 0}`,
      `Combined corpus ${summary.combined_snippet_count ?? 0}`,
      `Strict baseline ${summary.strict_snippet_count ?? 0}`,
    ];
  }

  async function refresh() {
    if (!artifactsRoot) return;
    loading = true;
    error = "";
    try {
      status = await loadThemeRescue(artifactsRoot);
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
    }
  }

  async function refreshProgress() {
    if (!artifactsRoot) return;
    try {
      const tail = await pipelineProgressTail(artifactsRoot, 80);
      const lines = tail.lines ?? [];
      const rescueLine =
        [...lines].reverse().find((line) => /Stage 0(4R|6R)|theme rescue/i.test(line)) ||
        tail.latest_progress_line ||
        tail.latest_line ||
        "";
      progressLatest = rescueLine;
    } catch {
      // Non-fatal polling noise.
    }
  }

  async function approveRescue() {
    if (!artifactsRoot) return;
    const confirmText =
      prompt?.confirm_message ||
      "Record theme rescue approval for this run?\n\n04R/06R will not start until you choose Run 04R/06R.";
    if (!window.confirm(confirmText)) {
      return;
    }
    busy = true;
    error = "";
    try {
      status = await approveThemeRescue(artifactsRoot);
      dispatch("changed");
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
      void refresh();
    }
  }

  async function runRescue() {
    if (!artifactsRoot) return;
    const confirmText =
      "Run theme rescue (04R then 06R) on this run?\n\nThis resumes the pipeline from Stage 06 for the rescue branch only.";
    if (!window.confirm(confirmText)) {
      return;
    }
    busy = true;
    error = "";
    try {
      await startPipeline(artifactsRoot, { resume: true, startStage: 6, ignorePending: false });
      dispatch("changed");
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
    }
  }

  onMount(() => {
    if (!status && artifactsRoot) {
      void refresh();
    }
    progressTimer = window.setInterval(() => {
      void refreshProgress();
    }, 3500);
  });

  onDestroy(() => {
    if (progressTimer !== undefined) window.clearInterval(progressTimer);
  });

  $: if (artifactsRoot && initialStatus) {
    status = initialStatus;
  }
</script>

<section class="theme-rescue-panel">
  <div class="theme-rescue-header">
    <div>
      <span class="caption">Optional branch after Stage 06C/06D</span>
      <h3>Theme Rescue (04R / 06R)</h3>
      <p>
        These steps sit between theme learning and the lore ledger. They rescore previously rejected windows and
        extract supplemental snippets when learned themes match.
      </p>
    </div>
    <button class="ghost-button" disabled={loading || disabled} on:click={refresh}>
      <RefreshCcw size={16} /> Refresh
    </button>
  </div>

  {#if error}
    <div class="error-banner">{error}</div>
  {/if}

  {#if !status?.enabled}
    <article class="rescue-notice">
      <Clock3 size={18} />
      <div>
        <strong>Theme rescue is disabled</strong>
        <p>Enable <code>theme_aware_rerun.enabled</code> in pipeline config to use 04R/06R.</p>
      </div>
    </article>
  {:else if loading && !status}
    <div class="loading-panel">Loading theme rescue status...</div>
  {:else}
    {#if showPrompt && prompt}
      <article class="rescue-prompt">
        <Sparkles size={20} />
        <div>
          <strong>{prompt.title}</strong>
          <p>{prompt.message}</p>
          <button disabled={!canApproveRescue} on:click={approveRescue}>
            <CheckCircle2 size={16} /> {prompt.action_label}
          </button>
        </div>
      </article>
    {:else if canRunRescue}
      <article class="rescue-prompt subtle">
        <Play size={18} />
        <div>
          <strong>Theme rescue approved</strong>
          <p>
            {#if status?.rescue_stale}
              04R/06R artifacts are older than the latest theme learning. Run rescue when ready to refresh them.
            {:else}
              04R/06R are pending. Run rescue when ready; theme learning will not rerun unless artifacts are stale.
            {/if}
          </p>
          <button disabled={busy || disabled} on:click={runRescue}>
            <Play size={16} /> Run 04R / 06R
          </button>
        </div>
      </article>
    {/if}

    <div class="rescue-process-grid">
      {#each processes as process (process.id)}
        {@const Icon = stateIcon(process.state)}
        <article class={`rescue-process-card ${process.state}`}>
          <header>
            <span class="process-id">{process.short_label}</span>
            <span class={`process-state ${process.state}`}>{stateLabel(process.state)}</span>
          </header>
          <h4>{process.name}</h4>
          <p>{process.description}</p>
          <div class="process-metrics">
            {#each metricLines(process) as line}
              <span>{line}</span>
            {/each}
          </div>
          <footer>
            <Icon size={16} />
            <small title={process.artifact_path}>{process.summary?.status ?? "unknown"}</small>
          </footer>
        </article>
      {/each}
    </div>

    {#if progressLatest}
      <section class="rescue-progress-feed">
        <span class="caption">Latest rescue log line</span>
        <code>{progressLatest}</code>
      </section>
    {/if}
  {/if}
</section>

<style>
  .theme-rescue-panel {
    display: grid;
    gap: 16px;
  }

  .theme-rescue-header {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    align-items: flex-start;
  }

  .theme-rescue-header h3 {
    margin: 4px 0;
  }

  .theme-rescue-header p {
    margin: 0;
    color: #64748b;
    max-width: 760px;
    line-height: 1.5;
  }

  .rescue-prompt {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 14px;
    align-items: start;
    padding: 16px;
    border: 1px solid #bfdbfe;
    border-radius: 12px;
    background: #eff6ff;
    color: #1e3a8a;
  }

  .rescue-prompt.subtle {
    border-color: #fde68a;
    background: #fffbeb;
    color: #78350f;
  }

  .rescue-prompt p {
    margin: 6px 0 12px;
    line-height: 1.45;
  }

  .rescue-notice {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 12px;
    padding: 14px 16px;
    border: 1px solid #d7dee9;
    border-radius: 12px;
    background: #fff;
  }

  .rescue-process-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 14px;
  }

  .rescue-process-card {
    border: 1px solid #d7dee9;
    border-radius: 12px;
    background: #fff;
    padding: 16px;
    display: grid;
    gap: 8px;
  }

  .rescue-process-card.ready {
    border-color: #93c5fd;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.08);
  }

  .rescue-process-card.done {
    border-color: #86efac;
  }

  .rescue-process-card.stale {
    border-color: #fcd34d;
    background: #fffbeb;
  }

  .process-state.stale {
    color: #b45309;
    background: #fef3c7;
  }

  .rescue-process-card header {
    display: flex;
    justify-content: space-between;
    gap: 8px;
    align-items: center;
  }

  .process-id {
    font-weight: 800;
    font-size: 13px;
    letter-spacing: 0.04em;
    color: #475569;
  }

  .process-state {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    padding: 2px 8px;
    border-radius: 999px;
    background: #f1f5f9;
    color: #64748b;
  }

  .process-state.ready {
    background: #dbeafe;
    color: #1d4ed8;
  }

  .process-state.done {
    background: #dcfce7;
    color: #166534;
  }

  .rescue-process-card h4 {
    margin: 0;
    font-size: 17px;
  }

  .rescue-process-card p {
    margin: 0;
    color: #64748b;
    line-height: 1.45;
    font-size: 13px;
  }

  .process-metrics {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }

  .process-metrics span {
    font-size: 12px;
    font-weight: 650;
    padding: 4px 8px;
    border-radius: 999px;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
  }

  .rescue-process-card footer {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #64748b;
    margin-top: 4px;
  }

  .rescue-progress-feed code {
    display: block;
    margin-top: 6px;
    padding: 10px 12px;
    border-radius: 8px;
    background: #0b1020;
    color: #d7e0ff;
    font-size: 12px;
    white-space: pre-wrap;
  }
</style>
