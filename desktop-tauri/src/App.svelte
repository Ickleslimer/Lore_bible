<script lang="ts">
  import { onMount } from "svelte";
  import { RefreshCcw } from "lucide-svelte";
  import IdentityMergePanel from "./components/IdentityMergePanel.svelte";
  import PipelineControlPanel from "./components/PipelineControlPanel.svelte";
  import ProgressRail from "./components/ProgressRail.svelte";
  import RunSelector from "./components/RunSelector.svelte";
  import { loadIdentityClusters, loadState, selectRun } from "./lib/api";
  import type { AppState, IdentityClusterRow } from "./lib/types";

  let state: AppState | null = null;
  let clusters: IdentityClusterRow[] = [];
  let loading = true;
  let busy = false;
  let error = "";
  let activeTab: "pipeline" | "identity" | "overview" = "pipeline";

  async function refresh() {
    busy = true;
    error = "";
    try {
      state = await loadState(state?.active_root);
      if (state.active_root) {
        clusters = (await loadIdentityClusters(state.active_root)).clusters;
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
      busy = false;
    }
  }

  async function handleRunChange(event: CustomEvent<string>) {
    busy = true;
    error = "";
    try {
      state = await selectRun(event.detail);
      clusters = (await loadIdentityClusters(state.active_root)).clusters;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
    }
  }

  async function handleClustersChanged() {
    if (!state?.active_root) return;
    busy = true;
    try {
      clusters = (await loadIdentityClusters(state.active_root)).clusters;
      state = await loadState(state.active_root);
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      busy = false;
    }
  }

  function handleStateChanged(event: CustomEvent<AppState>) {
    state = event.detail;
    clusters = [];
  }

  onMount(refresh);
</script>

<main class="app-shell">
  <aside class="sidebar">
    <div class="brand-block">
      <div class="brand-mark">T</div>
      <div>
        <h1>Theriac Lore</h1>
        <p>Pipeline Review</p>
      </div>
    </div>

    {#if state}
      <RunSelector runs={state.runs} activeRoot={state.active_root} on:changeRun={handleRunChange} />
      <div class="summary-panel">
        <span class="caption">Pending Review</span>
        <strong>{state.pending_total}</strong>
        <p>{state.pending_summary}</p>
      </div>
    {/if}

    <nav class="nav-tabs" aria-label="Review sections">
      <button class:active={activeTab === "pipeline"} on:click={() => (activeTab = "pipeline")}>Pipeline</button>
      <button class:active={activeTab === "identity"} on:click={() => (activeTab = "identity")}>Identity</button>
      <button class:active={activeTab === "overview"} on:click={() => (activeTab = "overview")}>Overview</button>
    </nav>
  </aside>

  <section class="workspace">
    <header class="topbar">
      <div>
        <span class="caption">Active Run</span>
        <h2>{state?.active_label ?? "Loading"}</h2>
      </div>
      <button class="icon-button" disabled={busy} on:click={refresh} title="Refresh">
        <RefreshCcw size={18} />
      </button>
    </header>

    {#if error}
      <div class="error-banner">{error}</div>
    {/if}

    {#if loading}
      <div class="loading-panel">Loading review workspace...</div>
    {:else if state}
      <ProgressRail progress={state.progress} />

      {#if activeTab === "pipeline"}
        <PipelineControlPanel
          {state}
          disabled={busy}
          on:stateChanged={handleStateChanged}
          on:refresh={refresh}
        />
      {:else if activeTab === "identity"}
        <IdentityMergePanel
          artifactsRoot={state.active_root}
          clusters={clusters}
          disabled={busy}
          on:changed={handleClustersChanged}
        />
      {:else}
        <section class="overview-grid">
          {#each Object.entries(state.counts) as [key, value]}
            <article class="metric-card">
              <span class="caption">{key.replaceAll("_", " ")}</span>
              <strong>{value}</strong>
            </article>
          {/each}
        </section>
      {/if}
    {/if}
  </section>
</main>
