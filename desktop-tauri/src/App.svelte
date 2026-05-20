<script lang="ts">
  import { onMount } from "svelte";
  import { RefreshCcw } from "lucide-svelte";
  import IdentityMergePanel from "./components/IdentityMergePanel.svelte";
  import InventoryPanel from "./components/InventoryPanel.svelte";
  import PipelineControlPanel from "./components/PipelineControlPanel.svelte";
  import ProgressRail from "./components/ProgressRail.svelte";
  import RunSelector from "./components/RunSelector.svelte";
  import { loadClaimInventory, loadEntityInventory, loadIdentityClusters, loadState, selectRun } from "./lib/api";
  import type { AppState, IdentityClusterRow, InventoryRow } from "./lib/types";

  let state: AppState | null = null;
  let clusters: IdentityClusterRow[] = [];
  let claimRows: InventoryRow[] = [];
  let entityRows: InventoryRow[] = [];
  let loading = true;
  let busy = false;
  let clusterLoading = false;
  let inventoryLoading = false;
  let error = "";
  let activeTab: "pipeline" | "claims" | "entities" | "identity" | "overview" = "pipeline";

  function withTimeout<T>(promise: Promise<T>, milliseconds: number, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        window.setTimeout(() => reject(new Error(`${label} timed out.`)), milliseconds);
      }),
    ]);
  }

  async function refreshClusters(artifactsRoot: string) {
    clusterLoading = true;
    try {
      clusters = (await withTimeout(loadIdentityClusters(artifactsRoot), 3500, "Identity cluster load")).clusters;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      clusters = [];
    } finally {
      clusterLoading = false;
    }
  }

  async function refreshInventory(artifactsRoot: string) {
    inventoryLoading = true;
    try {
      const [claims, entities] = await Promise.all([
        withTimeout(loadClaimInventory(artifactsRoot), 5000, "Claim inventory load"),
        withTimeout(loadEntityInventory(artifactsRoot), 5000, "Entity inventory load"),
      ]);
      claimRows = claims.rows;
      entityRows = entities.rows;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      claimRows = [];
      entityRows = [];
    } finally {
      inventoryLoading = false;
    }
  }

  async function refresh() {
    busy = true;
    error = "";
    try {
      const nextState = await withTimeout(loadState(state?.active_root), 4000, "Workspace state load");
      state = nextState;
      loading = false;
      busy = false;
      if (nextState.active_root) {
        void refreshClusters(nextState.active_root);
        void refreshInventory(nextState.active_root);
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      loading = false;
      busy = false;
    }
  }

  async function handleRunChange(event: CustomEvent<string>) {
    busy = true;
    error = "";
    try {
      const nextState = await withTimeout(selectRun(event.detail), 4000, "Run selection");
      state = nextState;
      clusters = [];
      claimRows = [];
      entityRows = [];
      busy = false;
      if (nextState.active_root) {
        void refreshClusters(nextState.active_root);
        void refreshInventory(nextState.active_root);
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
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

  async function handleInventoryChanged() {
    if (!state?.active_root) return;
    busy = true;
    try {
      await refreshInventory(state.active_root);
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
    claimRows = [];
    entityRows = [];
  }

  function setTab(tab: "pipeline" | "claims" | "entities" | "identity" | "overview") {
    activeTab = tab;
  }

  function runName(path: string | undefined) {
    if (!path) return "Loading";
    return path.split(/[\\/]/).filter(Boolean).pop() ?? path;
  }

  onMount(() => {
    void refresh();
  });
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
      <button class:active={activeTab === "pipeline"} on:click={() => setTab("pipeline")}>Pipeline</button>
      <button class:active={activeTab === "claims"} on:click={() => setTab("claims")}>Claims</button>
      <button class:active={activeTab === "entities"} on:click={() => setTab("entities")}>Entities</button>
      <button class:active={activeTab === "identity"} on:click={() => setTab("identity")}>Identity</button>
      <button class:active={activeTab === "overview"} on:click={() => setTab("overview")}>Overview</button>
    </nav>
  </aside>

  <section class="workspace">
    <header class="topbar">
      <div>
        <span class="caption">Active Run</span>
        <h2>{runName(state?.active_label)}</h2>
        {#if state?.active_label}
          <p class="run-path" title={state.active_root}>{state.active_label}</p>
        {/if}
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
      {:else if activeTab === "claims"}
        <InventoryPanel
          artifactsRoot={state.active_root}
          rows={claimRows}
          kind="claims"
          disabled={busy || inventoryLoading}
          on:changed={handleInventoryChanged}
        />
      {:else if activeTab === "entities"}
        <InventoryPanel
          artifactsRoot={state.active_root}
          rows={entityRows}
          kind="entities"
          disabled={busy || inventoryLoading}
          on:changed={handleInventoryChanged}
        />
      {:else if activeTab === "identity"}
        <IdentityMergePanel
          artifactsRoot={state.active_root}
          clusters={clusters}
          disabled={busy || clusterLoading}
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
