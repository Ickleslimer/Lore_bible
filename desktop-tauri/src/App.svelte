<script lang="ts">
  import { onMount } from "svelte";
  import { RefreshCcw, Settings } from "lucide-svelte";
  import AppConfigModal from "./components/AppConfigModal.svelte";
  import CardAgentActivityPanel from "./components/CardAgentActivityPanel.svelte";
  import DraftCardsPanel from "./components/DraftCardsPanel.svelte";
  import EntityInventoryPanel from "./components/EntityInventoryPanel.svelte";
  import IdentityMergePanel from "./components/IdentityMergePanel.svelte";
  import InventoryPanel from "./components/InventoryPanel.svelte";
  import PipelineControlPanel from "./components/PipelineControlPanel.svelte";
  import ProgressRail from "./components/ProgressRail.svelte";
  import RelationshipGraphPanel from "./components/RelationshipGraphPanel.svelte";
  import RunSelector from "./components/RunSelector.svelte";
  import ThemeLearningPanel from "./components/ThemeLearningPanel.svelte";
  import { loadClaimInventory, loadEntityInventory, loadIdentityClusters, loadState, selectRun } from "./lib/api";
  import type { AppState, IdentityClusterRow, InventoryRow } from "./lib/types";

  let state: AppState | null = null;
  let clusters: IdentityClusterRow[] = [];
  let claimRows: InventoryRow[] = [];
  let entityRows: InventoryRow[] = [];
  let mergedEntityRows: InventoryRow[] = [];
  let mergedEntityMetadata: Record<string, unknown> = {};
  let loading = true;
  let busy = false;
  let clusterLoading = false;
  let inventoryLoading = false;
  let error = "";
  let configOpen = false;
  type ActiveTab = "pipeline" | "claims" | "entities" | "themes" | "identity" | "relationships" | "drafts" | "agent" | "overview";
  let activeTab: ActiveTab = "pipeline";

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
      mergedEntityRows = entities.merged_rows ?? [];
      mergedEntityMetadata = entities.merged_metadata ?? {};
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      claimRows = [];
      entityRows = [];
      mergedEntityRows = [];
      mergedEntityMetadata = {};
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
      mergedEntityRows = [];
      mergedEntityMetadata = {};
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
    mergedEntityRows = [];
    mergedEntityMetadata = {};
  }

  function setTab(tab: ActiveTab) {
    activeTab = tab;
  }

  function openConfig() {
    configOpen = true;
  }

  function closeConfig() {
    configOpen = false;
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
      <button class:active={activeTab === "themes"} on:click={() => setTab("themes")}>Themes</button>
      <button class:active={activeTab === "identity"} on:click={() => setTab("identity")}>Identity</button>
      <button class:active={activeTab === "relationships"} on:click={() => setTab("relationships")}>Relationships</button>
      <button class:active={activeTab === "drafts"} on:click={() => setTab("drafts")}>Draft Cards</button>
      <button class:active={activeTab === "agent"} on:click={() => setTab("agent")}>Agent</button>
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
      <div class="topbar-actions">
        <button class="icon-button" disabled={busy} on:click={openConfig} title="Configuration">
          <Settings size={18} />
        </button>
        <button class="icon-button" disabled={busy} on:click={refresh} title="Refresh">
          <RefreshCcw size={18} />
        </button>
      </div>
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
        <EntityInventoryPanel
          artifactsRoot={state.active_root}
          rows={entityRows}
          mergedRows={mergedEntityRows}
          mergedMetadata={mergedEntityMetadata}
          disabled={busy || inventoryLoading}
          on:changed={handleInventoryChanged}
        />
      {:else if activeTab === "themes"}
        <ThemeLearningPanel artifactsRoot={state.active_root} disabled={busy} />
      {:else if activeTab === "identity"}
        <IdentityMergePanel
          artifactsRoot={state.active_root}
          clusters={clusters}
          disabled={busy || clusterLoading}
          on:changed={handleClustersChanged}
        />
      {:else if activeTab === "relationships"}
        <RelationshipGraphPanel artifactsRoot={state.active_root} disabled={busy} />
      {:else if activeTab === "drafts"}
        <DraftCardsPanel artifactsRoot={state.active_root} disabled={busy} />
      {:else if activeTab === "agent"}
        <CardAgentActivityPanel artifactsRoot={state.active_root} disabled={busy} on:changed={refresh} />
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

  {#if configOpen}
    <AppConfigModal on:close={closeConfig} on:saved={refresh} />
  {/if}
</main>
