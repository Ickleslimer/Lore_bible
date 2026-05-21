<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from "svelte";
  import {
    Activity,
    AlertTriangle,
    CheckCircle2,
    ClipboardList,
    Eraser,
    Play,
    RefreshCcw,
    RotateCcw,
    SendHorizontal,
    Target,
    XCircle,
  } from "lucide-svelte";
  import { loadCardAgentActivity, loadCardAgentProgress, runCardAgentRequest, undoCardAgentTransaction } from "../lib/api";
  import type { CardAgentTransaction } from "../lib/types";

  export let artifactsRoot = "";
  export let disabled = false;

  type ChangeLine = {
    sentence?: string;
    kind?: string;
    collection?: string;
    artifact?: string;
    id?: string;
  };
  type ChangeBucket = "added" | "updated" | "removed";
  type ChangeField = { field?: string; before?: unknown; after?: unknown };
  type ChangeItem = {
    id?: string;
    label?: string;
    details?: Record<string, unknown>;
    field_changes?: ChangeField[];
  };
  type ChangeCollection = {
    name?: string;
    added?: ChangeItem[];
    updated?: ChangeItem[];
    removed?: ChangeItem[];
  };
  type ArtifactChange = {
    path?: string;
    display_path?: string;
    changed?: boolean;
    change_type?: string;
    before?: { exists?: boolean; chars?: number };
    after?: { exists?: boolean; chars?: number };
    collections?: ChangeCollection[];
    changed_fields?: string[];
  };

  const dispatch = createEventDispatcher<{ changed: void }>();
  const affectedGroups = ["entities", "cards", "claims"] as const;
  const changeBuckets = ["added", "updated", "removed"] as const;

  let transactions: CardAgentTransaction[] = [];
  let total = 0;
  let sourcePath = "";
  let loading = false;
  let running = false;
  let busyTransaction = "";
  let localError = "";
  let requestText = "";
  let targetText = "";
  let rationaleText = "";
  let maxSteps = 16;
  let runSummary = "";
  let runTone: "ok" | "warn" | "bad" = "ok";
  let progressLines: string[] = [];
  let progressLatest = "";
  let progressUpdated = "";
  let progressPolling = false;
  let progressTimer: number | undefined;

  $: completedCount = transactions.filter((transaction) => transaction.status === "completed").length;
  $: reversalCount = transactions.filter((transaction) => String(transaction.status).includes("reversal")).length;
  $: failedCount = transactions.filter((transaction) => String(transaction.status).includes("failed")).length;
  $: requestCharacterCount = requestText.trim().length;
  $: canRun = Boolean(artifactsRoot && requestText.trim() && !running && !disabled);

  function stepNames(transaction: CardAgentTransaction): string {
    const names = (transaction.steps ?? [])
      .map((step) => String(step.tool_name || "tool"))
      .filter(Boolean);
    if (!names.length) return "No tool steps recorded";
    return names.slice(0, 8).join(" -> ") + (names.length > 8 ? " -> ..." : "");
  }

  function changedCount(transaction: CardAgentTransaction): number {
    return (transaction.write_set ?? []).filter((item) => Boolean(item.changed)).length;
  }

  function changeLines(transaction: CardAgentTransaction): ChangeLine[] {
    const lines = transaction.change_summary?.lines;
    return Array.isArray(lines) ? (lines as ChangeLine[]) : [];
  }

  function changeLineCount(transaction: CardAgentTransaction): number {
    return changeLines(transaction).length;
  }

  function lineTone(kind: unknown): "ok" | "warn" | "bad" {
    const text = String(kind || "");
    if (text === "removed" || text === "deleted") return "bad";
    if (text === "updated") return "warn";
    return "ok";
  }

  function changeArtifacts(transaction: CardAgentTransaction): ArtifactChange[] {
    const artifacts = transaction.change_summary?.artifacts;
    return Array.isArray(artifacts) ? (artifacts as ArtifactChange[]) : [];
  }

  function affectedIds(transaction: CardAgentTransaction, group: "entities" | "cards" | "claims"): string[] {
    const source = transaction.change_summary?.affected ?? transaction.affected ?? {};
    const values = source[group];
    return Array.isArray(values) ? values.map((value) => String(value)).filter(Boolean) : [];
  }

  function affectedTotal(transaction: CardAgentTransaction): number {
    return affectedIds(transaction, "entities").length + affectedIds(transaction, "cards").length + affectedIds(transaction, "claims").length;
  }

  function hasChangeDetails(transaction: CardAgentTransaction): boolean {
    return affectedTotal(transaction) > 0 || changeArtifacts(transaction).length > 0;
  }

  function changeRows(collection: ChangeCollection, bucket: ChangeBucket): ChangeItem[] {
    const rows = collection[bucket];
    return Array.isArray(rows) ? rows : [];
  }

  function collectionChangeTotal(collection: ChangeCollection): number {
    return changeRows(collection, "added").length + changeRows(collection, "updated").length + changeRows(collection, "removed").length;
  }

  function valueText(value: unknown): string {
    if (Array.isArray(value)) return value.map((item) => String(item)).join(", ");
    if (value && typeof value === "object") return JSON.stringify(value);
    return String(value ?? "");
  }

  function detailEntries(item: ChangeItem): Array<[string, unknown]> {
    const details = item.details ?? {};
    return Object.entries(details).filter(([, value]) => value !== "" && value !== undefined && value !== null);
  }

  function charsDelta(artifact: ArtifactChange): string {
    const before = Number(artifact.before?.chars ?? 0);
    const after = Number(artifact.after?.chars ?? 0);
    const delta = after - before;
    const sign = delta > 0 ? "+" : "";
    return `${before} -> ${after} chars (${sign}${delta})`;
  }

  function artifactTone(artifact: ArtifactChange): "ok" | "warn" | "bad" {
    if (!artifact.changed) return "warn";
    if (artifact.change_type === "deleted") return "bad";
    return "ok";
  }

  function statusTone(status: string): "ok" | "warn" | "bad" {
    if (status === "completed" || status === "completed_reversal") return "ok";
    if (status.includes("failed")) return "bad";
    return "warn";
  }

  function displayTime(value: unknown): string {
    const text = String(value || "").trim();
    if (!text) return "n/a";
    const date = new Date(text);
    return Number.isNaN(date.getTime()) ? text : date.toLocaleString();
  }

  function withTimeout<T>(promise: Promise<T>, milliseconds: number, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        window.setTimeout(() => reject(new Error(`${label} timed out.`)), milliseconds);
      }),
    ]);
  }

  function cleanProgressLine(line: string): string {
    return String(line || "").trim().replace(/\s+/g, " ");
  }

  async function refreshProgress() {
    if (!artifactsRoot || progressPolling) return;
    progressPolling = true;
    try {
      const response = await withTimeout(loadCardAgentProgress(artifactsRoot, 90), 1800, "Agent progress refresh");
      progressLines = (response.lines ?? []).map(cleanProgressLine);
      progressLatest = cleanProgressLine(response.latest_progress_line || response.latest_line || "");
      const epoch = Number(response.updated_at_epoch || 0);
      progressUpdated = epoch > 0 ? new Date(epoch * 1000).toLocaleTimeString() : "";
    } catch {
      // The activity list remains the source of truth; a missed live poll should stay quiet.
    } finally {
      progressPolling = false;
    }
  }

  async function load() {
    if (!artifactsRoot || loading) return;
    loading = true;
    localError = "";
    try {
      const response = await loadCardAgentActivity(artifactsRoot);
      transactions = response.transactions ?? [];
      total = response.total ?? transactions.length;
      sourcePath = response.source_path ?? "";
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
    }
  }

  async function undo(transaction: CardAgentTransaction) {
    const id = transaction.transaction_id;
    if (!id || busyTransaction) return;
    if (!window.confirm(`Undo card agent transaction ${id}? The reversal will be logged.`)) return;
    busyTransaction = id;
    localError = "";
    try {
      const response = await undoCardAgentTransaction({
        artifacts_root: artifactsRoot,
        transaction_id: id,
        reviewer: "desktop_user",
        rationale: "Undo requested from the Cardbase Agent activity view.",
      });
      transactions = response.transactions ?? [];
      total = response.total ?? transactions.length;
      sourcePath = response.source_path ?? sourcePath;
      dispatch("changed");
      await refreshProgress();
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    } finally {
      busyTransaction = "";
    }
  }

  async function runRequest() {
    const clean = requestText.trim();
    if (!artifactsRoot || !clean || running) return;
    running = true;
    localError = "";
    runSummary = "";
    progressLatest = "Agent request sent.";
    progressUpdated = new Date().toLocaleTimeString();
    progressLines = ["Agent request sent. Waiting for tool activity."];
    void refreshProgress();
    try {
      const response = await runCardAgentRequest({
        artifacts_root: artifactsRoot,
        instruction_text: clean,
        target_text: targetText.trim(),
        rationale: rationaleText.trim(),
        requester: "desktop_user",
        max_steps: maxSteps,
      });
      transactions = response.transactions ?? [];
      total = response.total ?? transactions.length;
      sourcePath = response.source_path ?? sourcePath;
      const lastRun = response.last_run ?? {};
      const status = String(lastRun.status || "");
      runTone = status === "completed" ? "ok" : status.includes("failed") ? "bad" : "warn";
      runSummary = status === "completed" ? "Request applied and logged." : String(lastRun.error || "Request finished with attention.");
      if (status === "completed") {
        requestText = "";
        targetText = "";
        rationaleText = "";
      }
      dispatch("changed");
      await refreshProgress();
    } catch (err) {
      runTone = "bad";
      localError = err instanceof Error ? err.message : String(err);
      await refreshProgress();
    } finally {
      running = false;
    }
  }

  function clearRequest() {
    requestText = "";
    targetText = "";
    rationaleText = "";
    maxSteps = 16;
    runSummary = "";
    localError = "";
  }

  $: if (artifactsRoot) {
    void load();
    void refreshProgress();
  }

  onMount(() => {
    refreshProgress().catch(() => undefined);
    progressTimer = window.setInterval(() => {
      refreshProgress().catch(() => undefined);
    }, 3000);
  });

  onDestroy(() => {
    if (progressTimer !== undefined) window.clearInterval(progressTimer);
  });
</script>

<section class="agent-activity-view">
  <div class="agent-activity-header">
    <div>
      <span class="caption">Cardbase Agent</span>
      <h3>Transaction Activity</h3>
      <p title={sourcePath}>{total} logged transaction{total === 1 ? "" : "s"}</p>
    </div>
    <button class="secondary" disabled={disabled || loading} on:click={load}>
      <RefreshCcw size={16} /> Refresh
    </button>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <form class="agent-run-panel" on:submit|preventDefault={runRequest}>
    <header>
      <div>
        <span class="caption">Outside Pipeline</span>
        <h3>Freeform Agent Request</h3>
      </div>
      <span class="request-count">{requestCharacterCount} chars</span>
    </header>

    <div class="agent-request-grid">
      <label class="agent-request-field">
        <span><ClipboardList size={15} /> Request</span>
        <textarea
          bind:value={requestText}
          disabled={disabled || running}
          placeholder="Merge Pandora's mother into Izanami and rewrite stale references"
        ></textarea>
      </label>

      <div class="agent-context-fields">
        <label>
          <span><Target size={15} /> Target</span>
          <input bind:value={targetText} disabled={disabled || running} placeholder="Entity, card, or section" />
        </label>
        <label>
          <span><ClipboardList size={15} /> Rationale</span>
          <textarea
            class="compact-textarea"
            bind:value={rationaleText}
            disabled={disabled || running}
            placeholder="Author correction, cleanup pass, or continuity note"
          ></textarea>
        </label>
        <label>
          <span>Max steps</span>
          <input bind:value={maxSteps} disabled={disabled || running} type="number" min="1" max="32" step="1" />
        </label>
      </div>
    </div>

    <div class="agent-run-actions">
      <div class="agent-run-status">
        {#if runSummary}
          <p class={runTone}>{runSummary}</p>
        {:else}
          <p>Ready for the selected run.</p>
        {/if}
      </div>
      <div class="agent-run-buttons">
        <button type="button" class="secondary" disabled={disabled || running || (!requestText && !targetText && !rationaleText)} on:click={clearRequest}>
          <Eraser size={16} /> Clear
        </button>
        <button type="submit" disabled={!canRun}>
          {#if running}
            <Play size={16} /> Running
          {:else}
            <SendHorizontal size={16} /> Send
          {/if}
        </button>
      </div>
    </div>
  </form>

  <section class="progress-feed agent-progress-feed">
    <div class="panel-heading">
      <span class="caption">Live Agent Progress</span>
      <div class="agent-progress-actions">
        {#if progressUpdated}
          <span class="quiet-meta">Updated {progressUpdated}</span>
        {/if}
        <button class="ghost-button compact" disabled={disabled || progressPolling} on:click={refreshProgress}>
          {progressPolling ? "Refreshing..." : "Refresh"}
        </button>
      </div>
    </div>
    <p>{progressLatest || (running ? "Waiting for the agent's first tool step." : "No live agent progress yet.")}</p>
    {#if progressLines.length}
      <div class="progress-lines" aria-live="polite">
        {#each progressLines as line}
          <code>{line}</code>
        {/each}
      </div>
    {/if}
  </section>

  <div class="agent-stats">
    <article>
      <span class="caption">Completed</span>
      <strong>{completedCount}</strong>
    </article>
    <article>
      <span class="caption">Reversals</span>
      <strong>{reversalCount}</strong>
    </article>
    <article>
      <span class="caption">Failures</span>
      <strong>{failedCount}</strong>
    </article>
  </div>

  {#if loading && !transactions.length}
    <div class="empty-agent-panel">
      <Activity size={20} />
      <div>
        <h3>Loading activity</h3>
        <p>Reading the Stage 11 transaction log.</p>
      </div>
    </div>
  {:else if !transactions.length}
    <div class="empty-agent-panel">
      <Activity size={20} />
      <div>
        <h3>No transactions yet</h3>
        <p>Freeform cardbase requests will appear here after the agent runs.</p>
      </div>
    </div>
  {:else}
    <div class="agent-transaction-list">
      {#each transactions as transaction}
        <article class="agent-transaction">
          <header>
            <div>
              <span class={`status-pill ${statusTone(transaction.status)}`}>
                {#if statusTone(transaction.status) === "ok"}
                  <CheckCircle2 size={14} />
                {:else if statusTone(transaction.status) === "bad"}
                  <XCircle size={14} />
                {:else}
                  <AlertTriangle size={14} />
                {/if}
                {transaction.status}
              </span>
              <h4>{transaction.request_text || transaction.request_id || transaction.transaction_id}</h4>
            </div>
            {#if transaction.status === "completed"}
              <button
                class="secondary"
                disabled={disabled || Boolean(busyTransaction)}
                on:click={() => undo(transaction)}
                title="Undo this transaction"
              >
                <RotateCcw size={16} /> Undo
              </button>
            {/if}
          </header>
          <p class="agent-rationale">{transaction.error || transaction.rationale || "No rationale recorded."}</p>
          <div class="agent-meta">
            <span>{displayTime(transaction.finished_at_utc)}</span>
            <span>{transaction.steps?.length ?? 0} tool step{(transaction.steps?.length ?? 0) === 1 ? "" : "s"}</span>
            <span>{changedCount(transaction)} changed artifact{changedCount(transaction) === 1 ? "" : "s"}</span>
            <span>{changeLineCount(transaction)} change line{changeLineCount(transaction) === 1 ? "" : "s"}</span>
          </div>
          <p class="agent-steps">{stepNames(transaction)}</p>
          {#if changeLines(transaction).length}
            <ul class="agent-change-lines" aria-label="Transaction changes">
              {#each changeLines(transaction) as line}
                <li>
                  <span class={`change-dot ${lineTone(line.kind)}`} aria-hidden="true"></span>
                  <span title={line.artifact || ""}>{line.sentence}</span>
                </li>
              {/each}
            </ul>
          {/if}
          {#if hasChangeDetails(transaction)}
            <details class="agent-change-log">
              <summary>
                <span>Entity and Artifact Changes</span>
                <span>{affectedTotal(transaction)} affected id{affectedTotal(transaction) === 1 ? "" : "s"} · {changeArtifacts(transaction).length} artifact{changeArtifacts(transaction).length === 1 ? "" : "s"}</span>
              </summary>

              <div class="agent-change-body">
                {#if affectedTotal(transaction)}
                  <section class="agent-change-section">
                    <h5>Affected IDs</h5>
                    <div class="agent-affected-groups">
                      {#each affectedGroups as group}
                        {@const ids = affectedIds(transaction, group)}
                        {#if ids.length}
                          <div class="agent-affected-group">
                            <span class="caption">{group}</span>
                            <div class="agent-id-list">
                              {#each ids as id}
                                <code>{id}</code>
                              {/each}
                            </div>
                          </div>
                        {/if}
                      {/each}
                    </div>
                  </section>
                {/if}

                {#if changeArtifacts(transaction).length}
                  <section class="agent-change-section">
                    <h5>Artifact Writes</h5>
                    <div class="agent-artifact-list">
                      {#each changeArtifacts(transaction) as artifact}
                        <div class="agent-artifact-change">
                          <div class="agent-artifact-header">
                            <span class={`status-pill ${artifactTone(artifact)}`}>{artifact.change_type || "updated"}</span>
                            <strong title={artifact.path}>{artifact.display_path || artifact.path}</strong>
                            <span>{charsDelta(artifact)}</span>
                          </div>

                          {#if artifact.changed_fields?.length}
                            <p class="agent-changed-fields">Fields: {artifact.changed_fields.join(", ")}</p>
                          {/if}

                          {#each artifact.collections ?? [] as collection}
                            {#if collectionChangeTotal(collection)}
                              <div class="agent-change-collection">
                                <span class="caption">{collection.name}</span>
                                {#each changeBuckets as bucket}
                                  {@const rows = changeRows(collection, bucket)}
                                  {#if rows.length}
                                    <div class={`agent-change-bucket ${bucket}`}>
                                      <strong>{bucket} {rows.length}</strong>
                                      {#each rows as row}
                                        <div class="agent-change-item">
                                          <code>{row.id}</code>
                                          <span>{row.label}</span>
                                          {#if row.field_changes?.length}
                                            <div class="agent-field-changes">
                                              {#each row.field_changes as field}
                                                <span><b>{field.field}</b>: {valueText(field.before)} -> {valueText(field.after)}</span>
                                              {/each}
                                            </div>
                                          {/if}
                                          {#if detailEntries(row).length}
                                            <dl>
                                              {#each detailEntries(row) as [key, value]}
                                                <div>
                                                  <dt>{key.replaceAll("_", " ")}</dt>
                                                  <dd>{valueText(value)}</dd>
                                                </div>
                                              {/each}
                                            </dl>
                                          {/if}
                                        </div>
                                      {/each}
                                    </div>
                                  {/if}
                                {/each}
                              </div>
                            {/if}
                          {/each}
                        </div>
                      {/each}
                    </div>
                  </section>
                {/if}
              </div>
            </details>
          {/if}
          {#if transaction.reverses_transaction_id}
            <p class="agent-reversal">Reverses {transaction.reverses_transaction_id}</p>
          {/if}
        </article>
      {/each}
    </div>
  {/if}
</section>
