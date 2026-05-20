<script lang="ts">
  import { Activity, AlertTriangle, CheckCircle2, RefreshCcw, RotateCcw, XCircle } from "lucide-svelte";
  import { loadCardAgentActivity, undoCardAgentTransaction } from "../lib/api";
  import type { CardAgentTransaction } from "../lib/types";

  export let artifactsRoot = "";
  export let disabled = false;

  let transactions: CardAgentTransaction[] = [];
  let total = 0;
  let sourcePath = "";
  let loading = false;
  let busyTransaction = "";
  let localError = "";

  $: completedCount = transactions.filter((transaction) => transaction.status === "completed").length;
  $: reversalCount = transactions.filter((transaction) => String(transaction.status).includes("reversal")).length;
  $: failedCount = transactions.filter((transaction) => String(transaction.status).includes("failed")).length;

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
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    } finally {
      busyTransaction = "";
    }
  }

  $: if (artifactsRoot) {
    void load();
  }
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
        <p>Freeform cardbase requests will appear here after Stage 11 runs them.</p>
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
          </div>
          <p class="agent-steps">{stepNames(transaction)}</p>
          {#if transaction.reverses_transaction_id}
            <p class="agent-reversal">Reverses {transaction.reverses_transaction_id}</p>
          {/if}
        </article>
      {/each}
    </div>
  {/if}
</section>
