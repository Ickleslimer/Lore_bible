<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import { Check, GitMerge, HelpCircle, Search, Split, X } from "lucide-svelte";
  import { decideIdentityCluster, decideIdentityEdge } from "../lib/api";
  import type { IdentityClusterRow, IdentityEdge, IdentityEntity } from "../lib/types";

  export let artifactsRoot = "";
  export let clusters: IdentityClusterRow[] = [];
  export let disabled = false;

  const dispatch = createEventDispatcher<{ changed: void }>();
  let search = "";
  let bucket = "pending";
  let rationale = "";
  let workingCanonical: Record<string, string> = {};
  let expanded: Record<string, boolean> = {};
  let localError = "";

  $: filtered = clusters.filter((row) => {
    const haystack = [
      row.candidate_name,
      row.canonical_name,
      row.triage_reason,
      ...(row.item.member_entities ?? []).map((entity) => entity.canonical_name),
    ]
      .join(" ")
      .toLowerCase();
    const bucketMatch = bucket === "all" || row.bucket === bucket;
    return bucketMatch && haystack.includes(search.trim().toLowerCase());
  });

  function canonicalFor(row: IdentityClusterRow): string {
    return workingCanonical[row.row_id] ?? row.canonical_name ?? row.item.canonical_name ?? "";
  }

  function edgeStatus(edge: IdentityEdge): string {
    return String(edge.edge_bucket || edge.edge_review_status || edge.latest_edge_decision?.decision || "pending").toLowerCase();
  }

  function entityClass(row: IdentityClusterRow, entity: IdentityEntity) {
    const canonicalId = row.item.canonical_entity_id;
    const splitIds = new Set(row.item.suggested_split_entity_ids ?? []);
    return [
      "entity-node",
      canonicalId && entity.entity_id === canonicalId ? "canonical" : "",
      splitIds.has(entity.entity_id) ? "split" : "",
    ]
      .filter(Boolean)
      .join(" ");
  }

  async function decideCluster(row: IdentityClusterRow, decision: "approve" | "reject" | "defer" | "needs_more_context") {
    localError = "";
    await decideIdentityCluster({
      artifacts_root: artifactsRoot,
      proposal_id: row.item.proposal_id,
      decision,
      canonical_name: canonicalFor(row),
      rationale,
    }).catch((err) => {
      localError = err instanceof Error ? err.message : String(err);
      throw err;
    });
    rationale = "";
    dispatch("changed");
  }

  async function decideEdge(row: IdentityClusterRow, edge: IdentityEdge, decision: "accept" | "reject" | "defer" | "needs_more_context") {
    localError = "";
    await decideIdentityEdge({
      artifacts_root: artifactsRoot,
      cluster_id: row.item.proposal_id,
      edge_proposal_id: edge.proposal_id,
      source_entity_id: edge.source_entity_id,
      source_entity_name: edge.source_entity_name,
      target_entity_id: edge.target_entity_id,
      target_entity_name: edge.target_entity_name,
      decision,
      rationale,
    }).catch((err) => {
      localError = err instanceof Error ? err.message : String(err);
      throw err;
    });
    rationale = "";
    dispatch("changed");
  }
</script>

<section class="identity-layout">
  <div class="review-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder="Search clusters" />
    </label>
    <select bind:value={bucket}>
      <option value="pending">Pending</option>
      <option value="approved">Approved</option>
      <option value="rejected">Rejected</option>
      <option value="deferred">Deferred</option>
      <option value="all">All</option>
    </select>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="cluster-stack">
    {#each filtered as row}
      <article class="cluster-card">
        <header class="cluster-header">
          <div class="cluster-title">
            <span class="caption">{row.bucket} · {row.evidence_count} claims</span>
            <h3>{row.candidate_name}</h3>
          </div>
          <button class="ghost-button" on:click={() => (expanded[row.row_id] = !expanded[row.row_id])}>
            {expanded[row.row_id] ? "Collapse" : "Expand"}
          </button>
        </header>

        <div class="canonical-row">
          <label>
            <span class="caption">Canonical Name</span>
            <input value={canonicalFor(row)} on:input={(event) => (workingCanonical[row.row_id] = event.currentTarget.value)} />
          </label>
          <div class="cluster-actions">
            <button disabled={disabled} on:click={() => decideCluster(row, "approve")}><Check size={16} /> Approve</button>
            <button class="secondary" disabled={disabled} on:click={() => decideCluster(row, "defer")}><HelpCircle size={16} /> Defer</button>
            <button class="danger" disabled={disabled} on:click={() => decideCluster(row, "reject")}><X size={16} /> Reject</button>
          </div>
        </div>

        <div class="entity-lane">
          {#each row.item.member_entities ?? [] as entity}
            <div class={entityClass(row, entity)}>
              <span>{entity.canonical_name}</span>
              <small>{entity.entity_type || "entity"}</small>
            </div>
          {/each}
        </div>

        {#if row.review_priority}
          <div class="warning-line"><Split size={15} /> {row.review_priority}</div>
        {/if}

        {#if expanded[row.row_id]}
          <div class="details-grid">
            <section>
              <h4><GitMerge size={16} /> Connections</h4>
              <div class="edge-list">
                {#each row.item.member_edges ?? [] as edge}
                  <div class={`edge-card ${edgeStatus(edge)}`}>
                    <div>
                      <strong>{edge.source_entity_name} → {edge.target_entity_name}</strong>
                      <span>{edgeStatus(edge)} · {(edge.evidence_claim_ids ?? []).length} claim links</span>
                    </div>
                    <div class="edge-actions">
                      <button class="compact" disabled={disabled} on:click={() => decideEdge(row, edge, "accept")}>Keep</button>
                      <button class="compact danger" disabled={disabled} on:click={() => decideEdge(row, edge, "reject")}>Refute</button>
                      <button class="compact secondary" disabled={disabled} on:click={() => decideEdge(row, edge, "defer")}>Defer</button>
                    </div>
                  </div>
                {/each}
              </div>
            </section>

            <section>
              <h4>Rationale</h4>
              <p>{row.triage_reason || row.item.rationale || "No rationale recorded."}</p>
              <textarea bind:value={rationale} placeholder="Decision rationale" />
            </section>
          </div>
        {/if}
      </article>
    {/each}
  </div>
</section>
