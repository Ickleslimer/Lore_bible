<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import { Check, HelpCircle, Search, X } from "lucide-svelte";
  import { decideClaim, decideEntity } from "../lib/api";
  import type { InventoryRow } from "../lib/types";

  export let artifactsRoot = "";
  export let rows: InventoryRow[] = [];
  export let kind: "claims" | "entities" = "claims";
  export let disabled = false;

  const dispatch = createEventDispatcher<{ changed: void }>();

  let search = "";
  let bucket = "all";
  let category = "all";
  let sortKey = "evidence";
  let descending = true;
  let visibleLimit = 350;
  let localError = "";
  let rationales: Record<string, string> = {};
  let workingCanonical: Record<string, string> = {};
  let workingType: Record<string, string> = {};

  const entityTypes = ["term", "theme", "quest", "event", "character", "faction", "organization", "location", "timeline_node"];

  $: buckets = uniqueValues(rows.map((row) => row.bucket));
  $: categories = uniqueValues(rows.map((row) => row.category));
  $: filtered = sortRows(
    rows.filter((row) => {
      const haystack = [
        row.candidate_name,
        row.raw_candidate_name,
        row.canonical_name,
        row.proposed_entity_type,
        row.triage_reason,
        row.decision,
        ...(row.topics ?? []),
        ...(row.tracks ?? []),
      ]
        .join(" ")
        .toLowerCase();
      const query = search.trim().toLowerCase();
      const bucketMatch = bucket === "all" || row.bucket === bucket;
      const categoryMatch = category === "all" || row.category === category;
      return bucketMatch && categoryMatch && (!query || haystack.includes(query));
    }),
  );
  $: visibleRows = filtered.slice(0, visibleLimit);

  function uniqueValues(values: Array<string | undefined>): string[] {
    return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
  }

  function textValue(row: InventoryRow): string {
    return String(row.raw_candidate_name || row.item?.claim_text || row.candidate_name || "");
  }

  function targetValue(row: InventoryRow): string {
    return String(row.canonical_name || row.item?.target_entity_name || row.item?.target_entity_id || "");
  }

  function canonicalFor(row: InventoryRow): string {
    return workingCanonical[row.row_id] ?? String(row.canonical_name || row.raw_candidate_name || row.candidate_name || "");
  }

  function entityTypeFor(row: InventoryRow): string {
    return workingType[row.row_id] ?? String(row.proposed_entity_type || "term");
  }

  function sortRows(input: InventoryRow[]): InventoryRow[] {
    const copy = [...input];
    copy.sort((a, b) => String(a.candidate_name || "").localeCompare(String(b.candidate_name || "")));
    copy.sort((a, b) => {
      const left = sortValue(a);
      const right = sortValue(b);
      if (typeof left === "number" && typeof right === "number") {
        return descending ? right - left : left - right;
      }
      return descending ? String(right).localeCompare(String(left)) : String(left).localeCompare(String(right));
    });
    return copy;
  }

  function sortValue(row: InventoryRow): string | number {
    if (sortKey === "evidence") return Number(row.evidence_count ?? 0);
    if (sortKey === "bucket") return row.bucket;
    if (sortKey === "category") return row.category;
    if (sortKey === "type") return row.proposed_entity_type || "";
    if (sortKey === "decision") return row.decision || "";
    return row.candidate_name || "";
  }

  async function decide(row: InventoryRow, decision: "approve" | "reject" | "defer" | "needs_more_context") {
    localError = "";
    try {
      if (kind === "claims") {
        await decideClaim({
          artifacts_root: artifactsRoot,
          row_id: row.row_id,
          decision,
          rationale: rationales[row.row_id] || "",
        });
      } else {
        await decideEntity({
          artifacts_root: artifactsRoot,
          row_id: row.row_id,
          decision,
          canonical_name: canonicalFor(row),
          entity_type: entityTypeFor(row),
          rationale: rationales[row.row_id] || "",
        });
      }
      rationales[row.row_id] = "";
      dispatch("changed");
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    }
  }
</script>

<section class="inventory-panel">
  <div class="review-toolbar inventory-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder={kind === "claims" ? "Search claims" : "Search entities"} />
    </label>
    <select bind:value={bucket}>
      <option value="all">All buckets</option>
      {#each buckets as value}
        <option value={value}>{value}</option>
      {/each}
    </select>
    <select bind:value={category}>
      <option value="all">All categories</option>
      {#each categories as value}
        <option value={value}>{value}</option>
      {/each}
    </select>
    <select bind:value={sortKey}>
      <option value="evidence">Sort: evidence</option>
      <option value="bucket">Sort: bucket</option>
      <option value="category">Sort: category</option>
      <option value="type">Sort: type</option>
      <option value="decision">Sort: decision</option>
      <option value="name">Sort: name</option>
    </select>
    <button class="secondary" on:click={() => (descending = !descending)}>
      {descending ? "High to low" : "Low to high"}
    </button>
  </div>

  <div class="inventory-summary">
    <strong>{filtered.length}</strong>
    <span>{kind === "claims" ? "claims" : "entity candidates"} matched. Showing {visibleRows.length}.</span>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="inventory-stack">
    {#each visibleRows as row}
      <article class={`inventory-card ${row.bucket}`}>
        <header class="inventory-card-header">
          <div>
            <span class="caption">{row.bucket} · {row.category} · {row.evidence_count ?? 0} evidence</span>
            <h3>{kind === "claims" ? targetValue(row) : row.candidate_name}</h3>
          </div>
          <span class="status-pill">{row.decision || row.review_priority || row.proposed_entity_type || "pending"}</span>
        </header>

        <p class="inventory-text">{textValue(row)}</p>

        <div class="inventory-meta">
          <span>{row.proposed_entity_type || "type unknown"}</span>
          {#if row.tracks?.length}<span>{row.tracks.join(", ")}</span>{/if}
          {#if row.topics?.length}<span>{row.topics.slice(0, 4).join(", ")}</span>{/if}
        </div>

        {#if row.triage_reason}
          <p class="inventory-reason">{row.triage_reason}</p>
        {/if}

        {#if kind === "entities"}
          <div class="entity-edit-row">
            <label>
              <span class="caption">Canonical</span>
              <input value={canonicalFor(row)} on:input={(event) => (workingCanonical[row.row_id] = event.currentTarget.value)} />
            </label>
            <label>
              <span class="caption">Type</span>
              <select value={entityTypeFor(row)} on:change={(event) => (workingType[row.row_id] = event.currentTarget.value)}>
                {#each entityTypes as value}
                  <option value={value}>{value}</option>
                {/each}
              </select>
            </label>
          </div>
        {/if}

        <textarea bind:value={rationales[row.row_id]} placeholder="Decision rationale"></textarea>

        <div class="inventory-actions">
          <button disabled={disabled} on:click={() => decide(row, "approve")}><Check size={16} /> Approve</button>
          <button class="secondary" disabled={disabled} on:click={() => decide(row, "defer")}><HelpCircle size={16} /> Defer</button>
          <button class="danger" disabled={disabled} on:click={() => decide(row, "reject")}><X size={16} /> Reject</button>
        </div>
      </article>
    {/each}
  </div>

  {#if visibleRows.length < filtered.length}
    <button class="secondary load-more" on:click={() => (visibleLimit += 350)}>Show more</button>
  {/if}
</section>
