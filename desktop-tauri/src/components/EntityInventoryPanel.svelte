<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import { Check, ChevronDown, ChevronRight, HelpCircle, Search, X } from "lucide-svelte";
  import { decideEntity, loadEntityEvidence } from "../lib/api";
  import type { EntityEvidenceResponse, InventoryRow, ThemeAssociationRow } from "../lib/types";

  export let artifactsRoot = "";
  export let rows: InventoryRow[] = [];
  export let mergedRows: InventoryRow[] = [];
  export let mergedMetadata: Record<string, unknown> = {};
  export let disabled = false;

  const dispatch = createEventDispatcher<{ changed: void }>();

  let search = "";
  let bucket = "all";
  let category = "all";
  let sortKey = "evidence";
  let descending = true;
  let visibleLimit = 350;
  let localError = "";
  let viewMode: "merged" | "candidates" = "merged";
  let expanded: Record<string, boolean> = {};
  let aliasExpanded: Record<string, boolean> = {};
  let themesExpanded: Record<string, boolean> = {};
  let rationales: Record<string, string> = {};
  let workingCanonical: Record<string, string> = {};
  let workingType: Record<string, string> = {};
  let evidenceByRow: Record<string, EntityEvidenceResponse> = {};
  let evidenceLoading: Record<string, boolean> = {};

  const entityTypes = ["term", "quest", "event", "character", "faction", "organization", "location", "timeline_node"];

  $: hasMergedRows = mergedRows.length > 0;
  $: if (!hasMergedRows && viewMode === "merged") viewMode = "candidates";
  $: activeRows = viewMode === "merged" && hasMergedRows ? mergedRows : rows;
  $: buckets = uniqueValues(activeRows.map((row) => row.bucket));
  $: categories = uniqueValues(activeRows.map((row) => row.category));
  $: filtered = sortRows(
    activeRows.filter((row) => {
      const haystack = [
        row.candidate_name,
        row.raw_candidate_name,
        row.canonical_name,
        row.proposed_entity_type,
        row.triage_reason,
        row.decision,
        ...rowAliases(row),
        ...(row.topics ?? []),
        ...(row.tracks ?? []),
      ]
        .join(" ")
        .toLowerCase();
      const query = search.trim().toLowerCase();
      return (bucket === "all" || row.bucket === bucket)
        && (category === "all" || row.category === category)
        && (!query || haystack.includes(query));
    }),
  );
  $: visibleRows = filtered.slice(0, visibleLimit);

  function uniqueValues(values: Array<string | undefined>): string[] {
    return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
  }

  function textValue(row: InventoryRow): string {
    return String(row.raw_candidate_name || row.candidate_name || "");
  }

  function canonicalFor(row: InventoryRow): string {
    return workingCanonical[row.row_id] ?? String(row.canonical_name || row.raw_candidate_name || row.candidate_name || "");
  }

  function entityTypeFor(row: InventoryRow): string {
    return workingType[row.row_id] ?? String(row.proposed_entity_type || "term");
  }

  function reviewLabel(row: InventoryRow): string {
    return String(row.decision || row.review_priority || row.bucket || "pending");
  }

  function isMergedRow(row: InventoryRow): boolean {
    return row.row_kind === "merged_entity";
  }

  async function toggleExpanded(row: InventoryRow) {
    expanded[row.row_id] = !expanded[row.row_id];
    if (expanded[row.row_id]) {
      await ensureEvidence(row);
    }
  }

  async function ensureEvidence(row: InventoryRow) {
    if (evidenceByRow[row.row_id] || evidenceLoading[row.row_id]) return;
    evidenceLoading[row.row_id] = true;
    try {
      evidenceByRow[row.row_id] = await loadEntityEvidence(
        artifactsRoot,
        row.row_id,
        isMergedRow(row) ? "merged" : "candidates",
      );
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    } finally {
      evidenceLoading[row.row_id] = false;
    }
  }

  function metadataText(key: string): string {
    const value = mergedMetadata?.[key];
    if (value === undefined || value === null || value === "") return "0";
    return String(value);
  }

  function textField(value: unknown): string {
    return String(value ?? "").trim();
  }

  function numberValue(value: unknown): number {
    const numeric = Number(value ?? 0);
    return Number.isFinite(numeric) ? numeric : 0;
  }

  function percent(value: unknown): string {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return "n/a";
    return `${Math.round(Math.max(0, Math.min(1, numeric)) * 100)}%`;
  }

  function themeAssociations(row: InventoryRow): ThemeAssociationRow[] {
    const seen = new Map<string, ThemeAssociationRow>();
    for (const association of row.theme_associations ?? []) {
      const themeId = String(association.theme_id || association.theme_label || "").trim();
      if (!themeId) continue;
      const existing = seen.get(themeId);
      if (!existing || Number(association.entity_theme_rank ?? 999) < Number(existing.entity_theme_rank ?? 999)) {
        seen.set(themeId, association);
      }
    }
    return Array.from(seen.values());
  }

  function visibleThemeAssociations(row: InventoryRow): ThemeAssociationRow[] {
    return themesExpanded[row.row_id] ? themeAssociations(row) : themeAssociations(row).slice(0, 3);
  }

  function themeAssociationCount(row: InventoryRow): number {
    return numberValue(row.theme_association_count ?? themeAssociations(row).length);
  }

  function hiddenThemeAssociationCount(row: InventoryRow): number {
    return Math.max(0, themeAssociationCount(row) - themeAssociations(row).slice(0, 3).length);
  }

  function toggleThemes(row: InventoryRow) {
    themesExpanded[row.row_id] = !themesExpanded[row.row_id];
  }

  function evidenceThemeAssociations(row: InventoryRow): ThemeAssociationRow[] {
    return evidenceByRow[row.row_id]?.theme_associations ?? themeAssociations(row);
  }

  function entityThemeRankLabel(association: ThemeAssociationRow): string {
    const role = String(association.entity_theme_role || "theme").trim();
    const rank = numberValue(association.entity_theme_rank);
    const count = numberValue(association.entity_theme_count);
    if (rank && count) return `${role} #${rank}/${count}`;
    return role;
  }

  function themeCandidateRankLabel(association: ThemeAssociationRow): string {
    const rank = numberValue(association.theme_candidate_rank);
    const count = numberValue(association.theme_candidate_count);
    if (rank && count) return `theme #${rank}/${count}`;
    if (rank) return `theme #${rank}`;
    return "";
  }

  function rowAliases(row: InventoryRow): string[] {
    const topLevel = row.aliases;
    if (Array.isArray(topLevel)) {
      return topLevel.map((alias) => String(alias)).filter(Boolean);
    }
    const aliases = row.item?.aliases;
    if (!Array.isArray(aliases)) return [];
    return aliases.map((alias) => String(alias)).filter(Boolean);
  }

  function visibleAliases(row: InventoryRow): string[] {
    const aliases = rowAliases(row);
    return aliasExpanded[row.row_id] ? aliases : aliases.slice(0, 8);
  }

  function hiddenAliasCount(row: InventoryRow): number {
    return Math.max(0, rowAliases(row).length - visibleAliases(row).length);
  }

  function toggleAliases(row: InventoryRow) {
    aliasExpanded[row.row_id] = !aliasExpanded[row.row_id];
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
      await decideEntity({
        artifacts_root: artifactsRoot,
        row_id: row.row_id,
        decision,
        canonical_name: canonicalFor(row),
        entity_type: entityTypeFor(row),
        rationale: rationales[row.row_id] || "",
      });
      rationales[row.row_id] = "";
      dispatch("changed");
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    }
  }
</script>

<section class="inventory-panel">
  <div class="entity-view-switcher">
    <div>
      <span class="caption">Entity View</span>
      <h3>{viewMode === "merged" ? "Merged Entity List" : "Candidate Review"}</h3>
      <p>
        {#if hasMergedRows}
          Stage 10 preview: {metadataText("source_entity_count")} source entities -> {metadataText("merged_entity_count")} merged entities, {metadataText("merge_record_count")} merge records.
        {:else}
          Stage 10 merged entity preview is not available yet.
        {/if}
      </p>
    </div>
    <div class="segmented-control">
      <button class:active={viewMode === "merged"} disabled={!hasMergedRows} on:click={() => (viewMode = "merged")}>
        Merged List
      </button>
      <button class:active={viewMode === "candidates"} on:click={() => (viewMode = "candidates")}>
        Candidates
      </button>
    </div>
  </div>

  <div class="review-toolbar inventory-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder="Search entities" />
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
    <span>{viewMode === "merged" ? "merged entities" : "entity candidates"} matched. Showing {visibleRows.length}.</span>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="entity-candidate-grid">
    {#each visibleRows as row}
      <article class={`entity-candidate-card ${row.bucket} ${expanded[row.row_id] ? "expanded" : ""}`}>
        <header class="entity-candidate-header">
          <div>
            <span class="caption">{row.bucket} - {row.category}</span>
            <h3>{row.candidate_name}</h3>
          </div>
          <button
            class="compact secondary"
            aria-expanded={Boolean(expanded[row.row_id])}
            on:click={() => toggleExpanded(row)}
          >
            {#if expanded[row.row_id]}
              <ChevronDown size={15} /> Open
            {:else}
              <ChevronRight size={15} /> Details
            {/if}
          </button>
        </header>

        <div class="entity-card-stats">
          <span><strong>{row.evidence_count ?? 0}</strong> evidence</span>
          <span>{row.proposed_entity_type || "type unknown"}</span>
          {#if row.referent_kind_label}
            <span class="referent-kind-pill" title={row.referent_kind || ""}>{row.referent_kind_label}</span>
          {/if}
          <span>{reviewLabel(row)}</span>
        </div>

        {#if visibleThemeAssociations(row).length}
          <div class="inventory-meta compact-meta">
            {#each visibleThemeAssociations(row) as association}
              <span>{association.theme_label}</span>
            {/each}
            {#if hiddenThemeAssociationCount(row) && !themesExpanded[row.row_id]}
              <button type="button" class="chip-toggle" on:click={() => toggleThemes(row)}>
                +{hiddenThemeAssociationCount(row)} themes
              </button>
            {:else if themesExpanded[row.row_id] && themeAssociations(row).length > 3}
              <button type="button" class="chip-toggle" on:click={() => toggleThemes(row)}>
                Show fewer
              </button>
            {/if}
          </div>
        {/if}

        <p class="entity-card-preview">{row.triage_reason || textValue(row) || "No preview recorded."}</p>

        {#if rowAliases(row).length}
          <div class="inventory-meta compact-meta alias-chip-list">
            <span class="caption">Also known as</span>
            {#each visibleAliases(row) as alias}
              <span>{alias}</span>
            {/each}
            {#if hiddenAliasCount(row)}
              <button type="button" class="chip-toggle" on:click={() => toggleAliases(row)}>
                +{hiddenAliasCount(row)} more names
              </button>
            {:else if aliasExpanded[row.row_id] && rowAliases(row).length > 8}
              <button type="button" class="chip-toggle" on:click={() => toggleAliases(row)}>
                Show fewer
              </button>
            {/if}
          </div>
        {/if}

        {#if row.tracks?.length || row.topics?.length}
          <div class="inventory-meta compact-meta">
            {#if row.tracks?.length}<span>{row.tracks.join(", ")}</span>{/if}
            {#if row.topics?.length}<span>{row.topics.slice(0, 3).join(", ")}</span>{/if}
          </div>
        {/if}

        {#if expanded[row.row_id]}
          <div class="entity-card-details">
            {#if textValue(row) && textValue(row) !== row.candidate_name}
              <p class="inventory-reason"><strong>Source:</strong> {textValue(row)}</p>
            {/if}

            {#if evidenceLoading[row.row_id]}
              <p class="quiet-meta">Loading evidence...</p>
            {:else if evidenceByRow[row.row_id]}
              <div class="entity-evidence-panel">
                {#if evidenceByRow[row.row_id].merged_from_entities.length}
                  <section>
                    <span class="caption">Merged From</span>
                    <div class="inventory-meta">
                      {#each evidenceByRow[row.row_id].merged_from_entities as source}
                        <span>{textField(source.canonical_name || source.entity_id)}</span>
                      {/each}
                    </div>
                  </section>
                {/if}

                {#if evidenceThemeAssociations(row).length}
                  <section>
                    <span class="caption">Theme Hierarchy</span>
                    <div class="entity-theme-list">
                      {#each evidenceThemeAssociations(row) as association}
                        <article class="entity-theme-row">
                          <div>
                            <strong>{association.theme_label}</strong>
                            <span>
                              {entityThemeRankLabel(association)}
                              {#if themeCandidateRankLabel(association)}
                                - {themeCandidateRankLabel(association)}
                              {/if}
                              - rank {percent(association.ranking_score)}
                            </span>
                          </div>
                          <span>{percent(association.match_strength)} match</span>
                        </article>
                      {/each}
                    </div>
                  </section>
                {/if}

                {#if evidenceByRow[row.row_id].claims.length}
                  <section>
                    <span class="caption">Accepted Claims</span>
                    {#each evidenceByRow[row.row_id].claims.slice(0, 10) as claim}
                      <p class="inventory-reason">{textField(claim.claim_text)}</p>
                    {/each}
                  </section>
                {/if}

                {#if evidenceByRow[row.row_id].sample_texts.length}
                  <section>
                    <span class="caption">Evidence Samples</span>
                    {#each evidenceByRow[row.row_id].sample_texts.slice(0, 8) as sample}
                      <p class="inventory-reason">{sample}</p>
                    {/each}
                  </section>
                {/if}

                {#if evidenceByRow[row.row_id].type_evidence.length}
                  <section>
                    <span class="caption">Type Evidence</span>
                    <div class="inventory-meta">
                      {#each evidenceByRow[row.row_id].type_evidence.slice(0, 12) as evidence}
                        <span>{textField(evidence.entity_type)}: {textField(evidence.basis)}</span>
                      {/each}
                    </div>
                  </section>
                {/if}

                {#if evidenceByRow[row.row_id].snippets.length}
                  <section>
                    <span class="caption">Source Snippets</span>
                    {#each evidenceByRow[row.row_id].snippets.slice(0, 8) as snippet}
                      <p class="inventory-reason">
                        <strong>{textField(snippet.topic_label || snippet.snippet_id)}</strong>
                        {#if snippet.text}: {textField(snippet.text)}{/if}
                      </p>
                    {/each}
                  </section>
                {/if}

                {#if evidenceByRow[row.row_id].merge_records.length}
                  <details class="entity-merge-details">
                    <summary>
                      <span>Merge Evidence</span>
                      <strong>{evidenceByRow[row.row_id].merge_records.length} records</strong>
                    </summary>
                    <div class="entity-merge-records">
                      {#each evidenceByRow[row.row_id].merge_records.slice(0, 8) as record}
                        <p class="inventory-reason">
                          {textField(record.source_entity_name)} -> {textField(record.target_entity_name)}
                          {#if record.rationale}: {textField(record.rationale)}{/if}
                        </p>
                      {/each}
                      {#if evidenceByRow[row.row_id].merge_records.length > 8}
                        <p class="quiet-meta">Showing 8 of {evidenceByRow[row.row_id].merge_records.length} merge records.</p>
                      {/if}
                    </div>
                  </details>
                {/if}
              </div>
            {/if}

            {#if !isMergedRow(row)}
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

              <textarea bind:value={rationales[row.row_id]} placeholder="Decision rationale"></textarea>

              <div class="inventory-actions">
                <button disabled={disabled} on:click={() => decide(row, "approve")}><Check size={16} /> Approve</button>
                <button class="secondary" disabled={disabled} on:click={() => decide(row, "defer")}><HelpCircle size={16} /> Defer</button>
                <button class="danger" disabled={disabled} on:click={() => decide(row, "reject")}><X size={16} /> Reject</button>
              </div>
            {/if}
          </div>
        {/if}
      </article>
    {/each}
  </div>

  {#if visibleRows.length < filtered.length}
    <button class="secondary load-more" on:click={() => (visibleLimit += 350)}>Show more</button>
  {/if}
</section>
