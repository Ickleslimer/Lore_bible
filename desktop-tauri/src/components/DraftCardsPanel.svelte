<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import { AlertTriangle, BookOpen, RefreshCcw, Search } from "lucide-svelte";
  import { loadDraftCards } from "../lib/api";
  import type { DraftCardItem, DraftCardsMetadata } from "../lib/types";

  export let artifactsRoot = "";
  export let disabled = false;

  let cards: DraftCardItem[] = [];
  let failures: Array<Record<string, unknown>> = [];
  let metadata: DraftCardsMetadata = {};
  let loading = false;
  let localError = "";
  let search = "";
  let statusFilter = "all";
  let sortKey = "name";
  let selectedId = "";
  let autoRefresh = true;
  let loadedRoot = "";
  let refreshTimer: number | undefined;

  $: statuses = uniqueValues(cards.map((card) => card.status));
  $: filtered = sortCards(
    cards.filter((card) => {
      const query = search.trim().toLowerCase();
      const haystack = [
        card.canonical_name,
        card.entity_type,
        card.status,
        card.summary,
        ...card.sections.map((section) => `${section.title} ${section.text}`),
      ]
        .join(" ")
        .toLowerCase();
      return (statusFilter === "all" || card.status === statusFilter) && (!query || haystack.includes(query));
    }),
  );
  $: if (filtered.length && !filtered.some((card) => card.card_id === selectedId)) selectedId = filtered[0].card_id;
  $: selected = filtered.find((card) => card.card_id === selectedId) ?? filtered[0];
  $: progressText = progressLabel(metadata);
  $: sourceText = sourceLabel(metadata);

  function uniqueValues(values: Array<string | undefined>): string[] {
    return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
  }

  function sortCards(input: DraftCardItem[]): DraftCardItem[] {
    const copy = [...input];
    copy.sort((a, b) => a.canonical_name.localeCompare(b.canonical_name));
    copy.sort((a, b) => {
      if (sortKey === "words") return b.word_count - a.word_count;
      if (sortKey === "claims") return b.claim_count - a.claim_count;
      if (sortKey === "evidence") return b.evidence_count - a.evidence_count;
      if (sortKey === "sections") return b.section_count - a.section_count;
      if (sortKey === "status") return a.status.localeCompare(b.status);
      return a.canonical_name.localeCompare(b.canonical_name);
    });
    return copy;
  }

  function progressLabel(value: DraftCardsMetadata): string {
    const processed = Number(value.processed_count || 0);
    const total = Number(value.total_count || 0);
    if (total > 0) return `${processed}/${total} work items processed`;
    return "No synthesis progress recorded yet";
  }

  function sourceLabel(value: DraftCardsMetadata): string {
    const source = String(value.source_kind || "missing");
    if (source === "partial" || source === "checkpoint") return `Live ${source}`;
    if (source === "final") return "Final draft file";
    return "No draft file yet";
  }

  function textValue(value: unknown): string {
    return String(value ?? "").trim();
  }

  async function load() {
    if (!artifactsRoot || loading) return;
    loading = true;
    localError = "";
    try {
      const response = await loadDraftCards(artifactsRoot);
      cards = response.cards;
      failures = response.failures ?? [];
      metadata = response.metadata ?? {};
      if (cards.length && !cards.some((card) => card.card_id === selectedId)) {
        selectedId = cards[0].card_id;
      }
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
    }
  }

  function scheduleRefresh() {
    if (refreshTimer) {
      window.clearInterval(refreshTimer);
      refreshTimer = undefined;
    }
    refreshTimer = window.setInterval(() => {
      if (autoRefresh && !disabled && artifactsRoot) void load();
    }, 4500);
  }

  $: if (artifactsRoot && artifactsRoot !== loadedRoot) {
    loadedRoot = artifactsRoot;
    selectedId = "";
    void load();
  }

  onMount(() => {
    void load();
    scheduleRefresh();
  });

  onDestroy(() => {
    if (refreshTimer) window.clearInterval(refreshTimer);
  });
</script>

<section class="draft-viewer">
  <div class="draft-header">
    <div>
      <span class="caption">Draft Cards</span>
      <h3>{sourceText}</h3>
      <p>{progressText}{metadata.current_entity_name ? `, currently ${metadata.current_entity_name}` : ""}</p>
    </div>
    <div class="draft-actions">
      <label class="check-line compact-check">
        <input type="checkbox" bind:checked={autoRefresh} />
        Live refresh
      </label>
      <button class="secondary" disabled={disabled || loading} on:click={load}>
        <RefreshCcw size={16} /> Refresh
      </button>
    </div>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="draft-stats">
    <article>
      <span class="caption">Cards</span>
      <strong>{cards.length}</strong>
    </article>
    <article>
      <span class="caption">Words</span>
      <strong>{cards.reduce((total, card) => total + card.word_count, 0)}</strong>
    </article>
    <article>
      <span class="caption">Failures</span>
      <strong>{metadata.failure_count ?? failures.length}</strong>
    </article>
    <article title={metadata.source_path || ""}>
      <span class="caption">Updated</span>
      <strong>{metadata.updated_at_utc ? metadata.updated_at_utc.slice(11, 19) : "n/a"}</strong>
    </article>
  </div>

  <div class="review-toolbar draft-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder="Search draft cards" />
    </label>
    <select bind:value={statusFilter}>
      <option value="all">All statuses</option>
      {#each statuses as status}
        <option value={status}>{status}</option>
      {/each}
    </select>
    <select bind:value={sortKey}>
      <option value="name">Sort: name</option>
      <option value="words">Sort: words</option>
      <option value="claims">Sort: claims</option>
      <option value="evidence">Sort: evidence</option>
      <option value="sections">Sort: sections</option>
      <option value="status">Sort: status</option>
    </select>
  </div>

  {#if cards.length === 0}
    <section class="empty-draft-panel">
      <BookOpen size={22} />
      <div>
        <h3>No draft cards yet</h3>
        <p>Stage 11 will appear here as soon as a checkpoint, partial draft file, or final draft file is written.</p>
      </div>
    </section>
  {:else}
    <div class="draft-layout">
      <aside class="draft-list">
        <div class="inventory-summary">
          <strong>{filtered.length}</strong>
          <span>matched cards</span>
        </div>
        {#each filtered as card}
          <button class:active={selectedId === card.card_id} class="draft-list-item" on:click={() => (selectedId = card.card_id)}>
            <span>{card.canonical_name}</span>
            <small>{card.word_count} words, {card.claim_count} claims</small>
          </button>
        {/each}
      </aside>

      {#if selected}
        <article class="draft-card-preview">
          <header>
            <div>
              <span class="caption">{selected.entity_type || "entity"} / {selected.status}</span>
              <h2>{selected.canonical_name}</h2>
            </div>
            <span class="status-pill">{selected.word_count} words</span>
          </header>

          <p class="draft-summary">{selected.summary || "No summary written yet."}</p>

          <div class="inventory-meta">
            <span>{selected.section_count} sections</span>
            <span>{selected.claim_count} accepted claims</span>
            <span>{selected.evidence_count} evidence items</span>
          </div>

          {#each selected.sections as section}
            <section class="draft-section">
              <h3>{section.title}</h3>
              <p>{section.text}</p>
            </section>
          {/each}

          {#if selected.relationships?.length}
            <section class="draft-section compact">
              <h3>Structured Relationships</h3>
              {#each selected.relationships.slice(0, 30) as relationship}
                <p><strong>{textValue(relationship.relation_type || "related")}</strong>: {textValue(relationship.target_card_id || relationship.target_entity_name)} {textValue(relationship.note)}</p>
              {/each}
            </section>
          {/if}

          {#if selected.wiki_links?.length}
            <section class="draft-section compact">
              <h3>Wiki Links</h3>
              {#each selected.wiki_links.slice(0, 40) as link}
                <p><strong>{textValue(link.relation_type || "related")}</strong>: {textValue(link.target_entity_name || link.target_card_id)} {textValue(link.section)}</p>
              {/each}
            </section>
          {/if}

          {#if selected.unresolved_conflicts?.length}
            <section class="draft-section warning">
              <h3><AlertTriangle size={16} /> Unresolved Conflicts</h3>
              {#each selected.unresolved_conflicts.slice(0, 20) as conflict}
                <p>{typeof conflict === "string" ? conflict : JSON.stringify(conflict)}</p>
              {/each}
            </section>
          {/if}
        </article>
      {/if}
    </div>
  {/if}
</section>
