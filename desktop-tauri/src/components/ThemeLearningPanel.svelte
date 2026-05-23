<script lang="ts">
  import { onMount } from "svelte";
  import { RefreshCcw, Search } from "lucide-svelte";
  import { loadThemeLearning } from "../lib/api";
  import type { ThemeAssociationRow, ThemeLearningResponse, ThemeProfileItem } from "../lib/types";

  export let artifactsRoot = "";
  export let disabled = false;

  let response: ThemeLearningResponse | null = null;
  let loading = false;
  let localError = "";
  let loadedRoot = "";
  let search = "";
  let statusFilter = "all";
  let selectedThemeId = "all";
  let associationSort = "strength";
  let visibleLimit = 180;

  $: themes = response?.themes ?? [];
  $: associations = response?.associations ?? [];
  $: activeThemeCount = themes.filter((theme) => theme.status === "active").length;
  $: candidateThemeCount = themes.filter((theme) => theme.status === "candidate").length;
  $: statuses = uniqueValues(themes.map((theme) => theme.status));
  $: filteredThemes = sortThemes(
    themes.filter((theme) => {
      const query = search.trim().toLowerCase();
      return (statusFilter === "all" || theme.status === statusFilter) && (!query || themeSearchText(theme).includes(query));
    }),
  );
  $: if (selectedThemeId !== "all" && !themes.some((theme) => theme.theme_id === selectedThemeId)) {
    selectedThemeId = "all";
  }
  $: selectedTheme = themes.find((theme) => theme.theme_id === selectedThemeId);
  $: filteredAssociations = sortAssociations(
    associations.filter((association) => {
      const query = search.trim().toLowerCase();
      return (selectedThemeId === "all" || association.theme_id === selectedThemeId)
        && (!query || associationSearchText(association).includes(query));
    }),
  );
  $: visibleAssociations = filteredAssociations.slice(0, visibleLimit);

  onMount(() => {
    if (artifactsRoot) void load();
  });

  $: if (artifactsRoot && artifactsRoot !== loadedRoot && !loading) {
    void load();
  }

  async function load() {
    if (!artifactsRoot || loading) return;
    loading = true;
    localError = "";
    try {
      response = await loadThemeLearning(artifactsRoot);
      loadedRoot = artifactsRoot;
      visibleLimit = 180;
    } catch (err) {
      localError = err instanceof Error ? err.message : String(err);
      response = null;
    } finally {
      loading = false;
    }
  }

  function uniqueValues(values: Array<string | undefined>): string[] {
    return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
  }

  function textList(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    return value.map((item) => String(item || "").trim()).filter(Boolean);
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

  function compactNumber(value: unknown): string {
    const numeric = numberValue(value);
    if (numeric >= 1000) return numeric.toLocaleString();
    return String(numeric);
  }

  function themeSearchText(theme: ThemeProfileItem): string {
    return [
      theme.label,
      theme.theme_id,
      theme.theme_type,
      theme.status,
      theme.canon_relevance,
      theme.description,
      theme.provenance_summary,
      ...textList(theme.evidence_entities),
      ...textList(theme.positive_indicators),
      ...textList(theme.negative_indicators),
      ...textList(theme.disambiguation_notes),
      ...textList(theme.pattern_notes),
    ]
      .join(" ")
      .toLowerCase();
  }

  function associationSearchText(association: ThemeAssociationRow): string {
    return [
      association.theme_label,
      association.candidate_name,
      association.normalized_key,
      association.externality_class,
      association.base_recommended_action,
      association.theme_adjusted_recommended_action,
      association.reason,
      association.model_reasoning_summary,
      association.human_review_question,
      ...textList(association.matched_indicators),
    ]
      .join(" ")
      .toLowerCase();
  }

  function sortThemes(input: ThemeProfileItem[]): ThemeProfileItem[] {
    return [...input].sort((a, b) => {
      const confidence = numberValue(b.confidence) - numberValue(a.confidence);
      if (confidence !== 0) return confidence;
      return a.label.localeCompare(b.label);
    });
  }

  function sortAssociations(input: ThemeAssociationRow[]): ThemeAssociationRow[] {
    const copy = [...input];
    copy.sort((a, b) => a.candidate_name.localeCompare(b.candidate_name));
    copy.sort((a, b) => {
      if (associationSort === "prior") return numberValue(b.theme_adjusted_lore_prior) - numberValue(a.theme_adjusted_lore_prior);
      if (associationSort === "boost") return numberValue(b.prior_boost) - numberValue(a.prior_boost);
      if (associationSort === "theme") return a.theme_label.localeCompare(b.theme_label);
      return numberValue(b.match_strength) - numberValue(a.match_strength);
    });
    return copy;
  }

  function associationCountFor(themeId: string): number {
    return associations.filter((association) => association.theme_id === themeId).length;
  }

  function summaryNumber(key: string): string {
    return compactNumber(response?.summary?.[key]);
  }

  function summaryText(key: string): string {
    const value = response?.summary?.[key];
    return value === undefined || value === null ? "" : String(value);
  }
</script>

<section class="theme-learning-panel">
  <div class="theme-learning-header">
    <div>
      <span class="caption">Theme Learning</span>
      <h3>Learned Associations</h3>
      <p>{response?.summary?.["theme_profile_updated_at_utc"] ? `Updated ${response.summary["theme_profile_updated_at_utc"]}` : "No theme profile update recorded"}</p>
    </div>
    <button class="secondary" disabled={disabled || loading} on:click={load}>
      <RefreshCcw size={16} /> Refresh
    </button>
  </div>

  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="theme-stats">
    <article>
      <span class="caption">Themes</span>
      <strong>{themes.length}</strong>
    </article>
    <article>
      <span class="caption">Active</span>
      <strong>{activeThemeCount}</strong>
    </article>
    <article>
      <span class="caption">Candidate</span>
      <strong>{candidateThemeCount}</strong>
    </article>
    <article>
      <span class="caption">Associations</span>
      <strong>{associations.length}</strong>
    </article>
    <article>
      <span class="caption">Evidence Packets</span>
      <strong>{summaryNumber("evidence_packet_count")}</strong>
    </article>
  </div>

  <div class="review-toolbar theme-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder="Search themes and associations" />
    </label>
    <select bind:value={statusFilter}>
      <option value="all">All statuses</option>
      {#each statuses as status}
        <option value={status}>{status}</option>
      {/each}
    </select>
    <select bind:value={associationSort}>
      <option value="strength">Sort: match</option>
      <option value="prior">Sort: adjusted prior</option>
      <option value="boost">Sort: boost</option>
      <option value="theme">Sort: theme</option>
    </select>
  </div>

  {#if loading && !response}
    <div class="loading-panel">Loading theme profile...</div>
  {:else if !themes.length}
    <section class="empty-draft-panel">
      <h3>No Themes Yet</h3>
      <p>Stage 07C has not produced learned theme associations for this workspace.</p>
    </section>
  {:else}
    <div class="theme-learning-layout">
      <aside class="theme-list-panel">
        <button
          type="button"
          class={`theme-select-card all-themes ${selectedThemeId === "all" ? "selected" : ""}`}
          on:click={() => (selectedThemeId = "all")}
        >
          <span class="caption">All Themes</span>
          <strong>{associations.length}</strong>
          <small>candidate associations</small>
        </button>

        {#each filteredThemes as theme}
          <button
            type="button"
            class={`theme-select-card ${selectedThemeId === theme.theme_id ? "selected" : ""}`}
            on:click={() => (selectedThemeId = theme.theme_id)}
          >
            <span class="caption">{theme.status} - {theme.theme_type || "theme"}</span>
            <strong>{theme.label}</strong>
            <small>{associationCountFor(theme.theme_id)} associations - {percent(theme.confidence)} confidence</small>
          </button>
        {/each}
      </aside>

      <div class="theme-detail-panel">
        {#if selectedTheme}
          <article class="theme-detail-card">
            <header class="theme-card-header">
              <div>
                <span class="caption">{selectedTheme.status} - {selectedTheme.theme_type || "theme"}</span>
                <h3>{selectedTheme.label}</h3>
              </div>
              <span class="status-pill">{percent(selectedTheme.confidence)} confidence</span>
            </header>
            {#if selectedTheme.description}
              <p class="theme-description">{selectedTheme.description}</p>
            {/if}
            {#if selectedTheme.provenance_summary}
              <p class="inventory-reason">{selectedTheme.provenance_summary}</p>
            {/if}
            <div class="theme-chip-groups">
              {#if textList(selectedTheme.evidence_entities).length}
                <section>
                  <span class="caption">Evidence Entities</span>
                  <div class="inventory-meta">
                    {#each textList(selectedTheme.evidence_entities).slice(0, 16) as entity}
                      <span>{entity}</span>
                    {/each}
                  </div>
                </section>
              {/if}
              {#if textList(selectedTheme.positive_indicators).length}
                <section>
                  <span class="caption">Positive Indicators</span>
                  <div class="inventory-meta">
                    {#each textList(selectedTheme.positive_indicators).slice(0, 12) as indicator}
                      <span>{indicator}</span>
                    {/each}
                  </div>
                </section>
              {/if}
              {#if textList(selectedTheme.disambiguation_notes).length}
                <section>
                  <span class="caption">Disambiguation</span>
                  <div class="theme-notes">
                    {#each textList(selectedTheme.disambiguation_notes).slice(0, 5) as note}
                      <p>{note}</p>
                    {/each}
                  </div>
                </section>
              {/if}
            </div>
          </article>
        {/if}

        <div class="theme-association-header">
          <div>
            <span class="caption">{selectedTheme ? selectedTheme.label : "All Themes"}</span>
            <h3>{filteredAssociations.length} Associations</h3>
          </div>
          <span class="status-pill">{summaryText("theme_reclassification_generated_at_utc") || "07D"}</span>
        </div>

        <div class="theme-association-list">
          {#each visibleAssociations as association}
            <article class="theme-association-card">
              <header class="theme-card-header">
                <div>
                  <span class="caption">{association.theme_label}</span>
                  <h4>{association.candidate_name}</h4>
                </div>
                <span class="status-pill">{percent(association.match_strength)} match</span>
              </header>
              <div class="entity-card-stats">
                <span>boost <strong>{percent(association.prior_boost)}</strong></span>
                <span>prior <strong>{percent(association.theme_adjusted_lore_prior)}</strong></span>
                <span>{association.theme_adjusted_recommended_action || association.base_recommended_action || "review"}</span>
                {#if association.externality_class}
                  <span>{association.externality_class}</span>
                {/if}
              </div>
              {#if textList(association.matched_indicators).length}
                <div class="inventory-meta compact-meta">
                  {#each textList(association.matched_indicators).slice(0, 10) as indicator}
                    <span>{indicator}</span>
                  {/each}
                </div>
              {/if}
              {#if association.reason}
                <p class="inventory-reason">{association.reason}</p>
              {/if}
              {#if association.model_reasoning_summary}
                <p class="theme-description">{association.model_reasoning_summary}</p>
              {/if}
              {#if association.human_review_question}
                <p class="theme-review-question">{association.human_review_question}</p>
              {/if}
            </article>
          {/each}
        </div>

        {#if visibleAssociations.length < filteredAssociations.length}
          <button class="secondary load-more" on:click={() => (visibleLimit += 180)}>Show more</button>
        {/if}
      </div>
    </div>
  {/if}
</section>
