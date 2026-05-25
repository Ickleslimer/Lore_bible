<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import type { PipelineProgress, PipelineStage, ThemeRescueResponse } from "../lib/types";

  export let progress: PipelineProgress | null = null;
  export let themeRescue: ThemeRescueResponse | null = null;
  export let disabled = false;

  const dispatch = createEventDispatcher<{ runFromStage: number }>();

  function handleStageClick(stage: PipelineStage) {
    if (disabled) return;
    if (!confirm(`Run pipeline starting from ${stage.name} (Stage ${stage.index})? Earlier stages will be skipped.`)) {
      return;
    }
    dispatch("runFromStage", stage.index);
  }

  function rescueStateLabel(state: string): string {
    switch (state) {
      case "done":
        return "done";
      case "ready":
        return "ready";
      case "waiting":
        return "waiting";
      default:
        return state;
    }
  }
</script>

<section class="progress-panel">
  <div class="panel-heading">
    <span class="caption">Pipeline</span>
    <strong>{progress?.summary ?? "No progress snapshot"}</strong>
  </div>
  <div class="stage-rail">
    {#each progress?.stages ?? [] as stage}
      <button
        class={`stage-node ${stage.state}`}
        class:row-end={stage.index % 6 === 0}
        class:clickable={!disabled}
        title={`${stage.short_label} ${stage.name} — Click to run from this stage`}
        on:click={() => handleStageClick(stage)}
      >
        <span class="dot">{stage.short_label}</span>
        <span>{stage.name}</span>
      </button>
      {#if stage.index === 6 && themeRescue?.enabled}
        <div class="rescue-branch" aria-label="Theme rescue branch">
          <span class="rescue-branch-label">Rescue branch</span>
          <div class="rescue-branch-steps">
            {#each themeRescue.processes ?? [] as process}
              <div class={`rescue-branch-step ${rescueStateLabel(process.state)}`} title={process.description}>
                <span class="dot">{process.short_label}</span>
                <span>{process.name}</span>
              </div>
            {/each}
          </div>
        </div>
      {/if}
    {/each}
  </div>
</section>
