<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import type { PipelineProgress, PipelineStage } from "../lib/types";

  export let progress: PipelineProgress | null = null;
  export let disabled = false;

  const dispatch = createEventDispatcher<{ runFromStage: number }>();

  function handleStageClick(stage: PipelineStage) {
    if (disabled) return;
    if (!confirm(`Run pipeline starting from ${stage.name} (Stage ${stage.index})? Earlier stages will be skipped.`)) {
      return;
    }
    dispatch("runFromStage", stage.index);
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
    {/each}
  </div>
</section>
