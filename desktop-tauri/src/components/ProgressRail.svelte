<script lang="ts">
  import type { PipelineProgress } from "../lib/types";

  export let progress: PipelineProgress | null = null;
</script>

<section class="progress-panel">
  <div class="panel-heading">
    <span class="caption">Pipeline</span>
    <strong>{progress?.summary ?? "No progress snapshot"}</strong>
  </div>
  <div class="stage-rail">
    {#each progress?.stages ?? [] as stage}
      <div class={`stage-node ${stage.state}`} title={`${stage.short_label} ${stage.name}`}>
        <span class="dot">{stage.short_label}</span>
        <span>{stage.name}</span>
      </div>
    {/each}
  </div>
</section>
