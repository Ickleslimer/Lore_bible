<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import type { ReviewRun } from "../lib/types";

  export let runs: ReviewRun[] = [];
  export let activeRoot = "";

  const dispatch = createEventDispatcher<{ changeRun: string }>();

  function onChange(event: Event) {
    const value = (event.currentTarget as HTMLSelectElement).value;
    dispatch("changeRun", value);
  }
</script>

<label class="run-select">
  <span class="caption">Run</span>
  <select value={activeRoot} on:change={onChange}>
    {#each runs as run}
      <option value={run.artifacts_root}>{run.label} · {run.pending_total} pending</option>
    {/each}
  </select>
</label>
