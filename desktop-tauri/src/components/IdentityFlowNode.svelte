<script lang="ts">
  import { Handle, Position } from "@xyflow/svelte";

  export let data: {
    kind?: "central" | "leaf";
    name?: string;
    entityType?: string;
    status?: string;
    evidenceCount?: number;
    bucket?: string;
    match?: boolean;
    split?: boolean;
  } = {};
  export let selected = false;

  const handleSides = [
    { id: "top", position: Position.Top },
    { id: "right", position: Position.Right },
    { id: "bottom", position: Position.Bottom },
    { id: "left", position: Position.Left },
  ];

  $: kind = data.kind ?? "leaf";
  $: status = data.status ?? "pending";
  $: evidenceCount = Number(data.evidenceCount ?? 0);
</script>

<div class={`identity-flow-node ${kind} ${status} ${data.split ? "split" : ""} ${data.match ? "match" : ""} ${selected ? "selected" : ""}`}>
  {#if kind === "central"}
    {#each handleSides as side}
      <Handle type="target" id={`target-${side.id}`} position={side.position} class="identity-flow-handle hidden" />
    {/each}
  {:else}
    {#each handleSides as side}
      <Handle type="source" id={`source-${side.id}`} position={side.position} class="identity-flow-handle hidden" />
    {/each}
  {/if}
  <span class="node-kicker">{kind === "central" ? "Central card" : data.split ? "Split suggested" : "Merge candidate"}</span>
  <strong>{data.name || "Unnamed"}</strong>
  <small>
    {data.entityType || "entity"}
    {#if kind !== "central"}
      / {status === "accepted" ? "kept" : status === "refuted" ? "refuted" : status === "deferred" ? "deferred" : "proposed"}
      / {evidenceCount} claims
    {:else if data.bucket}
      / {data.bucket}
    {/if}
  </small>
</div>

<style>
  .identity-flow-node {
    position: relative;
    width: 230px;
    min-height: 78px;
    padding: 11px 13px;
    border: 2px solid #16803c;
    border-radius: 8px;
    background: #fff;
    box-shadow: 0 12px 26px rgba(15, 23, 42, 0.14);
  }

  .identity-flow-node.central {
    width: 300px;
    min-height: 92px;
    border-color: #0f172a;
    background: #f8fafc;
    text-align: center;
  }

  .identity-flow-node.pending {
    border-style: dashed;
  }

  .identity-flow-node.refuted {
    border-color: #dc2626;
    background: #fff5f5;
  }

  .identity-flow-node.deferred,
  .identity-flow-node.split {
    border-color: #b7791f;
    background: #fffbeb;
  }

  .identity-flow-node.match {
    box-shadow:
      0 0 0 4px rgba(37, 99, 235, 0.18),
      0 12px 26px rgba(15, 23, 42, 0.14);
  }

  .identity-flow-node.selected {
    box-shadow:
      0 0 0 4px rgba(37, 99, 235, 0.26),
      0 14px 30px rgba(15, 23, 42, 0.18);
  }

  .identity-flow-node strong,
  .identity-flow-node small,
  .node-kicker {
    display: block;
    overflow-wrap: anywhere;
  }

  .identity-flow-node strong {
    margin-top: 3px;
    color: #0f172a;
    font-size: 15px;
    line-height: 1.18;
  }

  .identity-flow-node.central strong {
    font-size: 20px;
  }

  .identity-flow-node small {
    margin-top: 4px;
    color: #64748b;
    font-size: 11px;
  }

  .node-kicker {
    color: #64748b;
    font-size: 10px;
    font-weight: 900;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }

  :global(.identity-flow-handle) {
    width: 9px;
    height: 9px;
    border: 2px solid #fff;
    background: #0f172a;
  }

  :global(.identity-flow-handle.hidden) {
    opacity: 0;
    pointer-events: none;
  }
</style>
