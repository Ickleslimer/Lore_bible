<script lang="ts">
  import { Handle, Position } from "@xyflow/svelte";

  export let data: {
    name?: string;
    entityType?: string;
    degree?: number;
    evidenceCount?: number;
    resolved?: boolean;
    match?: boolean;
    track?: string;
    hub?: boolean;
  } = {};
  export let selected = false;

  const handleSides = [
    { id: "top", position: Position.Top },
    { id: "right", position: Position.Right },
    { id: "bottom", position: Position.Bottom },
    { id: "left", position: Position.Left },
  ];

  $: degree = Number(data.degree ?? 0);
  $: evidenceCount = Number(data.evidenceCount ?? 0);
</script>

<div
  class={`relationship-flow-node ${data.hub ? "hub" : ""} ${data.resolved === false ? "unresolved" : ""} ${data.match ? "match" : ""} ${selected ? "selected" : ""}`}
>
  {#each handleSides as side}
    <Handle type="source" id={`source-${side.id}`} position={side.position} class="relationship-flow-handle hidden" />
    <Handle type="target" id={`target-${side.id}`} position={side.position} class="relationship-flow-handle hidden" />
  {/each}
  <span class="node-kicker">{data.resolved === false ? "Unresolved reference" : data.hub ? "Relationship hub" : "Entity"}</span>
  <strong>{data.name || "Unnamed"}</strong>
  <small>{data.entityType || "entity"} / {degree} links / {evidenceCount} evidence</small>
</div>

<style>
  .relationship-flow-node {
    position: relative;
    width: 220px;
    min-height: 78px;
    padding: 11px 13px;
    border: 2px solid #2563eb;
    border-radius: 8px;
    background: #fff;
    box-shadow: 0 12px 26px rgba(15, 23, 42, 0.14);
  }

  .relationship-flow-node.hub {
    width: 260px;
    min-height: 92px;
    border-color: #0f172a;
    background: #f8fafc;
  }

  .relationship-flow-node.unresolved {
    border-color: #b7791f;
    border-style: dashed;
    background: #fffbeb;
  }

  .relationship-flow-node.match,
  .relationship-flow-node.selected {
    box-shadow:
      0 0 0 4px rgba(37, 99, 235, 0.22),
      0 14px 30px rgba(15, 23, 42, 0.18);
  }

  .relationship-flow-node strong,
  .relationship-flow-node small,
  .node-kicker {
    display: block;
    overflow-wrap: anywhere;
  }

  .relationship-flow-node strong {
    margin-top: 3px;
    color: #0f172a;
    font-size: 15px;
    line-height: 1.18;
  }

  .relationship-flow-node.hub strong {
    font-size: 18px;
  }

  .relationship-flow-node small {
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

  :global(.relationship-flow-handle) {
    width: 9px;
    height: 9px;
    border: 2px solid #fff;
    background: #0f172a;
  }

  :global(.relationship-flow-handle.hidden) {
    opacity: 0;
    pointer-events: none;
  }
</style>
