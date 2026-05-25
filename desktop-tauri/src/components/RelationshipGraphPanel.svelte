<script lang="ts">
  import { onMount } from "svelte";
  import {
    Background,
    BackgroundVariant,
    Controls,
    MarkerType,
    MiniMap,
    Panel,
    SvelteFlow,
    type Edge,
    type Node,
  } from "@xyflow/svelte";
  import "@xyflow/svelte/dist/style.css";
  import { Network, RefreshCcw, Search } from "lucide-svelte";
  import { loadEntityRelationships } from "../lib/api";
  import type { RelationshipGraphEdge, RelationshipGraphNode, RelationshipGraphResponse } from "../lib/types";
  import RelationshipFlowNode from "./RelationshipFlowNode.svelte";

  export let artifactsRoot = "";
  export let disabled = false;

  interface RelationshipNodeData extends Record<string, unknown> {
    relationshipNode: RelationshipGraphNode;
    name: string;
    entityType: string;
    degree: number;
    evidenceCount: number;
    resolved: boolean;
    match: boolean;
    hub: boolean;
  }

  interface RelationshipEdgeData extends Record<string, unknown> {
    relationshipEdge: RelationshipGraphEdge;
    match: boolean;
  }

  type RelationshipNode = Node<RelationshipNodeData, "relationshipCard">;
  type RelationshipFlowEdge = Edge<RelationshipEdgeData, "smoothstep">;
  type SelectedItem =
    | { kind: "node"; node: RelationshipGraphNode; nodeId: string }
    | { kind: "edge"; edge: RelationshipGraphEdge; edgeId: string };

  const nodeTypes = { relationshipCard: RelationshipFlowNode };
  const NODE_WIDTH = 220;
  const HUB_WIDTH = 260;
  const NODE_HEIGHT = 82;

  let response: RelationshipGraphResponse | null = null;
  let flowNodes: RelationshipNode[] = [];
  let flowEdges: RelationshipFlowEdge[] = [];
  let selected: SelectedItem | null = null;
  let loading = false;
  let error = "";
  let loadedRoot = "";
  let search = "";
  let trackFilter = "lore_both";
  let sourceFilter = "all";
  let minEvidence = 1;
  let edgeLimit = 650;

  $: if (artifactsRoot && artifactsRoot !== loadedRoot && !loading) {
    void refresh();
  }

  $: filteredEdges = filterEdges(response?.edges ?? []);
  $: visibleNodeIds = new Set(filteredEdges.flatMap((edge) => [edge.source_id, edge.target_id]));
  $: visibleNodes = (response?.nodes ?? [])
    .filter((node) => visibleNodeIds.has(node.node_id))
    .sort((a, b) => Number(b.degree ?? 0) - Number(a.degree ?? 0) || String(a.name).localeCompare(String(b.name)));
  $: graph = buildFlow(visibleNodes, filteredEdges);
  $: flowNodes = graph.nodes;
  $: flowEdges = graph.edges;
  $: if (selected && !selectionStillVisible(selected, graph.nodes, graph.edges)) {
    selected = null;
  }

  onMount(() => {
    if (artifactsRoot) void refresh();
  });

  async function refresh() {
    if (!artifactsRoot) return;
    loading = true;
    error = "";
    try {
      response = await loadEntityRelationships(artifactsRoot);
      loadedRoot = artifactsRoot;
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      response = null;
    } finally {
      loading = false;
    }
  }

  function filterEdges(edges: RelationshipGraphEdge[]): RelationshipGraphEdge[] {
    const query = search.trim().toLowerCase();
    return edges
      .filter((edge) => {
        if (Number(edge.evidence_count ?? 0) < minEvidence) return false;
        if (trackFilter === "lore_both" && !["lore", "both"].includes(String(edge.track || "").toLowerCase())) return false;
        if (trackFilter !== "all" && trackFilter !== "lore_both" && String(edge.track || "").toLowerCase() !== trackFilter) return false;
        if (sourceFilter !== "all" && !(edge.source_kinds ?? []).includes(sourceFilter)) return false;
        if (!query) return true;
        return edgeSearchText(edge).includes(query);
      })
      .sort((a, b) => Number(b.evidence_count ?? 0) - Number(a.evidence_count ?? 0))
      .slice(0, edgeLimit);
  }

  function edgeSearchText(edge: RelationshipGraphEdge): string {
    return [
      edge.source_name,
      edge.target_name,
      edge.relation_type,
      edge.track,
      ...(edge.descriptions ?? []),
      ...(edge.source_kinds ?? []),
    ]
      .join(" ")
      .toLowerCase();
  }

  function nodeMatches(node: RelationshipGraphNode): boolean {
    const query = search.trim().toLowerCase();
    if (!query) return false;
    return [node.name, node.entity_type, ...(node.aliases ?? [])].join(" ").toLowerCase().includes(query);
  }

  function edgeMatches(edge: RelationshipGraphEdge): boolean {
    const query = search.trim().toLowerCase();
    return Boolean(query && edgeSearchText(edge).includes(query));
  }

  function ringCapacity(ring: number): number {
    return 10 + ring * 8;
  }

  function layoutPositions(nodes: RelationshipGraphNode[]): Record<string, { x: number; y: number }> {
    const positions: Record<string, { x: number; y: number }> = {};
    if (!nodes.length) return positions;
    let remaining = Math.max(0, nodes.length - 1);
    let rings = 0;
    while (remaining > 0) {
      remaining -= ringCapacity(rings);
      rings += 1;
    }
    const maxRadius = 340 + Math.max(0, rings - 1) * 280;
    const centerX = maxRadius + 520;
    const centerY = maxRadius * 0.72 + 500;
    positions[nodes[0].node_id] = { x: centerX - HUB_WIDTH / 2, y: centerY - NODE_HEIGHT / 2 };
    let index = 1;
    for (let ring = 0; index < nodes.length; ring += 1) {
      const capacity = ringCapacity(ring);
      const count = Math.min(capacity, nodes.length - index);
      const radius = 340 + ring * 280;
      const yScale = 0.72;
      const offset = ring % 2 ? Math.PI / capacity : 0;
      for (let slot = 0; slot < count; slot += 1) {
        const angle = (Math.PI * 2 * slot) / count + offset;
        positions[nodes[index].node_id] = {
          x: centerX + Math.cos(angle) * radius - NODE_WIDTH / 2,
          y: centerY + Math.sin(angle) * radius * yScale - NODE_HEIGHT / 2,
        };
        index += 1;
      }
    }
    return positions;
  }

  function linkSides(sourcePosition: { x: number; y: number }, targetPosition: { x: number; y: number }) {
    const sourceCenter = { x: sourcePosition.x + NODE_WIDTH / 2, y: sourcePosition.y + NODE_HEIGHT / 2 };
    const targetCenter = { x: targetPosition.x + NODE_WIDTH / 2, y: targetPosition.y + NODE_HEIGHT / 2 };
    const dx = targetCenter.x - sourceCenter.x;
    const dy = targetCenter.y - sourceCenter.y;
    if (Math.abs(dx) > Math.abs(dy)) {
      return dx > 0
        ? { sourceHandle: "source-right", targetHandle: "target-left" }
        : { sourceHandle: "source-left", targetHandle: "target-right" };
    }
    return dy > 0
      ? { sourceHandle: "source-bottom", targetHandle: "target-top" }
      : { sourceHandle: "source-top", targetHandle: "target-bottom" };
  }

  function buildFlow(nodes: RelationshipGraphNode[], edges: RelationshipGraphEdge[]) {
    const positions = layoutPositions(nodes);
    const nodeList: RelationshipNode[] = nodes.map((node, index) => ({
      id: node.node_id,
      type: "relationshipCard",
      position: positions[node.node_id] ?? { x: 0, y: 0 },
      data: {
        relationshipNode: node,
        name: node.name,
        entityType: String(node.entity_type || "entity"),
        degree: Number(node.degree ?? 0),
        evidenceCount: Number(node.evidence_count ?? 0),
        resolved: node.resolved !== false,
        match: nodeMatches(node),
        hub: index === 0 || Number(node.degree ?? 0) >= 20,
      },
    }));
    const positionById = Object.fromEntries(nodeList.map((node) => [node.id, node.position]));
    const edgeList: RelationshipFlowEdge[] = edges
      .filter((edge) => positionById[edge.source_id] && positionById[edge.target_id])
      .map((edge) => {
        const sides = linkSides(positionById[edge.source_id], positionById[edge.target_id]);
        return {
          id: edge.edge_id,
          type: "smoothstep",
          source: edge.source_id,
          target: edge.target_id,
          sourceHandle: sides.sourceHandle,
          targetHandle: sides.targetHandle,
          class: `relationship-flow-edge ${trackClass(edge.track)} ${edgeMatches(edge) ? "match" : ""} ${edge.source_kinds?.includes("patch_note_relationship") ? "patch" : "structured"}`,
          markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18, color: edgeColor(edge.track) },
          style: `stroke: ${edgeColor(edge.track)}; stroke-width: ${Math.min(7, 1.7 + Number(edge.evidence_count ?? 1) * 0.45)};`,
          data: { relationshipEdge: edge, match: edgeMatches(edge) },
        };
      });
    return { nodes: nodeList, edges: edgeList };
  }

  function trackClass(track: string): string {
    const value = String(track || "unknown").toLowerCase();
    if (["lore", "meta", "both"].includes(value)) return value;
    return "unknown";
  }

  function edgeColor(track: string): string {
    const value = String(track || "unknown").toLowerCase();
    if (value === "lore") return "#16803c";
    if (value === "both") return "#2563eb";
    if (value === "meta") return "#b7791f";
    return "#64748b";
  }

  function minimapColor(node: Node): string {
    const data = node.data as Partial<RelationshipNodeData>;
    if (data.resolved === false) return "#b7791f";
    if (data.hub) return "#0f172a";
    return "#2563eb";
  }

  function selectNode({ node }: { node: RelationshipNode }) {
    selected = { kind: "node", node: node.data.relationshipNode, nodeId: node.id };
  }

  function selectEdge({ edge }: { edge: RelationshipFlowEdge }) {
    if (!edge.data) return;
    selected = { kind: "edge", edge: edge.data.relationshipEdge, edgeId: edge.id };
  }

  function clearSelection() {
    selected = null;
  }

  function selectionStillVisible(item: SelectedItem, nodes: RelationshipNode[], edges: RelationshipFlowEdge[]): boolean {
    if (item.kind === "node") return nodes.some((node) => node.id === item.nodeId);
    return edges.some((edge) => edge.id === item.edgeId);
  }

  function sourceKinds(): string[] {
    const values = new Set<string>();
    for (const edge of response?.edges ?? []) {
      for (const source of edge.source_kinds ?? []) values.add(source);
    }
    return [...values].sort();
  }

  function metadataNumber(key: string): number {
    return Number(response?.metadata?.[key] ?? 0);
  }

  function formatSourceKind(value: string): string {
    return value.replaceAll("_", " ");
  }
</script>

<section class="panel-card relationship-graph-panel">
  <div class="section-header">
    <div>
      <span class="caption">Entity Relationships</span>
      <h3><Network size={20} /> Relationship Graph</h3>
      <p>
        {visibleNodes.length} shown nodes, {filteredEdges.length} shown links
        {#if response}
          / {metadataNumber("node_count")} total related nodes, {metadataNumber("edge_count")} total links
        {/if}
      </p>
    </div>
    <button class="secondary" disabled={disabled || loading} on:click={refresh}><RefreshCcw size={16} /> Refresh</button>
  </div>

  {#if error}
    <div class="error-banner">{error}</div>
  {/if}

  <div class="relationship-toolbar">
    <label class="search-box">
      <Search size={16} />
      <input bind:value={search} placeholder="Search names, relationship types, evidence" />
    </label>
    <select bind:value={trackFilter}>
      <option value="lore_both">Lore + Both</option>
      <option value="all">All Tracks</option>
      <option value="lore">Lore</option>
      <option value="both">Both</option>
      <option value="meta">Meta</option>
      <option value="unknown">Unknown</option>
    </select>
    <select bind:value={sourceFilter}>
      <option value="all">All Sources</option>
      {#each sourceKinds() as source}
        <option value={source}>{formatSourceKind(source)}</option>
      {/each}
    </select>
    <label class="compact-input">
      <span>Min Evidence</span>
      <input type="number" min="1" max="99" bind:value={minEvidence} />
    </label>
    <label class="compact-input">
      <span>Max Links</span>
      <input type="number" min="50" max="2000" step="50" bind:value={edgeLimit} />
    </label>
  </div>

  <div class="graph-legend relationship-legend" aria-label="Relationship graph legend">
    <span><i class="legend-line lore"></i> lore</span>
    <span><i class="legend-line both"></i> both</span>
    <span><i class="legend-line meta"></i> meta</span>
    <span><i class="legend-line patch"></i> patch-note evidence</span>
  </div>

  <div class="relationship-flow-shell">
    {#if loading}
      <div class="graph-empty">Loading relationships...</div>
    {:else if !flowNodes.length}
      <div class="graph-empty">
        {#if !(response?.edges?.length)}
          Relationship links come from Stage 11 card drafts and the Stage 07 lore ledger. This run has no graph edges yet
          (Stage 11 failed; Stage 07 ledger not built).
        {:else}
          No relationships match the current filters.
        {/if}
      </div>
    {/if}
    <SvelteFlow
      bind:nodes={flowNodes}
      bind:edges={flowEdges}
      {nodeTypes}
      initialViewport={{ x: 80, y: 80, zoom: 0.46 }}
      minZoom={0.06}
      maxZoom={1.4}
      panOnDrag
      zoomOnScroll
      onnodeclick={selectNode}
      onedgeclick={selectEdge}
      onpaneclick={clearSelection}
      defaultEdgeOptions={{ type: "smoothstep" }}
    >
      <Background variant={BackgroundVariant.Dots} gap={30} size={1.4} patternColor="#cbd5e1" />
      <Controls />
      <MiniMap
        pannable
        zoomable
        nodeColor={minimapColor}
        nodeStrokeColor={minimapColor}
        nodeBorderRadius={6}
        maskColor="rgba(15, 23, 42, 0.08)"
      />

      <Panel position="top-right" class="relationship-stats-panel">
        <span class="caption">Sources</span>
        {#if response?.metadata?.source_counts}
          {#each Object.entries(response.metadata.source_counts as Record<string, number>) as [source, count]}
            <p><strong>{count}</strong> {formatSourceKind(source)}</p>
          {/each}
        {:else}
          <p>No sources loaded.</p>
        {/if}
      </Panel>

      {#if selected}
        <Panel position="bottom-left" class="relationship-inspector-panel">
          {#if selected.kind === "node"}
            <span class="caption">Entity</span>
            <h3>{selected.node.name}</h3>
            <p>
              {selected.node.entity_type || "entity"} / {selected.node.degree} links /
              {selected.node.evidence_count} evidence
            </p>
            {#if selected.node.aliases?.length}
              <div class="relationship-pill-list">
                {#each selected.node.aliases.slice(0, 18) as alias}
                  <span>{alias}</span>
                {/each}
              </div>
            {/if}
            {#if selected.node.track_counts}
              <div class="relationship-track-counts">
                {#each Object.entries(selected.node.track_counts) as [track, count]}
                  <span>{track}: <strong>{count}</strong></span>
                {/each}
              </div>
            {/if}
          {:else}
            <span class="caption">Relationship</span>
            <h3>{selected.edge.source_name} → {selected.edge.target_name}</h3>
            <p>
              <strong>{selected.edge.relation_type}</strong> / {selected.edge.track} /
              {selected.edge.evidence_count} evidence
              {#if selected.edge.confidence !== null && selected.edge.confidence !== undefined}
                / confidence {selected.edge.confidence}
              {/if}
            </p>
            <div class="relationship-pill-list">
              {#each selected.edge.source_kinds ?? [] as source}
                <span>{formatSourceKind(source)}</span>
              {/each}
            </div>
            {#if selected.edge.descriptions?.length}
              <div class="relationship-evidence-list">
                {#each selected.edge.descriptions.slice(0, 8) as description}
                  <blockquote>{description}</blockquote>
                {/each}
              </div>
            {/if}
            {#if selected.edge.support_ids?.length}
              <p class="support-id-preview">Support IDs: {selected.edge.support_ids.slice(0, 8).join(", ")}</p>
            {/if}
          {/if}
        </Panel>
      {/if}
    </SvelteFlow>
  </div>
</section>

<style>
  .relationship-graph-panel {
    display: grid;
    gap: 14px;
  }

  .section-header h3 {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .relationship-toolbar {
    display: grid;
    grid-template-columns: minmax(260px, 1fr) 150px 190px 120px 120px;
    gap: 10px;
    align-items: end;
  }

  .compact-input {
    display: grid;
    gap: 4px;
  }

  .compact-input span {
    color: #64748b;
    font-size: 11px;
    font-weight: 900;
    text-transform: uppercase;
  }

  .compact-input input {
    min-height: 36px;
  }

  .relationship-flow-shell {
    position: relative;
    height: calc(100vh - 300px);
    min-height: 620px;
    border: 1px solid #d7dee9;
    border-radius: 8px;
    overflow: hidden;
    background: #f8fafc;
  }

  .relationship-flow-shell :global(.svelte-flow) {
    background: #f8fafc;
  }

  :global(.relationship-stats-panel),
  :global(.relationship-inspector-panel) {
    border: 1px solid #d7dee9;
    border-radius: 8px;
    background: rgba(255, 255, 255, 0.96);
    box-shadow: 0 14px 32px rgba(15, 23, 42, 0.14);
    padding: 12px;
  }

  :global(.relationship-stats-panel) {
    min-width: 220px;
  }

  :global(.relationship-stats-panel p),
  :global(.relationship-inspector-panel p) {
    margin: 4px 0;
    color: #475569;
  }

  :global(.relationship-inspector-panel) {
    width: min(520px, calc(100vw - 420px));
    max-height: 320px;
    overflow: auto;
  }

  :global(.relationship-inspector-panel h3) {
    margin: 4px 0 8px;
    color: #0f172a;
    font-size: 19px;
    line-height: 1.2;
    overflow-wrap: anywhere;
  }

  .relationship-pill-list,
  .relationship-track-counts {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 10px;
  }

  .relationship-pill-list span,
  .relationship-track-counts span {
    border: 1px solid #dbe3ef;
    border-radius: 999px;
    background: #f8fafc;
    color: #475569;
    padding: 3px 8px;
    font-size: 12px;
    font-weight: 800;
  }

  .relationship-evidence-list {
    display: grid;
    gap: 8px;
    margin-top: 12px;
  }

  .relationship-evidence-list blockquote {
    margin: 0;
    padding: 9px 10px;
    border-left: 4px solid #cbd5e1;
    border-radius: 6px;
    background: #f8fafc;
    color: #334155;
    font-size: 12px;
    line-height: 1.4;
  }

  .support-id-preview {
    overflow-wrap: anywhere;
    font-size: 12px;
  }

  .relationship-legend {
    margin: 0;
  }

  .legend-line.lore {
    border-top-color: #16803c;
  }

  .legend-line.both {
    border-top-color: #2563eb;
  }

  .legend-line.meta {
    border-top-color: #b7791f;
  }

  .legend-line.patch {
    border-top-color: #64748b;
    border-top-style: dashed;
  }

  :global(.relationship-flow-edge.meta .svelte-flow__edge-path) {
    stroke: #b7791f;
  }

  :global(.relationship-flow-edge.both .svelte-flow__edge-path) {
    stroke: #2563eb;
  }

  :global(.relationship-flow-edge.lore .svelte-flow__edge-path) {
    stroke: #16803c;
  }

  :global(.relationship-flow-edge.unknown .svelte-flow__edge-path) {
    stroke: #64748b;
  }

  :global(.relationship-flow-edge.patch .svelte-flow__edge-path) {
    stroke-dasharray: 9 7;
  }

  :global(.relationship-flow-edge.match .svelte-flow__edge-path) {
    filter: drop-shadow(0 0 5px rgba(37, 99, 235, 0.5));
  }

  .relationship-flow-shell :global(.svelte-flow__controls),
  .relationship-flow-shell :global(.svelte-flow__minimap) {
    border: 1px solid #d7dee9;
    border-radius: 8px;
    overflow: hidden;
    background: rgba(255, 255, 255, 0.95);
    box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12);
  }

  @media (max-width: 1100px) {
    .relationship-toolbar {
      grid-template-columns: 1fr 1fr;
    }
  }
</style>
