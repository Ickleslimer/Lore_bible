<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import {
    Background,
    BackgroundVariant,
    Controls,
    MarkerType,
    MiniMap,
    Panel,
    Position,
    SvelteFlow,
    type Edge,
    type Node,
  } from "@xyflow/svelte";
  import "@xyflow/svelte/dist/style.css";
  import { Check, HelpCircle, Search, X } from "lucide-svelte";
  import { decideIdentityCluster, decideIdentityEdge } from "../lib/api";
  import type { IdentityClusterRow, IdentityEdge, IdentityEntity } from "../lib/types";
  import IdentityFlowNode from "./IdentityFlowNode.svelte";

  export let artifactsRoot = "";
  export let clusters: IdentityClusterRow[] = [];
  export let disabled = false;

  type GraphStatus = "accepted" | "refuted" | "deferred" | "pending";

  interface IdentityNodeData extends Record<string, unknown> {
    kind: "central" | "leaf";
    row: IdentityClusterRow;
    entity: IdentityEntity;
    edge?: IdentityEdge;
    name: string;
    entityType: string;
    status: GraphStatus;
    clusterId: string;
    bucket: string;
    evidenceCount: number;
    match: boolean;
    split: boolean;
  }

  interface IdentityEdgeData extends Record<string, unknown> {
    row: IdentityClusterRow;
    edge: IdentityEdge;
    status: GraphStatus;
    sourceRoute: string;
    evidenceCount: number;
    match: boolean;
  }

  type IdentityNode = Node<IdentityNodeData, "identityCard">;
  type IdentityFlowEdge = Edge<IdentityEdgeData, "smoothstep">;
  type SelectedItem =
    | { kind: "cluster"; row: IdentityClusterRow; entity: IdentityEntity; nodeId: string }
    | { kind: "leaf"; row: IdentityClusterRow; entity: IdentityEntity; edge?: IdentityEdge; nodeId: string }
    | { kind: "edge"; row: IdentityClusterRow; edge: IdentityEdge; edgeId: string };

  const dispatch = createEventDispatcher<{ changed: void }>();
  const nodeTypes = { identityCard: IdentityFlowNode };

  const CLUSTER_WIDTH = 840;
  const CLUSTER_HEIGHT = 560;
  const CLUSTERS_PER_ROW = 3;
  const CENTRAL_WIDTH = 300;
  const LEAF_WIDTH = 230;
  const CENTRAL_HEIGHT = 92;
  const LEAF_HEIGHT = 78;

  let search = "";
  let bucket = "all";
  let rationale = "";
  let workingCanonical: Record<string, string> = {};
  let flowNodes: IdentityNode[] = [];
  let flowEdges: IdentityFlowEdge[] = [];
  let selected: SelectedItem | null = null;
  let centralDrag: { nodeId: string; clusterId: string; x: number; y: number } | null = null;
  let localError = "";

  $: filtered = clusters.filter((row) => {
    const bucketMatch = bucket === "all" || row.bucket === bucket;
    if (!bucketMatch) return false;
    const query = search.trim().toLowerCase();
    if (!query) return true;
    return clusterSearchText(row).includes(query);
  });

  $: graph = buildFlow(filtered, search);
  $: flowNodes = graph.nodes;
  $: flowEdges = graph.edges;
  $: if (selected && !selectionStillVisible(selected, graph.nodes, graph.edges)) {
    selected = null;
  }

  function canonicalFor(row: IdentityClusterRow): string {
    return workingCanonical[row.row_id] ?? row.canonical_name ?? row.item.canonical_name ?? "";
  }

  function clusterSearchText(row: IdentityClusterRow): string {
    return [
      row.candidate_name,
      row.canonical_name,
      row.triage_reason,
      row.review_priority,
      ...(row.item.member_entities ?? []).flatMap((entity) => [entity.canonical_name, entity.entity_type, ...(entity.aliases ?? [])]),
      ...(row.item.member_edges ?? []).flatMap((edge) => [edge.source_entity_name, edge.target_entity_name, edge.alias_text]),
    ]
      .join(" ")
      .toLowerCase();
  }

  function entityMatches(entity: IdentityEntity, row: IdentityClusterRow, query: string): boolean {
    if (!query) return false;
    return [entity.canonical_name, entity.entity_type, ...(entity.aliases ?? []), row.candidate_name, row.canonical_name]
      .join(" ")
      .toLowerCase()
      .includes(query);
  }

  function edgeMatches(edge: IdentityEdge, row: IdentityClusterRow, query: string): boolean {
    if (!query) return false;
    return [edge.source_entity_name, edge.target_entity_name, edge.alias_text, edge.merge_type, row.candidate_name, row.canonical_name]
      .join(" ")
      .toLowerCase()
      .includes(query);
  }

  function edgeStatus(edge?: IdentityEdge): string {
    if (!edge) return "pending";
    return String(edge.latest_edge_decision?.decision || edge.edge_bucket || edge.edge_review_status || "pending").toLowerCase();
  }

  function rowDecision(row: IdentityClusterRow): string {
    return String(row.item.latest_decision?.decision || row.decision || row.bucket || row.item.review_status || "").trim().toLowerCase();
  }

  function rowApproved(row: IdentityClusterRow): boolean {
    return ["approve", "approved", "accept", "accepted"].includes(rowDecision(row));
  }

  function graphStatus(row: IdentityClusterRow, edge?: IdentityEdge): GraphStatus {
    const status = edgeStatus(edge);
    if (["reject", "rejected", "refute", "refuted"].includes(status)) return "refuted";
    if (["defer", "deferred", "needs_more_context"].includes(status)) return "deferred";
    if (["accept", "accepted", "approve", "approved", "keep", "kept", "restore"].includes(status)) return "accepted";
    if (rowApproved(row)) return "accepted";
    return "pending";
  }

  function statusRank(row: IdentityClusterRow, edge?: IdentityEdge): number {
    const status = graphStatus(row, edge);
    if (status === "refuted") return 0;
    if (status === "accepted") return 1;
    if (status === "deferred") return 2;
    return 3;
  }

  function statusLabel(status: GraphStatus): string {
    if (status === "accepted") return "kept";
    if (status === "refuted") return "refuted";
    if (status === "deferred") return "deferred";
    return "proposed";
  }

  function members(row: IdentityClusterRow): IdentityEntity[] {
    const seen = new Set<string>();
    const out: IdentityEntity[] = [];
    for (const entity of row.item.member_entities ?? []) {
      const id = String(entity.entity_id || entity.canonical_name || "").trim();
      if (!id || seen.has(id)) continue;
      seen.add(id);
      out.push(entity);
    }
    return out;
  }

  function canonicalEntityId(row: IdentityClusterRow): string {
    const explicit = String(row.item.canonical_entity_id || row.item.target_entity_id || "").trim();
    if (explicit) return explicit;
    const canonicalName = canonicalFor(row).trim().toLowerCase();
    return members(row).find((entity) => entity.canonical_name?.trim().toLowerCase() === canonicalName)?.entity_id ?? "";
  }

  function centralEntity(row: IdentityClusterRow): IdentityEntity {
    const centralId = canonicalEntityId(row);
    const found = members(row).find((entity) => entity.entity_id === centralId);
    if (found) return found;
    return {
      entity_id: centralId || `canonical:${row.row_id}`,
      canonical_name: canonicalFor(row),
      entity_type: "entity",
      aliases: [],
    };
  }

  function leafEntities(row: IdentityClusterRow): IdentityEntity[] {
    const centralId = canonicalEntityId(row);
    return members(row).filter((entity) => entity.entity_id !== centralId);
  }

  function edgeInvolves(edge: IdentityEdge, entityId: string): boolean {
    return edge.source_entity_id === entityId || edge.target_entity_id === entityId;
  }

  function edgeDirectlyConnects(edge: IdentityEdge, entityId: string, centralId: string): boolean {
    return (
      (edge.source_entity_id === entityId && edge.target_entity_id === centralId) ||
      (edge.target_entity_id === entityId && edge.source_entity_id === centralId)
    );
  }

  function edgeForEntity(row: IdentityClusterRow, entity: IdentityEntity): IdentityEdge | undefined {
    const centralId = canonicalEntityId(row);
    const entityId = entity.entity_id;
    const edges = row.item.member_edges ?? [];
    const direct = edges.filter((edge) => edgeDirectlyConnects(edge, entityId, centralId));
    const candidates = direct.length ? direct : edges.filter((edge) => edgeInvolves(edge, entityId));
    return [...candidates].sort((a, b) => statusRank(row, a) - statusRank(row, b))[0];
  }

  function nodeId(row: IdentityClusterRow, entity: IdentityEntity, kind: "central" | "leaf"): string {
    return `${kind}:${row.item.proposal_id}:${entity.entity_id || entity.canonical_name}`;
  }

  function clusterId(row: IdentityClusterRow): string {
    return String(row.item.proposal_id || row.row_id || row.candidate_name);
  }

  function leafPosition(index: number, count: number, originX: number, originY: number) {
    if (count <= 0) {
      return { x: originX + CLUSTER_WIDTH / 2 - LEAF_WIDTH / 2, y: originY + CLUSTER_HEIGHT / 2 + 150 };
    }
    const centerX = originX + CLUSTER_WIDTH / 2;
    const centerY = originY + CLUSTER_HEIGHT / 2;
    const radiusX = 270;
    const radiusY = 184;
    const startDeg = count === 1 ? 90 : -90;
    const angle = ((startDeg + (360 / count) * index) * Math.PI) / 180;
    return {
      x: centerX + Math.cos(angle) * radiusX - LEAF_WIDTH / 2,
      y: centerY + Math.sin(angle) * radiusY - LEAF_HEIGHT / 2,
    };
  }

  function linkSides(leafPositionValue: { x: number; y: number }, centralPosition: { x: number; y: number }) {
    const leafCenterX = leafPositionValue.x + LEAF_WIDTH / 2;
    const leafCenterY = leafPositionValue.y + LEAF_HEIGHT / 2;
    const centralCenterX = centralPosition.x + CENTRAL_WIDTH / 2;
    const centralCenterY = centralPosition.y + CENTRAL_HEIGHT / 2;
    const dx = leafCenterX - centralCenterX;
    const dy = leafCenterY - centralCenterY;
    if (Math.abs(dx) > Math.abs(dy)) {
      return dx < 0
        ? { sourceHandle: "source-right", targetHandle: "target-left" }
        : { sourceHandle: "source-left", targetHandle: "target-right" };
    }
    return dy < 0
      ? { sourceHandle: "source-bottom", targetHandle: "target-top" }
      : { sourceHandle: "source-top", targetHandle: "target-bottom" };
  }

  function buildFlow(rows: IdentityClusterRow[], queryText: string): { nodes: IdentityNode[]; edges: IdentityFlowEdge[] } {
    const query = queryText.trim().toLowerCase();
    const nodes: IdentityNode[] = [];
    const edges: IdentityFlowEdge[] = [];
    rows.forEach((row, index) => {
      const col = index % CLUSTERS_PER_ROW;
      const graphRow = Math.floor(index / CLUSTERS_PER_ROW);
      const originX = col * CLUSTER_WIDTH;
      const originY = graphRow * CLUSTER_HEIGHT;
      const central = centralEntity(row);
      const centralId = nodeId(row, central, "central");
      const rowClusterId = clusterId(row);
      const centralPosition = { x: originX + CLUSTER_WIDTH / 2 - CENTRAL_WIDTH / 2, y: originY + CLUSTER_HEIGHT / 2 - CENTRAL_HEIGHT / 2 };
      const splitIds = new Set(row.item.suggested_split_entity_ids ?? []);
      nodes.push({
        id: centralId,
        type: "identityCard",
        data: {
          kind: "central",
          row,
          entity: central,
          name: canonicalFor(row),
          entityType: central.entity_type || "entity",
          status: "accepted",
          clusterId: rowClusterId,
          bucket: row.bucket,
          evidenceCount: row.evidence_count,
          match: entityMatches(central, row, query),
          split: false,
        },
        position: centralPosition,
        sourcePosition: Position.Bottom,
        targetPosition: Position.Bottom,
        draggable: true,
      });

      const leaves = leafEntities(row);
      leaves.forEach((entity, leafIndex) => {
        const edge = edgeForEntity(row, entity);
        const status = graphStatus(row, edge);
        const id = nodeId(row, entity, "leaf");
        const position = leafPosition(leafIndex, leaves.length, originX, originY);
        const sides = linkSides(position, centralPosition);
        const match = entityMatches(entity, row, query) || Boolean(edge && edgeMatches(edge, row, query));
        nodes.push({
          id,
          type: "identityCard",
          data: {
            kind: "leaf",
            row,
            entity,
            edge,
            name: entity.canonical_name || "Unnamed",
            entityType: entity.entity_type || "entity",
            status,
            clusterId: rowClusterId,
            bucket: row.bucket,
            evidenceCount: (edge?.evidence_claim_ids ?? []).length || Number(entity.evidence_count || 0),
            match,
            split: splitIds.has(entity.entity_id),
          },
          position,
          sourcePosition: Position.Top,
          targetPosition: Position.Top,
          draggable: true,
        });
        if (edge) {
          const color = status === "refuted" ? "#dc2626" : status === "deferred" ? "#b7791f" : "#16803c";
          edges.push({
            id: `edge:${row.item.proposal_id}:${edge.proposal_id}:${entity.entity_id}`,
            type: "smoothstep",
            source: id,
            target: centralId,
            sourceHandle: sides.sourceHandle,
            targetHandle: sides.targetHandle,
            data: {
              row,
              edge,
              status,
              sourceRoute: edgeRoute(edge),
              evidenceCount: (edge.evidence_claim_ids ?? []).length,
              match: edgeMatches(edge, row, query),
            },
            markerEnd: { type: MarkerType.ArrowClosed, color },
            style: `stroke: ${color}; stroke-width: 2.8;`,
            class: `identity-flow-edge ${status} ${edgeMatches(edge, row, query) ? "match" : ""}`,
            animated: status === "pending",
            zIndex: status === "refuted" ? 4 : 2,
          });
        }
      });
    });
    return { nodes, edges };
  }

  function edgeRoute(edge: IdentityEdge): string {
    return `${edge.source_entity_name || edge.source_entity_id} -> ${edge.target_entity_name || edge.target_entity_id}`;
  }

  function selectionStillVisible(item: SelectedItem, nodes: IdentityNode[], edges: IdentityFlowEdge[]): boolean {
    if (item.kind === "edge") return edges.some((edge) => edge.id === item.edgeId);
    return nodes.some((node) => node.id === item.nodeId);
  }

  function selectNode({ node }: { node: IdentityNode }) {
    const data = node.data;
    selected =
      data.kind === "central"
        ? { kind: "cluster", row: data.row, entity: data.entity, nodeId: node.id }
        : { kind: "leaf", row: data.row, entity: data.entity, edge: data.edge, nodeId: node.id };
  }

  function selectEdge({ edge }: { edge: IdentityFlowEdge }) {
    if (!edge.data) return;
    selected = { kind: "edge", row: edge.data.row, edge: edge.data.edge, edgeId: edge.id };
  }

  function startNodeDrag({ targetNode }: { targetNode: IdentityNode | null }) {
    if (!targetNode || targetNode.data.kind !== "central") {
      centralDrag = null;
      return;
    }
    centralDrag = {
      nodeId: targetNode.id,
      clusterId: targetNode.data.clusterId,
      x: targetNode.position.x,
      y: targetNode.position.y,
    };
  }

  function dragNode({ targetNode }: { targetNode: IdentityNode | null }) {
    if (!targetNode || !centralDrag || targetNode.id !== centralDrag.nodeId || targetNode.data.kind !== "central") return;
    const dx = targetNode.position.x - centralDrag.x;
    const dy = targetNode.position.y - centralDrag.y;
    if (Math.abs(dx) < 0.01 && Math.abs(dy) < 0.01) return;
    const draggedClusterId = centralDrag.clusterId;
    flowNodes = flowNodes.map((node) => {
      if (node.id === targetNode.id || node.data.clusterId !== draggedClusterId) return node;
      return {
        ...node,
        position: {
          x: node.position.x + dx,
          y: node.position.y + dy,
        },
      };
    });
    centralDrag = {
      ...centralDrag,
      x: targetNode.position.x,
      y: targetNode.position.y,
    };
  }

  function stopNodeDrag() {
    centralDrag = null;
  }

  function clearSelection() {
    selected = null;
  }

  function minimapColor(node: Node): string {
    const data = node.data as Partial<IdentityNodeData>;
    if (data.kind === "central") return "#0f172a";
    if (data.status === "refuted") return "#dc2626";
    if (data.status === "deferred" || data.split) return "#b7791f";
    return "#16803c";
  }

  function selectedCaption(item: SelectedItem | null): string {
    if (!item) return "";
    if (item.kind === "cluster") return "Central Card";
    if (item.kind === "edge") return "Identity Link";
    return "Merge Candidate";
  }

  function selectedTitle(item: SelectedItem | null): string {
    if (!item) return "";
    if (item.kind === "cluster") return canonicalFor(item.row);
    if (item.kind === "edge") return edgeRoute(item.edge);
    return item.entity.canonical_name || "Unnamed";
  }

  function selectedDescription(item: SelectedItem | null): string {
    if (!item) return "";
    if (item.kind === "cluster") {
      return item.row.triage_reason || item.row.item.rationale || "No rationale recorded.";
    }
    if (item.kind === "edge") {
      return `${statusLabel(graphStatus(item.row, item.edge))} / ${(item.edge.evidence_claim_ids ?? []).length} claim links`;
    }
    if (item.edge) {
      return `${item.entity.entity_type || "entity"} in ${canonicalFor(item.row)} / ${edgeRoute(item.edge)} / ${statusLabel(graphStatus(item.row, item.edge))}`;
    }
    return `${item.entity.entity_type || "entity"} in ${canonicalFor(item.row)}. No direct edge is attached to this candidate.`;
  }

  function selectedEvidence(item: SelectedItem | null): Record<string, unknown>[] {
    if (!item || item.kind !== "edge") return [];
    return (item.edge.evidence ?? []).slice(0, 4) as Record<string, unknown>[];
  }

  function selectedHasEdge(item: SelectedItem | null): boolean {
    return Boolean(item && (item.kind === "edge" || item.kind === "leaf") && item.edge);
  }

  function updateSelectedCanonical(value: string) {
    if (selected?.kind !== "cluster") return;
    workingCanonical[selected.row.row_id] = value;
  }

  function decideSelectedCluster(decision: "approve" | "reject" | "defer" | "needs_more_context") {
    if (selected?.kind !== "cluster") return;
    void decideCluster(selected.row, decision);
  }

  function decideSelectedEdge(decision: "accept" | "reject" | "defer" | "needs_more_context") {
    const item = selected;
    const edge = item?.kind === "edge" ? item.edge : item?.kind === "leaf" ? item.edge : undefined;
    if (!item || !edge) return;
    void decideEdge(item.row, edge, decision);
  }

  async function decideCluster(row: IdentityClusterRow, decision: "approve" | "reject" | "defer" | "needs_more_context") {
    localError = "";
    await decideIdentityCluster({
      artifacts_root: artifactsRoot,
      proposal_id: row.item.proposal_id,
      decision,
      canonical_name: canonicalFor(row),
      rationale,
    }).catch((err) => {
      localError = err instanceof Error ? err.message : String(err);
      throw err;
    });
    rationale = "";
    selected = null;
    dispatch("changed");
  }

  async function decideEdge(row: IdentityClusterRow, edge: IdentityEdge, decision: "accept" | "reject" | "defer" | "needs_more_context") {
    localError = "";
    await decideIdentityEdge({
      artifacts_root: artifactsRoot,
      cluster_id: row.item.proposal_id,
      edge_proposal_id: edge.proposal_id,
      source_entity_id: edge.source_entity_id,
      source_entity_name: edge.source_entity_name,
      target_entity_id: edge.target_entity_id,
      target_entity_name: edge.target_entity_name,
      decision,
      rationale,
    }).catch((err) => {
      localError = err instanceof Error ? err.message : String(err);
      throw err;
    });
    rationale = "";
    selected = null;
    dispatch("changed");
  }
</script>

<section class="identity-layout identity-canvas-layout">
  {#if localError}
    <div class="error-banner">{localError}</div>
  {/if}

  <div class="identity-flow-shell">
    <SvelteFlow
      bind:nodes={flowNodes}
      bind:edges={flowEdges}
      {nodeTypes}
      initialViewport={{ x: 64, y: 78, zoom: 0.86 }}
      minZoom={0.12}
      maxZoom={1.6}
      panOnDrag
      zoomOnScroll
      onnodeclick={selectNode}
      onedgeclick={selectEdge}
      onnodedragstart={startNodeDrag}
      onnodedrag={dragNode}
      onnodedragstop={stopNodeDrag}
      onpaneclick={clearSelection}
      defaultEdgeOptions={{ type: "smoothstep" }}
    >
      <Background variant={BackgroundVariant.Dots} gap={28} size={1.5} patternColor="#cbd5e1" />
      <Controls />
      <MiniMap
        pannable
        zoomable
        nodeColor={minimapColor}
        nodeStrokeColor={minimapColor}
        nodeBorderRadius={6}
        maskColor="rgba(15, 23, 42, 0.08)"
      />

      <Panel position="top-left" class="identity-canvas-toolbar">
        <label class="search-box canvas-search">
          <Search size={16} />
          <input bind:value={search} placeholder="Search canvas" />
        </label>
        <select bind:value={bucket}>
          <option value="all">All</option>
          <option value="pending">Pending</option>
          <option value="approved">Approved</option>
          <option value="rejected">Rejected</option>
          <option value="deferred">Deferred</option>
        </select>
        <div class="graph-legend" aria-label="Graph legend">
          <span><i class="legend-line accepted"></i> kept or proposed</span>
          <span><i class="legend-line refuted"></i> refuted</span>
          <span><i class="legend-line deferred"></i> deferred</span>
        </div>
        <span class="canvas-count">{filtered.length} identity graph{filtered.length === 1 ? "" : "s"}</span>
      </Panel>

      {#if selected}
        <Panel position="top-right" class="identity-inspector-panel">
          <span class="caption">{selectedCaption(selected)}</span>
          <h3>{selectedTitle(selected)}</h3>
          <p>{selectedDescription(selected)}</p>
          {#if selected.kind === "cluster"}
            <label>
              <span class="caption">Canonical name</span>
              <input
                value={selectedTitle(selected)}
                on:input={(event) => updateSelectedCanonical(event.currentTarget.value)}
              />
            </label>
            <textarea bind:value={rationale} placeholder="Decision rationale"></textarea>
            <div class="cluster-actions inspector-actions">
              <button disabled={disabled} on:click={() => decideSelectedCluster("approve")}><Check size={16} /> Approve graph</button>
              <button class="secondary" disabled={disabled} on:click={() => decideSelectedCluster("defer")}><HelpCircle size={16} /> Defer</button>
              <button class="danger" disabled={disabled} on:click={() => decideSelectedCluster("reject")}><X size={16} /> Reject</button>
            </div>
          {:else}
            {#if selectedEvidence(selected).length}
              <div class="inspector-evidence">
                {#each selectedEvidence(selected) as evidence}
                  <blockquote>{String(evidence.claim_text || evidence.trigger || "Evidence claim")}</blockquote>
                {/each}
              </div>
            {/if}
            {#if selectedHasEdge(selected)}
              <textarea bind:value={rationale} placeholder="Decision rationale"></textarea>
              <div class="edge-actions inspector-actions">
                <button disabled={disabled} on:click={() => decideSelectedEdge("accept")}>Keep</button>
                <button class="danger" disabled={disabled} on:click={() => decideSelectedEdge("reject")}>Refute</button>
                <button class="secondary" disabled={disabled} on:click={() => decideSelectedEdge("defer")}>Defer</button>
              </div>
            {/if}
          {/if}
        </Panel>
      {/if}
    </SvelteFlow>
  </div>
</section>
