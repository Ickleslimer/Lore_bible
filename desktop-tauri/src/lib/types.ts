export interface BridgeResponse<T> {
  ok: boolean;
  result?: T;
  error?: string;
}

export interface PendingCounts {
  conversation_entities?: number;
  claims?: number;
  identity_merges?: number;
  card_architecture?: number;
  cards?: number;
}

export interface PipelineStage {
  index: number;
  short_label: string;
  name: string;
  state: "done" | "current" | "attention" | "failed" | "waiting";
}

export interface PipelineProgress {
  status: string;
  summary: string;
  current_stage_index?: number | null;
  completed_count: number;
  total_stages: number;
  review_gate: boolean;
  stages: PipelineStage[];
}

export interface ReviewRun {
  artifacts_root: string;
  label: string;
  counts: PendingCounts;
  pending_total: number;
  summary: string;
  is_active: boolean;
  latest_mtime: number;
}

export interface AppState {
  repo_root: string;
  active_root: string;
  active_label: string;
  counts: PendingCounts;
  pending_total: number;
  pending_summary: string;
  progress: PipelineProgress | null;
  runs: ReviewRun[];
}

export interface PipelineRuntimeStatus {
  status: "idle" | "running" | "succeeded" | "failed" | "cancelled" | string;
  message: string;
  logs: string[];
  child_pid?: number | null;
  artifacts_root?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  last_exit_code?: number | null;
}

export interface PipelineLogTail {
  logs: string[];
  total_lines?: number;
}

export interface PipelineProgressTail {
  latest_line: string;
  latest_progress_line: string;
  lines: string[];
  total_scanned: number;
  updated_at_epoch: string;
}

export interface IdentityEntity {
  entity_id: string;
  canonical_name: string;
  entity_type?: string;
  aliases?: string[];
  evidence_count?: number;
  [key: string]: unknown;
}

export interface IdentityEdge {
  proposal_id: string;
  cluster_id?: string;
  source_entity_id: string;
  source_entity_name: string;
  target_entity_id: string;
  target_entity_name: string;
  evidence_claim_ids?: string[];
  evidence?: Array<Record<string, unknown>>;
  latest_edge_decision?: Record<string, unknown>;
  edge_review_status?: string;
  edge_bucket?: string;
  [key: string]: unknown;
}

export interface IdentityClusterItem {
  proposal_id: string;
  cluster_id?: string;
  canonical_entity_id?: string;
  canonical_name?: string;
  member_entity_ids?: string[];
  member_entities?: IdentityEntity[];
  member_edges?: IdentityEdge[];
  rejected_edge_proposal_ids?: string[];
  rationale?: string;
  cluster_review_flags?: string[];
  suggested_split_entity_ids?: string[];
  evidence_claim_ids?: string[];
  latest_decision?: Record<string, unknown>;
  review_status?: string;
  [key: string]: unknown;
}

export interface IdentityClusterRow {
  row_id: string;
  row_kind: "identity_merge";
  bucket: string;
  candidate_name: string;
  canonical_name: string;
  evidence_count: number;
  review_priority?: string;
  triage_reason?: string;
  decision?: string;
  item: IdentityClusterItem;
}

export interface IdentityClustersResponse {
  active_root: string;
  clusters: IdentityClusterRow[];
}

export interface InventoryRow {
  row_id: string;
  row_kind?: string;
  bucket: string;
  source_bucket?: string;
  category: string;
  candidate_name: string;
  raw_candidate_name?: string;
  canonical_name?: string;
  proposed_entity_type?: string;
  evidence_count?: number;
  topics?: string[];
  tracks?: string[];
  triage_reason?: string;
  review_priority?: string;
  decision?: string;
  latest_decision?: Record<string, unknown>;
  item: Record<string, unknown>;
}

export interface InventoryResponse {
  active_root: string;
  rows: InventoryRow[];
  total: number;
}
