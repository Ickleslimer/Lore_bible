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

export interface AppConfigResponse {
  repo_root: string;
  config_path: string;
  env_path: string;
  bootstrap_doc_path: string;
  bootstrap_doc_config_value: string;
  bootstrap_doc_exists: boolean;
  openrouter_key_present: boolean;
  openrouter_key_source?: string;
  openrouter_key_preview?: string;
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
  merged_rows?: InventoryRow[];
  merged_total?: number;
  merged_metadata?: Record<string, unknown>;
}

export interface EntityEvidenceResponse {
  active_root: string;
  row_id: string;
  view: "candidates" | "merged" | string;
  claims: Array<Record<string, unknown>>;
  snippets: Array<Record<string, unknown>>;
  sample_texts: string[];
  type_evidence: Array<Record<string, unknown>>;
  merge_records: Array<Record<string, unknown>>;
  merged_from_entities: Array<Record<string, unknown>>;
  aliases: Array<unknown>;
}

export interface DraftCardSection {
  key: string;
  title: string;
  text: string;
  word_count: number;
}

export interface DraftCardItem {
  card_id: string;
  canonical_name: string;
  entity_type?: string;
  status: string;
  summary: string;
  sections: DraftCardSection[];
  word_count: number;
  section_count: number;
  claim_count: number;
  evidence_count: number;
  relationships?: Array<Record<string, unknown>>;
  timeline?: Array<Record<string, unknown>>;
  wiki_links?: Array<Record<string, unknown>>;
  unresolved_conflicts?: Array<unknown>;
  item: Record<string, unknown>;
}

export interface DraftCardsMetadata {
  source_kind?: string;
  source_path?: string;
  updated_at_utc?: string;
  status?: string;
  processed_count?: number;
  total_count?: number;
  current_entity_id?: string;
  current_entity_name?: string;
  failure_count?: number;
}

export interface DraftCardsResponse {
  active_root: string;
  metadata: DraftCardsMetadata;
  cards: DraftCardItem[];
  total: number;
  failures: Array<Record<string, unknown>>;
}

export interface RelationshipGraphNode {
  node_id: string;
  entity_id?: string;
  card_id?: string;
  name: string;
  entity_type?: string;
  aliases?: string[];
  resolved?: boolean;
  degree: number;
  evidence_count: number;
  track_counts?: Record<string, number>;
  source?: string;
  [key: string]: unknown;
}

export interface RelationshipGraphEdge {
  edge_id: string;
  source_id: string;
  target_id: string;
  source_name: string;
  target_name: string;
  relation_type: string;
  track: string;
  evidence_count: number;
  confidence?: number | null;
  descriptions?: string[];
  support_ids?: string[];
  source_refs?: string[];
  source_kinds?: string[];
  [key: string]: unknown;
}

export interface RelationshipGraphResponse {
  active_root: string;
  nodes: RelationshipGraphNode[];
  edges: RelationshipGraphEdge[];
  metadata: Record<string, unknown>;
}

export interface CardAgentTransaction {
  transaction_id: string;
  request_id?: string;
  request_text?: string;
  status: string;
  started_at_utc?: string;
  finished_at_utc?: string;
  rationale?: string;
  error?: string;
  steps?: Array<Record<string, unknown>>;
  write_set?: Array<Record<string, unknown>>;
  affected?: {
    entities?: string[];
    cards?: string[];
    claims?: string[];
    [key: string]: unknown;
  };
  change_summary?: {
    affected?: {
      entities?: string[];
      cards?: string[];
      claims?: string[];
      [key: string]: unknown;
    };
    artifacts?: Array<Record<string, unknown>>;
    lines?: Array<{
      sentence?: string;
      kind?: string;
      collection?: string;
      artifact?: string;
      id?: string;
      [key: string]: unknown;
    }>;
    [key: string]: unknown;
  };
  validation?: Record<string, unknown>;
  reverses_transaction_id?: string;
  [key: string]: unknown;
}

export interface CardAgentActivityResponse {
  active_root: string;
  transactions: CardAgentTransaction[];
  total: number;
  source_path?: string;
  last_run?: Record<string, unknown>;
}

export interface CardAgentProgressTail {
  active_root: string;
  source_path?: string;
  latest_line: string;
  latest_progress_line?: string;
  lines: string[];
  events?: Array<Record<string, unknown>>;
  total_scanned: number;
  updated_at_epoch: string;
}
