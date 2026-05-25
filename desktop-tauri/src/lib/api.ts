import { invoke } from "@tauri-apps/api/core";
import type {
  AppState,
  AppConfigResponse,
  BridgeResponse,
  CardAgentActivityResponse,
  CardAgentProgressTail,
  DraftCardsResponse,
  EntityEvidenceResponse,
  IdentityClustersResponse,
  InventoryResponse,
  PipelineLogTail,
  PipelineProgressTail,
  PipelineRuntimeStatus,
  RelationshipGraphResponse,
  ThemeLearningResponse,
  ThemeRescueResponse,
} from "./types";

let repoRoot = "";

export function setRepoRoot(value: string) {
  repoRoot = value;
}

async function bridge<T>(command: string, payload: Record<string, unknown> = {}): Promise<T> {
  const response = await invoke<BridgeResponse<T>>("python_bridge", {
    repoRoot: repoRoot || null,
    command,
    payload,
  });
  if (!response.ok) {
    throw new Error(response.error || "Python bridge failed.");
  }
  return response.result as T;
}

export async function loadState(artifactsRoot?: string): Promise<AppState> {
  const state = await bridge<AppState>("state", artifactsRoot ? { artifacts_root: artifactsRoot } : {});
  setRepoRoot(state.repo_root);
  return state;
}

export async function loadAppConfig(): Promise<AppConfigResponse> {
  return bridge<AppConfigResponse>("app_config", {});
}

export async function saveAppConfig(payload: Record<string, unknown>): Promise<AppConfigResponse> {
  return bridge<AppConfigResponse>("save_app_config", payload);
}

export async function selectBootstrapDoc(initialPath?: string): Promise<{ path: string }> {
  return bridge<{ path: string }>("select_bootstrap_doc", initialPath ? { initial_path: initialPath } : {});
}

export async function selectRun(artifactsRoot: string): Promise<AppState> {
  const state = await bridge<AppState>("select_run", { artifacts_root: artifactsRoot });
  setRepoRoot(state.repo_root);
  return state;
}

export async function createRun(): Promise<AppState> {
  const state = await bridge<AppState>("create_run", {});
  setRepoRoot(state.repo_root);
  return state;
}

export async function loadIdentityClusters(artifactsRoot: string): Promise<IdentityClustersResponse> {
  return bridge<IdentityClustersResponse>("identity_clusters", { artifacts_root: artifactsRoot });
}

export async function decideIdentityCluster(payload: Record<string, unknown>): Promise<IdentityClustersResponse> {
  return bridge<IdentityClustersResponse>("identity_cluster_decision", payload);
}

export async function decideIdentityEdge(payload: Record<string, unknown>): Promise<IdentityClustersResponse> {
  return bridge<IdentityClustersResponse>("identity_edge_decision", payload);
}

export async function loadClaimInventory(artifactsRoot: string): Promise<InventoryResponse> {
  return bridge<InventoryResponse>("claim_inventory", { artifacts_root: artifactsRoot });
}

export async function decideClaim(payload: Record<string, unknown>): Promise<InventoryResponse> {
  return bridge<InventoryResponse>("claim_decision", payload);
}

export async function loadEntityInventory(artifactsRoot: string): Promise<InventoryResponse> {
  return bridge<InventoryResponse>("entity_inventory", { artifacts_root: artifactsRoot });
}

export async function loadThemeLearning(artifactsRoot: string): Promise<ThemeLearningResponse> {
  return bridge<ThemeLearningResponse>("theme_learning", { artifacts_root: artifactsRoot });
}

export async function loadThemeRescue(artifactsRoot: string): Promise<ThemeRescueResponse> {
  return bridge<ThemeRescueResponse>("theme_rescue", { artifacts_root: artifactsRoot });
}

export async function approveThemeRescue(artifactsRoot: string, note = ""): Promise<ThemeRescueResponse> {
  return bridge<ThemeRescueResponse>("approve_theme_rescue", { artifacts_root: artifactsRoot, note });
}

export async function loadEntityEvidence(
  artifactsRoot: string,
  rowId: string,
  view: "candidates" | "merged" | string,
): Promise<EntityEvidenceResponse> {
  return bridge<EntityEvidenceResponse>("entity_evidence", { artifacts_root: artifactsRoot, row_id: rowId, view });
}

export async function decideEntity(payload: Record<string, unknown>): Promise<InventoryResponse> {
  return bridge<InventoryResponse>("entity_decision", payload);
}

export async function loadDraftCards(artifactsRoot: string): Promise<DraftCardsResponse> {
  return bridge<DraftCardsResponse>("draft_cards", { artifacts_root: artifactsRoot });
}

export async function loadEntityRelationships(artifactsRoot: string): Promise<RelationshipGraphResponse> {
  return bridge<RelationshipGraphResponse>("entity_relationships", { artifacts_root: artifactsRoot });
}

export async function loadCardAgentActivity(artifactsRoot: string): Promise<CardAgentActivityResponse> {
  return bridge<CardAgentActivityResponse>("card_agent_activity", { artifacts_root: artifactsRoot });
}

export async function loadCardAgentProgress(artifactsRoot: string, maxLines = 80): Promise<CardAgentProgressTail> {
  return bridge<CardAgentProgressTail>("card_agent_progress", { artifacts_root: artifactsRoot, max_lines: maxLines });
}

export async function runCardAgentRequest(payload: Record<string, unknown>): Promise<CardAgentActivityResponse> {
  return bridge<CardAgentActivityResponse>("run_card_agent_request", payload);
}

export async function undoCardAgentTransaction(payload: Record<string, unknown>): Promise<CardAgentActivityResponse> {
  return bridge<CardAgentActivityResponse>("undo_card_agent_transaction", payload);
}

export async function startPipeline(
  artifactsRoot: string,
  options: { resume?: boolean; ignorePending?: boolean; startStage?: number } = {},
): Promise<PipelineRuntimeStatus> {
  const payload: Record<string, unknown> = {
    repoRoot: repoRoot || null,
    artifactsRoot,
    resume: Boolean(options.resume),
    ignorePending: Boolean(options.ignorePending),
  };
  if (options.startStage !== undefined && options.startStage >= 1) {
    payload.startStage = options.startStage;
  }
  return invoke<PipelineRuntimeStatus>("pipeline_start", payload);
}

export async function pipelineStatus(): Promise<PipelineRuntimeStatus> {
  return invoke<PipelineRuntimeStatus>("pipeline_status");
}

export async function pipelineLogTail(artifactsRoot?: string, maxLines = 250): Promise<PipelineLogTail> {
  return invoke<PipelineLogTail>("pipeline_log_tail", {
    artifactsRoot: artifactsRoot || null,
    maxLines,
  });
}

export async function pipelineProgressTail(artifactsRoot?: string, maxLines = 120): Promise<PipelineProgressTail> {
  return invoke<PipelineProgressTail>("pipeline_progress_tail", {
    artifactsRoot: artifactsRoot || null,
    maxLines,
  });
}

export async function cancelPipeline(): Promise<PipelineRuntimeStatus> {
  return invoke<PipelineRuntimeStatus>("pipeline_cancel");
}
