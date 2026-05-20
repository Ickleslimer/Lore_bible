import { invoke } from "@tauri-apps/api/core";
import type {
  AppState,
  BridgeResponse,
  IdentityClustersResponse,
  PipelineLogTail,
  PipelineRuntimeStatus,
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

export async function startPipeline(
  artifactsRoot: string,
  options: { resume?: boolean; ignorePending?: boolean } = {},
): Promise<PipelineRuntimeStatus> {
  return invoke<PipelineRuntimeStatus>("pipeline_start", {
    repoRoot: repoRoot || null,
    artifactsRoot,
    resume: Boolean(options.resume),
    ignorePending: Boolean(options.ignorePending),
  });
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

export async function cancelPipeline(): Promise<PipelineRuntimeStatus> {
  return invoke<PipelineRuntimeStatus>("pipeline_cancel");
}
