<script lang="ts">
  import { createEventDispatcher, onMount } from "svelte";
  import { AlertTriangle, CheckCircle2, FileText, FolderOpen, KeyRound, Save, Settings, X } from "lucide-svelte";
  import { loadAppConfig, saveAppConfig, selectBootstrapDoc } from "../lib/api";
  import type { AppConfigResponse } from "../lib/types";

  const dispatch = createEventDispatcher<{ close: void; saved: AppConfigResponse }>();

  let config: AppConfigResponse | null = null;
  let bootstrapDocPath = "";
  let openrouterApiKey = "";
  let loading = true;
  let saving = false;
  let browsing = false;
  let error = "";
  let savedMessage = "";

  $: keyStatus = config?.openrouter_key_present
    ? `Stored ${config.openrouter_key_preview || ""}`.trim()
    : "No key stored";
  $: bootstrapStatus = config?.bootstrap_doc_exists ? "Found" : "Missing";

  async function load() {
    loading = true;
    error = "";
    try {
      config = await loadAppConfig();
      bootstrapDocPath = config.bootstrap_doc_path || "";
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      loading = false;
    }
  }

  async function browse() {
    browsing = true;
    error = "";
    try {
      const result = await selectBootstrapDoc(bootstrapDocPath);
      if (result.path) {
        bootstrapDocPath = result.path;
        if (config) {
          config = { ...config, bootstrap_doc_path: result.path, bootstrap_doc_exists: true };
        }
        savedMessage = "";
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      browsing = false;
    }
  }

  async function save() {
    saving = true;
    error = "";
    savedMessage = "";
    try {
      const response = await saveAppConfig({
        bootstrap_doc_path: bootstrapDocPath.trim(),
        openrouter_api_key: openrouterApiKey.trim(),
      });
      config = response;
      bootstrapDocPath = response.bootstrap_doc_path || bootstrapDocPath;
      openrouterApiKey = "";
      savedMessage = "Configuration saved.";
      dispatch("saved", response);
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
    } finally {
      saving = false;
    }
  }

  function close() {
    dispatch("close");
  }

  onMount(() => {
    void load();
  });
</script>

<div class="modal-backdrop" role="presentation" on:click={close}>
  <div
    class="config-window"
    role="dialog"
    aria-modal="true"
    aria-labelledby="config-title"
    tabindex="-1"
    on:click|stopPropagation
    on:keydown|stopPropagation
  >
    <header class="config-window-header">
      <div>
        <span class="caption">Workspace Config</span>
        <h3 id="config-title"><Settings size={19} /> App Configuration</h3>
      </div>
      <button class="icon-button" type="button" disabled={saving} on:click={close} title="Close">
        <X size={18} />
      </button>
    </header>

    {#if error}
      <div class="error-banner">{error}</div>
    {/if}
    {#if savedMessage}
      <div class="success-banner">{savedMessage}</div>
    {/if}

    {#if loading}
      <div class="loading-panel">Loading configuration...</div>
    {:else}
      <div class="config-form">
        <label class="config-field">
          <span><FileText size={16} /> Bootstrap DOCX</span>
          <div class="path-picker-row">
            <input bind:value={bootstrapDocPath} disabled={saving || browsing} spellcheck="false" />
            <button class="secondary" type="button" disabled={saving || browsing} on:click={browse}>
              <FolderOpen size={16} /> {browsing ? "Selecting" : "Browse"}
            </button>
          </div>
          <small class:bad={!config?.bootstrap_doc_exists}>
            {#if config?.bootstrap_doc_exists}
              <CheckCircle2 size={13} /> {bootstrapStatus}
            {:else}
              <AlertTriangle size={13} /> {bootstrapStatus}
            {/if}
          </small>
        </label>

        <label class="config-field">
          <span><KeyRound size={16} /> OpenRouter API Key</span>
          <input
            bind:value={openrouterApiKey}
            disabled={saving}
            type="password"
            autocomplete="off"
            placeholder={config?.openrouter_key_present ? "Stored key present" : "Paste key"}
          />
          <small>
            {#if config?.openrouter_key_present}
              <CheckCircle2 size={13} /> {keyStatus}
            {:else}
              <AlertTriangle size={13} /> {keyStatus}
            {/if}
          </small>
        </label>

        {#if config}
          <div class="config-paths">
            <div>
              <span class="caption">Config</span>
              <code title={config.config_path}>{config.config_path}</code>
            </div>
            <div>
              <span class="caption">Env</span>
              <code title={config.env_path}>{config.env_path}</code>
            </div>
          </div>
        {/if}
      </div>
    {/if}

    <footer class="config-window-actions">
      <button class="secondary" type="button" disabled={saving} on:click={close}>Close</button>
      <button type="button" disabled={loading || saving || !bootstrapDocPath.trim()} on:click={save}>
        <Save size={16} /> {saving ? "Saving" : "Save"}
      </button>
    </footer>
  </div>
</div>
