"""One-off smoke test for NVIDIA NIM via pipeline model_provider."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline.common import read_json
from pipeline.model_provider import call_model_chat, get_model_runtime_status, model_call_kwargs


def main() -> None:
    cfg = read_json(Path("config/pipeline_config.json"))
    kwargs = model_call_kwargs(cfg, "stage_05_lore_development_ledger")
    prompt = 'Return strict JSON only: {"ok": true, "provider": "nvidia_nim"}'
    resp = call_model_chat(prompt=prompt, **kwargs)
    status = get_model_runtime_status()
    print("provider", kwargs.get("provider"), "model", kwargs.get("api_model"))
    print("skip_reason", status.get("last_model_skip_reason"))
    print("billed_usd", status.get("last_call_billed_cost_usd"))
    print("api_model_used", status.get("last_call_api_model"))
    if resp:
        print("keys", sorted(resp.keys())[:8])
        print("payload", json.dumps(resp)[:300])
    else:
        print("response", None)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
