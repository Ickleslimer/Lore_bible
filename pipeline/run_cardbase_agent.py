from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from pipeline.cardbase_agent import card_agent_activity_payload, run_card_agent_request


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Cardbase Agent once against an artifact root.")
    parser.add_argument("--artifacts-root", required=True, help="Artifact root to mutate, for example artifacts/runs/<run_id>.")
    parser.add_argument("--request", required=True, help="Freeform author request for the Cardbase Agent.")
    parser.add_argument("--requester", default="cli_user", help="Requester label recorded on the request row.")
    parser.add_argument("--target-text", default="", help="Optional target text or entity/card hint.")
    parser.add_argument("--rationale", default="", help="Optional human rationale recorded with the request.")
    parser.add_argument("--review-memory-json", default="", help="Optional review memory path. Defaults to <repo>/canon/review_memory.json when discoverable.")
    parser.add_argument("--pipeline-config-json", default="", help="Optional pipeline config path. Defaults to <repo>/config/pipeline_config.json when discoverable.")
    parser.add_argument("--max-steps", type=int, default=16, help="Maximum model/tool loop steps.")
    args = parser.parse_args()

    artifacts_root = Path(args.artifacts_root)
    review_memory_path = Path(args.review_memory_json) if args.review_memory_json else None
    config_path = Path(args.pipeline_config_json) if args.pipeline_config_json else None
    result = run_card_agent_request(
        artifacts_root=artifacts_root,
        instruction_text=args.request,
        requester=args.requester,
        target_text=args.target_text,
        rationale=args.rationale,
        review_memory_path=review_memory_path,
        config_path=config_path,
        max_steps=max(1, args.max_steps),
    )
    payload: dict[str, Any] = {
        "result": result,
        "activity": card_agent_activity_payload(artifacts_root),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
