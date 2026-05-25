#!/usr/bin/env python3
"""Capture Antigravity model quota (local UI or --worker shared-folder handoff to quota_worker.py)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.quota_capture import main

if __name__ == "__main__":
    raise SystemExit(main())
