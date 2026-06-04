# -*- coding: utf-8 -*-
"""Lightweight content-audit model helpers."""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict


AUDIT_STATUSES = {"pass", "block", "unknown", "error"}
AUDIT_FAIL_POLICIES = {"allow", "block"}
AUDIT_SEND_STATES = ("audit_disabled", "audit_error", "audit_hit", "audit_pass")
AUDIT_MEDIA_TYPES = ("text", "image", "video")
AUDIT_SEND_METHODS = {
    "direct",
    "obfuscated",
    "none",
}


def default_send_policy() -> Dict[str, Dict[str, str]]:
    direct = {media_type: "direct" for media_type in AUDIT_MEDIA_TYPES}
    return {
        "audit_disabled": dict(direct),
        "audit_error": dict(direct),
        "audit_hit": {"text": "direct", "image": "none", "video": "none"},
        "audit_pass": dict(direct),
    }


def now_ts() -> float:
    return time.time()


def new_record_id() -> str:
    return f"audit_{uuid.uuid4().hex}"


def normalize_status(value: Any, default: str = "unknown") -> str:
    status = str(value or "").strip().lower()
    return status if status in AUDIT_STATUSES else default


def normalize_fail_policy(value: Any, default: str = "allow") -> str:
    policy = str(value or "").strip().lower()
    return policy if policy in AUDIT_FAIL_POLICIES else default


def normalize_send_policy(value: Any) -> Dict[str, Dict[str, str]]:
    policy = default_send_policy()
    if not isinstance(value, dict):
        return policy
    for state in AUDIT_SEND_STATES:
        source_row = value.get(state)
        if not isinstance(source_row, dict):
            continue
        for media_type in AUDIT_MEDIA_TYPES:
            method = str(source_row.get(media_type) or "").strip().lower()
            if method in AUDIT_SEND_METHODS and (method != "obfuscated" or media_type == "image"):
                policy[state][media_type] = method
    return policy


def public_record(record: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(record)
    data.pop("raw", None)
    return data
