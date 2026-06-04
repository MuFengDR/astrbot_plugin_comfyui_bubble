# -*- coding: utf-8 -*-
"""Shared helpers for management handlers."""

import re
from pathlib import Path


SAFE_FILENAME_RE = re.compile(r"^[a-zA-Z0-9_\-\+=\.\u4e00-\u9fff]+$")


def safe_basename(name: str) -> str:
    """Return a safe basename to prevent path traversal."""
    return Path(name).name.strip()


async def maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
