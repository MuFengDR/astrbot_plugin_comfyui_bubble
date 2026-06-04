# -*- coding: utf-8 -*-
"""Placeholder audit provider used until a real classifier is configured."""

from __future__ import annotations

from typing import Any, Dict

from .base import AuditProvider


class PlaceholderAuditProvider(AuditProvider):
    name = "placeholder"

    async def audit_image(self, image_url: str, context: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "status": "unknown",
            "categories": [],
            "scores": {},
            "reason": "审核器未配置，已按失败策略处理。",
            "provider": self.name,
            "raw": {"image_url": image_url},
        }
