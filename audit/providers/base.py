# -*- coding: utf-8 -*-
"""Provider interface for generated content audit."""

from __future__ import annotations

from typing import Any, Dict


class AuditProvider:
    name = "base"

    async def audit_image(self, image_url: str, context: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    async def audit_text(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
