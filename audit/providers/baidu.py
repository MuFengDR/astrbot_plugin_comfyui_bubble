# -*- coding: utf-8 -*-
"""Baidu Cloud ICR image/text audit providers."""

from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote

import aiohttp

from .base import AuditProvider


class BaiduImageAuditProvider(AuditProvider):
    name = "baidu_icr"
    token_url = "https://aip.baidubce.com/oauth/2.0/token"
    image_audit_url = "https://aip.baidubce.com/rest/2.0/solution/v1/img_censor/v2/user_defined"

    def __init__(self, plugin_data_dir: Path, api_key: str = "", secret_key: str = ""):
        self.plugin_data_dir = Path(plugin_data_dir)
        self.api_key = str(api_key or "").strip()
        self.secret_key = str(secret_key or "").strip()
        self._access_token = ""
        self._token_expires_at = 0.0

    def configured(self) -> bool:
        return bool(self.api_key and self.secret_key)

    def _resolve_image_path(self, image_url: str) -> Optional[Path]:
        value = str(image_url or "").strip()
        if not value:
            return None
        if value.startswith("/api/media/history/"):
            filename = Path(value.rsplit("/", 1)[-1]).name
            path = self.plugin_data_dir / "media" / "history" / filename
            return path if path.exists() and path.is_file() else None
        if value.startswith("/api/debug/output/"):
            filename = Path(value.rsplit("/", 1)[-1]).name
            path = self.plugin_data_dir / "webui" / "output" / filename
            return path if path.exists() and path.is_file() else None
        path = Path(value)
        return path if path.exists() and path.is_file() else None

    async def _get_access_token(self) -> str:
        if self._access_token and time.time() < self._token_expires_at - 120:
            return self._access_token
        params = {
            "grant_type": "client_credentials",
            "client_id": self.api_key,
            "client_secret": self.secret_key,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.token_url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                data = await resp.json(content_type=None)
        token = str(data.get("access_token") or "")
        if not token:
            error = data.get("error_description") or data.get("error") or "access_token 获取失败"
            raise RuntimeError(str(error))
        try:
            expires_in = int(data.get("expires_in") or 0)
        except Exception:
            expires_in = 0
        self._access_token = token
        self._token_expires_at = time.time() + max(expires_in, 300)
        return token

    @staticmethod
    def _map_result(data: Dict[str, Any]) -> Dict[str, Any]:
        conclusion_type = int(data.get("conclusionType") or 0)
        if conclusion_type == 1:
            status = "pass"
            reason = str(data.get("conclusion") or "百度内容审核：合规")
        elif conclusion_type == 2:
            status = "block"
            reason = str(data.get("conclusion") or "百度内容审核：不合规")
        elif conclusion_type == 3:
            status = "block"
            reason = str(data.get("conclusion") or "百度内容审核：疑似")
        else:
            status = "error"
            reason = str(data.get("conclusion") or data.get("error_msg") or "百度内容审核失败")

        categories = []
        scores: Dict[str, Any] = {}
        for item in data.get("data") or []:
            if not isinstance(item, dict):
                continue
            label = str(item.get("msg") or item.get("type") or item.get("subType") or "").strip()
            if label:
                categories.append(label)
            score_key = label or str(item.get("type") or len(scores) + 1)
            if "probability" in item:
                scores[score_key] = item.get("probability")
            elif "stars" in item:
                scores[score_key] = item.get("stars")

        return {
            "status": status,
            "categories": categories,
            "scores": scores,
            "reason": reason,
            "provider": BaiduImageAuditProvider.name,
            "raw": data,
        }

    async def audit_image(self, image_url: str, context: Dict[str, Any]) -> Dict[str, Any]:
        if not self.configured():
            return {
                "status": "error",
                "categories": [],
                "scores": {},
                "reason": "百度内容审核未配置 API Key 或 Secret Key。",
                "provider": self.name,
                "raw": {},
            }
        path = self._resolve_image_path(image_url)
        if not path:
            return {
                "status": "error",
                "categories": [],
                "scores": {},
                "reason": "百度内容审核无法读取本地图片文件。",
                "provider": self.name,
                "raw": {"image_url": image_url},
            }
        image_data = base64.b64encode(path.read_bytes()).decode("ascii")
        token = await self._get_access_token()
        url = f"{self.image_audit_url}?access_token={quote(token)}"
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                data={"image": image_data},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json(content_type=None)
        return self._map_result(data if isinstance(data, dict) else {})


class BaiduTextAuditProvider(BaiduImageAuditProvider):
    name = "baidu_text_censor"
    text_audit_url = "https://aip.baidubce.com/rest/2.0/solution/v1/text_censor/v2/user_defined"

    async def audit_text(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        if not self.configured():
            return {
                "status": "error",
                "categories": [],
                "scores": {},
                "reason": "百度文本审核未配置 API Key 或 Secret Key。",
                "provider": self.name,
                "raw": {},
            }
        content = str(text or "").strip()
        if not content:
            return {
                "status": "pass",
                "categories": [],
                "scores": {},
                "reason": "空文本无需审核。",
                "provider": self.name,
                "raw": {},
            }
        token = await self._get_access_token()
        url = f"{self.text_audit_url}?access_token={quote(token)}"
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                data={"text": content},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json(content_type=None)
        result = self._map_result(data if isinstance(data, dict) else {})
        result["provider"] = self.name
        return result
