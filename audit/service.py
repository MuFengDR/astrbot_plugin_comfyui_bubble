# -*- coding: utf-8 -*-
"""Persistent content-audit service for generated images."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrbot.api import logger

from .models import (
    default_send_policy,
    new_record_id,
    normalize_fail_policy,
    normalize_send_policy,
    normalize_status,
    now_ts,
    public_record,
)
from .providers.placeholder import PlaceholderAuditProvider


class ContentAuditService:
    def __init__(self, plugin_data_dir: Path):
        self.plugin_data_dir = Path(plugin_data_dir)
        self.audit_dir = self.plugin_data_dir / "media" / "audit"
        self.records_path = self.audit_dir / "audit_records.json"
        self.settings_path = self.audit_dir / "audit_settings.json"
        self.provider = PlaceholderAuditProvider()

    def _ensure_dir(self) -> None:
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    def default_settings(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "provider": self.provider.name,
            "fail_policy": "allow",
            "send_policy": default_send_policy(),
        }

    def load_settings(self) -> Dict[str, Any]:
        settings = self.default_settings()
        try:
            if self.settings_path.exists():
                data = json.loads(self.settings_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    settings.update(data)
        except Exception as e:
            logger.warning("ComfyUI content audit settings read failed: %s", e)
        settings["enabled"] = bool(settings.get("enabled", True))
        settings["provider"] = str(settings.get("provider") or self.provider.name)
        settings["fail_policy"] = normalize_fail_policy(settings.get("fail_policy"), "allow")
        settings["send_policy"] = normalize_send_policy(settings.get("send_policy"))
        return settings

    def save_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.load_settings()
        if isinstance(payload, dict):
            if "enabled" in payload:
                current["enabled"] = bool(payload.get("enabled"))
            if "fail_policy" in payload:
                current["fail_policy"] = normalize_fail_policy(payload.get("fail_policy"), "allow")
            if "send_policy" in payload:
                current["send_policy"] = normalize_send_policy(payload.get("send_policy"))
            current["provider"] = self.provider.name
        self._ensure_dir()
        self.settings_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        return current

    def _read_records(self) -> List[Dict[str, Any]]:
        try:
            if not self.records_path.exists():
                return []
            data = json.loads(self.records_path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("ComfyUI content audit records read failed: %s", e)
            return []

    def _write_records(self, records: Iterable[Dict[str, Any]]) -> None:
        self._ensure_dir()
        self.records_path.write_text(
            json.dumps(list(records), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _decision_for_status(self, status: str, settings: Dict[str, Any]) -> str:
        if status == "pass":
            return "allow"
        if status == "block":
            return "block"
        return normalize_fail_policy(settings.get("fail_policy"), "allow")

    async def audit_images_for_task(self, task: Dict[str, Any], images: List[str]) -> Dict[str, Any]:
        origin = str(task.get("origin") or "")
        if origin not in {"command", "llm_tool"} or not images:
            return {"allowed_images": list(images), "blocked": [], "records": []}

        settings = self.load_settings()
        if not settings.get("enabled", True):
            return {"allowed_images": list(images), "blocked": [], "records": []}

        records = self._read_records()
        allowed: List[str] = []
        blocked: List[str] = []
        made: List[Dict[str, Any]] = []
        for index, image_url in enumerate(images, 1):
            try:
                result = await self.provider.audit_image(image_url, {"task": task, "index": index})
            except Exception as e:
                result = {
                    "status": "error",
                    "categories": [],
                    "scores": {},
                    "reason": f"审核执行失败：{e}",
                    "provider": self.provider.name,
                    "raw": {},
                }
            status = normalize_status(result.get("status"))
            decision = self._decision_for_status(status, settings)
            record = {
                "id": new_record_id(),
                "task_id": str(task.get("task_id") or ""),
                "prompt_id": str(task.get("prompt_id") or ""),
                "origin": origin,
                "origin_label": task.get("origin_label") or origin,
                "session_label": task.get("session_label") or "",
                "session_key": task.get("session_key") or "",
                "workflow_name": task.get("workflow_name") or "",
                "workflow_file": task.get("workflow_file") or "",
                "port_name": task.get("port_name") or "",
                "image_url": image_url,
                "thumbnail": image_url,
                "status": status,
                "decision": decision,
                "sent": decision != "block",
                "reason": str(result.get("reason") or ""),
                "categories": result.get("categories") or [],
                "scores": result.get("scores") or {},
                "provider": result.get("provider") or self.provider.name,
                "manual": False,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "raw": result.get("raw") or {},
            }
            records.append(record)
            made.append(public_record(record))
            if decision == "block":
                blocked.append(image_url)
            else:
                allowed.append(image_url)
        self._write_records(records)
        return {"allowed_images": allowed, "blocked": blocked, "records": made}

    def list_records(self, filters: Dict[str, Any] | None = None) -> Dict[str, Any]:
        filters = filters or {}
        records = [public_record(item) for item in self._read_records()]
        status = str(filters.get("status") or "").strip()
        origin = str(filters.get("origin") or "").strip()
        workflow = str(filters.get("workflow") or "").strip()
        port = str(filters.get("port") or "").strip()
        if status:
            records = [r for r in records if str(r.get("status") or "") == status or str(r.get("decision") or "") == status]
        if origin:
            records = [r for r in records if str(r.get("origin") or "") == origin]
        if workflow:
            records = [r for r in records if str(r.get("workflow_name") or "") == workflow]
        if port:
            records = [r for r in records if str(r.get("port_name") or "") == port]
        records.sort(key=lambda item: float(item.get("created_at") or 0), reverse=True)
        try:
            limit = int(filters.get("limit") or 200)
        except Exception:
            limit = 200
        return {"ok": True, "records": records[: max(1, min(limit, 500))]}

    def stats(self) -> Dict[str, Any]:
        records = self._read_records()
        return {
            "ok": True,
            "stats": {
                "total": len(records),
                "unknown": sum(1 for r in records if r.get("status") == "unknown"),
                "pass": sum(1 for r in records if r.get("status") == "pass"),
                "block": sum(1 for r in records if r.get("decision") == "block"),
                "error": sum(1 for r in records if r.get("status") == "error"),
            },
        }

    def manual_review(self, record_id: str, decision: str, reason: str = "") -> Dict[str, Any]:
        decision = "block" if str(decision or "").strip() == "block" else "allow"
        records = self._read_records()
        for record in records:
            if str(record.get("id") or "") == str(record_id or ""):
                record["decision"] = decision
                record["status"] = "block" if decision == "block" else "pass"
                record["sent"] = bool(record.get("sent")) if decision == "allow" else False
                record["manual"] = True
                record["reason"] = reason or ("人工拦截" if decision == "block" else "人工通过")
                record["updated_at"] = now_ts()
                self._write_records(records)
                return {"ok": True, "record": public_record(record)}
        return {"ok": False, "error": "审核记录不存在。"}

    async def retry(self, record_id: str) -> Dict[str, Any]:
        records = self._read_records()
        for record in records:
            if str(record.get("id") or "") == str(record_id or ""):
                task = {
                    "task_id": record.get("task_id"),
                    "prompt_id": record.get("prompt_id"),
                    "origin": record.get("origin"),
                    "origin_label": record.get("origin_label"),
                    "session_label": record.get("session_label"),
                    "workflow_name": record.get("workflow_name"),
                    "workflow_file": record.get("workflow_file"),
                    "port_name": record.get("port_name"),
                }
                result = await self.provider.audit_image(str(record.get("image_url") or ""), {"task": task, "retry": True})
                status = normalize_status(result.get("status"))
                settings = self.load_settings()
                decision = self._decision_for_status(status, settings)
                record.update(
                    {
                        "status": status,
                        "decision": decision,
                        "reason": str(result.get("reason") or ""),
                        "categories": result.get("categories") or [],
                        "scores": result.get("scores") or {},
                        "provider": result.get("provider") or self.provider.name,
                        "manual": False,
                        "updated_at": now_ts(),
                        "raw": result.get("raw") or {},
                    }
                )
                self._write_records(records)
                return {"ok": True, "record": public_record(record)}
        return {"ok": False, "error": "审核记录不存在。"}

    def remove_task_records(self, task_id: str) -> None:
        task_id = str(task_id or "")
        if not task_id:
            return
        records = [r for r in self._read_records() if str(r.get("task_id") or "") != task_id]
        self._write_records(records)
