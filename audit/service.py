# -*- coding: utf-8 -*-
"""Persistent content-audit service for generated images."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrbot.api import logger

from .models import (
    default_send_policy,
    new_record_id,
    normalize_fail_policy,
    normalize_send_method,
    normalize_send_policy,
    normalize_status,
    now_ts,
    public_record,
)
from .providers.baidu import BaiduImageAuditProvider, BaiduTextAuditProvider


DEFAULT_IMAGE_PROVIDER = "baidu_icr"
DEFAULT_TEXT_PROVIDER = "baidu_text_censor"
IMAGE_PROVIDERS = {DEFAULT_IMAGE_PROVIDER}
TEXT_PROVIDERS = {DEFAULT_TEXT_PROVIDER}
TEXT_CREDENTIAL_MODES = {"reuse_image", "separate"}
TEST_IMAGE_PNG = (
    "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAIAAAAlC+aJAAAAfUlEQVR4nNXOMREAIADEsFL/"
    "dn9HBAPXKMjZRpnESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzE"
    "SZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzESZzE"
    "SZzESZy/A68uywwDXzN02MoAAAAASUVORK5CYII="
)


class ContentAuditService:
    def __init__(self, plugin_data_dir: Path):
        self.plugin_data_dir = Path(plugin_data_dir)
        self.audit_dir = self.plugin_data_dir / "media" / "audit"
        self.records_path = self.audit_dir / "audit_records.json"
        self.settings_path = self.audit_dir / "audit_settings.json"

    def _ensure_dir(self) -> None:
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    def default_settings(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "provider": DEFAULT_IMAGE_PROVIDER,
            "providers": {"image": DEFAULT_IMAGE_PROVIDER, "video": "", "text": DEFAULT_TEXT_PROVIDER},
            "baidu_icr": {"api_key": "", "secret_key": ""},
            "baidu_text_censor": {"credential_mode": "reuse_image", "api_key": "", "secret_key": ""},
            "fail_policy": "allow",
            "send_policy": default_send_policy(),
        }

    def _normalize_providers(self, value: Any, fallback: str = "") -> Dict[str, str]:
        image = fallback if fallback in IMAGE_PROVIDERS else DEFAULT_IMAGE_PROVIDER
        text = DEFAULT_TEXT_PROVIDER
        if isinstance(value, dict):
            if "image" in value:
                candidate = str(value.get("image") or "").strip()
                image = candidate if candidate in IMAGE_PROVIDERS or candidate == "" else DEFAULT_IMAGE_PROVIDER
            if "text" in value:
                text_candidate = str(value.get("text") or "").strip()
                text = text_candidate if text_candidate in TEXT_PROVIDERS or text_candidate == "" else DEFAULT_TEXT_PROVIDER
        return {"image": image, "video": "", "text": text}

    def _normalize_baidu_settings(self, value: Any, current: Dict[str, Any] | None = None) -> Dict[str, str]:
        current = current or {}
        data = value if isinstance(value, dict) else {}
        api_key = str(data.get("api_key") or current.get("api_key") or "").strip()
        secret_value = str(data.get("secret_key") or "").strip()
        secret_key = secret_value if secret_value and secret_value != "********" else str(current.get("secret_key") or "").strip()
        return {"api_key": api_key, "secret_key": secret_key}

    def _normalize_text_baidu_settings(self, value: Any, current: Dict[str, Any] | None = None) -> Dict[str, str]:
        current = current or {}
        data = value if isinstance(value, dict) else {}
        mode = str(data.get("credential_mode") or current.get("credential_mode") or "reuse_image").strip()
        if mode not in TEXT_CREDENTIAL_MODES:
            mode = "reuse_image"
        baidu = self._normalize_baidu_settings(data, current)
        baidu["credential_mode"] = mode
        return baidu

    def _public_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(settings)
        raw_baidu = settings.get("baidu_icr") if isinstance(settings.get("baidu_icr"), dict) else {}
        baidu = dict(raw_baidu)
        baidu["secret_key"] = "********" if raw_baidu.get("secret_key") else ""
        baidu["configured"] = bool(raw_baidu.get("api_key") and raw_baidu.get("secret_key"))
        data["baidu_icr"] = baidu
        raw_text = settings.get("baidu_text_censor") if isinstance(settings.get("baidu_text_censor"), dict) else {}
        text_baidu = dict(raw_text)
        text_baidu["credential_mode"] = str(raw_text.get("credential_mode") or "reuse_image")
        text_baidu["secret_key"] = "********" if raw_text.get("secret_key") else ""
        separate_configured = bool(raw_text.get("api_key") and raw_text.get("secret_key"))
        text_baidu["configured"] = baidu["configured"] if text_baidu["credential_mode"] == "reuse_image" else separate_configured
        text_baidu["separate_configured"] = separate_configured
        data["baidu_text_censor"] = text_baidu
        return data

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
        legacy_provider = str(settings.get("provider") or DEFAULT_IMAGE_PROVIDER).strip()
        settings["providers"] = self._normalize_providers(settings.get("providers"), legacy_provider)
        settings["provider"] = settings["providers"]["image"]
        settings["baidu_icr"] = self._normalize_baidu_settings(settings.get("baidu_icr"))
        settings["baidu_text_censor"] = self._normalize_text_baidu_settings(settings.get("baidu_text_censor"))
        settings["fail_policy"] = normalize_fail_policy(settings.get("fail_policy"), "allow")
        settings["send_policy"] = normalize_send_policy(settings.get("send_policy"))
        return settings

    def public_settings(self) -> Dict[str, Any]:
        return self._public_settings(self.load_settings())

    def save_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.load_settings()
        if isinstance(payload, dict):
            if "enabled" in payload:
                current["enabled"] = bool(payload.get("enabled"))
            if "fail_policy" in payload:
                current["fail_policy"] = normalize_fail_policy(payload.get("fail_policy"), "allow")
            if "providers" in payload:
                current["providers"] = self._normalize_providers(
                    payload.get("providers"),
                    current.get("providers", {}).get("image", ""),
                )
                current["provider"] = current["providers"]["image"]
            if "baidu_icr" in payload:
                current["baidu_icr"] = self._normalize_baidu_settings(
                    payload.get("baidu_icr"),
                    current.get("baidu_icr") or {},
                )
            if "baidu_text_censor" in payload:
                current["baidu_text_censor"] = self._normalize_text_baidu_settings(
                    payload.get("baidu_text_censor"),
                    current.get("baidu_text_censor") or {},
                )
            if "send_policy" in payload:
                current["send_policy"] = normalize_send_policy(payload.get("send_policy"))
            current["providers"] = self._normalize_providers(current.get("providers"), current.get("provider", ""))
            current["provider"] = current["providers"]["image"]
        self._ensure_dir()
        self.settings_path.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        return self._public_settings(current)

    def _image_provider_for_settings(self, settings: Dict[str, Any]) -> Any:
        providers = settings.get("providers") if isinstance(settings.get("providers"), dict) else {}
        provider_name = str(providers.get("image") if "image" in providers else settings.get("provider") or DEFAULT_IMAGE_PROVIDER)
        if provider_name == "baidu_icr":
            baidu = settings.get("baidu_icr") if isinstance(settings.get("baidu_icr"), dict) else {}
            return BaiduImageAuditProvider(
                self.plugin_data_dir,
                api_key=str(baidu.get("api_key") or ""),
                secret_key=str(baidu.get("secret_key") or ""),
            )
        return None

    def _text_provider_for_settings(self, settings: Dict[str, Any]) -> Any:
        providers = settings.get("providers") if isinstance(settings.get("providers"), dict) else {}
        provider_name = str(providers.get("text") if "text" in providers else DEFAULT_TEXT_PROVIDER)
        if provider_name == "baidu_text_censor":
            text_baidu = settings.get("baidu_text_censor") if isinstance(settings.get("baidu_text_censor"), dict) else {}
            if str(text_baidu.get("credential_mode") or "reuse_image") == "separate":
                baidu = text_baidu
            else:
                baidu = settings.get("baidu_icr") if isinstance(settings.get("baidu_icr"), dict) else {}
            return BaiduTextAuditProvider(
                self.plugin_data_dir,
                api_key=str(baidu.get("api_key") or ""),
                secret_key=str(baidu.get("secret_key") or ""),
            )
        return None

    def _settings_with_payload(self, payload: Dict[str, Any] | None) -> Dict[str, Any]:
        current = self.load_settings()
        if not isinstance(payload, dict):
            return current
        merged = dict(current)
        if "enabled" in payload:
            merged["enabled"] = bool(payload.get("enabled"))
        if "providers" in payload:
            merged["providers"] = self._normalize_providers(
                payload.get("providers"),
                current.get("providers", {}).get("image", ""),
            )
            merged["provider"] = merged["providers"]["image"]
        if "baidu_icr" in payload:
            merged["baidu_icr"] = self._normalize_baidu_settings(
                payload.get("baidu_icr"),
                current.get("baidu_icr") or {},
            )
        if "baidu_text_censor" in payload:
            merged["baidu_text_censor"] = self._normalize_text_baidu_settings(
                payload.get("baidu_text_censor"),
                current.get("baidu_text_censor") or {},
            )
        merged["providers"] = self._normalize_providers(merged.get("providers"), merged.get("provider", ""))
        merged["provider"] = merged["providers"]["image"]
        merged["baidu_icr"] = self._normalize_baidu_settings(merged.get("baidu_icr"), current.get("baidu_icr") or {})
        merged["baidu_text_censor"] = self._normalize_text_baidu_settings(
            merged.get("baidu_text_censor"),
            current.get("baidu_text_censor") or {},
        )
        merged["fail_policy"] = normalize_fail_policy(merged.get("fail_policy"), "allow")
        merged["send_policy"] = normalize_send_policy(merged.get("send_policy"))
        return merged

    async def test_image_provider(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        settings = self._settings_with_payload(payload)
        scope = str((payload or {}).get("test_scope") or "all").strip().lower() if isinstance(payload, dict) else "all"
        if scope not in {"all", "image", "text"}:
            scope = "all"
        providers = settings.get("providers") if isinstance(settings.get("providers"), dict) else {}
        provider_name = str(providers.get("image") if "image" in providers else settings.get("provider") or DEFAULT_IMAGE_PROVIDER)
        provider = self._image_provider_for_settings(settings)
        text_provider_name = str(providers.get("text") if "text" in providers else DEFAULT_TEXT_PROVIDER)
        text_provider = self._text_provider_for_settings(settings)
        self._ensure_dir()
        test_path = self.audit_dir / "baidu_icr_test.png"
        result: Dict[str, Any] = {}
        text_result: Dict[str, Any] = {}
        try:
            image_configured = bool(getattr(provider, "configured", lambda: False)())
            text_configured = bool(getattr(text_provider, "configured", lambda: False)())
            if scope in {"all", "image"} and image_configured:
                test_path.write_bytes(base64.b64decode(TEST_IMAGE_PNG))
                result = await provider.audit_image(str(test_path), {"test": True})
            elif scope in {"all", "image"} and not provider_name:
                result = {"status": "skipped", "reason": "图片审核未配置，将跳过图片审核。"}
            elif scope in {"all", "image"}:
                result = {"status": "skipped", "reason": "图片审核器未配置。"}
            if scope in {"all", "text"} and text_configured:
                text_result = await text_provider.audit_text("测试文本", {"test": True})
            elif scope in {"all", "text"} and not text_provider_name:
                text_result = {"status": "skipped", "reason": "文本审核未配置，将跳过文本审核。"}
            elif scope in {"all", "text"}:
                text_result = {"status": "skipped", "reason": "文本审核器未配置。"}
            image_required = scope in {"all", "image"}
            text_required = scope in {"all", "text"}
            status = normalize_status(result.get("status")) if image_required and image_configured else "pass"
            text_status = normalize_status(text_result.get("status")) if text_required and text_configured else "pass"
            image_ready = not image_required or image_configured or not provider_name
            text_ready = not text_required or text_configured or not text_provider_name
            ok = image_ready and text_ready and status in {"pass", "block"} and text_status in {"pass", "block"}
            if ok:
                if scope == "image" and not provider_name:
                    message = "图片审核未配置，将跳过图片审核。"
                elif scope == "text" and not text_provider_name:
                    message = "文本审核未配置，将跳过文本审核。"
                else:
                    message = "百度图片审核连接成功。" if scope == "image" else "百度文本审核连接成功。" if scope == "text" else "百度内容审核连接成功。"
            else:
                message = str(result.get("reason") or text_result.get("reason") or "百度内容审核测试失败。")
            response = {
                "ok": ok,
                "test_scope": scope,
                "provider": getattr(provider, "name", provider_name),
                "text_provider": getattr(text_provider, "name", text_provider_name),
                "message": message,
                "result": result,
                "text_result": text_result,
            }
            if not ok:
                response["error"] = message
            return response
        except Exception as e:
            message = f"百度内容审核连接失败：{e}"
            return {
                "ok": False,
                "test_scope": scope,
                "provider": getattr(provider, "name", provider_name),
                "text_provider": getattr(text_provider, "name", text_provider_name),
                "message": message,
                "error": message,
                "result": {"status": "error", "reason": str(e)},
                "text_result": text_result or {"status": "error", "reason": str(e)},
            }
        finally:
            try:
                test_path.unlink(missing_ok=True)
            except Exception:
                pass

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

    def _audit_state_for_record(self, status: str, decision: str) -> str:
        if decision == "block" or status == "block":
            return "audit_hit"
        if status == "pass":
            return "audit_pass"
        if status in {"error", "unknown"}:
            return "audit_error"
        return "audit_disabled"

    def _send_method_for_audit_state(self, settings: Dict[str, Any], audit_state: str, media_type: str = "image") -> str:
        policy = normalize_send_policy(settings.get("send_policy"))
        return normalize_send_method((policy.get(audit_state) or {}).get(media_type), media_type)

    @staticmethod
    def _text_preview(text: str, limit: int = 160) -> str:
        value = " ".join(str(text or "").split())
        return value if len(value) <= limit else value[:limit] + "..."

    async def audit_images_for_task(self, task: Dict[str, Any], images: List[str]) -> Dict[str, Any]:
        origin = str(task.get("origin") or "")
        if origin not in {"command", "llm_tool"} or not images:
            return {"allowed_images": list(images), "blocked": [], "records": []}

        settings = self.load_settings()
        if not settings.get("enabled", True):
            return {"allowed_images": list(images), "blocked": [], "records": []}
        if not (settings.get("providers") or {}).get("image"):
            return {"allowed_images": list(images), "blocked": [], "records": []}
        image_provider = self._image_provider_for_settings(settings)
        if not image_provider:
            return {"allowed_images": list(images), "blocked": [], "records": []}

        records = self._read_records()
        allowed: List[str] = []
        blocked: List[str] = []
        made: List[Dict[str, Any]] = []
        for index, image_url in enumerate(images, 1):
            try:
                result = await image_provider.audit_image(image_url, {"task": task, "index": index})
            except Exception as e:
                result = {
                    "status": "error",
                    "categories": [],
                    "scores": {},
                    "reason": f"审核执行失败：{e}",
                    "provider": getattr(image_provider, "name", DEFAULT_IMAGE_PROVIDER),
                    "raw": {},
                }
            status = normalize_status(result.get("status"))
            decision = self._decision_for_status(status, settings)
            audit_state = self._audit_state_for_record(status, decision)
            send_method = self._send_method_for_audit_state(settings, audit_state, "image")
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
                "media_type": "image",
                "status": status,
                "decision": decision,
                "audit_state": audit_state,
                "send_method": send_method,
                "sent": send_method != "dont_send",
                "reason": str(result.get("reason") or ""),
                "categories": result.get("categories") or [],
                "scores": result.get("scores") or {},
                "provider": result.get("provider") or getattr(image_provider, "name", DEFAULT_IMAGE_PROVIDER),
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

    async def audit_texts_for_task(self, task: Dict[str, Any], texts: List[str]) -> Dict[str, Any]:
        origin = str(task.get("origin") or "")
        text_values = [str(text) for text in (texts or []) if str(text or "").strip()]
        if origin not in {"command", "llm_tool"} or not text_values:
            return {"allowed_texts": list(texts or []), "blocked": [], "records": []}

        settings = self.load_settings()
        if not settings.get("enabled", True):
            return {"allowed_texts": list(texts or []), "blocked": [], "records": []}
        if not (settings.get("providers") or {}).get("text"):
            return {"allowed_texts": list(texts or []), "blocked": [], "records": []}
        text_provider = self._text_provider_for_settings(settings)
        if not text_provider:
            return {"allowed_texts": list(texts or []), "blocked": [], "records": []}

        records = self._read_records()
        allowed: List[str] = []
        blocked: List[str] = []
        made: List[Dict[str, Any]] = []
        for index, text in enumerate(text_values, 1):
            try:
                result = await text_provider.audit_text(text, {"task": task, "index": index})
            except Exception as e:
                result = {
                    "status": "error",
                    "categories": [],
                    "scores": {},
                    "reason": f"文本审核执行失败：{e}",
                    "provider": getattr(text_provider, "name", DEFAULT_TEXT_PROVIDER),
                    "raw": {},
                }
            status = normalize_status(result.get("status"))
            decision = self._decision_for_status(status, settings)
            audit_state = self._audit_state_for_record(status, decision)
            send_method = self._send_method_for_audit_state(settings, audit_state, "text")
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
                "media_type": "text",
                "text_index": index,
                "text_preview": self._text_preview(text),
                "status": status,
                "decision": decision,
                "audit_state": audit_state,
                "send_method": send_method,
                "sent": send_method != "dont_send",
                "reason": str(result.get("reason") or ""),
                "categories": result.get("categories") or [],
                "scores": result.get("scores") or {},
                "provider": result.get("provider") or getattr(text_provider, "name", DEFAULT_TEXT_PROVIDER),
                "manual": False,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "raw": result.get("raw") or {},
            }
            records.append(record)
            made.append(public_record(record))
            if send_method == "dont_send":
                blocked.append(text)
            else:
                allowed.append(text)
        self._write_records(records)
        return {"allowed_texts": allowed, "blocked": blocked, "records": made}

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

    def remove_task_records(self, task_id: str) -> None:
        task_id = str(task_id or "")
        if not task_id:
            return
        records = [r for r in self._read_records() if str(r.get("task_id") or "") != task_id]
        self._write_records(records)
