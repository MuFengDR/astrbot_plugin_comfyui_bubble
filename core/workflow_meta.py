# -*- coding: utf-8 -*-
"""Workflow metadata and input/output rule helpers."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..workflow_engine import list_workflows_in_dir
from .paths import META_PATH, PLUGIN_DATA_DIR, WORKFLOWS_DIR


def _ensure_workflows_dir() -> None:
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)


def _load_workflow_meta() -> Dict[str, Any]:
    """从 workflow_meta.json 读取，返回 filename -> {short, detailed} 格式。"""
    if not META_PATH.exists():
        return {}
    try:
        data = json.loads(META_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            # 检查是否是旧格式（直接是 filename -> string）
            descriptions = data.get("descriptions", data)
            result = {}
            for k, v in descriptions.items():
                if isinstance(v, str):
                    # 旧格式，转为新格式
                    result[k] = {"short": v, "detailed": v}
                elif isinstance(v, dict):
                    result[k] = v
                else:
                    result[k] = {"short": "", "detailed": ""}
            return result
    except Exception:
        return {}
    return {}


def _load_workflow_text_slots() -> Dict[str, List[str]]:
    """
    从 workflow_meta.json 读取 filename -> 文本槽位说明列表（与工作流中 Simple String 节点顺序一致）。
    用于 list_workflows 时告知 LLM 每个 text 的用途，例如 ["正面提示词", "负面提示词"] 或 ["修改说明"]。
    """
    if not META_PATH.exists():
        return {}
    try:
        data = json.loads(META_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        raw = data.get("text_slots")
        if not isinstance(raw, dict):
            return {}
        return {k: v if isinstance(v, list) else [] for k, v in raw.items()}
    except Exception:
        return {}


def _load_workflow_params() -> Dict[str, Any]:
    if not META_PATH.exists():
        return {}
    try:
        data = json.loads(META_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {}
        raw = data.get("workflow_params")
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _list_workflows_in_configured_dir(workflow_dir: Path) -> List[Dict[str, Any]]:
    return list_workflows_in_dir(workflow_dir, _load_workflow_params())


def _get_configured_workflow_info(workflow_dir: Path, filename: str, workflow_params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    for item in list_workflows_in_dir(workflow_dir, workflow_params or _load_workflow_params()):
        if item.get("filename") == filename:
            return item
    return None


def _apply_input_rule(values: List[Any], rule: Dict[str, Any], label: str) -> tuple[bool, List[Any], str]:
    limit = rule.get("limit") if isinstance(rule, dict) else None
    mode = rule.get("mode") if isinstance(rule, dict) else "loose"
    if limit is None:
        return True, values, ""
    limit = max(0, int(limit))
    count = len(values)
    if mode == "strict" and count != limit:
        return False, values, f"{label}需要严格输入 {limit} 个，当前提供 {count} 个。"
    if mode != "strict" and count > limit:
        return True, values[:limit], ""
    return True, values, ""


def _apply_workflow_input_rules(info: Dict[str, Any], texts: List[str], images: List[str], videos: List[str]) -> tuple[bool, List[str], List[str], List[str], str]:
    params = info.get("params") if isinstance(info.get("params"), dict) else {}
    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else {}
    ok_texts, texts, msg_texts = _apply_input_rule(texts, inputs.get("text", {}), "文本")
    ok_images, images, msg_images = _apply_input_rule(images, inputs.get("image", {}), "图片")
    ok_videos, videos, msg_videos = _apply_input_rule(videos, inputs.get("video", {}), "视频")
    messages = [m for m in (msg_texts, msg_images, msg_videos) if m]
    return ok_texts and ok_images and ok_videos, texts, images, videos, " ".join(messages)


def _workflow_input_mismatch_message(
    workflow_name: str,
    candidates: List[Dict[str, Any]],
    text_count: int,
    image_count: int,
    video_count: int,
) -> str:
    details: List[str] = []
    sample_texts = [""] * text_count
    sample_images = [""] * image_count
    sample_videos = [""] * video_count
    for item in candidates:
        ok, _, _, _, msg = _apply_workflow_input_rules(
            item, list(sample_texts), list(sample_images), list(sample_videos)
        )
        if ok:
            continue
        filename = item.get("filename") or "workflow.json"
        details.append(f"- {filename}: {msg or '输入数量不符合该工作流设置。'}")
    suffix = ("\n" + "\n".join(details)) if details else ""
    return (
        f"工作流「{workflow_name}」存在，但入参数量不符合条件。"
        f"当前提供：文本{text_count}，图片{image_count}，视频{video_count}。"
        + suffix
    )


async def _load_workflow_descriptions(config: Any) -> Dict[str, str]:
    """工作流说明：优先从 workflow_meta.json 读取（管理页编辑），兼容旧配置 workflow_descriptions。"""
    meta = _load_workflow_meta()
    if meta:
        return meta
    raw = getattr(config, "workflow_descriptions", None) or "{}"
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw) if raw.strip() else {}
    except Exception:
        return {}


def _save_workflow_meta(descriptions: Dict[str, Any]) -> None:
    """将 filename -> {short, detailed} 写入 workflow_meta.json，保留已有 text_slots 等字段。"""
    PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    existing: Dict[str, Any] = {}
    if META_PATH.exists():
        try:
            data = json.loads(META_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                existing = dict(data)
        except Exception:
            pass
    existing["descriptions"] = descriptions
    META_PATH.write_text(
        json.dumps(existing, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_workflow_dir() -> Path:
    """工作流目录：优先使用插件数据目录，若为空则回退到 sd_json（兼容旧路径）。"""
    _ensure_workflows_dir()
    if any(WORKFLOWS_DIR.glob("*.json")):
        return WORKFLOWS_DIR
    fallback = Path("sd_json")
    return fallback if fallback.exists() else WORKFLOWS_DIR


__all__ = [
    "_apply_input_rule",
    "_apply_workflow_input_rules",
    "_ensure_workflows_dir",
    "_get_configured_workflow_info",
    "_get_workflow_dir",
    "_list_workflows_in_configured_dir",
    "_load_workflow_descriptions",
    "_load_workflow_meta",
    "_load_workflow_params",
    "_load_workflow_text_slots",
    "_save_workflow_meta",
    "_workflow_input_mismatch_message",
]
