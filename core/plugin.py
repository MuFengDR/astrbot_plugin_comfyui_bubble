# -*- coding: utf-8 -*-
"""
AstrBot ComfyUI 插件：将工作流封装为 LLM 工具，支持配置上传/管理、等待策略。
"""
import asyncio
import base64
import io
import json
import os
import tempfile
import time
from urllib.parse import parse_qs, urlparse
import uuid

import aiohttp


async def _download_url_to_local(url: str) -> str:
    """下载远程图片到本地临时目录，返回本地路径。"""
    if not url:
        return url
    try:
        import uuid
        from pathlib import Path
        # 使用 comfyui input 目录
        plugin_dir = globals().get("PLUGIN_DATA_DIR")
        local_dir = (Path(plugin_dir) / "media" / "history") if plugin_dir else Path("data/plugin_data/astrbot_plugin_comfyui_bubble/media/history")
        local_dir.mkdir(parents=True, exist_ok=True)
        # 生成唯一文件名
        ext = ".png"
        if "." in url:
            path_parts = url.split("?")[0].split("/")
            if path_parts:
                fname = path_parts[-1]
                if "." in fname:
                    ext = "." + fname.split(".")[-1]
                    if ext not in [".png", ".jpg", ".jpeg", ".gif", ".webp"]:
                        ext = ".png"
        local_name = f"temp_{uuid.uuid4().hex}{ext}"
        local_path = local_dir / local_name
        # 下载
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(url)
            if r.status_code == 200:
                async with aiofiles.open(local_path, "wb") as f:
                    await f.write(r.content)
                return str(local_path.resolve())
    except Exception as e:
        import logging
        logging.getLogger().warning(f"Download URL to local failed: {e}")
    return url
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
import httpx
from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from pydantic import Field
from pydantic.dataclasses import dataclass

from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext
from astrbot.core.utils.quoted_message import extract_quoted_message_images

from ..audit import ContentAuditService
from ..audit.models import default_send_policy, normalize_send_method, normalize_send_policy
from ..workflow_engine import (
    ComfyUIWorkflow,
    find_workflow_file,
    list_workflows_in_dir,
    parse_workflow_filename,
)
from .paths import (
    ACTIVE_PORT_STATE_PATH,
    META_PATH,
    PLUGIN_DATA_DIR,
    PORTS_CONFIG_PATH,
    WORKFLOWS_DIR,
)

try:
    from astrbot.api import AstrBotConfig
except ImportError:
    AstrBotConfig = dict

# 每个任务预估耗时（秒），用于等待策略
ESTIMATE_SECONDS_PER_JOB = 45
WAIT_THRESHOLD_SECONDS = 30
DEFAULT_QUERY_WAIT_SECONDS = 900
MAX_QUERY_WAIT_SECONDS = 3600
COMFYUI_WS_UNAVAILABLE_MESSAGE = (
    "ComfyUI WebSocket 不可用，请检查 server_ip、反代是否支持 websocket、client_id 是否一致。"
)

# 会话最近提交的任务：session_key -> { "prompt_id", "server_ip", "client_id" }
# 同时写入 "default" 以便在工具内拿不到 event 时仍能查到当前会话任务
_session_pending: Dict[str, Dict[str, Any]] = {}

# 以 ComfyUI 返回的 prompt_id（UUID）为唯一键的任务注册表，便于跨轮次/跨会话按任务 ID 查询
# prompt_id -> { "server_ip", "client_id", "session_key", "session_tag" }
_task_registry: Dict[str, Dict[str, Any]] = {}

# session_tag（角色标识）-> 任务 prompt_id 列表，用于批量管理多任务
# LLM 需要提供自己的唯一标识（如 QQ 号或昵称）来追踪所有任务
_session_tag_tasks: Dict[str, List[str]] = {}

# 占位符：LLM 在回复中写入此字符串，on_decorating_result 会替换为实际媒体（解决工具内拿不到 session_id / LLM 误用 record 发视频的问题）
COMFYUI_IMAGE_PLACEHOLDER = "[COMFYUI_IMAGE]"
COMFYUI_VIDEO_PLACEHOLDER = "[COMFYUI_VIDEO]"
# 发送时在消息中追加「ComfyUI 图片/视频路径: /abs/path」，便于 qts_get_recent_messages 等返回的 content 里带路径，Bot 可解析后用于下一轮 image_urls
# 会话 key -> 该会话已完成任务的图片/视频 URL 队列（FIFO），按顺序消费
_session_image_url_queue: Dict[str, List[str]] = {}
_session_video_url_queue: Dict[str, List[str]] = {}

# Explicit definitions kept separate from legacy mojibake comments above.
_session_pending: Dict[str, Dict[str, Any]] = {}
_session_tag_tasks: Dict[str, List[str]] = {}
COMFYUI_IMAGE_PLACEHOLDER = "[COMFYUI_IMAGE]"
_session_image_url_queue: Dict[str, List[str]] = {}
_plugin_config: Any = None

# 当前插件配置（由插件 __init__ 设置，供 LLM 工具读取）
_plugin_config: Any = None
# 插件 Context，供工具内调用 send_message 发送图片等
_plugin_context: Any = None
_task_service: Any = None


from .workflow_meta import (
    _apply_input_rule,
    _apply_workflow_input_rules,
    _ensure_workflows_dir,
    _get_configured_workflow_info,
    _get_workflow_dir,
    _list_workflows_in_configured_dir,
    _load_workflow_descriptions,
    _load_workflow_meta,
    _load_workflow_params,
    _load_workflow_text_slots,
    _save_workflow_meta,
    _workflow_availability_error,
    _workflow_input_mismatch_message,
    _workflow_is_available,
)

from .config import (
    _config_get,
    _filter_workflows_for_port,
    _get_active_comfyui_port,
    _get_comfyui_host,
    _get_comfyui_http_base,
    _get_comfyui_ports,
    _get_server_config,
    _normalize_comfyui_http,
    _save_ports_config_file,
    _sync_active_interface_config,
    _workflow_allowed_for_port,
)

def _get_wait_threshold(config: Any) -> int:
    """从配置读取 query_wait 等待阈值（秒），未配置或非法时返回默认 30，并限制在 5～300 之间。"""
    return WAIT_THRESHOLD_SECONDS


def _get_websocket_wait_timeout(config: Any) -> int:
    raw = (
        getattr(config, "websocket_wait_timeout_seconds", None)
        if not isinstance(config, dict)
        else config.get("websocket_wait_timeout_seconds")
    )
    if raw is None:
        return DEFAULT_QUERY_WAIT_SECONDS
    try:
        n = int(raw)
        return max(0, min(MAX_QUERY_WAIT_SECONDS, n))
    except (TypeError, ValueError):
        return DEFAULT_QUERY_WAIT_SECONDS


def _get_event_from_context(context: Any) -> Optional[Any]:
    try:
        event = getattr(context, "event", None)
        agent_ctx = getattr(context, "context", None)
        if event is None:
            event = getattr(agent_ctx, "event", None) if agent_ctx else None
        if event is None and agent_ctx is not None:
            event = getattr(getattr(agent_ctx, "context", None), "event", None)
        if event is None and hasattr(context, "extra"):
            extra = getattr(context, "extra", None) or {}
            if isinstance(extra, dict):
                event = extra.get("event")
        if event is None and agent_ctx is not None and hasattr(agent_ctx, "extra"):
            extra = getattr(agent_ctx, "extra", None) or {}
            if isinstance(extra, dict):
                event = extra.get("event")
        if event is None and (
            hasattr(context, "unified_msg_origin")
            or hasattr(context, "get_session_id")
            or hasattr(context, "get_sender_id")
            or hasattr(context, "message_obj")
        ):
            event = context
        return event
    except Exception as e:
        logger.debug("get_event_from_context: %s", e)
    return None


def _get_session_key(context: Any) -> str:
    """从工具调用的 context 中解析会话 key（unified_msg_origin），拿不到时返回 'default' 以便仍能命中最近一次提交。"""
    try:
        event = _get_event_from_context(context)
        if event is not None:
            umo = getattr(event, "unified_msg_origin", None) or ""
            if umo:
                return umo
            if hasattr(event, "get_session_id"):
                sid = event.get_session_id()
                if sid:
                    return str(sid)
    except Exception:
        pass
    return "default"


def _get_session_id_from_context(context: Any) -> Optional[str]:
    """从工具调用的 context 中解析 session_id，用于 send_message。
    Agent 工具中 context.context 为 AstrAgentContext，其 .event 即当前消息事件。"""
    def _sid_from_event(ev: Any) -> Optional[str]:
        if ev is None:
            return None
        if hasattr(ev, "get_session_id"):
            sid = ev.get_session_id()
            if sid is not None:
                return str(sid)
        if hasattr(ev, "message_obj"):
            mobj = getattr(ev, "message_obj", None)
            if mobj is not None and hasattr(mobj, "session_id"):
                sid = getattr(mobj, "session_id", None)
                if sid is not None:
                    return str(sid)
        return None

    try:
        event = _get_event_from_context(context)
        sid = _sid_from_event(event)
        if sid is not None:
            return sid
    except Exception as e:
        logger.debug("get_session_id_from_context: %s", e)
    return None


def _get_sender_id_from_context(context: Any) -> Optional[str]:
    """从工具调用的 context 中解析发送者的 QQ 号（user_id）。"""
    try:
        event = _get_event_from_context(context)
        if event is None:
            return None
        # 尝试从 event 获取 sender 或 user_id
        if hasattr(event, "get_sender_id"):
            uid = event.get_sender_id()
            if uid is not None:
                return str(uid)
        if hasattr(event, "user_id"):
            uid = getattr(event, "user_id", None)
            if uid is not None:
                return str(uid)
        if hasattr(event, "sender"):
            sender = getattr(event, "sender", None)
            if sender:
                if hasattr(sender, "user_id"):
                    uid = getattr(sender, "user_id", None)
                    if uid is not None:
                        return str(uid)
        if hasattr(event, "message_obj"):
            mobj = getattr(event, "message_obj", None)
            if mobj:
                if hasattr(mobj, "sender"):
                    sender = getattr(mobj, "sender", None)
                    if sender and hasattr(sender, "user_id"):
                        return str(getattr(sender, "user_id", None))
                if hasattr(mobj, "user_id"):
                    uid = getattr(mobj, "user_id", None)
                    if uid is not None:
                        return str(uid)
    except Exception as e:
        logger.debug("get_sender_id_from_context: %s", e)
    return None


def _is_local_image_url(url: str, server_ip: Optional[str] = None) -> bool:
    """判断是否为 QQ 无法访问的本地地址（127.0.0.1 / localhost / 内网）。"""
    if not url or not isinstance(url, str):
        return False
    u = url.strip().lower()
    if "127.0.0.1" in u or "localhost" in u:
        return True
    if server_ip:
        host = _get_comfyui_host(server_ip)
        if host in ("127.0.0.1", "localhost") or host.startswith("192.168.") or host.startswith("10."):
            return True
    return False


async def _download_image_to_temp(image_url: str) -> Optional[str]:
    """
    将 ComfyUI 图片 URL 下载到临时文件。
    QQ 等平台无法访问 127.0.0.1，必须先下载再以本地文件形式发送。
    返回临时文件路径，失败返回 None。调用方负责在发送后删除临时文件。
    """
    if not image_url or not image_url.strip():
        return None
    url = image_url.strip()
    if url.startswith("/api/media/history/"):
        path = _get_comfyui_output_image_dir() / Path(url.rsplit("/", 1)[-1]).name
        return str(path) if path.exists() and path.is_file() else None
    if Path(url).exists() and Path(url).is_file():
        return str(Path(url))
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
    except Exception as e:
        logger.warning("ComfyUI download image failed from %s: %s", url, e)
        return None
    if not data:
        return None
    suffix = ".png"
    if b"JFIF" in data[:32] or b"\xff\xd8" in data[:2]:
        suffix = ".jpg"
    elif b"GIF" in data[:6]:
        suffix = ".gif"
    try:
        tmp_dir = PLUGIN_DATA_DIR / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        path = tmp_dir / f"comfyui_{uuid.uuid4().hex}{suffix}"
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        return str(path)
    except Exception as e:
        logger.warning("ComfyUI write temp image failed: %s", e)
        return None


def _gilbert_curve(width: int, height: int) -> List[tuple[int, int]]:
    result: List[tuple[int, int]] = []
    if width >= height:
        _gilbert_curve_inner(0, 0, width, 0, 0, height, result)
    else:
        _gilbert_curve_inner(0, 0, 0, height, width, 0, result)
    return result


def _gilbert_curve_inner(
    x: int,
    y: int,
    ax: int,
    ay: int,
    bx: int,
    by: int,
    result: List[tuple[int, int]],
) -> None:
    w = abs(ax + ay)
    h = abs(bx + by)
    dax = 0 if ax == 0 else (1 if ax > 0 else -1)
    day = 0 if ay == 0 else (1 if ay > 0 else -1)
    dbx = 0 if bx == 0 else (1 if bx > 0 else -1)
    dby = 0 if by == 0 else (1 if by > 0 else -1)
    if h == 1:
        for _ in range(w):
            result.append((x, y))
            x += dax
            y += day
        return
    if w == 1:
        for _ in range(h):
            result.append((x, y))
            x += dbx
            y += dby
        return
    ax2, ay2 = ax // 2, ay // 2
    bx2, by2 = bx // 2, by // 2
    w2 = abs(ax2 + ay2)
    h2 = abs(bx2 + by2)
    if 2 * w > 3 * h:
        if w2 % 2 and w > 2:
            ax2 += dax
            ay2 += day
        _gilbert_curve_inner(x, y, ax2, ay2, bx, by, result)
        _gilbert_curve_inner(x + ax2, y + ay2, ax - ax2, ay - ay2, bx, by, result)
    else:
        if h2 % 2 and h > 2:
            bx2 += dbx
            by2 += dby
        _gilbert_curve_inner(x, y, bx2, by2, ax2, ay2, result)
        _gilbert_curve_inner(x + bx2, y + by2, ax, ay, bx - bx2, by - by2, result)
        _gilbert_curve_inner(
            x + (ax - dax) + (bx2 - dbx),
            y + (ay - day) + (by2 - dby),
            -bx2,
            -by2,
            -(ax - ax2),
            -(ay - ay2),
            result,
        )


def _obfuscate_image_file(image_path: str) -> Optional[str]:
    if not image_path or not Path(image_path).exists():
        return None
    try:
        from PIL import Image as PILImage

        with open(image_path, "rb") as f:
            img = PILImage.open(io.BytesIO(f.read())).convert("RGB")
        w, h = img.size
        max_pixels = 8_000_000
        max_side = 4000
        if w * h > max_pixels or w > max_side or h > max_side:
            scale = min((max_pixels / (w * h)) ** 0.5, max_side / w, max_side / h, 1)
            w = max(1, round(w * scale))
            h = max(1, round(h * scale))
            img = img.resize((w, h), PILImage.LANCZOS)
        curve = _gilbert_curve(w, h)
        total = w * h
        offset = round(((5**0.5 - 1) / 2) * total)
        src_pixels = list(img.getdata())
        dst_pixels: List[Any] = [None] * total
        for i in range(total):
            sx, sy = curve[i]
            dx, dy = curve[(i + offset) % total]
            src_idx = sy * w + sx
            dst_idx = dy * w + dx
            dst_pixels[dst_idx] = src_pixels[src_idx]
        out_img = PILImage.new("RGB", (w, h))
        out_img.putdata(dst_pixels)
        tmp_dir = PLUGIN_DATA_DIR / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        out_path = tmp_dir / f"comfyui_obfuscated_{uuid.uuid4().hex}.jpg"
        out_img.save(out_path, format="JPEG", quality=95)
        return str(out_path)
    except Exception as e:
        logger.warning("ComfyUI obfuscate image failed: %s", e)
        return None


def _load_send_policy() -> Dict[str, Dict[str, str]]:
    default_policy = default_send_policy()
    try:
        if _task_service and hasattr(_task_service, "get_audit_settings"):
            result = _task_service.get_audit_settings()
            settings = result.get("settings") if isinstance(result, dict) else {}
            default_policy = normalize_send_policy(settings.get("send_policy") if isinstance(settings, dict) else {})
    except Exception as e:
        logger.debug("ComfyUI load send policy failed: %s", e)
    return default_policy


def _resolve_send_method(media_type: str, audit_state: str = "audit_disabled") -> str:
    media_type = media_type if media_type in {"text", "image", "video"} else "image"
    audit_state = audit_state if audit_state in {"audit_disabled", "audit_error", "audit_hit", "audit_pass"} else "audit_disabled"
    return normalize_send_method(_load_send_policy().get(audit_state, {}).get(media_type), media_type)


def _audit_state_from_record(record: Optional[Dict[str, Any]]) -> str:
    if not record:
        return "audit_disabled"
    status = str(record.get("status") or "").strip().lower()
    decision = str(record.get("decision") or "").strip().lower()
    if decision == "block" or status == "block":
        return "audit_hit"
    if status == "pass":
        return "audit_pass"
    if status in {"error", "unknown"}:
        return "audit_error"
    return "audit_disabled"


def _normalize_media_delivery_item(item: Any, media_type: str) -> Dict[str, Any]:
    if isinstance(item, dict):
        url = str(item.get("url") or item.get("image_url") or item.get("video_url") or "")
        audit_state = str(item.get("audit_state") or "audit_disabled")
        method = normalize_send_method(item.get("send_method") or _resolve_send_method(media_type, audit_state), media_type)
        notice = str(item.get("notice") or "")
        return {"url": url, "audit_state": audit_state, "send_method": method, "notice": notice}
    url = str(item or "")
    audit_state = "audit_disabled"
    return {"url": url, "audit_state": audit_state, "send_method": _resolve_send_method(media_type, audit_state), "notice": ""}


def _delivery_items_for_urls(media_type: str, urls: List[str], audit_records: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    records_by_url: Dict[str, Dict[str, Any]] = {}
    for record in audit_records or []:
        image_url = str(record.get("image_url") or "")
        if image_url:
            records_by_url[image_url] = record
    items: List[Dict[str, Any]] = []
    for url in [str(u) for u in urls if u]:
        record = records_by_url.get(url)
        audit_state = str((record or {}).get("audit_state") or _audit_state_from_record(record))
        method = normalize_send_method((record or {}).get("send_method") or _resolve_send_method(media_type, audit_state), media_type)
        notice = ""
        if method == "dont_send":
            notice = "内容已生成，但按发送策略未发送。"
        items.append({"url": url, "audit_state": audit_state, "send_method": method, "notice": notice})
    return items


def _is_queued_send_method(method: str) -> bool:
    return normalize_send_method(method, "image") != "dont_send"


def _filename_from_media_url(url: str) -> str:
    raw = str(url or "")
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        filename = (parse_qs(parsed.query).get("filename") or [""])[0]
        if filename:
            return Path(filename).name
        return Path(parsed.path).name or Path(raw).name
    except Exception:
        return Path(raw).name


def _build_media_delivery_report(media_type: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[tuple[str, str, bool], Dict[str, Any]] = {}
    for item in items:
        method = normalize_send_method(item.get("send_method"), media_type)
        audit_state = str(item.get("audit_state") or "audit_disabled")
        queued = method != "dont_send"
        key = (audit_state, method, queued)
        group = groups.setdefault(
            key,
            {
                "audit_state": audit_state,
                "send_method": method,
                "queued": queued,
                "count": 0,
                "files": [],
            },
        )
        group["count"] += 1
        filename = _filename_from_media_url(str(item.get("url") or ""))
        if filename:
            group["files"].append(filename)
    ordered_groups = sorted(
        groups.values(),
        key=lambda group: (
            {"audit_disabled": 0, "audit_pass": 1, "audit_hit": 2, "audit_error": 3}.get(group["audit_state"], 9),
            group["send_method"],
            not group["queued"],
        ),
    )
    queued_count = sum(1 for item in items if normalize_send_method(item.get("send_method"), media_type) != "dont_send")
    return {
        "generated_count": len(items),
        "local_path": str(_get_comfyui_output_image_dir().resolve()),
        "queued_count": queued_count,
        "skipped_count": max(0, len(items) - queued_count),
        "groups": ordered_groups,
    }


def _build_text_delivery_report(texts: List[str], audit_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    groups: Dict[tuple[str, str, bool], Dict[str, Any]] = {}
    for record in audit_records or []:
        method = normalize_send_method(record.get("send_method"), "text")
        audit_state = str(record.get("audit_state") or "audit_disabled")
        queued = method != "dont_send"
        key = (audit_state, method, queued)
        group = groups.setdefault(
            key,
            {
                "audit_state": audit_state,
                "send_method": method,
                "queued": queued,
                "count": 0,
                "text_indexes": [],
                "previews": [],
            },
        )
        group["count"] += 1
        index = record.get("text_index")
        if index:
            group["text_indexes"].append(index)
        preview = str(record.get("text_preview") or "").strip()
        if preview:
            group["previews"].append(preview)
    ordered_groups = sorted(
        groups.values(),
        key=lambda group: (
            {"audit_disabled": 0, "audit_pass": 1, "audit_hit": 2, "audit_error": 3}.get(group["audit_state"], 9),
            group["send_method"],
            not group["queued"],
        ),
    )
    queued_count = sum(1 for record in audit_records or [] if normalize_send_method(record.get("send_method"), "text") != "dont_send")
    return {
        "generated_count": len([text for text in texts or [] if str(text or "").strip()]),
        "queued_count": queued_count,
        "skipped_count": max(0, len(audit_records or []) - queued_count),
        "groups": ordered_groups,
    }


def _build_text_policy_delivery(texts: List[str]) -> tuple[List[str], Dict[str, Any]]:
    raw_texts = [str(text) for text in (texts or []) if str(text or "").strip()]
    if not raw_texts:
        return [], {}
    method = _resolve_send_method("text", "audit_disabled")
    queued = method != "dont_send"
    records = [
        {
            "audit_state": "audit_disabled",
            "send_method": method,
            "text_index": index,
            "text_preview": text if len(text) <= 160 else text[:160] + "...",
        }
        for index, text in enumerate(raw_texts, 1)
    ]
    return (raw_texts if queued else []), _build_text_delivery_report(raw_texts, records)


async def _audit_texts_for_delivery(prompt_id: str, texts: List[str]) -> tuple[List[str], Dict[str, Any]]:
    raw_texts = [str(text) for text in (texts or []) if str(text or "").strip()]
    if not raw_texts:
        return [], {}
    task_for_audit = None
    if _task_service and hasattr(_task_service, "get_external_task"):
        try:
            task_for_audit = _task_service.get_external_task(prompt_id)
        except Exception as e:
            logger.warning("ComfyUI task center lookup for text audit failed: %s", e)
    if _task_service and task_for_audit and hasattr(_task_service, "audit_task_texts"):
        try:
            audit_result = await _task_service.audit_task_texts(task_for_audit, raw_texts)
            records = audit_result.get("records") if isinstance(audit_result, dict) else []
            if records:
                allowed_texts = [str(text) for text in (audit_result.get("allowed_texts") or []) if str(text or "").strip()]
                return allowed_texts, _build_text_delivery_report(raw_texts, records)
        except Exception as e:
            logger.warning("ComfyUI text audit failed: %s", e)
    return _build_text_policy_delivery(raw_texts)


def _llm_media_delivery_message(media_label: str = "Media") -> str:
    return (
        f"{media_label} is queued for automatic sending by the plugin. "
        "Do NOT call send_message_to_user with image_url/video_url. "
        "Reply with normal text only; the plugin will attach/send the media automatically."
    )


def _llm_image_delivery_report_message(report: Dict[str, Any]) -> str:
    generated_count = int(report.get("generated_count") or 0)
    queued_count = int(report.get("queued_count") or 0)
    if queued_count <= 0:
        return "Image generated, but automatic sending was skipped or blocked."
    return (
        f"Image generation completed: {generated_count} generated, {queued_count} queued for automatic sending. "
        "In media_delivery_report.image.groups, queued=true means the plugin will automatically send that group; "
        "queued=false means it will not be sent. Do NOT call send_message_to_user with image_url/video_url. "
        "Reply with normal text only; the plugin will attach/send the media automatically."
    )


def _llm_media_delivery_message_from_report(
    image_report: Dict[str, Any],
    video_sendable_count: int,
) -> str:
    image_sendable_count = int(image_report.get("queued_count") or 0)
    generated_count = int(image_report.get("generated_count") or 0)
    if image_sendable_count <= 0 and video_sendable_count <= 0:
        return "Media generated, but automatic sending was skipped or blocked."
    return (
        f"Media generation completed: {generated_count} image(s) generated, {image_sendable_count} image(s) queued for automatic sending. "
        "In media_delivery_report.image.groups, queued=true means the plugin will automatically send that group; "
        "queued=false means it will not be sent. Do NOT call send_message_to_user with image_url/video_url. "
        "Reply with normal text only; the plugin will attach/send media automatically."
    )



async def _prepare_image_delivery_for_send(item: Any, session_key: str) -> tuple[Optional[str], Optional[str]]:
    delivery = _normalize_media_delivery_item(item, "image")
    method = delivery.get("send_method") or "direct_send"
    image_url = delivery.get("url") or ""
    if method == "dont_send":
        return None, delivery.get("notice") or "图片已生成，但按发送策略未发送。"
    temp_path = await _download_image_to_temp(image_url) if image_url else None
    if not temp_path or not Path(temp_path).exists():
        return None, "图片已生成，但下载失败，未发送。"
    if method == "obfuscated_send":
        obfuscated_path = _obfuscate_image_file(temp_path)
        if not obfuscated_path or not Path(obfuscated_path).exists():
            return None, "图片混淆失败，未发送。"
        return obfuscated_path, None
    persistent_image_path = await _save_image_to_persistent_path(temp_path, session_key or "")
    return persistent_image_path or temp_path, None


async def _prepare_video_delivery_for_send(item: Any) -> tuple[Optional[str], Optional[str]]:
    delivery = _normalize_media_delivery_item(item, "video")
    method = delivery.get("send_method") or "direct_send"
    video_url = delivery.get("url") or ""
    if method == "dont_send":
        return None, delivery.get("notice") or "视频已生成，但按发送策略未发送。"
    video_temp_path = await _download_media_to_temp(video_url, ".mp4") if video_url else None
    if not video_temp_path or not Path(video_temp_path).exists():
        return None, "视频已生成，但下载失败，未发送。"
    return video_temp_path, None


def _get_comfyui_output_image_dir() -> Path:
    """返回生成结果的本地历史媒体目录，便于 Bot/WebUI 复用输出文件。"""
    p = PLUGIN_DATA_DIR / "media" / "history"
    p.mkdir(parents=True, exist_ok=True)
    return p


async def _save_image_to_persistent_path(temp_path: str, session_key: str) -> Optional[str]:
    """
    将临时图片复制到持久化目录，返回绝对路径。
    保存路径用于插件内部复用与发送，不主动暴露到聊天文本中。
    """
    if not temp_path or not Path(temp_path).exists():
        return None
    try:
        out_dir = _get_comfyui_output_image_dir()
        safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in (session_key or "default")[:32])
        ext = Path(temp_path).suffix or ".png"
        # 使用完整的 UUID 避免同一秒内生成重复文件名
        name = f"comfyui_out_{safe_key}_{uuid.uuid4().hex}{ext}"
        dest = out_dir / name
        async with aiofiles.open(temp_path, "rb") as f:
            data = await f.read()
        async with aiofiles.open(dest, "wb") as f:
            await f.write(data)
        return str(dest.resolve())
    except Exception as e:
        logger.warning("ComfyUI save image to persistent path failed: %s", e)
        return None


def _is_persistent_media_path(file_path: str) -> bool:
    """判断路径是否在持久化输出目录下（此类文件发送后不删除，供 qts 等解析后再次使用）。"""
    try:
        resolved = Path(file_path).resolve()
        for base in _get_allowed_local_image_base_dirs():
            if str(resolved).startswith(str(base) + os.sep) or resolved == base:
                return True
        return False
    except Exception:
        return False


async def _save_video_to_persistent_path(temp_path: str, session_key: str) -> Optional[str]:
    """Copy a temporary video to persistent storage and return its absolute path."""
    if not temp_path or not Path(temp_path).exists():
        return None
    try:
        out_dir = _get_comfyui_output_image_dir()
        safe_key = "".join(c if c.isalnum() or c in "-_" else "_" for c in (session_key or "default")[:32])
        ext = Path(temp_path).suffix or ".mp4"
        # 使用完整的 UUID 避免同一秒内生成重复文件名
        name = f"comfyui_out_{safe_key}_{uuid.uuid4().hex}{ext}"
        dest = out_dir / name
        async with aiofiles.open(temp_path, "rb") as f:
            data = await f.read()
        async with aiofiles.open(dest, "wb") as f:
            await f.write(data)
        return str(dest.resolve())
    except Exception as e:
        logger.warning("ComfyUI save video to persistent path failed: %s", e)
        return None


async def _download_media_to_temp(media_url: str, suffix: str = ".mp4", timeout: float = 120.0) -> Optional[str]:
    """
    将 ComfyUI 视频/音频 URL 下载到临时文件。QQ 无法访问 127.0.0.1，需下载后以本地文件发送。
    返回临时文件路径，失败返回 None。
    """
    if not media_url or not media_url.strip():
        return None
    url = media_url.strip()
    if url.startswith("/api/media/history/"):
        path = _get_comfyui_output_image_dir() / Path(url.rsplit("/", 1)[-1]).name
        return str(path) if path.exists() and path.is_file() else None
    if Path(url).exists() and Path(url).is_file():
        return str(Path(url))
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.content
    except Exception as e:
        logger.warning("ComfyUI download media failed: %s", e)
        return None
    if not data:
        return None
    try:
        tmp_dir = PLUGIN_DATA_DIR / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        path = tmp_dir / f"comfyui_media_{uuid.uuid4().hex}{suffix}"
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)
        return str(path)
    except Exception as e:
        logger.warning("ComfyUI write temp media failed: %s", e)
        return None


async def _send_image_to_session(session_id: str, image_url: str, plain_text: Optional[str] = None) -> bool:
    """
    向指定会话发送图片（可选带一句文本）。
    先将 ComfyUI 图片 URL 下载到临时文件，再用 Image.fromFileSystem + chain 发送，
    参考 astrbot_plugin_bilibili 的混合回复方式；发送后删除临时文件。
    """
    if not session_id or not image_url:
        return False
    ctx = _plugin_context
    if not ctx:
        return False
    temp_path = None
    try:
        method = _resolve_send_method("image", "audit_disabled")
        if method == "dont_send":
            return await _send_plain_to_session(session_id, "图片已生成，但按发送策略未发送。")
        temp_path = await _download_image_to_temp(image_url)
        if not temp_path or not Path(temp_path).exists():
            return False
        if method == "obfuscated_send":
            obfuscated_path = _obfuscate_image_file(temp_path)
            if not obfuscated_path or not Path(obfuscated_path).exists():
                await _send_plain_to_session(session_id, "图片混淆失败，未发送。")
                return False
            temp_path = obfuscated_path
        from astrbot.api.message_components import Image, Plain
        try:
            from astrbot.api.event import MessageEventResult
        except ImportError:
            from astrbot.core.message.message_event_result import MessageEventResult
        chain: List[Any] = []
        if plain_text and plain_text.strip():
            chain.append(Plain(plain_text.strip()))
        try:
            chain.append(Image.fromFileSystem(temp_path))
        except AttributeError:
            chain.append(Image.from_file_system(temp_path))
        if len(chain) == 1:
            result = MessageEventResult().image_result(temp_path)
        else:
            try:
                result = MessageEventResult(chain=chain)
            except TypeError:
                result = MessageEventResult().chain_result(chain)
        await ctx.send_message(session_id, result)
        return True
    except Exception as e:
        logger.warning("ComfyUI send image to session failed: %s", e)
        return False
    finally:
        if temp_path and Path(temp_path).exists():
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass


async def _send_video_to_session(session_id: str, video_path: str) -> bool:
    """
    向指定会话单独发送一条仅包含视频的消息。
    视频不能与文本混在同一条消息中，因此独立发送。
    """
    if not session_id or not video_path or not Path(video_path).exists():
        return False
    ctx = _plugin_context
    if not ctx:
        return False
    try:
        try:
            from astrbot.api.message_components import Video
        except ImportError:
            logger.warning("ComfyUI: Video component not available, skip sending video.")
            return False
        try:
            from astrbot.api.event import MessageEventResult
        except ImportError:
            from astrbot.core.message.message_event_result import MessageEventResult
        try:
            seg = Video.fromFileSystem(video_path)
        except AttributeError:
            seg = Video.from_file_system(video_path)
        try:
            result = MessageEventResult().video_result(video_path)
        except (AttributeError, TypeError):
            try:
                result = MessageEventResult().chain_result([seg])
            except (TypeError, AttributeError):
                result = MessageEventResult(chain=[seg])
        await ctx.send_message(session_id, result)
        return True
    except Exception as e:
        logger.warning("ComfyUI send video to session failed: %s", e)
        return False
    finally:
        # 持久化目录下的文件不删除，供 qts_get_recent_messages 等返回的 content 中路径再次被 image_urls 使用
        if video_path and Path(video_path).exists() and not _is_persistent_media_path(video_path):
            try:
                Path(video_path).unlink(missing_ok=True)
            except Exception:
                pass


async def _send_plain_to_session(session_id: str, text: str) -> bool:
    if not session_id or not str(text or "").strip():
        return False
    ctx = _plugin_context
    if not ctx:
        return False
    try:
        from astrbot.api.message_components import Plain
        try:
            from astrbot.api.event import MessageEventResult
        except ImportError:
            from astrbot.core.message.message_event_result import MessageEventResult
        chain = [Plain(str(text).strip())]
        try:
            result = MessageEventResult(chain=chain)
        except TypeError:
            result = MessageEventResult().chain_result(chain)
        await ctx.send_message(session_id, result)
        return True
    except Exception as e:
        logger.warning("ComfyUI send plain text to session failed: %s", e)
        return False


async def _get_queue_status(server_ip: str) -> tuple:
    """返回 (running_count, pending_count)，失败返回 (-1, -1)。"""
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{base}/queue")
            data = r.json()
            running = len(data.get("queue_running", []))
            pending = len(data.get("queue_pending", []))
            return running, pending
    except Exception as e:
        logger.warning("get ComfyUI queue failed: %s", e)
        return -1, -1


async def _get_first_task_from_queue(server_ip: str) -> Optional[tuple]:
    """
    从 ComfyUI 队列取第一个任务（running 优先，否则 pending）。
    返回 (prompt_id, client_id) 或 None。队列项格式通常为 [client_id, prompt_id] 或 [num, prompt_id]。
    仅当队列中恰好有任务时返回，用于「本会话无 pending 但用户回来查进度」时恢复会话。
    """
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{base}/queue")
            data = r.json()
            running = data.get("queue_running", [])
            pending = data.get("queue_pending", [])
            for item in running + pending:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    prompt_id = item[1]
                    client_id = item[0] if item[0] else "astrbot-comfyui-bubble-1"
                    if prompt_id:
                        return (str(prompt_id), str(client_id))
    except Exception as e:
        logger.debug("get first task from queue failed: %s", e)
    return None


async def _estimate_remaining_seconds(server_ip: str, prompt_id: str) -> int:
    """
    估算当前任务（prompt_id）完成还需多少秒。
    若已不在队列中则返回 0；否则用 (running+pending) * ESTIMATE_SECONDS_PER_JOB 粗估。
    """
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{base}/queue")
            data = r.json()
            running = data.get("queue_running", [])
            pending = data.get("queue_pending", [])
            for item in running:
                if isinstance(item, (list, tuple)) and len(item) >= 2 and item[1] == prompt_id:
                    return 1
            for idx, item in enumerate(pending):
                if isinstance(item, (list, tuple)) and len(item) >= 2 and item[1] == prompt_id:
                    # 还在队列中：粗略估计剩余时间
                    return (idx + 1) * ESTIMATE_SECONDS_PER_JOB
    except Exception:
        pass
    return 0


def _build_output_rules(info: Dict[str, Any]) -> Dict[str, Any]:
    params = (info or {}).get("params") if isinstance(info, dict) else {}
    if not isinstance(params, dict):
        params = {}
    slots = [
        slot
        for slot in (params.get("slots") or [])
        if isinstance(slot, dict) and slot.get("direction") == "output"
        and not slot.get("hidden")
    ]
    return {
        "text": params.get("outputs", {}).get("text", {}) if isinstance(params.get("outputs"), dict) else {},
        "image": params.get("outputs", {}).get("image", {}) if isinstance(params.get("outputs"), dict) else {},
        "video": params.get("outputs", {}).get("video", {}) if isinstance(params.get("outputs"), dict) else {},
        "slots": slots,
        "allow_other_outputs": bool(params.get("allow_other_outputs", False)),
    }


def _provided_input_count(values: List[Any]) -> int:
    return sum(1 for item in (values or []) if str(item or "").strip())


def _available_workflow_file_by_name(workflows: List[Dict[str, Any]], workflow_name: str, wf_dir: Path) -> Optional[str]:
    workflow = next(
        (
            w
            for w in workflows
            if w.get("name") == workflow_name and _workflow_is_available(w, workflows)
        ),
        None,
    )
    filename = str((workflow or {}).get("filename") or "").strip()
    return str(wf_dir / filename) if filename else None


def _normalize_output_rules_arg(output_rules: Any) -> Dict[str, Any]:
    if isinstance(output_rules, int):
        return {
            "text": {"limit": output_rules, "mode": "loose"},
            "image": {"limit": None, "mode": "loose"},
            "video": {"limit": None, "mode": "loose"},
            "slots": [],
            "allow_other_outputs": False,
        }
    if not isinstance(output_rules, dict):
        output_rules = {}
    return {
        "text": output_rules.get("text", {}) if isinstance(output_rules.get("text"), dict) else {},
        "image": output_rules.get("image", {}) if isinstance(output_rules.get("image"), dict) else {},
        "video": output_rules.get("video", {}) if isinstance(output_rules.get("video"), dict) else {},
        "slots": output_rules.get("slots", []) if isinstance(output_rules.get("slots"), list) else [],
        "allow_other_outputs": bool(output_rules.get("allow_other_outputs", False)),
    }


def _apply_output_rule(values: List[Any], rule: Dict[str, Any], label: str) -> tuple[bool, List[Any], str]:
    limit = rule.get("limit") if isinstance(rule, dict) else None
    mode = rule.get("mode") if isinstance(rule, dict) else "loose"
    if limit is None:
        return True, values, ""
    limit = max(0, int(limit))
    count = len(values)
    if mode == "strict" and count < limit:
        return False, values, f"{label}至少需要输出 {limit} 个，实际输出 {count} 个。"
    return True, values[:limit], ""


def _output_slot_label(slot: Dict[str, Any]) -> str:
    kind_label = {"text": "文本", "image": "图片", "video": "视频"}.get(str(slot.get("kind") or ""), "输出")
    index = int(slot.get("index") or 0)
    title = str(slot.get("title") or "").strip()
    explain = str(slot.get("explain") or "").strip()
    parts = [f"[{kind_label}{index}]"]
    if title:
        parts.append(title)
    if explain:
        parts.append(explain)
    return " ".join(parts).strip()


def _extract_node_text_outputs(out: Any, max_texts: Optional[int] = None) -> List[str]:
    texts: List[str] = []
    if not isinstance(out, dict) or "text" not in out:
        return texts
    text_value = out.get("text")
    if isinstance(text_value, str):
        candidates = [text_value]
    elif isinstance(text_value, list):
        candidates = text_value
    else:
        return texts
    for item in candidates:
        text = str(item or "").strip()
        if text:
            texts.append(text)
            if max_texts is not None and len(texts) >= max_texts:
                return texts
    return texts


def _extract_node_media_outputs(base: str, out: Any, key: str) -> List[str]:
    values: List[str] = []
    if not isinstance(out, dict) or key not in out:
        return values
    for item in out.get(key) or []:
        if not isinstance(item, dict) or item.get("type") != "output":
            continue
        fn, sub = item.get("filename"), item.get("subfolder", "")
        if not fn:
            continue
        url = f"{base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{base}/view?filename={fn}&type=output"
        values.append(url)
    return values


def _append_unique(target: List[str], values: List[str]) -> None:
    seen = set(target)
    for value in values:
        if value and value not in seen:
            target.append(value)
            seen.add(value)


def _extract_other_history_outputs(base: str, outputs: Any, excluded_node_ids: set[str]) -> tuple[List[str], List[str], List[str], List[str]]:
    texts: List[str] = []
    images: List[str] = []
    videos: List[str] = []
    audios: List[str] = []
    if not isinstance(outputs, dict):
        return texts, images, videos, audios
    for node_id, out in outputs.items():
        if str(node_id) in excluded_node_ids:
            continue
        _append_unique(texts, _extract_node_text_outputs(out))
        _append_unique(images, _extract_node_media_outputs(base, out, "images"))
        _append_unique(videos, _extract_node_media_outputs(base, out, "gifs"))
        _append_unique(audios, _extract_node_media_outputs(base, out, "audio"))
    return texts, images, videos, audios


def _extract_slot_outputs(base: str, outputs: Any, slots: List[Dict[str, Any]]) -> tuple[bool, List[str], List[str], List[str], List[str]]:
    texts: List[str] = []
    images: List[str] = []
    videos: List[str] = []
    messages: List[str] = []
    if not isinstance(outputs, dict):
        return False, texts, images, videos, ["ComfyUI history 中没有可读取的输出。"]
    ordered_slots = sorted(
        [slot for slot in slots if isinstance(slot, dict) and slot.get("direction") == "output"],
        key=lambda slot: ({"text": 0, "image": 1, "video": 2}.get(str(slot.get("kind") or ""), 9), int(slot.get("index") or 0)),
    )
    for slot in ordered_slots:
        if slot.get("enabled", True) is False:
            continue
        out = outputs.get(str(slot.get("node_id") or ""))
        kind = str(slot.get("kind") or "")
        if kind == "text":
            values = _extract_node_text_outputs(out)
            texts.extend(values)
        elif kind == "image":
            values = _extract_node_media_outputs(base, out, "images")
            images.extend(values)
        elif kind == "video":
            values = _extract_node_media_outputs(base, out, "gifs")
            videos.extend(values)
        else:
            values = []
        if not values and not bool(slot.get("optional", False)):
            messages.append(f"缺少必填输出：{_output_slot_label(slot)}")
    return not messages, texts, images, videos, messages


async def _get_result_for_prompt(server_ip: str, prompt_id: str, output_rules: Any = None) -> tuple:
    """任务已完成时，从 history 拉取结果。返回 (media_outputs, file_type, text_outputs)。"""
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            hist = await client.get(f"{base}/history/{prompt_id}")
            info = hist.json()
    except Exception:
        return None, "unknown", []
    if prompt_id not in info or "outputs" not in info[prompt_id]:
        return None, "unknown", []
    outputs = info[prompt_id]["outputs"]
    rules = _normalize_output_rules_arg(output_rules)
    slot_rules = [
        slot
        for slot in (rules.get("slots") or [])
        if isinstance(slot, dict) and slot.get("direction") == "output"
    ]
    if slot_rules:
        ok_slots, texts, images, videos, messages = _extract_slot_outputs(base, outputs, slot_rules)
        audios: List[str] = []
        if rules.get("allow_other_outputs", False):
            excluded_node_ids = {str(slot.get("node_id") or "") for slot in slot_rules if isinstance(slot, dict)}
            other_texts, other_images, other_videos, other_audios = _extract_other_history_outputs(
                base, outputs, excluded_node_ids
            )
            _append_unique(texts, other_texts)
            _append_unique(images, other_images)
            _append_unique(videos, other_videos)
            _append_unique(audios, other_audios)
        if not ok_slots:
            return None, "error", messages
        media = {"images": images, "videos": videos, "audio": audios}
        media_count = len(images) + len(videos) + len(audios)
        if media_count == 0:
            return None, "text" if texts else "unknown", texts
        if len(images) and not videos and not audios:
            return media, "image", texts
        if len(videos) and not images and not audios:
            return media, "video", texts
        if len(audios) and not images and not videos:
            return media, "audio", texts
        return media, "mixed", texts

    texts = _extract_history_text_outputs(outputs, None)
    images: List[str] = []
    videos: List[str] = []
    audios: List[str] = []
    for key in outputs:
        out = outputs[key]
        if isinstance(out, dict) and "audio" in out:
            for audio in out["audio"]:
                if audio.get("type") == "output":
                    fn, sub = audio["filename"], audio.get("subfolder", "")
                    url = f"{base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{base}/view?filename={fn}&type=output"
                    audios.append(url)
        if isinstance(out, dict) and "gifs" in out:
            for video in out["gifs"]:
                if video.get("type") == "output":
                    fn, sub = video["filename"], video.get("subfolder", "")
                    url = f"{base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{base}/view?filename={fn}&type=output"
                    videos.append(url)
        if isinstance(out, dict) and "images" in out:
            for img in out["images"]:
                if img.get("type") == "output":
                    fn, sub = img["filename"], img.get("subfolder", "")
                    url = f"{base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{base}/view?filename={fn}&type=output"
                    images.append(url)
    ok_texts, texts, msg_texts = _apply_output_rule(texts, rules.get("text", {}), "文本")
    ok_images, images, msg_images = _apply_output_rule(images, rules.get("image", {}), "图片")
    ok_videos, videos, msg_videos = _apply_output_rule(videos, rules.get("video", {}), "视频")
    messages = [m for m in (msg_texts, msg_images, msg_videos) if m]
    if not (ok_texts and ok_images and ok_videos):
        return None, "error", messages
    media = {"images": images, "videos": videos, "audio": audios}
    media_count = len(images) + len(videos) + len(audios)
    if media_count == 0:
        return None, "text" if texts else "unknown", texts
    if len(images) and not videos and not audios:
        return media, "image", texts
    if len(videos) and not images and not audios:
        return media, "video", texts
    if len(audios) and not images and not videos:
        return media, "audio", texts
    return media, "mixed", texts


def _extract_history_text_outputs(outputs: Any, max_texts: Optional[int] = None) -> List[str]:
    texts: List[str] = []
    if not isinstance(outputs, dict):
        return texts
    for out in outputs.values():
        if not isinstance(out, dict) or "text" not in out:
            continue
        text_value = out.get("text")
        if isinstance(text_value, str):
            candidates = [text_value]
        elif isinstance(text_value, list):
            candidates = text_value
        else:
            continue
        for item in candidates:
            text = str(item or "").strip()
            if text:
                texts.append(text)
                if max_texts is not None and len(texts) >= max_texts:
                    return texts
    return texts


async def _get_prompt_history_state(server_ip: str, prompt_id: str) -> Dict[str, Any]:
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            hist = await client.get(f"{base}/history/{prompt_id}")
            info = hist.json()
    except Exception:
        return {"exists": False}
    entry = info.get(prompt_id) if isinstance(info, dict) else None
    if not isinstance(entry, dict):
        return {"exists": False}
    status = entry.get("status") if isinstance(entry.get("status"), dict) else {}
    messages = status.get("messages") if isinstance(status, dict) else []
    message_text = ""
    if isinstance(messages, list):
        message_text = "; ".join(str(item) for item in messages[-3:])
    return {
        "exists": True,
        "completed": bool(status.get("completed")) if isinstance(status, dict) else False,
        "status_str": str(status.get("status_str") or "") if isinstance(status, dict) else "",
        "message": message_text,
        "has_outputs": "outputs" in entry,
    }


def _get_comfyui_ws_url(server_ip: str, client_id: str) -> str:
    raw = (server_ip or "").strip().lstrip("/")
    secure = raw.startswith("https://")
    raw = raw.replace("http://", "", 1).replace("https://", "", 1).rstrip("/")
    scheme = "wss" if secure else "ws"
    return f"{scheme}://{raw}/ws?clientId={client_id}"


def _extract_ws_prompt_id(data: dict) -> Optional[str]:
    prompt_id = data.get("prompt_id")
    if prompt_id:
        return str(prompt_id)
    prompt = data.get("prompt")
    if isinstance(prompt, (list, tuple)) and len(prompt) >= 2:
        return str(prompt[1])
    return None


def _format_comfyui_ws_error(data: dict) -> str:
    parts = []
    for key in ("exception_type", "exception_message", "node_id", "node_type"):
        value = data.get(key)
        if value:
            parts.append(f"{key}: {value}")
    return "; ".join(parts) if parts else "ComfyUI execution_error"


def _extract_comfyui_history_elapsed_seconds(entry: Dict[str, Any]) -> Optional[float]:
    status = entry.get("status") if isinstance(entry.get("status"), dict) else {}
    messages = status.get("messages") if isinstance(status, dict) else []
    if not isinstance(messages, list):
        return None
    start_ts = None
    end_ts = None
    for message in messages:
        if not isinstance(message, (list, tuple)) or len(message) < 2:
            continue
        event_type, data = message[0], message[1]
        if not isinstance(data, dict):
            continue
        timestamp = data.get("timestamp") or data.get("time")
        if not isinstance(timestamp, (int, float)):
            continue
        if event_type == "execution_start":
            start_ts = float(timestamp)
        elif event_type in ("execution_success", "execution_cached"):
            end_ts = float(timestamp)
    if start_ts is None or end_ts is None or end_ts < start_ts:
        return None
    elapsed = end_ts - start_ts
    # ComfyUI history timestamps are usually milliseconds; epoch seconds are much smaller.
    if start_ts > 1_000_000_000_000 or elapsed > 3600:
        elapsed = elapsed / 1000.0
    return elapsed


async def _get_prompt_elapsed_seconds(server_ip: str, prompt_id: str) -> Optional[float]:
    base = _get_comfyui_http_base(server_ip)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            hist = await client.get(f"{base}/history/{prompt_id}")
            info = hist.json()
    except Exception:
        return None
    entry = info.get(prompt_id) if isinstance(info, dict) else None
    if not isinstance(entry, dict):
        return None
    return _extract_comfyui_history_elapsed_seconds(entry)


async def _wait_for_comfyui_ws_completion(
    server_ip: str, client_id: str, prompt_id: str, timeout: int
) -> Dict[str, str]:
    results = await _wait_for_comfyui_ws_completion_many(server_ip, client_id, [prompt_id], timeout)
    return results.get(prompt_id, {"status": "timeout", "message": f"wait timed out after {timeout} seconds"})


async def _wait_for_comfyui_ws_completion_many(
    server_ip: str, client_id: str, prompt_ids: List[str], timeout: int
) -> Dict[str, Dict[str, str]]:
    pending_prompt_ids = {str(prompt_id) for prompt_id in prompt_ids if prompt_id}
    results: Dict[str, Dict[str, str]] = {}
    ws_url = _get_comfyui_ws_url(server_ip, client_id)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                deadline = asyncio.get_event_loop().time() + timeout
                while pending_prompt_ids:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            continue
                        msg_type = payload.get("type")
                        data = payload.get("data") or {}
                        if not isinstance(data, dict):
                            continue
                        event_prompt_id = _extract_ws_prompt_id(data)
                        if event_prompt_id not in pending_prompt_ids:
                            continue
                        if msg_type == "executing" and data.get("node") is None:
                            results[event_prompt_id] = {"status": "completed", "message": "completed"}
                            pending_prompt_ids.remove(event_prompt_id)
                        if msg_type == "execution_error":
                            results[event_prompt_id] = {"status": "error", "message": _format_comfyui_ws_error(data)}
                            pending_prompt_ids.remove(event_prompt_id)
                        if msg_type == "execution_interrupted":
                            results[event_prompt_id] = {"status": "interrupted", "message": "ComfyUI execution interrupted"}
                            pending_prompt_ids.remove(event_prompt_id)
                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        for prompt_id in pending_prompt_ids:
                            results[prompt_id] = {"status": "ws_unavailable", "message": COMFYUI_WS_UNAVAILABLE_MESSAGE}
                        pending_prompt_ids.clear()
                for prompt_id in pending_prompt_ids:
                    results[prompt_id] = {"status": "timeout", "message": f"wait timed out after {timeout} seconds"}
                return results
    except Exception as e:
        logger.warning("ComfyUI WebSocket unavailable: %s", e)
        return {
            prompt_id: {"status": "ws_unavailable", "message": COMFYUI_WS_UNAVAILABLE_MESSAGE}
            for prompt_id in pending_prompt_ids
        }


def _cleanup_completed_task(prompt_id: str, session_tag: str = "") -> None:
    for k in list(_session_pending.keys()):
        if _session_pending.get(k) and _session_pending.get(k).get("prompt_id") == prompt_id:
            _session_pending.pop(k, None)
    _task_registry.pop(prompt_id, None)
    if session_tag and session_tag in _session_tag_tasks:
        if prompt_id in _session_tag_tasks[session_tag]:
            _session_tag_tasks[session_tag].remove(prompt_id)


async def _append_completed_task_result(
    results: list,
    context: Any,
    prompt_id: str,
    task_server_ip: str,
    task_session_key: str,
    url: Any,
    ftype: str,
    texts: List[str],
) -> None:
    if ftype == "error":
        if _task_service:
            try:
                await _task_service.complete_external_task(
                    prompt_id,
                    task_server_ip,
                    url,
                    ftype,
                    texts,
                    "\n".join(texts) if texts else "ComfyUI 输出错误。",
                )
            except Exception as e:
                logger.warning("ComfyUI task center complete failed: %s", e)
        results.append(
            {
                "task_id": prompt_id,
                "status": "error",
                "type": "error",
                "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。",
            }
        )
        return
    texts, text_delivery_report = await _audit_texts_for_delivery(prompt_id, texts)
    completed_task: Optional[Dict[str, Any]] = None
    if _task_service:
        try:
            completed_task = await _task_service.complete_external_task(prompt_id, task_server_ip, url, ftype, texts)
        except Exception as e:
            logger.warning("ComfyUI task center complete failed: %s", e)
    if isinstance(url, dict):
        images = [str(u) for u in (url.get("images") or []) if u]
        videos = [str(u) for u in (url.get("videos") or []) if u]
        audios = [str(u) for u in (url.get("audio") or []) if u]
        original_images = list(images)
        delivery_images = list(original_images)
        audit_result = {"allowed_images": images, "blocked": [], "records": []}
        if _task_service and images:
            try:
                task_images = []
                if completed_task and isinstance(completed_task.get("result"), dict):
                    task_images = [str(u) for u in (completed_task["result"].get("images") or []) if u]
                delivery_images = task_images or delivery_images
                audit_result = await _task_service.audit_task_images(completed_task, task_images or images)
            except Exception as e:
                logger.warning("ComfyUI content audit failed: %s", e)
        image_delivery_items = _delivery_items_for_urls("image", delivery_images, audit_result.get("records") or [])
        image_report = _build_media_delivery_report("image", image_delivery_items)
        sendable_image_count = int(image_report.get("queued_count") or 0)
        if image_delivery_items:
            _session_image_url_queue.setdefault(task_session_key, []).extend(image_delivery_items)
        video_delivery_items = _delivery_items_for_urls("video", videos)
        sendable_video_count = sum(1 for item in video_delivery_items if item.get("send_method") != "dont_send")
        if videos:
            _session_video_url_queue.setdefault(task_session_key, []).extend(video_delivery_items)
        blocked_count = len(audit_result.get("blocked") or [])
        skipped_count = int(image_report.get("skipped_count") or 0)
        skipped_video_count = sum(1 for item in video_delivery_items if item.get("send_method") == "dont_send")
        any_sendable = bool(sendable_image_count or sendable_video_count)
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": ftype,
                "media_ready": bool(image_delivery_items or videos or audios),
                "auto_send": any_sendable,
                "image_count": len(image_delivery_items),
                "sent_image_count": sendable_image_count,
                "skipped_image_count": skipped_count,
                "blocked_image_count": blocked_count,
                "video_count": len(videos),
                "sent_video_count": sendable_video_count,
                "skipped_video_count": skipped_video_count,
                "audio_count": len(audios),
                "media_delivery_report": {"image": image_report} if image_delivery_items else {},
                "text_delivery_report": text_delivery_report,
                "texts": texts,
                "description": "\n\n".join(texts).strip(),
                "delivery": "partial_audit_block" if blocked_count and any_sendable else ("blocked_by_audit" if blocked_count else ("skipped_by_send_policy" if (skipped_count + skipped_video_count) and not any_sendable else ("queued_by_plugin" if any_sendable else ""))),
                "reason": "内容审核拦截，未自动发送。" if blocked_count and not any_sendable else ("按发送策略跳过，未自动发送。" if (skipped_count + skipped_video_count) and not any_sendable else ""),
                "message": _llm_media_delivery_message_from_report(image_report, sendable_video_count),
            }
        )
        return
    if not url:
        filtered_texts = texts
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "text" if filtered_texts else "unknown",
                "texts": filtered_texts,
                "text_delivery_report": text_delivery_report,
                "message": "\n\n".join(filtered_texts) if filtered_texts else "no output file",
            }
        )
        return
    extra = "\n\n".join(texts).strip()
    if ftype == "image":
        audit_result = {"allowed_images": [url] if url else [], "blocked": [], "records": []}
        delivery_images = [str(url)] if url else []
        if _task_service and url:
            try:
                task_images = []
                if completed_task and isinstance(completed_task.get("result"), dict):
                    task_images = [str(u) for u in (completed_task["result"].get("images") or []) if u]
                delivery_images = task_images or delivery_images
                audit_result = await _task_service.audit_task_images(completed_task, task_images or [url])
            except Exception as e:
                logger.warning("ComfyUI content audit failed: %s", e)
        image_delivery_items = _delivery_items_for_urls("image", delivery_images, audit_result.get("records") or [])
        image_report = _build_media_delivery_report("image", image_delivery_items)
        sendable_image_count = int(image_report.get("queued_count") or 0)
        if image_delivery_items:
            _session_image_url_queue.setdefault(task_session_key, []).extend(image_delivery_items)
        blocked_count = len(audit_result.get("blocked") or [])
        skipped_count = int(image_report.get("skipped_count") or 0)
        filtered_texts = texts
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "image",
                "media_ready": bool(image_delivery_items),
                "auto_send": bool(sendable_image_count),
                "image_count": len(image_delivery_items),
                "sent_image_count": sendable_image_count,
                "skipped_image_count": skipped_count,
                "blocked_image_count": blocked_count,
                "media_delivery_report": {"image": image_report},
                "text_delivery_report": text_delivery_report,
                "delivery": "blocked_by_audit" if blocked_count and not sendable_image_count else ("partial_audit_block" if blocked_count else ("skipped_by_send_policy" if skipped_count and not sendable_image_count else ("queued_by_plugin" if sendable_image_count else ""))),
                "reason": "内容审核拦截，未自动发送。" if blocked_count and not sendable_image_count else ("按发送策略跳过，未自动发送。" if skipped_count and not sendable_image_count else ""),
                "message": _llm_image_delivery_report_message(image_report),
                "texts": filtered_texts,
                "description": "\n\n".join(filtered_texts).strip(),
            }
        )
    elif ftype == "video":
        video_delivery_items = _delivery_items_for_urls("video", [str(url)] if url else [])
        sendable_video_count = sum(1 for item in video_delivery_items if item.get("send_method") != "dont_send")
        if video_delivery_items:
            _session_video_url_queue.setdefault(task_session_key, []).extend(video_delivery_items)
        filtered_texts = texts
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "video",
                "media_ready": bool(url),
                "auto_send": bool(sendable_video_count),
                "video_count": len(video_delivery_items),
                "sent_video_count": sendable_video_count,
                "skipped_video_count": sum(1 for item in video_delivery_items if item.get("send_method") == "dont_send"),
                "delivery": "queued_by_plugin" if sendable_video_count else "skipped_by_send_policy",
                "reason": "" if sendable_video_count else "按发送策略跳过，未自动发送。",
                "message": _llm_media_delivery_message("Video") if sendable_video_count else "Video generated, but automatic sending was skipped by send policy.",
                "texts": filtered_texts,
                "text_delivery_report": text_delivery_report,
                "description": "\n\n".join(filtered_texts).strip(),
            }
        )
    else:
        filtered_texts = texts
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": ftype,
                "url": url,
                "texts": filtered_texts,
                "text_delivery_report": text_delivery_report,
                "description": "\n\n".join(filtered_texts).strip(),
            }
        )


async def _submit_comfyui_workflow(
    context: Any,
    workflow_name: str,
    texts: List[str],
    videos: List[str],
    image_urls_arg: List[str],
    session_tag: str,
    event: Optional[Any] = None,
    origin: str = "command",
) -> Dict[str, Any]:
    workflow_name = (workflow_name or "").strip()
    texts = [str(t) for t in (texts or [])]
    videos = [str(v).strip() for v in (videos or [])]
    image_urls_arg = [str(u).strip() for u in (image_urls_arg or [])]
    explicit_image_urls = bool(image_urls_arg)
    session_tag = (session_tag or "").strip()
    if not workflow_name:
        return {"ok": False, "message": "缺少工作流名称。"}
    if not session_tag:
        return {"ok": False, "message": "无法识别发送者标识，无法登记 ComfyUI 任务。"}
    config = _plugin_config
    if not config:
        return {"ok": False, "message": "插件配置不可用。"}

    active_port = _get_active_comfyui_port(config)
    server_ip, client_id = _get_server_config(config)
    wf_dir = _get_workflow_dir()
    workflow_params = _load_workflow_params()
    all_workflows = list_workflows_in_dir(wf_dir, workflow_params)
    workflows = _filter_workflows_for_port(all_workflows, active_port)
    unavailable_matches = [w for w in workflows if w.get("name") == workflow_name and not _workflow_is_available(w, workflows)]
    if unavailable_matches:
        return {
            "ok": False,
            "message": f"工作流「{workflow_name}」当前不可用：{_workflow_availability_error(unavailable_matches[0], workflows)} 请在管理页修复后保存。",
        }
    if any(w["name"] == workflow_name for w in all_workflows) and not any(w["name"] == workflow_name for w in workflows):
        available = sorted({w["name"] for w in workflows})
        available_text = "、".join(available) if available else "无"
        return {
            "ok": False,
            "message": (
                f"当前 ComfyUI 接口「{active_port['name']}」不允许使用工作流「{workflow_name}」。\n"
                f"当前接口可用工作流：{available_text}\n"
                "可以使用 /comfyui_port <接口名称> 切换到其他接口，或在 Management page 中调整该接口的可用工作流。"
            ),
        }
    images_b64 = [] if explicit_image_urls else (await _extract_images_from_event_async(event) if event else [])
    if explicit_image_urls:
        from_sources = await _image_sources_to_base64(image_urls_arg)
        images_b64.extend(from_sources)
        if from_sources:
            logger.info("[ComfyUI Tool] Injected %d image(s) from image_urls placeholder (URL or local path).", len(from_sources))

    workflow_file = find_workflow_file(
        workflow_name, len(texts), len(images_b64), len(videos), wf_dir, workflow_params
    )
    if not workflow_file:
        workflow_file = _available_workflow_file_by_name(workflows, workflow_name, wf_dir)
    if not workflow_file:
        matching_workflow = next((w for w in workflows if w["name"] == workflow_name), None)
        if matching_workflow:
            return {
                "ok": False,
                "message": _workflow_input_mismatch_message(
                    workflow_name,
                    [matching_workflow],
                    texts,
                    images_b64,
                    videos,
                ),
            }
        hint = ""
        if len(images_b64) == 0:
            hint = (
                " 当前消息没有图片（图片0）。可以提供图片附件、HTTP 图片链接，"
                "或插件数据目录/data/agent/comfyui/input 下的本地路径。"
            )
        return {
            "ok": False,
            "message": (
                f"没有找到匹配的工作流「{workflow_name}」（当前提供：文本{_provided_input_count(texts)}，图片{_provided_input_count(images_b64)}，视频{_provided_input_count(videos)}）。"
                "请使用 /comfyui list 或 comfyui_list_workflows 查看可用工作流说明。"
                + hint
            ),
        }

    info = _get_configured_workflow_info(wf_dir, Path(workflow_file).name, workflow_params)
    if not info:
        return {"ok": False, "message": "工作流配置不可用，无法解析输入输出参数。请在工作流管理页保存该工作流的参数配置。"}

    wf_filename = Path(workflow_file).name
    descriptions = await _load_workflow_descriptions(config)
    workflow_desc_data = descriptions.get(wf_filename)
    if isinstance(workflow_desc_data, dict):
        workflow_desc = workflow_desc_data.get("detailed", "") or workflow_desc_data.get("short", "")
    else:
        workflow_desc = str(workflow_desc_data) if workflow_desc_data else ""
    desc_reminder = ""
    if workflow_desc:
        desc_reminder = (
            f"\n\n[工作流「{workflow_name}」说明：{workflow_desc}]"
        )

    ok_inputs, texts, images_b64, videos, input_error = _apply_workflow_input_rules(info, texts, images_b64, videos)
    if not ok_inputs:
        return {
            "ok": False,
            "message": (
                f"工作流「{workflow_name}」参数数量不匹配。"
                f"当前提供：文本{_provided_input_count(texts)}，图片{_provided_input_count(images_b64)}，视频{_provided_input_count(videos)}。"
                + (" " + input_error if input_error else "")
                + desc_reminder
            ),
        }

    try:
        debug = bool(getattr(config, "debug_mode", False) if not isinstance(config, dict) else config.get("debug_mode", False))
        workflow = ComfyUIWorkflow(server_ip, client_id)
        workflow.load_workflow_api(workflow_file)
        prompt_id = await workflow.submit_only(images_b64, texts, videos, debug=debug)
        session_key = _get_session_key(context)
        output_rules = _build_output_rules(info)
        pending_data = {
            "prompt_id": prompt_id,
            "server_ip": server_ip,
            "client_id": client_id,
            "session_key": session_key,
            "session_tag": session_tag,
            "output_rules": output_rules,
            "workflow_name": workflow_name,
            "workflow_file": wf_filename,
        }
        _session_pending[session_key] = pending_data
        if session_key != "default":
            _session_pending["default"] = pending_data
        _task_registry[prompt_id] = pending_data
        if session_tag not in _session_tag_tasks:
            _session_tag_tasks[session_tag] = []
        if prompt_id not in _session_tag_tasks[session_tag]:
            _session_tag_tasks[session_tag].append(prompt_id)
        if _task_service:
            try:
                _task_service.remember_external_task(
                    origin,
                    {**pending_data, "workflow_file": wf_filename},
                    workflow_name,
                    texts=texts,
                    images=images_b64,
                    videos=videos,
                    session_tag=session_tag,
                )
            except Exception as e:
                logger.warning("ComfyUI task center register failed: %s", e)
        return {
            "ok": True,
            "prompt_id": prompt_id,
            "workflow_name": workflow_name,
            "workflow_file": wf_filename,
            "server_ip": server_ip,
            "client_id": client_id,
            "session_key": session_key,
            "session_tag": session_tag,
            "output_rules": output_rules,
            "desc_reminder": desc_reminder,
            "all_task_ids": list(_session_tag_tasks.get(session_tag, [])),
        }
    except httpx.HTTPStatusError as e:
        body = ""
        try:
            if e.response is not None:
                body = e.response.text
        except Exception:
            pass
        summary = _parse_comfyui_400_summary(body)
        msg = (
            f"执行失败：ComfyUI 返回 {e.response.status_code if e.response else '?'}。"
            + (summary if summary else (f"服务端信息：{body[:1500]}" if body else str(e)))
        )
        logger.exception("comfyui_execute failed: %s", msg)
        return {"ok": False, "message": msg + (" 建议修复工作流，或换用当前 ComfyUI 服务器可运行的工作流。" if summary else " 可能原因：工作流节点/输入不匹配、图片格式无效，或服务器错误。") + desc_reminder}
    except Exception as e:
        logger.exception("comfyui_execute failed")
        return {
            "ok": False,
            "message": (
                f"执行失败：{e}。"
                "可能原因：ComfyUI 服务器不可达或超时、工作流节点错误、输入无效。"
                "请检查服务器地址和工作流 JSON 是否有效。"
                + desc_reminder
            ),
        }


def _get_client_id(config: Any) -> str:
    return str(
        _config_get(config, "client_id", "astrbot-comfyui-bubble-1")
        or "astrbot-comfyui-bubble-1"
    ).strip()


async def _submit_comfyui_workflow_to_port(
    port: Dict[str, Any],
    workflow_name: str,
    texts: List[str],
    images_b64: List[str],
    videos: List[str],
) -> Dict[str, Any]:
    workflow_name = (workflow_name or "").strip()
    texts = [str(t) for t in (texts or [])]
    images_b64 = [str(img) for img in (images_b64 or [])]
    videos = [str(v).strip() for v in (videos or [])]
    if not workflow_name:
        return {"ok": False, "message": "缺少工作流名称。"}
    if not isinstance(port, dict) or not port.get("name") or not port.get("http"):
        return {"ok": False, "message": "接口配置不可用。"}

    config = _plugin_config or {}
    server_ip = str(port.get("http") or "").strip()
    client_id = f"{_get_client_id(config)}-webui-{uuid.uuid4().hex[:8]}"
    wf_dir = _get_workflow_dir()
    workflow_params = _load_workflow_params()
    all_workflows = list_workflows_in_dir(wf_dir, workflow_params)
    workflows = _filter_workflows_for_port(all_workflows, port)
    unavailable_matches = [w for w in workflows if w.get("name") == workflow_name and not _workflow_is_available(w, workflows)]
    if unavailable_matches:
        return {
            "ok": False,
            "message": f"工作流「{workflow_name}」当前不可用：{_workflow_availability_error(unavailable_matches[0], workflows)} 请在管理页修复后保存。",
        }
    if any(w["name"] == workflow_name for w in all_workflows) and not any(w["name"] == workflow_name for w in workflows):
        return {
            "ok": False,
            "message": f"接口「{port.get('name')}」不允许使用工作流「{workflow_name}」。",
        }

    workflow_file = find_workflow_file(
        workflow_name, len(texts), len(images_b64), len(videos), wf_dir, workflow_params
    )
    if not workflow_file:
        workflow_file = _available_workflow_file_by_name(workflows, workflow_name, wf_dir)
    if not workflow_file:
        matching_workflow = next((w for w in workflows if w["name"] == workflow_name), None)
        if matching_workflow:
            return {
                "ok": False,
                "message": _workflow_input_mismatch_message(
                    workflow_name,
                    [matching_workflow],
                    texts,
                    images_b64,
                    videos,
                ),
            }
        return {
            "ok": False,
            "message": (
                f"没有找到匹配的工作流「{workflow_name}」"
                f"（当前提供：文本{_provided_input_count(texts)}，图片{_provided_input_count(images_b64)}，视频{_provided_input_count(videos)}）。"
            ),
        }

    info = _get_configured_workflow_info(wf_dir, Path(workflow_file).name, workflow_params)
    if not info:
        return {"ok": False, "message": "工作流配置不可用，请先在工作流管理页保存参数配置。"}
    ok_inputs, texts, images_b64, videos, input_error = _apply_workflow_input_rules(
        info, texts, images_b64, videos
    )
    if not ok_inputs:
        return {
            "ok": False,
            "message": (
                f"工作流「{workflow_name}」参数数量不匹配。"
                f"当前提供：文本{_provided_input_count(texts)}，图片{_provided_input_count(images_b64)}，视频{_provided_input_count(videos)}。"
                + (" " + input_error if input_error else "")
            ),
        }

    try:
        debug = bool(
            getattr(config, "debug_mode", False)
            if not isinstance(config, dict)
            else config.get("debug_mode", False)
        )
        workflow = ComfyUIWorkflow(server_ip, client_id)
        workflow.load_workflow_api(workflow_file)
        prompt_id = await workflow.submit_only(images_b64, texts, videos, debug=debug)
        output_rules = _build_output_rules(info)
        return {
            "ok": True,
            "prompt_id": prompt_id,
            "workflow_name": workflow_name,
            "workflow_file": Path(workflow_file).name,
            "port_name": str(port.get("name") or ""),
            "server_ip": server_ip,
            "client_id": client_id,
            "output_rules": output_rules,
        }
    except httpx.HTTPStatusError as e:
        body = ""
        try:
            if e.response is not None:
                body = e.response.text
        except Exception:
            pass
        summary = _parse_comfyui_400_summary(body)
        return {
            "ok": False,
            "message": (
                f"执行失败：ComfyUI 返回 {e.response.status_code if e.response else '?'}。"
                + (summary if summary else (f"服务端信息：{body[:1500]}" if body else str(e)))
            ),
        }
    except Exception as e:
        logger.exception("webui comfyui debug submit failed")
        return {"ok": False, "message": f"执行失败：{e}"}


from ..commands.comfyui import (
    _escape_telegram_code_block_text,
    _extract_command_media_sources,
    _extract_command_media_sources_async,
    _format_command_result,
    _format_workflow_input_requirements,
    _format_workflow_required_params,
    _normalize_comfyui_command_text,
    _normalize_prefixed_command_text,
    _resolve_workflow_selector,
    _split_comfyui_command_args,
    _wait_for_command_result,
)

async def _wait_for_completion(
    server_ip: str, client_id: str, prompt_id: str, timeout: int = 600, output_rules: Any = None
) -> tuple:
    """
    轮询直到任务完成，返回 (file_url, file_type, text_outputs)。
    超时或失败返回 (None, "unknown", [])。
    """
    base = _get_comfyui_http_base(server_ip)
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(f"{base}/queue")
                data = r.json()
                running = data.get("queue_running", [])
                pending = data.get("queue_pending", [])
                if not any(item[1] == prompt_id for item in running + pending):
                    break
        except Exception:
            pass
        await asyncio.sleep(2)
    return await _get_result_for_prompt(server_ip, prompt_id, output_rules)


async def _extract_images_from_event_async(event: Any) -> List[str]:
    """异步从事件中提取图片 base64。"""
    base64_list: List[str] = []
    try:
        msg_obj = getattr(event, "message_obj", None)
        if not msg_obj:
            return base64_list
        chain = getattr(msg_obj, "message", None) or []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for comp in chain:
                comp_type = getattr(comp, "type", None) or (comp.get("type") if isinstance(comp, dict) else None)
                if comp_type in ("image", "Image"):
                    url = getattr(comp, "url", None) or (comp.get("url") if isinstance(comp, dict) else None)
                    if not url:
                        file_path = getattr(comp, "file", None) or (comp.get("file") if isinstance(comp, dict) else None)
                        if file_path and Path(file_path).exists():
                            async with aiofiles.open(file_path, "rb") as f:
                                data = await f.read()
                            base64_list.append(base64.b64encode(data).decode("utf-8"))
                        continue
                    try:
                        resp = await client.get(url.replace("\n", ""))
                        if resp.status_code == 200:
                            base64_list.append(base64.b64encode(resp.content).decode("utf-8"))
                    except Exception as e:
                        logger.warning("download image for tool failed: %s", e)
    except Exception as e:
        logger.warning("extract images from event failed: %s", e)
    return base64_list


def _get_allowed_local_image_base_dirs() -> List[Path]:
    """
    返回允许读取图片的根目录列表。位于这些目录下的文件可作为 image_urls 本地路径传入。
    - 插件数据目录（PLUGIN_DATA_DIR）
    - data/agent/comfyui/input（Agent 等可能写入的通用输入目录）
    - data/temp（平台/适配器可能存放用户上传图片的临时目录，避免「图在 temp 没权限」导致 images=0）
    """
    bases = [PLUGIN_DATA_DIR.resolve()]
    try:
        data_dir = PLUGIN_DATA_DIR.resolve().parent.parent
        agent_input = data_dir / "agent" / "comfyui" / "input"
        bases.append(agent_input)
        temp_dir = data_dir / "temp"
        bases.append(temp_dir)
    except Exception:
        pass
    return bases


def _is_allowed_local_image_path(file_path: Path) -> bool:
    """
    仅允许指定白名单根目录下的本地路径，防止路径穿越。
    白名单包括：插件数据目录、data/agent/comfyui/input、data/temp（平台临时图目录）。
    """
    try:
        resolved = file_path.resolve()
        for base in _get_allowed_local_image_base_dirs():
            if resolved == base or str(resolved).startswith(str(base) + os.sep):
                return True
        return False
    except Exception:
        return False


def _parse_comfyui_400_summary(body: str) -> Optional[str]:
    """
    解析 ComfyUI /prompt 返回的 400 JSON，生成给 LLM 看的简短说明。
    例如：工作流里用的模型在服务器上不存在（value_not_in_list, ckpt_name）。
    """
    if not body or not body.strip():
        return None
    try:
        data = json.loads(body)
    except Exception:
        return None
    node_errors = data.get("node_errors") if isinstance(data, dict) else None
    if not isinstance(node_errors, dict):
        return None
    parts = []
    for _node_id, node_data in node_errors.items():
        if not isinstance(node_data, dict):
            continue
        err_list = node_data.get("errors")
        if not isinstance(err_list, list):
            continue
        for err in err_list:
            if not isinstance(err, dict):
                continue
            if err.get("type") == "value_not_in_list":
                details = err.get("details") or ""
                extra = err.get("extra_info") or {}
                input_name = extra.get("input_name", "")
                received = extra.get("received_value", "")
                config_list = extra.get("input_config")
                if isinstance(config_list, list) and len(config_list) and isinstance(config_list[0], list):
                    allowed = config_list[0][:10]
                else:
                    allowed = []
                if input_name == "ckpt_name" and received:
                    allowed_str = "、".join(allowed) if allowed else "(见服务器模型目录)"
                    parts.append(
                        f"工作流中使用的模型 '{received}' 在当前 ComfyUI 服务器上不存在；"
                        f"服务器可用模型包括：{allowed_str}。请改用「改图」等其它工作流，或在该工作流中把模型改为已有模型。"
                    )
                    break
                if not parts and details:
                    parts.append(f"ComfyUI 校验失败: {details[:500]}")
    return " ".join(parts) if parts else None


def _looks_like_base64(s: str) -> bool:
    """判断字符串是否像 base64 数据（用于日志脱敏、避免 base64 进入 LLM 上下文）。"""
    if not s or not isinstance(s, str) or len(s) < 50:
        return False
    t = s.strip()[:200]
    return all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r \t" for c in t)


def _sanitize_image_urls_for_log(image_urls: Any) -> str:
    """将 image_urls 转为可安全写入日志的字符串，不输出 base64 内容。"""
    if not image_urls:
        return "[]"
    if isinstance(image_urls, str):
        return "<base64 or long string>" if _looks_like_base64(image_urls) else (image_urls[:80] + "..." if len(image_urls) > 80 else image_urls)
    if isinstance(image_urls, (list, tuple)):
        parts = []
        for u in image_urls[:10]:
            if isinstance(u, str):
                if u.startswith("http"):
                    parts.append(u[:80] + ("..." if len(u) > 80 else ""))
                elif _looks_like_base64(u) or u.startswith("data:image") or u.startswith("base64:"):
                    parts.append("<base64>")
                else:
                    parts.append(u[:60] + ("..." if len(u) > 60 else ""))
            else:
                parts.append(str(type(u)))
        return "[" + ", ".join(parts) + (" ..." if len(image_urls) > 10 else "") + "]"
    return str(type(image_urls))


def _extract_base64_from_data_uri(s: str) -> Optional[str]:
    """从 data:image/xxx;base64,<payload> 中提取纯 base64 字符串，用于直接注入工作流。"""
    if not s or "base64," not in s:
        return None
    try:
        idx = s.index("base64,") + 7
        payload = s[idx:].strip()
        if not payload:
            return None
        # 校验是否为合法 base64（可含换行，需去掉）
        payload = payload.replace("\n", "").replace("\r", "")
        base64.b64decode(payload, validate=True)
        return payload
    except Exception:
        return None


async def _image_sources_to_base64(sources: List[str]) -> List[str]:
    """
    将「图片来源」列表转为 base64 列表，支持：
    - data:image/xxx;base64,<payload>：直接使用 payload 作为 base64；
    - base64: 或 base64://<payload>：直接使用 payload 作为 base64（qts 等工具可能返回此类）；
    - 服务器 URL（http/https）：插件下载后转 base64；
    - 本地路径：仅允许插件数据目录内，拒绝路径穿越。
    用于 comfyui_execute 的 image_urls 参数。
    """
    result: List[str] = []
    for s in sources:
        if not s or not isinstance(s, str):
            result.append("")
            continue
        s = s.strip()
        if not s:
            result.append("")
            continue
        # 1) data:image/xxx;base64,<payload>
        if s.startswith("data:image") and "base64," in s:
            b64 = _extract_base64_from_data_uri(s)
            if b64:
                result.append(b64)
                logger.info("[ComfyUI Tool] Using image from data URI (base64) in image_urls.")
            else:
                result.append("")
            continue
        # 2) base64: 或 base64://<payload>（工具如 qts_get_message_detail 可能返回的「乱码」实为 base64）
        if s.startswith("base64://"):
            raw = s[9:].strip().replace("\n", "").replace("\r", "")
        elif s.startswith("base64:"):
            raw = s[7:].strip().replace("\n", "").replace("\r", "")
        else:
            raw = None
        if raw:
            try:
                base64.b64decode(raw, validate=True)
                result.append(raw)
                logger.info("[ComfyUI Tool] Using image from base64: prefix in image_urls.")
            except Exception as e:
                logger.warning("ComfyUI invalid base64 in image_urls: %s", e)
                result.append("")
            continue
        # 3) 无前缀的纯 base64（如 qts_get_message_detail 返回的「乱码」实为 base64）
        if len(s) >= 100 and all(c in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n\r" for c in s):
            clean = s.replace("\n", "").replace("\r", "")
            try:
                base64.b64decode(clean, validate=True)
                result.append(clean)
                logger.info("[ComfyUI Tool] Using image from raw base64 string in image_urls.")
                continue
            except Exception:
                pass
        # 4) 本地文件路径：仅允许在 PLUGIN_DATA_DIR 内
        if not s.startswith("http"):
            p = Path(s)
            if not p.exists() or not p.is_file():
                result.append("")
                continue
            if not _is_allowed_local_image_path(p):
                logger.warning("ComfyUI rejected local image path (outside allowed dir): %s", s[:80])
                result.append("")
                continue
            try:
                async with aiofiles.open(p, "rb") as f:
                    data = await f.read()
                result.append(base64.b64encode(data).decode("utf-8"))
            except Exception as e:
                logger.warning("ComfyUI read local image failed: %s", e)
                result.append("")
            continue
        # 4) 服务器 URL
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(s.replace("\n", ""))
                if resp.status_code == 200 and resp.content:
                    result.append(base64.b64encode(resp.content).decode("utf-8"))
                else:
                    result.append("")
        except Exception as e:
            logger.warning("ComfyUI fetch image_url failed: %s", e)
            result.append("")
    return result


# --------------- LLM Tools ---------------

from ..tools.execute import ComfyUIExecuteTool
from ..tools.list_workflows import ComfyUIListWorkflowsTool
from ..tools.query_wait import ComfyUIQueryWaitTool
from ..tools.status import ComfyUIStatusTool
from ..tools.workflow_detail import ComfyUIGetWorkflowDetailTool


# --------------- Plugin ---------------


@register(
    "astrbot_plugin_comfyui_bubble",
    "MuFengDR",
    "将 ComfyUI 工作流封装为 LLM 工具和 /comfyui 手动命令，支持 WebSocket 事件等待、工作流 WebUI 管理、多 ComfyUI 接口切换、输入输出参数配置和媒体自动发送。",
    "1.0.5",
)
class ComfyUIPlugin(Star):
    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        global _plugin_config, _plugin_context, _task_service
        _plugin_config = self.config = config or {}
        _plugin_context = self.context
        _task_service = self
        _sync_active_interface_config(self.config)
        self.context.add_llm_tools(
            ComfyUIListWorkflowsTool(),
            ComfyUIStatusTool(),
            ComfyUIQueryWaitTool(),
            ComfyUIExecuteTool(),
        )
        self._web_server = None  # ManagementServer 实例，在 initialize 中启动
        self._webui_debug_tasks: Dict[str, Dict[str, Any]] = {}
        self._webui_debug_order: List[str] = []
        self._webui_debug_runners: Dict[str, asyncio.Task] = {}
        self._webui_debug_watchers: Dict[str, asyncio.Task] = {}
        self._content_audit = ContentAuditService(PLUGIN_DATA_DIR)

    def _media_history_dir(self) -> Path:
        path = PLUGIN_DATA_DIR / "media" / "history"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _media_history_url(self, filename: str) -> str:
        return f"/api/media/history/{Path(filename).name}"

    def _safe_task_filename(self, value: str) -> str:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(value or "task"))
        return safe[:96] or "task"

    async def _download_history_media(self, url: str, task_id: str, kind: str, index: int) -> Optional[Dict[str, Any]]:
        if not url or not str(url).startswith(("http://", "https://")):
            return None
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.get(str(url))
                resp.raise_for_status()
            content_type = resp.headers.get("content-type", "").lower()
            suffix = ".mp4" if kind == "video" else ".png"
            if "jpeg" in content_type or "jpg" in content_type:
                suffix = ".jpg"
            elif "webp" in content_type:
                suffix = ".webp"
            elif "gif" in content_type:
                suffix = ".gif"
            elif "webm" in content_type:
                suffix = ".webm"
            elif "quicktime" in content_type:
                suffix = ".mov"
            name = f"{self._safe_task_filename(task_id)}_{kind}_{index}{suffix}"
            path = self._media_history_dir() / name
            async with aiofiles.open(path, "wb") as f:
                await f.write(resp.content)
            return {
                "url": self._media_history_url(name),
                "original_url": str(url),
                "filename": name,
                "type": kind,
                "size": len(resp.content or b""),
            }
        except Exception as e:
            logger.warning("ComfyUI history media download failed: %s", e)
            return None

    async def _localize_task_result_media(self, task: Dict[str, Any]) -> None:
        result = task.get("result") if isinstance(task.get("result"), dict) else {}
        media_files: List[Dict[str, Any]] = list(task.get("media_files") or [])
        for key, kind in (("images", "image"), ("videos", "video")):
            values = [str(u) for u in (result.get(key) or []) if u]
            localized: List[str] = []
            local_meta: List[Dict[str, Any]] = []
            for idx, url in enumerate(values, 1):
                if url.startswith("/api/media/history/"):
                    localized.append(url)
                    continue
                item = await self._download_history_media(url, str(task.get("task_id") or task.get("prompt_id") or "task"), kind, idx)
                if item:
                    localized.append(item["url"])
                    local_meta.append(item)
                    media_files.append(item)
                else:
                    localized.append(url)
            result[key] = localized
            result[f"{key}_original"] = values
            result[f"{key}_local"] = local_meta
        task["result"] = result
        task["media_files"] = media_files

    def _serialize_webui_debug_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(task)
        now = time.time()
        if data.get("status") == "queued":
            data["elapsed"] = 0
        else:
            started = float(data.get("started_at") or data.get("created_at") or now)
            data["elapsed"] = max(0, round(float(data.get("finished_at") or now) - started, 1))
        return data

    def _serialize_webui_debug_history_summary(self, task: Dict[str, Any]) -> Dict[str, Any]:
        input_data = task.get("input") if isinstance(task.get("input"), dict) else {}
        result = task.get("result") if isinstance(task.get("result"), dict) else {}
        return {
            "task_id": task.get("task_id", ""),
            "prompt_id": task.get("prompt_id", ""),
            "origin": task.get("origin", "webui"),
            "origin_label": task.get("origin_label", ""),
            "session_label": task.get("session_label", ""),
            "status": task.get("status", ""),
            "port_name": task.get("port_name", ""),
            "workflow_name": task.get("workflow_name", ""),
            "workflow_file": task.get("workflow_file", ""),
            "created_at": task.get("created_at"),
            "started_at": task.get("started_at"),
            "finished_at": task.get("finished_at"),
            "elapsed": task.get("elapsed", 0),
            "thumbnail": task.get("thumbnail", ""),
            "media_files": task.get("media_files", []),
            "error": task.get("error", ""),
            "input_summary": task.get("input_summary")
            or {
                "texts": len(input_data.get("texts") or []),
                "images": len(input_data.get("images") or []),
            },
            "result_summary": {
                "texts": len(result.get("texts") or []),
                "images": len(result.get("images") or []),
                "videos": len(result.get("videos") or []),
                "audio": len(result.get("audio") or []),
            },
            "summary": True,
        }

    def _webui_debug_output_dir(self) -> Path:
        path = PLUGIN_DATA_DIR / "media" / "history"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _webui_debug_history_path(self, task_id: str) -> Path:
        safe_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(task_id or ""))
        return self._webui_debug_output_dir() / f"{safe_id}.json"

    def _remember_webui_debug_task(self, task: Dict[str, Any]) -> None:
        task_id = str(task.get("task_id") or "")
        if not task_id:
            return
        task.setdefault("origin", "webui")
        task.setdefault("origin_label", {"webui": "WebUI", "command": "command", "llm_tool": "LLM 工具"}.get(str(task.get("origin")), str(task.get("origin") or "")))
        self._webui_debug_tasks[task_id] = task
        if task_id in self._webui_debug_order:
            self._webui_debug_order.remove(task_id)
        self._webui_debug_order.append(task_id)

    def _session_label(self, value: str) -> str:
        raw = str(value or "").strip()
        if not raw or raw.lower() in {"default", "unknown", "none", "null"}:
            return ""
        return raw

    def remember_external_task(
        self,
        origin: str,
        submit: Dict[str, Any],
        workflow_name: str,
        texts: Optional[List[str]] = None,
        images: Optional[List[str]] = None,
        videos: Optional[List[str]] = None,
        session_tag: str = "",
    ) -> None:
        prompt_id = str(submit.get("prompt_id") or "")
        if not prompt_id:
            return
        task_id = prompt_id
        port_name = ""
        port_http = str(submit.get("server_ip") or "")
        try:
            active = _get_active_comfyui_port(self.config or {})
            port_name = str(active.get("name") or "")
            port_http = str(active.get("http") or port_http)
        except Exception:
            pass
        task = self._webui_debug_tasks.get(task_id) or {}
        task.update(
            {
                "task_id": task_id,
                "prompt_id": prompt_id,
                "origin": origin,
                "origin_label": {"command": "command", "llm_tool": "LLM 工具", "webui": "WebUI"}.get(origin, origin),
                "session_label": self._session_label(str(submit.get("session_key") or "") or session_tag or str(submit.get("session_tag") or "")),
                "session_key": str(submit.get("session_key") or ""),
                "session_tag": session_tag or str(submit.get("session_tag") or ""),
                "status": task.get("status") or "queued",
                "port_name": port_name,
                "port_http": port_http,
                "workflow_name": workflow_name,
                "workflow_file": submit.get("workflow_file") or task.get("workflow_file", ""),
                "server_ip": submit.get("server_ip") or port_http,
                "client_id": submit.get("client_id") or "",
                "output_rules": submit.get("output_rules") or {},
                "queue_key": self._webui_debug_queue_key(port_http),
                "input_summary": {"texts": len(texts or []), "images": len(images or []), "videos": len(videos or [])},
                "input": {
                    "port_name": port_name,
                    "workflow_name": workflow_name,
                    "texts": list(texts or []),
                    "images": [],
                    "videos": [{"name": Path(v).name, "filename": Path(v).name, "size": 0} for v in (videos or [])],
                },
                "created_at": task.get("created_at") or time.time(),
                "result": task.get("result") or {"texts": [], "images": [], "videos": [], "audio": []},
            }
        )
        self._remember_webui_debug_task(task)
        self._ensure_webui_debug_watcher(task_id)

    async def complete_external_task(
        self,
        prompt_id: str,
        server_ip: str,
        url: Any,
        ftype: str,
        texts: List[str],
        error: str = "",
    ) -> Optional[Dict[str, Any]]:
        task = self._webui_debug_tasks.get(str(prompt_id))
        if not task:
            if self._webui_debug_history_path(str(prompt_id)).exists():
                return
            task = {
                "task_id": str(prompt_id),
                "prompt_id": str(prompt_id),
                "origin": "unknown",
                "origin_label": "外部任务",
                "status": "completed",
                "port_name": "",
                "workflow_name": "",
                "server_ip": server_ip,
                "created_at": time.time(),
            }
            self._remember_webui_debug_task(task)
        if error or ftype == "error":
            task["status"] = "failed"
            task["error"] = error or ("\n".join(texts) if texts else "ComfyUI 输出错误。")
        else:
            media = url if isinstance(url, dict) else {}
            images = media.get("images", []) if isinstance(media, dict) else ([url] if ftype == "image" and url else [])
            videos = media.get("videos", []) if isinstance(media, dict) else ([url] if ftype == "video" and url else [])
            audio = media.get("audio", []) if isinstance(media, dict) else []
            task["status"] = "completed"
            task["result"] = {
                "type": ftype,
                "texts": texts or [],
                "images": images,
                "videos": videos,
                "audio": audio,
            }
        task["finished_at"] = time.time()
        await self._persist_and_remove_webui_debug_task(task)
        return task

    async def audit_task_images(self, task: Optional[Dict[str, Any]], images: List[str]) -> Dict[str, Any]:
        if not task or not images:
            return {"allowed_images": list(images), "blocked": [], "records": []}
        return await self._content_audit.audit_images_for_task(task, images)

    def get_external_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        task = self._webui_debug_tasks.get(str(task_id))
        return dict(task) if isinstance(task, dict) else None

    async def audit_task_texts(self, task: Optional[Dict[str, Any]], texts: List[str]) -> Dict[str, Any]:
        if not task or not texts:
            return {"allowed_texts": list(texts or []), "blocked": [], "records": []}
        return await self._content_audit.audit_texts_for_task(task, texts)

    def list_audit_records(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._content_audit.list_records(filters or {})

    def get_audit_stats(self) -> Dict[str, Any]:
        return self._content_audit.stats()

    def get_audit_settings(self) -> Dict[str, Any]:
        return {"ok": True, "settings": self._content_audit.public_settings()}

    def save_audit_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"ok": True, "settings": self._content_audit.save_settings(payload)}

    async def test_audit_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._content_audit.test_image_provider(payload)

    async def _save_webui_debug_thumbnail(self, task: Dict[str, Any]) -> None:
        result = task.get("result") if isinstance(task.get("result"), dict) else {}
        images = result.get("images") if isinstance(result, dict) else []
        if not images:
            return
        image_url = str(images[0] or "")
        if not image_url:
            return
        try:
            if image_url.startswith("/api/media/history/"):
                filename = Path(image_url.rsplit("/", 1)[-1]).name
                source = self._media_history_dir() / filename
                if not source.exists() or not source.is_file():
                    return
                data = source.read_bytes()
                suffix = source.suffix or ".png"
            else:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.get(image_url)
                    resp.raise_for_status()
                content_type = resp.headers.get("content-type", "").lower()
                suffix = ".jpg" if "jpeg" in content_type or "jpg" in content_type else ".png"
                data = resp.content
            thumb_name = f"{task['task_id']}_thumb{suffix}"
            thumb_path = self._webui_debug_output_dir() / thumb_name
            thumb_path.write_bytes(data)
            task["thumbnail"] = f"/api/debug/output/{thumb_name}"
        except Exception as e:
            logger.warning("webui debug thumbnail save failed: %s", e)

    async def _persist_webui_debug_task(self, task: Dict[str, Any]) -> None:
        task["finished_at"] = task.get("finished_at") or time.time()
        await self._localize_task_result_media(task)
        await self._save_webui_debug_thumbnail(task)
        record = self._serialize_webui_debug_task(task)
        record.pop("server_ip", None)
        record.pop("client_id", None)
        record.pop("output_rules", None)
        record.pop("port_http", None)
        record.pop("port_workflows", None)
        record.pop("queue_key", None)
        record.pop("texts_for_submit", None)
        record.pop("images_for_submit", None)
        record.pop("videos_for_submit", None)
        self._webui_debug_history_path(str(task.get("task_id") or "")).write_text(
            json.dumps(record, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def _finish_webui_debug_task(self, task_id: str) -> None:
        task = self._webui_debug_tasks.get(task_id)
        if not task:
            return
        task["status"] = "running"
        task["started_at"] = time.time()
        timeout = _get_websocket_wait_timeout(self.config or {})
        try:
            port = {"name": task.get("port_name"), "http": task.get("port_http"), "workflows": task.get("port_workflows", [])}
            submit = await _submit_comfyui_workflow_to_port(
                port,
                str(task.get("workflow_name") or ""),
                list(task.get("texts_for_submit") or []),
                list(task.get("images_for_submit") or []),
                [],
            )
            if not submit.get("ok"):
                task["status"] = "failed"
                task["error"] = submit.get("message") or "提交失败。"
                task["finished_at"] = time.time()
                return
            task["prompt_id"] = submit["prompt_id"]
            task["workflow_file"] = submit.get("workflow_file") or task.get("workflow_file", "")
            task["server_ip"] = submit["server_ip"]
            task["client_id"] = submit["client_id"]
            task["output_rules"] = submit.get("output_rules") or {}
            wait_result = await _wait_for_comfyui_ws_completion(
                task["server_ip"], task["client_id"], task["prompt_id"], timeout
            )
            status = wait_result.get("status")
            if status != "completed":
                history_state = await _get_prompt_history_state(task["server_ip"], task["prompt_id"])
                if status == "ws_unavailable":
                    deadline = time.time() + max(5, timeout)
                    while not history_state.get("completed") and time.time() < deadline:
                        await asyncio.sleep(2)
                        history_state = await _get_prompt_history_state(task["server_ip"], task["prompt_id"])
                if not history_state.get("completed"):
                    task["status"] = "timeout" if status == "timeout" else "failed"
                    task["error"] = wait_result.get("message") or history_state.get("message") or "ComfyUI 任务未完成。"
                    task["finished_at"] = time.time()
                    return

            media, ftype, texts = await _get_result_for_prompt(
                task["server_ip"], task["prompt_id"], task.get("output_rules")
            )
            if ftype == "error":
                task["status"] = "failed"
                task["error"] = "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"
            else:
                media = media if isinstance(media, dict) else {}
                task["status"] = "completed"
                task["result"] = {
                    "type": ftype,
                    "texts": texts or [],
                    "images": media.get("images", []) if isinstance(media, dict) else [],
                    "videos": media.get("videos", []) if isinstance(media, dict) else [],
                    "audio": media.get("audio", []) if isinstance(media, dict) else [],
                }
            task["finished_at"] = time.time()
        except Exception as e:
            logger.exception("webui comfyui debug task failed")
            task["status"] = "failed"
            task["error"] = str(e)
            task["finished_at"] = time.time()
        finally:
            if task.get("status") in {"completed", "failed", "timeout"}:
                await self._persist_webui_debug_task(task)
                self._webui_debug_tasks.pop(task_id, None)
                if task_id in self._webui_debug_order:
                    self._webui_debug_order.remove(task_id)

    async def _run_webui_debug_queue(self, port_name: str) -> None:
        try:
            while True:
                queued = [
                    task
                    for task in self._webui_debug_tasks.values()
                    if task.get("port_name") == port_name and task.get("status") == "queued"
                ]
                if not queued:
                    return
                queued.sort(key=lambda item: float(item.get("created_at") or 0))
                await self._finish_webui_debug_task(str(queued[0].get("task_id") or ""))
        finally:
            self._webui_debug_runners.pop(port_name, None)

    def _ensure_webui_debug_runner(self, port_name: str) -> None:
        runner = self._webui_debug_runners.get(port_name)
        if runner and not runner.done():
            return
        self._webui_debug_runners[port_name] = asyncio.create_task(
            self._run_webui_debug_queue(port_name)
        )

    def _webui_debug_queue_key(self, port_http: str) -> str:
        return _get_comfyui_http_base(str(port_http or "")).rstrip("/")

    def _webui_debug_unfinished_count(self, queue_key: str) -> int:
        return sum(
            1
            for task in self._webui_debug_tasks.values()
            if task.get("queue_key") == queue_key
            and task.get("status") not in {"completed", "failed", "timeout", "canceled"}
        )

    async def _get_webui_debug_queue_sets(self, server_ip: str) -> tuple[set[str], set[str], bool]:
        base = _get_comfyui_http_base(server_ip)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{base}/queue")
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.debug("webui debug queue sync failed for %s: %s", base, e)
            return set(), set(), False

        def _ids(items: Any) -> set[str]:
            result: set[str] = set()
            if not isinstance(items, list):
                return result
            for item in items:
                if isinstance(item, (list, tuple)) and len(item) >= 2 and item[1]:
                    result.add(str(item[1]))
            return result

        return _ids(data.get("queue_running")), _ids(data.get("queue_pending")), True

    async def _stop_comfyui_prompt(self, task: Dict[str, Any]) -> tuple[bool, str]:
        prompt_id = str(task.get("prompt_id") or "")
        server_ip = str(task.get("server_ip") or task.get("port_http") or "")
        if not prompt_id or not server_ip:
            return False, "任务缺少 ComfyUI prompt 信息。"
        base = _get_comfyui_http_base(server_ip)
        running_ids, pending_ids, queue_ok = await self._get_webui_debug_queue_sets(server_ip)
        if not queue_ok:
            return False, "无法获取 ComfyUI 队列状态，停止失败。"
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                if prompt_id in pending_ids:
                    resp = await client.post(f"{base}/queue", json={"delete": [prompt_id]})
                    resp.raise_for_status()
                    return True, "queued"
                if prompt_id in running_ids:
                    try:
                        resp = await client.post(f"{base}/interrupt", json={"prompt_id": prompt_id})
                        resp.raise_for_status()
                        return True, "running"
                    except Exception:
                        if len(running_ids) == 1 and prompt_id in running_ids:
                            resp = await client.post(f"{base}/interrupt", json={})
                            resp.raise_for_status()
                            return True, "running"
                        raise
        except Exception as e:
            return False, f"停止 ComfyUI 任务失败：{e}"

        history_state = await _get_prompt_history_state(server_ip, prompt_id)
        if history_state.get("completed") or history_state.get("has_outputs"):
            return False, "任务已经完成，无法停止。"
        return True, "missing"

    async def _after_manual_stop_feedback(self, task: Dict[str, Any]) -> None:
        prompt_id = str(task.get("prompt_id") or "")
        origin = str(task.get("origin") or "")
        session_key = str(task.get("session_key") or task.get("session_label") or "")
        session_tag = str(task.get("session_tag") or "")
        if origin == "command":
            if session_key:
                await _send_plain_to_session(session_key, "ComfyUI 任务已被手动停止。")
            _cleanup_completed_task(prompt_id, session_tag)
        elif origin == "llm_tool":
            pending = dict(_task_registry.get(prompt_id) or {})
            pending.update(
                {
                    "prompt_id": prompt_id,
                    "server_ip": task.get("server_ip") or pending.get("server_ip", ""),
                    "client_id": task.get("client_id") or pending.get("client_id", ""),
                    "session_key": session_key or pending.get("session_key", ""),
                    "session_tag": session_tag or pending.get("session_tag", ""),
                    "status": "canceled",
                    "message": "ComfyUI task was manually stopped from WebUI.",
                }
            )
            _task_registry[prompt_id] = pending
            if pending.get("session_tag"):
                tasks = _session_tag_tasks.setdefault(str(pending["session_tag"]), [])
                if prompt_id not in tasks:
                    tasks.append(prompt_id)
        else:
            _cleanup_completed_task(prompt_id, session_tag)

    async def _persist_and_remove_webui_debug_task(self, task: Dict[str, Any]) -> None:
        task_id = str(task.get("task_id") or "")
        if not task_id:
            return
        await self._persist_webui_debug_task(task)
        self._webui_debug_tasks.pop(task_id, None)
        if task_id in self._webui_debug_order:
            self._webui_debug_order.remove(task_id)
        watcher = self._webui_debug_watchers.pop(task_id, None)
        current = asyncio.current_task()
        if watcher and watcher is not current and not watcher.done():
            watcher.cancel()

    async def _complete_webui_debug_task_from_history(self, task: Dict[str, Any]) -> None:
        media, ftype, texts = await _get_result_for_prompt(
            str(task.get("server_ip") or ""),
            str(task.get("prompt_id") or ""),
            task.get("output_rules"),
        )
        if ftype == "error":
            task["status"] = "failed"
            task["error"] = "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"
        else:
            media = media if isinstance(media, dict) else {}
            task["status"] = "completed"
            task["result"] = {
                "type": ftype,
                "texts": texts or [],
                "images": media.get("images", []) if isinstance(media, dict) else [],
                "videos": media.get("videos", []) if isinstance(media, dict) else [],
                "audio": media.get("audio", []) if isinstance(media, dict) else [],
            }
        task["finished_at"] = time.time()
        await self._persist_and_remove_webui_debug_task(task)

    async def _sync_webui_debug_task_states(self) -> None:
        tasks = [
            task
            for task in self._webui_debug_tasks.values()
            if task.get("status") not in {"completed", "failed", "timeout", "canceled"}
        ]
        if not tasks:
            return
        by_server: Dict[str, List[Dict[str, Any]]] = {}
        for task in tasks:
            server_ip = str(task.get("server_ip") or task.get("port_http") or "")
            if server_ip:
                by_server.setdefault(self._webui_debug_queue_key(server_ip), []).append(task)

        timeout = _get_websocket_wait_timeout(self.config or {})
        now = time.time()
        for grouped in by_server.values():
            server_ip = str(grouped[0].get("server_ip") or grouped[0].get("port_http") or "")
            running_ids, pending_ids, queue_ok = await self._get_webui_debug_queue_sets(server_ip)
            for task in list(grouped):
                task_id = str(task.get("task_id") or "")
                prompt_id = str(task.get("prompt_id") or "")
                if not prompt_id or task_id not in self._webui_debug_tasks:
                    continue
                if prompt_id in running_ids:
                    task["status"] = "running"
                    task["started_at"] = task.get("started_at") or now
                    continue
                if prompt_id in pending_ids:
                    task["status"] = "queued"
                    continue

                history_state = await _get_prompt_history_state(server_ip, prompt_id)
                if history_state.get("completed") or history_state.get("has_outputs"):
                    await self._complete_webui_debug_task_from_history(task)
                    continue
                status_str = str(history_state.get("status_str") or "").lower()
                if history_state.get("exists") and any(key in status_str for key in ("error", "failed", "interrupted")):
                    task["status"] = "failed"
                    task["error"] = history_state.get("message") or history_state.get("status_str")
                    task["finished_at"] = now
                    await self._persist_and_remove_webui_debug_task(task)
                    continue
                if queue_ok and now - float(task.get("created_at") or now) > max(5, timeout):
                    task["status"] = "timeout"
                    task["error"] = "ComfyUI 任务未在队列或历史记录中找到，已超时。"
                    task["finished_at"] = now
                    await self._persist_and_remove_webui_debug_task(task)
                elif not queue_ok and now - float(task.get("created_at") or now) > max(5, timeout):
                    task["status"] = "failed"
                    task["error"] = "无法获取 ComfyUI 队列状态，任务已超时。"
                    task["finished_at"] = now
                    await self._persist_and_remove_webui_debug_task(task)

    async def _watch_webui_debug_task(self, task_id: str) -> None:
        try:
            timeout = _get_websocket_wait_timeout(self.config or {})
            while task_id in self._webui_debug_tasks:
                task = self._webui_debug_tasks.get(task_id)
                if not task:
                    return
                prompt_id = str(task.get("prompt_id") or "")
                server_ip = str(task.get("server_ip") or "")
                if not prompt_id or not server_ip:
                    return
                history_state = await _get_prompt_history_state(server_ip, prompt_id)
                if history_state.get("completed") or history_state.get("has_outputs"):
                    await self._complete_webui_debug_task_from_history(task)
                    return
                status_str = str(history_state.get("status_str") or "").lower()
                if history_state.get("exists") and any(key in status_str for key in ("error", "failed", "interrupted")):
                    task["status"] = "failed"
                    task["error"] = history_state.get("message") or history_state.get("status_str")
                    task["finished_at"] = time.time()
                    await self._persist_and_remove_webui_debug_task(task)
                    return
                if time.time() - float(task.get("created_at") or time.time()) > max(5, timeout):
                    running_ids, pending_ids, queue_ok = await self._get_webui_debug_queue_sets(server_ip)
                    if queue_ok and prompt_id not in running_ids and prompt_id not in pending_ids:
                        task["status"] = "timeout"
                        task["error"] = "ComfyUI 任务未在队列或历史记录中找到，已超时。"
                        task["finished_at"] = time.time()
                        await self._persist_and_remove_webui_debug_task(task)
                        return
                    if not queue_ok:
                        task["status"] = "failed"
                        task["error"] = "无法获取 ComfyUI 队列状态，任务已超时。"
                        task["finished_at"] = time.time()
                        await self._persist_and_remove_webui_debug_task(task)
                        return
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("webui comfyui debug watcher failed")
            task = self._webui_debug_tasks.get(task_id)
            if task:
                task["status"] = "failed"
                task["error"] = str(e)
                task["finished_at"] = time.time()
                await self._persist_and_remove_webui_debug_task(task)
        finally:
            current = asyncio.current_task()
            watcher = self._webui_debug_watchers.get(task_id)
            if watcher is current:
                self._webui_debug_watchers.pop(task_id, None)

    def _ensure_webui_debug_watcher(self, task_id: str) -> None:
        watcher = self._webui_debug_watchers.get(task_id)
        if watcher and not watcher.done():
            return
        self._webui_debug_watchers[task_id] = asyncio.create_task(
            self._watch_webui_debug_task(task_id)
        )

    async def submit_webui_debug_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        port_name = str(payload.get("port_name") or payload.get("portName") or "").strip()
        workflow_name = str(payload.get("workflow_name") or payload.get("workflowName") or "").strip()
        texts = [str(t) for t in (payload.get("texts") or []) if str(t).strip()]
        images = [str(img) for img in (payload.get("images") or []) if str(img).strip()]
        videos = [str(v) for v in (payload.get("videos") or []) if str(v).strip()]
        image_inputs = [
            item
            for item in (payload.get("image_inputs") or [])
            if isinstance(item, dict) and item.get("data_url")
        ]
        video_inputs = [
            item
            for item in (payload.get("video_inputs") or [])
            if isinstance(item, dict) and item.get("filename")
        ]
        ports = _get_comfyui_ports(self.config or {})
        port = next((p for p in ports if str(p.get("name") or "") == port_name), None)
        if not port:
            return {"ok": False, "error": "接口不存在。"}

        queue_key = self._webui_debug_queue_key(str(port.get("http") or ""))
        if self._webui_debug_unfinished_count(queue_key) >= 10:
            return {"ok": False, "error": "当前接口调试队列已满，最多允许 10 个未完成任务。"}

        task_id = f"webui_{uuid.uuid4().hex}"
        task = {
            "task_id": task_id,
            "prompt_id": "",
            "origin": "webui",
            "origin_label": "WebUI",
            "session_label": "WebUI",
            "status": "queued",
            "port_name": port_name,
            "port_http": str(port.get("http") or ""),
            "port_workflows": list(port.get("workflows") or []),
            "queue_key": queue_key,
            "workflow_name": workflow_name,
            "workflow_file": "",
            "server_ip": "",
            "client_id": "",
            "output_rules": {},
            "input_summary": {"texts": len(texts), "images": len(images), "videos": len(videos)},
            "input": {
                "port_name": port_name,
                "workflow_name": workflow_name,
                "texts": texts,
                "images": image_inputs,
                "videos": video_inputs,
            },
            "texts_for_submit": texts,
            "images_for_submit": images,
            "videos_for_submit": videos,
            "created_at": time.time(),
            "result": {"texts": [], "images": [], "videos": [], "audio": []},
        }
        submit = await _submit_comfyui_workflow_to_port(
            port,
            workflow_name,
            texts,
            images,
            videos,
        )
        if not submit.get("ok"):
            task["status"] = "failed"
            task["error"] = submit.get("message") or "提交失败。"
            task["finished_at"] = time.time()
            await self._persist_webui_debug_task(task)
            return {"ok": False, "error": task["error"], "task": self._serialize_webui_debug_task(task)}

        task["prompt_id"] = submit["prompt_id"]
        task["workflow_file"] = submit.get("workflow_file") or task.get("workflow_file", "")
        task["server_ip"] = submit["server_ip"]
        task["client_id"] = submit["client_id"]
        task["output_rules"] = submit.get("output_rules") or {}
        self._remember_webui_debug_task(task)
        self._ensure_webui_debug_watcher(task_id)
        return {"ok": True, "task": self._serialize_webui_debug_task(task)}

    async def list_webui_debug_tasks(self, origin: str = "") -> Dict[str, Any]:
        await self._sync_webui_debug_task_states()
        origin = str(origin or "").strip()
        return {
            "ok": True,
            "tasks": [
                self._serialize_webui_debug_task(self._webui_debug_tasks[task_id])
                for task_id in self._webui_debug_order
                if task_id in self._webui_debug_tasks
                and (not origin or str(self._webui_debug_tasks[task_id].get("origin") or "") == origin)
            ],
        }

    async def list_webui_debug_history(self, origin: str = "") -> Dict[str, Any]:
        origin = str(origin or "").strip()
        items: List[Dict[str, Any]] = []
        paths = sorted(
            self._webui_debug_output_dir().glob("*.json"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        for path in paths:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    if origin and str(data.get("origin") or "webui") != origin:
                        continue
                    items.append(self._serialize_webui_debug_history_summary(data))
            except Exception:
                continue
            if len(items) >= 120:
                break
        return {"ok": True, "tasks": items}

    async def get_webui_debug_task(self, task_id: str) -> Dict[str, Any]:
        task = self._webui_debug_tasks.get(str(task_id or ""))
        if not task:
            path = self._webui_debug_history_path(str(task_id or ""))
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        return {"ok": True, "task": data}
                except Exception:
                    pass
        if not task:
            return {"ok": False, "error": "任务不存在。"}
        return {"ok": True, "task": self._serialize_webui_debug_task(task)}

    async def delete_webui_debug_task(self, task_id: str) -> Dict[str, Any]:
        task_id = str(task_id or "").strip()
        if not task_id:
            return {"ok": False, "error": "任务不存在。"}

        task = self._webui_debug_tasks.pop(task_id, None)
        if task_id in self._webui_debug_order:
            self._webui_debug_order.remove(task_id)
        watcher = self._webui_debug_watchers.pop(task_id, None)
        if watcher and not watcher.done():
            watcher.cancel()
        if task:
            return {"ok": True, "deleted": 1, "scope": "active"}

        path = self._webui_debug_history_path(task_id)
        if not path.exists() or not path.is_file():
            return {"ok": False, "error": "任务不存在。"}

        thumbnail = ""
        media_files: List[Dict[str, Any]] = []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                thumbnail = str(data.get("thumbnail") or "")
                media_files = [item for item in (data.get("media_files") or []) if isinstance(item, dict)]
        except Exception:
            thumbnail = ""
        if thumbnail.startswith("/api/debug/output/"):
            thumb_name = Path(thumbnail.rsplit("/", 1)[-1]).name
            thumb_path = self._webui_debug_output_dir() / thumb_name
            if thumb_path.exists() and thumb_path.is_file():
                try:
                    thumb_path.unlink()
                except Exception:
                    pass
        for item in media_files:
            filename = Path(str(item.get("filename") or item.get("url", "").rsplit("/", 1)[-1])).name
            if not filename:
                continue
            media_path = self._media_history_dir() / filename
            if media_path.exists() and media_path.is_file():
                try:
                    media_path.unlink()
                except Exception:
                    pass
        try:
            path.unlink()
            self._content_audit.remove_task_records(task_id)
        except Exception as e:
            return {"ok": False, "error": f"删除任务失败：{e}"}
        return {"ok": True, "deleted": 1, "scope": "history"}

    async def stop_webui_debug_task(self, task_id: str) -> Dict[str, Any]:
        task_id = str(task_id or "").strip()
        if not task_id:
            return {"ok": False, "error": "任务不存在。"}
        task = self._webui_debug_tasks.get(task_id)
        if not task:
            return {"ok": False, "error": "任务不存在或已经结束。"}
        if str(task.get("status") or "") not in {"queued", "running"}:
            return {"ok": False, "error": "只有排队中或运行中的任务可以停止。"}

        ok, scope_or_error = await self._stop_comfyui_prompt(task)
        if not ok:
            return {"ok": False, "error": scope_or_error}

        task["status"] = "canceled"
        task["error"] = "任务已被 WebUI 手动停止。"
        task["finished_at"] = time.time()
        await self._after_manual_stop_feedback(task)
        await self._persist_and_remove_webui_debug_task(task)
        return {"ok": True, "stopped": 1, "scope": scope_or_error, "task": self._serialize_webui_debug_task(task)}

    def cleanup_task_history(self, hours: int = 48) -> int:
        try:
            hours = int(hours)
        except Exception:
            hours = 48
        cutoff = 0 if hours <= 0 else time.time() - hours * 3600
        deleted = 0
        for path in list(self._webui_debug_output_dir().glob("*.json")):
            try:
                if cutoff and path.stat().st_mtime >= cutoff:
                    continue
                data = json.loads(path.read_text(encoding="utf-8"))
                thumbnail = str(data.get("thumbnail") or "") if isinstance(data, dict) else ""
                media_files = data.get("media_files") if isinstance(data, dict) else []
                if thumbnail.startswith("/api/debug/output/"):
                    thumb_name = Path(thumbnail.rsplit("/", 1)[-1]).name
                    thumb_path = self._webui_debug_output_dir() / thumb_name
                    if thumb_path.exists() and thumb_path.is_file():
                        thumb_path.unlink()
                for item in media_files or []:
                    if not isinstance(item, dict):
                        continue
                    filename = Path(str(item.get("filename") or item.get("url", "").rsplit("/", 1)[-1])).name
                    if not filename:
                        continue
                    media_path = self._media_history_dir() / filename
                    if media_path.exists() and media_path.is_file():
                        media_path.unlink()
                path.unlink()
                self._content_audit.remove_task_records(str(data.get("task_id") or path.stem) if isinstance(data, dict) else path.stem)
                deleted += 1
            except Exception as e:
                logger.warning("ComfyUI cleanup task history failed for %s: %s", path, e)
        return deleted

    async def initialize(self) -> None:
        """插件加载完成后启动工作流管理页（若启用）。"""
        config = self.config or {}
        enabled = bool(getattr(config, "webui_enabled", True))
        if not enabled:
            logger.info("ComfyUI 工作流管理页已禁用")
            return
        try:
            from ..management_server import ManagementServer
        except ImportError as e:
            logger.warning("ComfyUI 管理页不可用（请安装 aiohttp）: %s", e)
            return
        host = (getattr(config, "webui_host", None) or "127.0.0.1").strip()
        port = int(getattr(config, "webui_port", 6187) or 6187)
        try:
            self._web_server = ManagementServer(
                workflows_dir=WORKFLOWS_DIR,
                meta_path=META_PATH,
                load_meta=_load_workflow_meta,
                save_meta=_save_workflow_meta,
                plugin_data_dir=PLUGIN_DATA_DIR,
                cleanup_history_func=self.cleanup_task_history,
                ports_config_path=PORTS_CONFIG_PATH,
                active_port_state_path=ACTIVE_PORT_STATE_PATH,
                load_ports_func=lambda: _get_comfyui_ports(self.config or {}),
                save_ports_func=_save_ports_config_file,
                active_port_changed_func=lambda: _sync_active_interface_config(
                    self.config, persist=True
                ),
                debug_submit_func=self.submit_webui_debug_task,
                debug_tasks_func=self.list_webui_debug_tasks,
                debug_history_func=self.list_webui_debug_history,
                debug_task_func=self.get_webui_debug_task,
                debug_delete_func=self.delete_webui_debug_task,
                debug_stop_func=self.stop_webui_debug_task,
                audit_records_func=self.list_audit_records,
                audit_stats_func=self.get_audit_stats,
                audit_get_settings_func=self.get_audit_settings,
                audit_save_settings_func=self.save_audit_settings,
                audit_test_func=self.test_audit_settings,
            )
            await self._web_server.start(host, port)
            if host == "0.0.0.0":
                logger.info(
                    "ComfyUI 工作流管理页已启动，监听 0.0.0.0:%s（本机访问 http://127.0.0.1:%s）",
                    port,
                    port,
                )
            else:
                logger.info("ComfyUI 工作流管理页已启动: http://%s:%s", host, port)
        except Exception as e:
            logger.error("启动 ComfyUI 工作流管理页失败: %s", e, exc_info=True)
            self._web_server = None

    async def terminate(self) -> None:
        """插件卸载时关闭工作流管理页。"""
        if getattr(self, "_web_server", None):
            try:
                await self._web_server.stop()
                logger.info("ComfyUI 工作流管理页已关闭")
            except Exception as e:
                logger.warning("关闭 ComfyUI 工作流管理页时出错: %s", e)
            self._web_server = None

    @filter.on_decorating_result()
    async def on_decorating_result(self, event: AstrMessageEvent) -> None:
        """发送前将消息链中的 [COMFYUI_IMAGE] / [COMFYUI_VIDEO] 占位符替换为实际图片/视频（下载 ComfyUI 输出后以本地文件形式插入）。"""
        session_key = getattr(event, "unified_msg_origin", None) or ""
        if not session_key and hasattr(event, "get_session_id"):
            session_key = event.get_session_id() or ""
        # 图片只在实际插入时消费队列；没有占位符时会自动追加到本次回复末尾。
        image_queue_key = session_key if (session_key and _session_image_url_queue.get(session_key)) else "default"
        image_queue = _session_image_url_queue.get(image_queue_key)
        # 取本会话视频 delivery item（FIFO）
        vq = _session_video_url_queue.get(session_key) or _session_video_url_queue.get("default")
        video_items = list(vq or [])
        vk = session_key if (session_key and _session_video_url_queue.get(session_key)) else "default"
        if vq is not None:
            _session_video_url_queue.pop(vk, None)
        first_video_item = video_items[0] if video_items else None
        if not image_queue and not video_items:
            return
        try:
            result = event.get_result()
        except Exception:
            return
        if result is None:
            return
        chain = getattr(result, "chain", None)
        if chain is None or not isinstance(chain, list):
            return
        try:
            from astrbot.api.message_components import Image, Plain, Video
        except ImportError:
            from astrbot.api.message_components import Image, Plain
            Video = None  # 部分版本可能无 Video 组件
        new_chain: List[Any] = []

        async def _pop_next_image_delivery() -> tuple[Optional[str], Optional[str]]:
            current_queue = _session_image_url_queue.get(image_queue_key)
            if not current_queue:
                return None, None
            item = current_queue.pop(0)
            if not current_queue:
                _session_image_url_queue.pop(image_queue_key, None)
            return await _prepare_image_delivery_for_send(item, session_key)

        def _append_image_segment(image_path: Optional[str], image_notice: Optional[str]) -> None:
            if image_path:
                try:
                    new_chain.append(Image.fromFileSystem(image_path))
                except AttributeError:
                    new_chain.append(Image.from_file_system(image_path))
            elif image_notice:
                new_chain.append(Plain(image_notice))

        image_placeholder_seen = False
        # 先替换图片占位符
        for seg in chain:
            text = getattr(seg, "text", None) if seg is not None else None
            if text is not None and COMFYUI_IMAGE_PLACEHOLDER in (text if isinstance(text, str) else ""):
                image_placeholder_seen = True
                parts = (text or "").split(COMFYUI_IMAGE_PLACEHOLDER)
                for i, p in enumerate(parts):
                    if p:
                        new_chain.append(Plain(p))
                    if i < len(parts) - 1:
                        img_path, img_notice = await _pop_next_image_delivery()
                        _append_image_segment(img_path, img_notice)
            else:
                new_chain.append(seg)
        # 新协议：LLM 不需要写占位符。没有占位符时自动附图；有占位符但图比占位符多，也把剩余图片补在末尾。
        if not image_placeholder_seen or _session_image_url_queue.get(image_queue_key):
            while _session_image_url_queue.get(image_queue_key):
                img_path, img_notice = await _pop_next_image_delivery()
                _append_image_segment(img_path, img_notice)
        # 视频不与文本混在同一条消息：另存到持久化路径，消息中带出路径，再单独发一条视频
        video_path_for_send, video_notice_for_placeholder = await _prepare_video_delivery_for_send(
            first_video_item
        ) if first_video_item else (None, None)
        persistent_video_path: Optional[str] = None
        if video_path_for_send:
            persistent_video_path = await _save_video_to_persistent_path(video_path_for_send, session_key or "")
            if persistent_video_path:
                video_path_for_send = persistent_video_path
        if video_path_for_send or video_notice_for_placeholder:
            # send_message 需要 unified_msg_origin 格式（platform:MessageType:id），不能只用 get_session_id
            session_id = getattr(event, "unified_msg_origin", None) or ""
            if not session_id and hasattr(event, "get_session_id"):
                session_id = str(event.get_session_id() or "")
            # 从 chain 中移除 [COMFYUI_VIDEO] 占位符，并在消息中追加视频路径（便于 qts 返回的 content 被 Bot 解析）
            chain_2: List[Any] = []
            for seg in new_chain:
                text = getattr(seg, "text", None) if seg is not None else None
                if text is not None and COMFYUI_VIDEO_PLACEHOLDER in (text if isinstance(text, str) else ""):
                    replacement = video_notice_for_placeholder or ""
                    new_text = (text or "").replace(COMFYUI_VIDEO_PLACEHOLDER, replacement).strip()
                    if new_text:
                        chain_2.append(Plain(new_text))
                else:
                    chain_2.append(seg)
            new_chain = chain_2
            # 先让本条消息发出，再单独发视频（视频只能独立一条）
            if video_path_for_send and session_id and ":" in session_id:
                _sid = session_id
                _vpath = video_path_for_send

                async def _send_video_later() -> None:
                    await asyncio.sleep(0.3)
                    await _send_video_to_session(_sid, _vpath)

                asyncio.create_task(_send_video_later())
            elif video_path_for_send and session_id:
                logger.warning(
                    "ComfyUI: skip sending video - session_id must be unified_msg_origin (e.g. napcat:GroupMessage:123), got: %s",
                    session_id[:50] if len(session_id) > 50 else session_id,
                )
            if session_id and ":" in session_id and len(video_items) > 1:
                remaining_items = video_items[1:]

                async def _send_remaining_videos_later() -> None:
                    await asyncio.sleep(0.6)
                    for next_item in remaining_items:
                        next_temp, next_notice = await _prepare_video_delivery_for_send(next_item)
                        if next_notice:
                            await _send_plain_to_session(session_id, next_notice)
                            await asyncio.sleep(0.3)
                            continue
                        if not next_temp or not Path(next_temp).exists():
                            continue
                        next_path = await _save_video_to_persistent_path(next_temp, session_key or "") or next_temp
                        await _send_video_to_session(session_id, next_path)
                        await asyncio.sleep(0.3)

                asyncio.create_task(_send_remaining_videos_later())
        if new_chain != chain:
            try:
                chain.clear()
                chain.extend(new_chain)
            except Exception:
                try:
                    setattr(result, "chain", new_chain)
                except Exception:
                    pass

    @filter.command("comfyui_port")
    async def cmd_comfyui_port(self, event: AstrMessageEvent):
        msg = _normalize_prefixed_command_text(event.message_str or "", "comfyui_port")
        config = self.config or {}
        ports = _get_comfyui_ports(config)
        active_port = _get_active_comfyui_port(config)
        if not msg:
            if not ports:
                yield event.plain_result(
                    '当前没有可用 ComfyUI 接口。请去 Management page 添加接口。'
                )
                return
            lines = [f"当前 ComfyUI 接口：{active_port['name']} ({active_port['http']})", "", "可用接口："]
            for port in ports:
                marker = "*" if port["name"] == active_port["name"] else "-"
                workflows = port.get("workflows") or []
                workflow_text = "全部工作流" if not workflows else "、".join(workflows)
                lines.append(f"{marker} {port['name']} ({port['http']})：{workflow_text}")
            lines.append("")
            lines.append("使用 /comfyui_port <接口名称> 切换接口。")
            yield event.plain_result("\n".join(lines))
            return

        target = None
        for port in ports:
            if port["name"] == msg:
                target = port
                break
        if not target:
            names = "、".join(port["name"] for port in ports) or "无"
            yield event.plain_result(f"没有找到 ComfyUI 接口「{msg}」。可用接口：{names}")
            return

        try:
            _write_active_port_name(target["name"])
            _sync_active_interface_config(self.config, persist=True)
        except Exception as e:
            logger.warning("ComfyUI write active port state failed: %s", e)
            yield event.plain_result(f"切换失败：无法保存当前接口配置。{e}")
            return

        workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(_get_workflow_dir()), target)
        yield event.plain_result(
            f"已切换 ComfyUI 接口：{target['name']} ({target['http']})\n"
            f"当前可用工作流：{len(workflows)} 个"
        )

    @filter.command("comfyui")
    async def cmd_comfyui(self, event: AstrMessageEvent):
        """ComfyUI 插件：使用 /comfyui 查询 或 回复一条包含 JSON 文件的消息后发送 /comfyui 上传"""
        msg = _normalize_comfyui_command_text(event.message_str or "")
        if msg == "查询" or msg == "list" or msg == "help":
            active_port = _get_active_comfyui_port(self.config or {})
            wf_dir = _get_workflow_dir()
            workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(wf_dir), active_port)
            if not workflows:
                yield event.plain_result(f"当前 ComfyUI 接口「{active_port['name']}」没有可用工作流。请使用 /comfyui_port 切换接口，或调整该接口的可用工作流配置。")
                return
            lines = []
            command_workflows = [
                w
                for w in workflows
                if _workflow_is_available(w, workflows)
                and (w.get("params") if isinstance(w.get("params"), dict) else {}).get("inspection", {}).get("command_compatible", True)
            ]
            if not command_workflows:
                yield event.plain_result(f"当前 ComfyUI 接口「{active_port['name']}」没有可通过 /comfyui 命令调用的工作流。")
                return
            for idx, w in enumerate(command_workflows, start=1):
                if idx > 1:
                    lines.append("")
                lines.append(f"『{idx}』{w['name']}")
                lines.append("```")
                lines.extend(_escape_telegram_code_block_text(_format_workflow_input_requirements(w)).splitlines())
                lines.append("```")
            yield event.plain_result("\n".join(lines))
            return
        if msg == "上传" or msg == "upload":
            # 从当前消息或回复中取第一个 .json 文件
            chain = getattr(getattr(event, "message_obj", None), "message", None) or []
            reply = getattr(event, "reply", None)
            if reply:
                reply_chain = getattr(getattr(reply, "message_obj", None), "message", None) or getattr(reply, "message", None) or []
                chain = list(reply_chain) + list(chain)
            file_url = None
            file_name = None
            for comp in chain:
                ctype = getattr(comp, "type", None) or (comp.get("type") if isinstance(comp, dict) else None)
                if ctype in ("file", "File", "image", "Image"):
                    url = getattr(comp, "url", None) or (comp.get("url") if isinstance(comp, dict) else None)
                    name = getattr(comp, "name", None) or getattr(comp, "filename", None) or (comp.get("name") or comp.get("filename") if isinstance(comp, dict) else None)
                    if url and name and str(name).endswith(".json"):
                        file_url = url
                        file_name = name
                        break
            if not file_url:
                yield event.plain_result("请回复一条包含 .json 工作流文件的消息，然后发送 /comfyui 上传。")
                return
            _ensure_workflows_dir()
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    r = await client.get(file_url.replace("\n", ""))
                    r.raise_for_status()
                out_path = WORKFLOWS_DIR / (file_name or "workflow.json")
                async with aiofiles.open(out_path, "wb") as f:
                    await f.write(r.content)
                yield event.plain_result(
                    f"已保存工作流到 {out_path.name}。"
                    "请在「工作流管理页」（配置中启用 webui_enabled 并设置 webui_port 后访问对应地址）为该文件填写说明，供 LLM 选择。"
                )
            except Exception as e:
                logger.exception("comfyui upload failed")
                yield event.plain_result(f"上传失败: {e}")
            return
        selector, texts = _split_comfyui_command_args(msg)
        active_port = _get_active_comfyui_port(self.config or {})
        wf_dir = _get_workflow_dir()
        workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(wf_dir), active_port)
        command_workflows = [
            w
            for w in workflows
            if _workflow_is_available(w, workflows)
            and (w.get("params") if isinstance(w.get("params"), dict) else {}).get("inspection", {}).get("command_compatible", True)
        ]
        workflow_name = _resolve_workflow_selector(selector, command_workflows)
        if not workflow_name:
            hidden = next((w for w in workflows if str(w.get("name") or "") == selector), None)
            if hidden:
                unavailable = _workflow_availability_error(hidden, workflows)
                if unavailable:
                    yield event.plain_result(f"该工作流当前不可用：{unavailable} 请在管理页修复后保存。")
                else:
                    yield event.plain_result("该工作流存在媒体可缺省项位于必填项之前，/comfyui 命令无法可靠调用；请使用 LLM 工具调用，或在管理页调整输入编号。")
                return
            yield event.plain_result("用法：/comfyui list | /comfyui upload | /comfyui <工作流名称或编号> <文本1>|<文本2>")
            return
        image_urls, videos = await _extract_command_media_sources_async(event)
        session_tag = _get_sender_id_from_context(event) or _get_session_key(event)
        submit = await _submit_comfyui_workflow(
            event,
            workflow_name,
            texts,
            videos,
            image_urls,
            session_tag,
            event,
            origin="command",
        )
        if not submit.get("ok"):
            yield event.plain_result(submit.get("message", "执行失败。"))
            return
        prompt_id = submit["prompt_id"]
        yield event.plain_result(f"已提交 {workflow_name}，正在等待 ComfyUI 完成...")
        timeout = _get_websocket_wait_timeout(self.config or {})
        wait_result = await _wait_for_command_result(
            event,
            prompt_id,
            submit["server_ip"],
            submit["client_id"],
            submit["session_key"],
            submit["session_tag"],
            timeout,
            submit.get("output_rules"),
        )
        yield event.plain_result(_format_command_result(wait_result))

