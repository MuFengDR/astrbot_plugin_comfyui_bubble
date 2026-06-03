# -*- coding: utf-8 -*-
"""
AstrBot ComfyUI 插件：将工作流封装为 LLM 工具，支持配置上传/管理、等待策略。
"""
import asyncio
import base64
import json
import os
import tempfile
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
        local_dir = Path("/home/ubuntu/AstrBot/data/agent/comfyui/input")
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

from .workflow_engine import (
    ComfyUIWorkflow,
    find_workflow_file,
    list_workflows_in_dir,
    parse_workflow_filename,
)

try:
    from astrbot.api import AstrBotConfig
except ImportError:
    AstrBotConfig = dict

# 插件数据目录：优先使用框架 API，兼容运行目录/多实例
def _resolve_plugin_data_dir() -> Path:
    try:
        from astrbot.api.star import StarTools
        return Path(StarTools.get_data_dir("astrbot_plugin_comfyui_pro"))
    except Exception:
        return Path("data/plugin_data/astrbot_plugin_comfyui_pro").resolve()


PLUGIN_DATA_DIR = _resolve_plugin_data_dir()
WORKFLOWS_DIR = PLUGIN_DATA_DIR / "workflows"
META_PATH = PLUGIN_DATA_DIR / "workflow_meta.json"
ACTIVE_PORT_STATE_PATH = PLUGIN_DATA_DIR / "active_port.json"
PORTS_CONFIG_PATH = PLUGIN_DATA_DIR / "ports_config.json"

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
        return False, values, f"{label}需要 {limit} 个，当前提供 {count} 个。"
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


def _config_get(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def _normalize_comfyui_http(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    return f"http://{raw}"


def _get_comfyui_http_base(server_ip: str) -> str:
    return _normalize_comfyui_http(server_ip or "127.0.0.1:8188")


def _get_comfyui_host(server_ip: str) -> str:
    raw = _get_comfyui_http_base(server_ip)
    try:
        from urllib.parse import urlparse

        parsed = urlparse(raw)
        return (parsed.hostname or "").lower()
    except Exception:
        return raw.replace("http://", "").replace("https://", "").split(":")[0].lower()


def _split_workflow_names(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_items = [str(v) for v in value]
    else:
        text = str(value).replace("，", ",").replace("\n", ",")
        raw_items = text.split(",")
    names: List[str] = []
    seen = set()
    for item in raw_items:
        name = item.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _normalize_comfyui_port_entry(entry: Any, idx: int) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    name = str(entry.get("name") or "").strip()
    http = _normalize_comfyui_http(entry.get("http") or entry.get("server_ip") or "")
    workflows = _split_workflow_names(entry.get("workflows", []))
    if not name and not http:
        return None
    if not name:
        name = f"port{idx}"
    if not http:
        return None
    return {"name": name, "http": http, "workflows": workflows}


def _load_ports_config_file() -> Optional[List[Dict[str, Any]]]:
    try:
        if not PORTS_CONFIG_PATH.exists():
            return None
        data = json.loads(PORTS_CONFIG_PATH.read_text(encoding="utf-8"))
        raw_ports = data.get("ports") if isinstance(data, dict) else data
        if not isinstance(raw_ports, list):
            return None
        ports: List[Dict[str, Any]] = []
        for idx, item in enumerate(raw_ports[:4], start=1):
            port = _normalize_comfyui_port_entry(item, idx)
            if port:
                ports.append(port)
        return ports or None
    except Exception as e:
        logger.warning("ComfyUI read ports config failed: %s", e)
        return None


def _save_ports_config_file(ports: List[Dict[str, Any]]) -> None:
    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate((ports or [])[:4], start=1):
        port = _normalize_comfyui_port_entry(item, idx)
        if port:
            normalized.append(port)
    PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PORTS_CONFIG_PATH.write_text(
        json.dumps({"ports": normalized}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_schema_comfyui_ports(config: Any) -> List[Dict[str, Any]]:
    ports: List[Dict[str, Any]] = []
    for idx in range(1, 5):
        name = str(_config_get(config, f"comfyui_port_{idx}_name", "") or "").strip()
        http = _normalize_comfyui_http(_config_get(config, f"comfyui_port_{idx}_http", "") or "")
        workflows = _split_workflow_names(_config_get(config, f"comfyui_port_{idx}_workflows", ""))
        if not name and not http:
            continue
        if not name:
            name = f"port{idx}"
        if not http:
            continue
        ports.append({"name": name, "http": http, "workflows": workflows})
    if not ports:
        ports.append({"name": "default", "http": "http://127.0.0.1:8188", "workflows": []})
    return ports


def _get_comfyui_ports(config: Any) -> List[Dict[str, Any]]:
    return _load_ports_config_file() or _get_schema_comfyui_ports(config)


def _read_active_port_name() -> str:
    try:
        if ACTIVE_PORT_STATE_PATH.exists():
            data = json.loads(ACTIVE_PORT_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return str(data.get("name") or "").strip()
    except Exception as e:
        logger.warning("ComfyUI read active port state failed: %s", e)
    return ""


def _write_active_port_name(name: str) -> None:
    PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ACTIVE_PORT_STATE_PATH.write_text(
        json.dumps({"name": name}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_active_comfyui_port(config: Any) -> Dict[str, Any]:
    ports = _get_comfyui_ports(config)
    preferred = _read_active_port_name()
    if preferred:
        for port in ports:
            if port["name"] == preferred:
                return port
    return ports[0]


def _workflow_allowed_for_port(workflow: Dict[str, Any], port: Dict[str, Any]) -> bool:
    allowed = port.get("workflows") or []
    return not allowed or workflow.get("name") in allowed


def _filter_workflows_for_port(workflows: List[Dict[str, Any]], port: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [w for w in workflows if _workflow_allowed_for_port(w, port)]


def _get_server_config(config: Any) -> tuple:
    port = _get_active_comfyui_port(config)
    server_ip = port["http"]
    client_id = str(_config_get(config, "client_id", "astrbot-comfyui-pro-1") or "astrbot-comfyui-pro-1").strip()
    return server_ip, client_id


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


def _get_session_key(context: Any) -> str:
    """从工具调用的 context 中解析会话 key（unified_msg_origin），拿不到时返回 'default' 以便仍能命中最近一次提交。"""
    try:
        ctx = getattr(context, "context", None)
        event = getattr(ctx, "event", None) if ctx else None
        if event is None and ctx is not None:
            event = getattr(getattr(ctx, "context", None), "event", None)
        if event is not None:
            umo = getattr(event, "unified_msg_origin", None) or ""
            if umo:
                return umo
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
        agent_ctx = getattr(context, "context", None)
        event = getattr(agent_ctx, "event", None) if agent_ctx else None
        if event is None and agent_ctx is not None:
            event = getattr(getattr(agent_ctx, "context", None), "event", None)
        if event is None and agent_ctx is not None and hasattr(agent_ctx, "extra"):
            extra = getattr(agent_ctx, "extra", None) or {}
            if isinstance(extra, dict):
                event = extra.get("event")
        sid = _sid_from_event(event)
        if sid is not None:
            return sid
    except Exception as e:
        logger.debug("get_session_id_from_context: %s", e)
    return None


def _get_sender_id_from_context(context: Any) -> Optional[str]:
    """从工具调用的 context 中解析发送者的 QQ 号（user_id）。"""
    try:
        agent_ctx = getattr(context, "context", None)
        event = getattr(agent_ctx, "event", None) if agent_ctx else None
        if event is None and agent_ctx is not None:
            event = getattr(getattr(agent_ctx, "context", None), "event", None)
        if event is None and agent_ctx is not None and hasattr(agent_ctx, "extra"):
            extra = getattr(agent_ctx, "extra", None) or {}
            if isinstance(extra, dict):
                event = extra.get("event")
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


def _get_comfyui_output_image_dir() -> Path:
    """返回持久化图片保存目录（data/agent/comfyui/input），与 image_urls 允许的本地路径一致，便于 Bot 从消息 content 中拿到路径后再传回 comfyui_execute。"""
    p = PLUGIN_DATA_DIR.resolve().parent.parent / "agent" / "comfyui" / "input"
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
        temp_path = await _download_image_to_temp(image_url)
        if not temp_path or not Path(temp_path).exists():
            return False
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
                    client_id = item[0] if item[0] else "astrbot-comfyui-pro-1"
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


def _normalize_output_rules_arg(output_rules: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(output_rules, int):
        return {
            "text": {"limit": output_rules, "mode": "loose"},
            "image": {"limit": None, "mode": "loose"},
            "video": {"limit": None, "mode": "loose"},
        }
    if not isinstance(output_rules, dict):
        output_rules = {}
    return {
        "text": output_rules.get("text", {}) if isinstance(output_rules.get("text"), dict) else {},
        "image": output_rules.get("image", {}) if isinstance(output_rules.get("image"), dict) else {},
        "video": output_rules.get("video", {}) if isinstance(output_rules.get("video"), dict) else {},
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
        results.append(
            {
                "task_id": prompt_id,
                "status": "error",
                "type": "error",
                "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。",
            }
        )
        return
    if isinstance(url, dict):
        images = [str(u) for u in (url.get("images") or []) if u]
        videos = [str(u) for u in (url.get("videos") or []) if u]
        audios = [str(u) for u in (url.get("audio") or []) if u]
        if images:
            _session_image_url_queue.setdefault(task_session_key, []).extend(images)
        if videos:
            _session_video_url_queue.setdefault(task_session_key, []).extend(videos)
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": ftype,
                "image_count": len(images),
                "video_count": len(videos),
                "audio_count": len(audios),
                "texts": texts,
                "description": "\n\n".join(texts).strip(),
                "auto_sent": bool(videos),
                "delivery": "queued_by_plugin" if videos else "",
            }
        )
        return
    if not url:
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "text" if texts else "unknown",
                "texts": texts,
                "message": "\n\n".join(texts) if texts else "no output file",
            }
        )
        return
    extra = "\n\n".join(texts).strip()
    if ftype == "image":
        if url:
            _session_image_url_queue.setdefault(task_session_key, []).append(url)
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "image",
                "url": url,
                "texts": texts,
                "description": extra,
            }
        )
    elif ftype == "video":
        _session_video_url_queue.setdefault(task_session_key, []).append(url)
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": "video",
                "auto_sent": True,
                "delivery": "queued_by_plugin",
                "message": "Video is queued for automatic sending. Do NOT call send_message_to_user. Reply with normal text only.",
                "texts": texts,
                "description": extra,
            }
        )
    else:
        results.append(
            {
                "task_id": prompt_id,
                "status": "completed",
                "type": ftype,
                "url": url,
                "texts": texts,
                "description": extra,
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
) -> Dict[str, Any]:
    workflow_name = (workflow_name or "").strip()
    texts = [str(t) for t in (texts or []) if str(t).strip()]
    videos = [str(v).strip() for v in (videos or []) if str(v).strip()]
    image_urls_arg = [str(u).strip() for u in (image_urls_arg or []) if str(u).strip()]
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
    if any(w["name"] == workflow_name for w in all_workflows) and not any(w["name"] == workflow_name for w in workflows):
        available = sorted({w["name"] for w in workflows})
        available_text = "、".join(available) if available else "无"
        return {
            "ok": False,
            "message": (
                f"当前 ComfyUI 来源「{active_port['name']}」不允许使用工作流「{workflow_name}」。\n"
                f"当前来源可用工作流：{available_text}\n"
                "可以使用 /comfyuiport <name> 切换到其他来源，或在插件配置中调整该来源的可用工作流。"
            ),
        }
    images_b64 = await _extract_images_from_event_async(event) if event else []
    if image_urls_arg:
        from_sources = await _image_sources_to_base64(image_urls_arg)
        images_b64.extend(from_sources)
        if from_sources:
            logger.info("[ComfyUI Tool] Injected %d image(s) from image_urls placeholder (URL or local path).", len(from_sources))

    workflow_file = find_workflow_file(
        workflow_name, len(texts), len(images_b64), len(videos), wf_dir, workflow_params
    )
    if not workflow_file:
        matching_names = [w for w in workflows if w["name"] == workflow_name]
        if matching_names:
            required = []
            for w in matching_names:
                required.append(f"'{w['filename']}'（{_format_workflow_required_params(w)}）")
            if required:
                return {
                    "ok": False,
                    "message": (
                        f"工作流「{workflow_name}」存在，但参数数量不匹配。"
                        f"当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}。\n"
                        "可用版本：\n" + "\n".join(f"- {r}" for r in required)
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
                f"没有找到匹配的工作流「{workflow_name}」（当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}）。"
                "请使用 /comfyui list 或 comfyui_list_workflows 查看可用工作流和所需参数。"
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
                f"当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}。"
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
        output_rules = (info.get("params") or {}).get("outputs") or {}
        pending_data = {
            "prompt_id": prompt_id,
            "server_ip": server_ip,
            "client_id": client_id,
            "session_key": session_key,
            "session_tag": session_tag,
            "output_rules": output_rules,
        }
        _session_pending[session_key] = pending_data
        if session_key != "default":
            _session_pending["default"] = pending_data
        _task_registry[prompt_id] = pending_data
        if session_tag not in _session_tag_tasks:
            _session_tag_tasks[session_tag] = []
        if prompt_id not in _session_tag_tasks[session_tag]:
            _session_tag_tasks[session_tag].append(prompt_id)
        return {
            "ok": True,
            "prompt_id": prompt_id,
            "workflow_name": workflow_name,
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


def _split_comfyui_command_args(msg: str) -> tuple[str, List[str]]:
    parts = (msg or "").strip().split(maxsplit=1)
    if not parts:
        return "", []
    selector = parts[0].strip()
    text_part = parts[1].strip() if len(parts) > 1 else ""
    text_part = text_part.replace("｜", "|")
    texts = [part.strip() for part in text_part.split("|") if part.strip()]
    return selector, texts


def _normalize_comfyui_command_text(raw: str) -> str:
    msg = (raw or "").strip()
    for prefix in ("/comfyui", "comfyui"):
        if msg == prefix:
            return ""
        if msg.startswith(prefix + " "):
            return msg[len(prefix) :].strip()
    return msg


def _normalize_prefixed_command_text(raw: str, command: str) -> str:
    msg = (raw or "").strip()
    command = command.strip().lstrip("/")
    for prefix in (f"/{command}", command):
        if msg == prefix:
            return ""
        if msg.startswith(prefix + " "):
            return msg[len(prefix) :].strip()
    return msg


def _resolve_workflow_selector(selector: str, workflows: List[Dict[str, Any]]) -> Optional[str]:
    selector = (selector or "").strip()
    if not selector:
        return None
    if selector.isdigit():
        index = int(selector)
        if 1 <= index <= len(workflows):
            return workflows[index - 1]["name"]
        return None
    return selector


def _format_workflow_required_params(workflow: Dict[str, Any]) -> str:
    def fmt_rule(label: str, rule: Dict[str, Any]) -> str:
        limit = rule.get("limit") if isinstance(rule, dict) else None
        mode = "强" if isinstance(rule, dict) and rule.get("mode") == "strict" else "弱"
        return f"{label}任意" if limit is None else f"{label}{limit}({mode})"

    params = workflow.get("params") if isinstance(workflow.get("params"), dict) else {}
    inputs = params.get("inputs") if isinstance(params.get("inputs"), dict) else {}
    outputs = params.get("outputs") if isinstance(params.get("outputs"), dict) else {}
    in_text = "输入：" + "、".join(
        [
            fmt_rule("文本", inputs.get("text", {})),
            fmt_rule("图片", inputs.get("image", {})),
            fmt_rule("视频", inputs.get("video", {})),
        ]
    )
    out_text = "输出：" + "、".join(
        [
            fmt_rule("文本", outputs.get("text", {})),
            fmt_rule("图片", outputs.get("image", {})),
            fmt_rule("视频", outputs.get("video", {})),
        ]
    )
    return f"{in_text}；{out_text}"


def _escape_markdown_table_cell(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def _escape_telegram_code_block_text(value: Any) -> str:
    text = str(value or "").strip()
    return text.replace("```", "｀｀｀") or "（未填写说明）"


def _extract_command_media_sources(event: AstrMessageEvent) -> tuple[List[str], List[str]]:
    image_urls: List[str] = []
    videos: List[str] = []
    chain = getattr(getattr(event, "message_obj", None), "message", None) or []
    for comp in chain:
        ctype = getattr(comp, "type", None) or (comp.get("type") if isinstance(comp, dict) else None)
        url = getattr(comp, "url", None) or (comp.get("url") if isinstance(comp, dict) else None)
        file_path = getattr(comp, "file", None) or (comp.get("file") if isinstance(comp, dict) else None)
        name = (
            getattr(comp, "name", None)
            or getattr(comp, "filename", None)
            or ((comp.get("name") or comp.get("filename")) if isinstance(comp, dict) else None)
        )
        source = str(url or file_path or "").strip()
        ctype_text = str(ctype or "").lower()
        name_text = str(name or source).lower()
        if ctype_text in ("video",) and source:
            videos.append(Path(source).name if not source.startswith(("http://", "https://")) else source)
        elif ctype_text in ("file",) and source:
            if name_text.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp")):
                image_urls.append(source)
            elif name_text.endswith((".mp4", ".mov", ".webm", ".mkv", ".avi")):
                videos.append(Path(source).name if not source.startswith(("http://", "https://")) else source)
    return image_urls, videos


async def _extract_command_media_sources_async(event: AstrMessageEvent) -> tuple[List[str], List[str]]:
    image_urls, videos = _extract_command_media_sources(event)
    try:
        quoted_images = await extract_quoted_message_images(event)
        if quoted_images:
            logger.info("ComfyUI command extracted %d quoted image(s).", len(quoted_images))
            image_urls.extend(quoted_images)
    except Exception as e:
        logger.warning("ComfyUI command extract quoted images failed: %s", e)

    deduped_images: List[str] = []
    seen_images = set()
    for image_url in image_urls:
        image_url = str(image_url or "").strip()
        if not image_url or image_url in seen_images:
            continue
        seen_images.add(image_url)
        deduped_images.append(image_url)
    return deduped_images, videos


async def _wait_for_command_result(
    context: Any,
    prompt_id: str,
    server_ip: str,
    client_id: str,
    session_key: str,
    session_tag: str,
    timeout: int,
    output_rules: Any = None,
) -> Dict[str, Any]:
    results: List[Dict[str, Any]] = []
    url, ftype, texts = await _get_result_for_prompt(server_ip, prompt_id, output_rules)
    if ftype == "error":
        return {"status": "error", "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"}
    if url:
        _cleanup_completed_task(prompt_id, session_tag)
        await _append_completed_task_result(results, context, prompt_id, server_ip, session_key, url, ftype, texts)
        elapsed = await _get_prompt_elapsed_seconds(server_ip, prompt_id)
        return {"status": "completed", "results": results, "elapsed_seconds": elapsed}

    wait_result = await _wait_for_comfyui_ws_completion_many(server_ip, client_id, [prompt_id], timeout)
    status_info = wait_result.get(prompt_id) or {"status": "timeout", "message": f"wait timed out after {timeout} seconds"}
    status = status_info.get("status")
    if status == "completed":
        url, ftype, texts = await _get_result_for_prompt(server_ip, prompt_id, output_rules)
        if ftype == "error":
            _cleanup_completed_task(prompt_id, session_tag)
            return {"status": "error", "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"}
        _cleanup_completed_task(prompt_id, session_tag)
        await _append_completed_task_result(results, context, prompt_id, server_ip, session_key, url, ftype, texts)
        elapsed = await _get_prompt_elapsed_seconds(server_ip, prompt_id)
        return {"status": "completed", "results": results, "elapsed_seconds": elapsed}
    if status in ("error", "interrupted"):
        _cleanup_completed_task(prompt_id, session_tag)
        return {"status": status, "message": status_info.get("message", status)}
    if status == "ws_unavailable":
        return {"status": "error", "message": status_info.get("message", COMFYUI_WS_UNAVAILABLE_MESSAGE)}
    return {"status": "pending", "message": status_info.get("message", "not completed yet")}


def _format_command_result(wait_result: Dict[str, Any]) -> str:
    if wait_result.get("status") != "completed":
        return wait_result.get("message", "ComfyUI 任务未完成。")
    elapsed = wait_result.get("elapsed_seconds")
    elapsed_text = ""
    if isinstance(elapsed, (int, float)) and elapsed >= 0:
        elapsed_text = f"\uff08\u8017\u65f6 {elapsed:.1f} \u79d2\uff09"
    results = wait_result.get("results") or []
    for item in results:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "completed":
            continue
        ftype = item.get("type")
        texts = item.get("texts") or []
        text_body = "\n\n".join(str(t).strip() for t in texts if str(t).strip())
        prefix = f"完成{elapsed_text}："
        image_count = int(item.get("image_count", 0) or (1 if ftype == "image" else 0))
        video_count = int(item.get("video_count", 0) or (1 if ftype == "video" else 0))
        image_placeholders = COMFYUI_IMAGE_PLACEHOLDER * max(0, image_count)
        if ftype in ("image", "mixed") and image_count:
            if text_body:
                suffix = f"\n\n{image_placeholders}"
                if video_count:
                    suffix += "\n\n视频已发送。"
                return f"{prefix}{text_body}{suffix}"
            suffix = image_placeholders
            if video_count:
                suffix += "\n\n视频已发送。"
            return prefix + suffix
        if ftype == "video" or (ftype == "mixed" and video_count):
            if text_body:
                return f"{prefix}{text_body}\n\n视频已发送。"
            return f"\u5b8c\u6210{elapsed_text}\uff0c\u89c6\u9891\u5df2\u53d1\u9001\u3002"
        if ftype == "text" and text_body:
            return f"{prefix}{text_body}"
        if ftype:
            if text_body:
                return f"{prefix}{text_body}\n\n输出类型：{ftype}。"
            return f"\u5b8c\u6210{elapsed_text}\uff0c\u8f93\u51fa\u7c7b\u578b\uff1a{ftype}\u3002"
    return f"\u5b8c\u6210{elapsed_text}\uff0c\u4f46\u6ca1\u6709\u8f93\u51fa\u6587\u4ef6\u3002"


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
            continue
        s = s.strip()
        # 1) data:image/xxx;base64,<payload>
        if s.startswith("data:image") and "base64," in s:
            b64 = _extract_base64_from_data_uri(s)
            if b64:
                result.append(b64)
                logger.info("[ComfyUI Tool] Using image from data URI (base64) in image_urls.")
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
                continue
            if not _is_allowed_local_image_path(p):
                logger.warning("ComfyUI rejected local image path (outside allowed dir): %s", s[:80])
                continue
            try:
                async with aiofiles.open(p, "rb") as f:
                    data = await f.read()
                result.append(base64.b64encode(data).decode("utf-8"))
            except Exception as e:
                logger.warning("ComfyUI read local image failed: %s", e)
            continue
        # 4) 服务器 URL
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(s.replace("\n", ""))
                if resp.status_code == 200 and resp.content:
                    result.append(base64.b64encode(resp.content).decode("utf-8"))
        except Exception as e:
            logger.warning("ComfyUI fetch image_url failed: %s", e)
    return result


# --------------- LLM Tools ---------------


@dataclass
class ComfyUIListWorkflowsTool(FunctionTool[AstrAgentContext]):
    """查询当前可用的 ComfyUI 工作流列表及说明、所需参数，供 LLM 选择工作流时使用。"""

    name: str = "comfyui_list_workflows"
    description: str = "列出所有可用的 ComfyUI 工作流名称及详细说明（包括文本参数说明）。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {},
            "required": [],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        logger.info("[ComfyUI Tool] comfyui_list_workflows called with args: %s", kwargs)
        config = _plugin_config
        if not config:
            return "插件配置不可用。"
        server_ip, _ = _get_server_config(config)
        active_port = _get_active_comfyui_port(config)
        descriptions = await _load_workflow_descriptions(config)
        text_slots = _load_workflow_text_slots()
        wf_dir = _get_workflow_dir()
        workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(wf_dir), active_port)
        if not workflows:
            return f"当前 ComfyUI 来源「{active_port['name']}」没有可用工作流。请使用 /comfyuiport 切换来源，或调整该来源的可用工作流配置。"
        
        # 返回工作流名称和简短说明
        lines = [f"Current ComfyUI: {active_port['name']} ({server_ip})", "Available workflows:"]
        for w in workflows:
            name = w["name"]
            filename = w.get("filename", "")
            # 取简短说明
            desc_data = descriptions.get(filename, {})
            if isinstance(desc_data, dict):
                short_desc = desc_data.get("short", "") or "(无说明)"
            else:
                short_desc = str(desc_data)[:50] if desc_data else "(无说明)"
            slots = text_slots.get(filename, [])
            slot_info = ""
            if slots:
                slot_info = f" | Text slots: {', '.join(slots)}"
            lines.append(f"- {name}: {short_desc} | {_format_workflow_required_params(w)}{slot_info}")
        
        return "\n".join(lines)


class ComfyUIGetWorkflowDetailTool(FunctionTool[AstrAgentContext]):
    """
    获取指定工作流的详细说明。
    当需要了解某个工作流的详细用途、参数说明时使用。
    """

    name: str = "comfyui_get_workflow_detail"
    description: str = "获取指定工作流的详细说明。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "workflow_name": {
                    "type": "string",
                    "description": "工作流名称（从 comfyui_list_workflows 获取）。",
                },
            },
            "required": ["workflow_name"],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        logger.info("[ComfyUI Tool] comfyui_get_workflow_detail called with args: %s", kwargs)
        workflow_name = (kwargs.get("workflow_name") or "").strip()
        if not workflow_name:
            return "缺少工作流名称。"
        
        config = _plugin_config
        if not config:
            return "插件配置不可用。"
        
        active_port = _get_active_comfyui_port(config)
        descriptions = await _load_workflow_descriptions(config)
        text_slots = _load_workflow_text_slots()
        wf_dir = _get_workflow_dir()
        workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(wf_dir), active_port)
        
        # 查找对应的工作流
        target_wf = None
        for w in workflows:
            if w["name"] == workflow_name:
                target_wf = w
                break
        
        if not target_wf:
            return f"未找到工作流「{workflow_name}」。"
        
        filename = target_wf.get("filename", "")
        desc_data = descriptions.get(filename, {})
        
        if isinstance(desc_data, dict):
            short_desc = desc_data.get("short", "")
            detailed_desc = desc_data.get("detailed", "")
        else:
            short_desc = str(desc_data) if desc_data else ""
            detailed_desc = short_desc
        
        slots = text_slots.get(filename, [])
        
        result = f"Workflow: {workflow_name}\n"
        result += f"Filename: {filename}\n"
        result += f"Params: {_format_workflow_required_params(target_wf)}\n"
        result += f"Short description: {short_desc or '(无)'}\n"
        result += f"Detailed description: {detailed_desc or '(无)'}\n"
        if slots:
            result += f"Text slots: {', '.join(slots)}"
        
        return result



@dataclass
class ComfyUIStatusTool(FunctionTool[AstrAgentContext]):
    """
    查询 ComfyUI 队列状态。
    查询运行中/等待中的任务数量；任务结果等待由 comfyui_query_wait 通过 WebSocket 处理。
    """

    name: str = "comfyui_status"
    description: str = "查询 ComfyUI 队列状态，包括运行中/等待中的任务数量。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {},
            "required": [],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        logger.info("[ComfyUI Tool] comfyui_status called with args: %s", kwargs)
        config = _plugin_config
        if not config:
            return "插件配置不可用。"
        wait_threshold = _get_wait_threshold(config)
        server_ip, _ = _get_server_config(config)
        session_key = _get_session_key(context.context)
        pending = _session_pending.get(session_key) or _session_pending.get("default")
        if pending and pending.get("prompt_id") and server_ip:
            output_rules = pending.get("output_rules")
            remaining = await _estimate_remaining_seconds(server_ip, pending["prompt_id"])
            if remaining == 0:
                url, ftype, texts = await _get_result_for_prompt(server_ip, pending["prompt_id"], output_rules)
                for k in list(_session_pending.keys()):
                    if _session_pending.get(k) == pending:
                        _session_pending.pop(k, None)
                _task_registry.pop(pending.get("prompt_id"), None)
                if isinstance(url, dict):
                    images = url.get("images") or []
                    videos = url.get("videos") or []
                    if images:
                        _session_image_url_queue.setdefault(session_key, []).extend(images)
                    if videos:
                        _session_video_url_queue.setdefault(session_key, []).extend(videos)
                    placeholders = COMFYUI_IMAGE_PLACEHOLDER * len(images)
                    video_text = " Video is queued for automatic sending." if videos else ""
                    return f"Task completed. Output: {ftype}. {placeholders}{video_text} Queue: 0 running, 0 pending."
                if url:
                    if ftype == "image":
                        if _is_local_image_url(url, server_ip):
                            _session_image_url_queue.setdefault(session_key, []).append(url)
                            return (
                                f"Task completed. Output: image. In your reply you MUST include exactly this placeholder to show the result image: {COMFYUI_IMAGE_PLACEHOLDER}. "
                                "Do not use any URL or markdown image. Example: '完成！" + COMFYUI_IMAGE_PLACEHOLDER + " 这是手办化效果。' Queue: 0 running, 0 pending."
                            )
                        session_id = _get_session_id_from_context(context.context)
                        if session_id:
                            await _send_image_to_session(session_id, url, "图好了～")
                        return f"Task completed. Output: image. Image has been sent to the user. IMAGE_URL: {url} Queue: 0 running, 0 pending."
                    if ftype == "video":
                        _session_video_url_queue.setdefault(session_key, []).append(url)
                        return (
                            f"Task completed. Output: video. Do NOT call send_message_to_user for this video (it will become voice). "
                            f"In your reply you MUST include only text containing {COMFYUI_VIDEO_PLACEHOLDER}; the plugin will send the video as a separate message. Queue: 0 running, 0 pending."
                        )
                    return f"Task completed. Output: {ftype}. URL: {url} Queue: 0 running, 0 pending."
                return "Task completed (no output file). Queue: 0 running, 0 pending."
            if remaining < wait_threshold:
                client_id = pending.get("client_id", "")
                url, ftype, texts = await _wait_for_completion(
                    server_ip, client_id, pending["prompt_id"], timeout=remaining + 120, output_rules=output_rules
                )
                for k in list(_session_pending.keys()):
                    if _session_pending.get(k) == pending:
                        _session_pending.pop(k, None)
                _task_registry.pop(pending.get("prompt_id"), None)
                if isinstance(url, dict):
                    images = url.get("images") or []
                    videos = url.get("videos") or []
                    if images:
                        _session_image_url_queue.setdefault(session_key, []).extend(images)
                    if videos:
                        _session_video_url_queue.setdefault(session_key, []).extend(videos)
                    placeholders = COMFYUI_IMAGE_PLACEHOLDER * len(images)
                    video_text = " Video is queued for automatic sending." if videos else ""
                    return f"Task completed. Output: {ftype}. {placeholders}{video_text} Queue: 0 running, 0 pending."
                if url:
                    if ftype == "image":
                        if _is_local_image_url(url, server_ip):
                            _session_image_url_queue.setdefault(session_key, []).append(url)
                            return (
                                f"Task completed. Output: image. In your reply you MUST include exactly this placeholder to show the result image: {COMFYUI_IMAGE_PLACEHOLDER}. "
                                "Do not use any URL or markdown image. Example: '完成！" + COMFYUI_IMAGE_PLACEHOLDER + " 这是手办化效果。' Queue: 0 running, 0 pending."
                            )
                        session_id = _get_session_id_from_context(context.context)
                        if session_id:
                            await _send_image_to_session(session_id, url, "图好了～")
                        return f"Task completed. Output: image. Image has been sent to the user. IMAGE_URL: {url} Queue: 0 running, 0 pending."
                    if ftype == "video":
                        _session_video_url_queue.setdefault(session_key, []).append(url)
                        return (
                            f"Task completed. Output: video. Do NOT call send_message_to_user for this video (it will become voice). "
                            f"In your reply you MUST include only text containing {COMFYUI_VIDEO_PLACEHOLDER}; the plugin will send the video as a separate message. Queue: 0 running, 0 pending."
                        )
                    return f"Task completed. Output: {ftype}. URL: {url} Queue: 0 running, 0 pending."
                return "Task finished. Queue: 0 running, 0 pending."
            await asyncio.sleep(wait_threshold)
            running, pending_count = await _get_queue_status(server_ip)
            remaining_after = await _estimate_remaining_seconds(server_ip, pending["prompt_id"])
            return (
                f"ComfyUI queue: {running} running, {pending_count} pending. "
                f"Your task estimated remaining: about {remaining_after} seconds. Call again to re-check."
            )
        running, pending_count = await _get_queue_status(server_ip)
        if running < 0:
            return "ComfyUI server unreachable. Please check server_ip and network."
        return f"ComfyUI queue: {running} running, {pending_count} pending."


@dataclass
class ComfyUIQueryWaitTool(FunctionTool[AstrAgentContext]):
    """
    查询 ComfyUI 任务状态并等待完成。
    ⚠️ 重要：查询时传入 session_tag（发送者的 QQ 号），会自动返回该用户提交的所有任务结果。
    如果需要生成 N 张图，先用 comfyui_execute 调用 N 次（每次返回不同 task_id），
    然后调用本工具一次（带 session_tag），批量获取所有任务结果。
    """

    name: str = "comfyui_query_wait"
    description: str = "批量查询所有任务状态（传入 session_tag）。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "session_tag": {
                    "type": "string",
                    "description": "REQUIRED. The sender's QQ number (the person who sent the command). Example: '123456789'. Use this to query all tasks submitted by this user.",
                },
                "task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional. List of specific task IDs (prompt_id) to query. Example: ['uuid1', 'uuid2'].",
                },
                "count": {
                    "type": "integer",
                    "description": "Optional. Query the most recent N tasks. Default: 20.",
                },
            },
            "required": ["session_tag"],
        }
    )

    description = (
        "Wait for ComfyUI WebSocket completion events and return task results. "
        "Pass session_tag and optionally task_ids/count. Do not pass a wait time; "
        "timeout is configured by websocket_wait_timeout_seconds. Images and videos are handled by the plugin. "
        "If a video result is returned as queued_by_plugin/auto_sent, do NOT call send_message_to_user; reply with normal text only."
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        logger.info("[ComfyUI Tool] comfyui_query_wait called with args: %s", kwargs)
        config = _plugin_config or {}
        server_ip, client_id_cfg = _get_server_config(config)
        session_key = _get_session_key(context.context)
        
        # 支持的查询方式：
        # 1. session_tag: 查询该标识下所有任务（默认自动填充为发送者的 QQ 号）
        # 2. task_ids: 精确查询指定任务列表
        # 3. 兼容旧版: task_id (单个)
        
        # 自动获取发送者的 QQ 号作为 session_tag
        sender_id = _get_sender_id_from_context(context.context)
        session_tag = (kwargs.get("session_tag") or "").strip()
        if not session_tag and sender_id:
            session_tag = sender_id
            logger.info("[ComfyUI Tool] Auto-filled session_tag with sender_id: %s", session_tag)
        task_ids_arg = kwargs.get("task_ids") or []
        if isinstance(task_ids_arg, str):
            task_ids_arg = [task_ids_arg]
        task_ids_arg = [tid.strip() for tid in task_ids_arg if tid and isinstance(tid, str)]
        
        # 兼容旧版
        old_task_id = (kwargs.get("task_id") or "").strip()
        if old_task_id and old_task_id not in task_ids_arg:
            task_ids_arg.append(old_task_id)
        
        # 等待一段时间后再查询（避免频繁轮询）
        # 默认等待 30 秒，最小 30 秒，最大 900 秒（15 分钟）
        wait_seconds = _get_websocket_wait_timeout(config)
        
        logger.info("[ComfyUI Tool] Will wait up to %d seconds for ComfyUI WebSocket events", wait_seconds)
        
        # 如果提供了 session_tag，从 session_tag_tasks 获取任务列表
        if session_tag:
            all_task_ids = _session_tag_tasks.get(session_tag, [])
            # 支持 count 参数限制数量，默认最多 20 个
            count = kwargs.get("count")
            if count and isinstance(count, int) and count > 0:
                all_task_ids = all_task_ids[-count:]
            else:
                all_task_ids = all_task_ids[-20:]  # 默认最多返回 20 个
            task_ids_arg = all_task_ids
            logger.info("[ComfyUI Tool] Query by session_tag '%s', got %d tasks", session_tag, len(task_ids_arg))
        
        # 如果仍然没有任务，尝试从队列恢复
        if not task_ids_arg:
            running_n, pending_n = await _get_queue_status(server_ip)
            if running_n >= 0 and (running_n + pending_n) == 1:
                first = await _get_first_task_from_queue(server_ip)
                if first:
                    prompt_id_first, client_id_first = first
                    task_ids_arg = [prompt_id_first]
                    pending = {
                        "prompt_id": prompt_id_first,
                        "server_ip": server_ip,
                        "client_id": client_id_first or client_id_cfg,
                        "session_key": session_key,
                    }
                    _session_pending[session_key] = pending
                    if session_key != "default":
                        _session_pending["default"] = pending
                    _task_registry[prompt_id_first] = pending
                    logger.info("[ComfyUI Tool] Recovered pending from queue: %s", prompt_id_first)
        
        if not task_ids_arg:
            return "No pending ComfyUI task found. Submit a workflow with comfyui_execute first."

        results = []
        wait_targets = []
        for task_id in task_ids_arg:
            pending = _task_registry.get(task_id)
            if not pending:
                results.append({"task_id": task_id, "status": "error", "message": "not found in registry"})
                continue

            pending = dict(pending)
            task_session_key = pending.get("session_key") or session_key
            task_session_tag = pending.get("session_tag", "")
            prompt_id = pending.get("prompt_id")
            task_server_ip = pending.get("server_ip") or server_ip
            output_rules = pending.get("output_rules")
            task_client_id = pending.get("client_id") or client_id_cfg
            output_rules = pending.get("output_rules")

            if not prompt_id or not task_server_ip:
                results.append({"task_id": task_id, "status": "error", "message": "invalid task data"})
                continue

            url, ftype, texts = await _get_result_for_prompt(task_server_ip, prompt_id, output_rules)
            if url or ftype in ("text", "error"):
                _cleanup_completed_task(prompt_id, task_session_tag)
                await _append_completed_task_result(
                    results,
                    context.context,
                    prompt_id,
                    task_server_ip,
                    task_session_key,
                    url,
                    ftype,
                    texts,
                )
                continue

            history_state = await _get_prompt_history_state(task_server_ip, prompt_id)
            history_status = history_state.get("status_str", "")
            if history_state.get("exists") and history_status in ("error", "failed"):
                _cleanup_completed_task(prompt_id, task_session_tag)
                results.append(
                    {
                        "task_id": prompt_id,
                        "status": "error",
                        "message": history_state.get("message") or "ComfyUI execution failed",
                    }
                )
                continue
            if history_state.get("exists") and history_state.get("completed"):
                _cleanup_completed_task(prompt_id, task_session_tag)
                results.append({"task_id": prompt_id, "status": "completed", "message": "no output file"})
                continue

            if wait_seconds <= 0:
                results.append({"task_id": prompt_id, "status": "pending", "message": "not completed yet"})
                continue

            wait_targets.append(
                {
                    "prompt_id": prompt_id,
                    "server_ip": task_server_ip,
                    "client_id": task_client_id,
                    "session_key": task_session_key,
                    "session_tag": task_session_tag,
                    "output_rules": output_rules,
                }
            )

        if wait_targets:
            grouped_wait_targets = {}
            for item in wait_targets:
                grouped_wait_targets.setdefault((item["server_ip"], item["client_id"]), []).append(item)
            grouped_wait_results = await asyncio.gather(
                *[
                    _wait_for_comfyui_ws_completion_many(
                        server_ip,
                        client_id,
                        [item["prompt_id"] for item in items],
                        wait_seconds,
                    )
                    for (server_ip, client_id), items in grouped_wait_targets.items()
                ]
            )
            wait_results_by_prompt = {}
            for group_result in grouped_wait_results:
                wait_results_by_prompt.update(group_result)
            wait_results = [
                wait_results_by_prompt.get(
                    item["prompt_id"],
                    {"status": "timeout", "message": f"wait timed out after {wait_seconds} seconds"},
                )
                for item in wait_targets
            ]
            for item, wait_result in zip(wait_targets, wait_results):
                prompt_id = item["prompt_id"]
                status = wait_result.get("status")
                if status == "completed":
                    url, ftype, texts = await _get_result_for_prompt(item["server_ip"], prompt_id, item.get("output_rules"))
                    _cleanup_completed_task(prompt_id, item["session_tag"])
                    await _append_completed_task_result(
                        results,
                        context.context,
                        prompt_id,
                        item["server_ip"],
                        item["session_key"],
                        url,
                        ftype,
                        texts,
                    )
                elif status in ("error", "interrupted"):
                    _cleanup_completed_task(prompt_id, item["session_tag"])
                    results.append(
                        {
                            "task_id": prompt_id,
                            "status": status,
                            "message": wait_result.get("message", status),
                        }
                    )
                elif status == "ws_unavailable":
                    results.append(
                        {
                            "task_id": prompt_id,
                            "status": "error",
                            "message": wait_result.get("message", COMFYUI_WS_UNAVAILABLE_MESSAGE),
                        }
                    )
                else:
                    url, ftype, texts = await _get_result_for_prompt(item["server_ip"], prompt_id, item.get("output_rules"))
                    if url or ftype in ("text", "error"):
                        _cleanup_completed_task(prompt_id, item["session_tag"])
                        await _append_completed_task_result(
                            results,
                            context.context,
                            prompt_id,
                            item["server_ip"],
                            item["session_key"],
                            url,
                            ftype,
                            texts,
                        )
                    else:
                        history_state = await _get_prompt_history_state(item["server_ip"], prompt_id)
                        history_status = history_state.get("status_str", "")
                        if history_state.get("exists") and history_status in ("error", "failed"):
                            _cleanup_completed_task(prompt_id, item["session_tag"])
                            results.append(
                                {
                                    "task_id": prompt_id,
                                    "status": "error",
                                    "message": history_state.get("message") or "ComfyUI execution failed",
                                }
                            )
                        elif history_state.get("exists") and history_state.get("completed"):
                            _cleanup_completed_task(prompt_id, item["session_tag"])
                            results.append({"task_id": prompt_id, "status": "completed", "message": "no output file"})
                        else:
                            results.append(
                                {
                                    "task_id": prompt_id,
                                    "status": "pending",
                                    "message": wait_result.get("message", "not completed yet"),
                                }
                            )

        completed_tasks = []
        pending_count = 0
        for r in results:
            if isinstance(r, dict):
                if r.get("status") == "completed" and r.get("type") == "image" and r.get("url", "").startswith("http"):
                    completed_tasks.append(r)
                elif r.get("status") == "pending":
                    pending_count += 1

        for task in completed_tasks:
            url = task.get("url", "")
            if url and url.startswith("http"):
                local_path = await _download_url_to_local(url)
                if local_path and local_path != url:
                    task["local_path"] = local_path
                    task["url"] = local_path

        response = {
            "results": results,
            "summary": {
                "total": len(results),
                "completed": len(results) - pending_count,
                "pending": pending_count,
            },
        }
        if pending_count > 0:
            response["message"] = f"{pending_count} task(s) still pending. Call comfyui_query_wait again to check."

        return json.dumps(response, ensure_ascii=False, indent=2)
        
        # 批量查询多个任务
        results = []
        completed_tasks = []
        for task_id in task_ids_arg:
            pending = _task_registry.get(task_id)
            if not pending:
                results.append({"task_id": task_id, "status": "error", "message": "not found in registry"})
                continue
            
            pending = dict(pending)
            task_session_key = pending.get("session_key") or session_key
            task_session_tag = pending.get("session_tag", "")
            prompt_id = pending.get("prompt_id")
            task_server_ip = pending.get("server_ip") or server_ip
            
            if not prompt_id or not task_server_ip:
                results.append({"task_id": task_id, "status": "error", "message": "invalid task data"})
                continue
            
            remaining = await _estimate_remaining_seconds(task_server_ip, prompt_id)
            
            if remaining == 0:
                # 任务完成
                url, ftype, texts = await _get_result_for_prompt(task_server_ip, prompt_id, output_rules)
                # 清理
                for k in list(_session_pending.keys()):
                    if _session_pending.get(k) and _session_pending.get(k).get("prompt_id") == prompt_id:
                        _session_pending.pop(k, None)
                _task_registry.pop(prompt_id, None)
                # 从 session_tag_tasks 中移除
                if task_session_tag and task_session_tag in _session_tag_tasks:
                    if prompt_id in _session_tag_tasks[task_session_tag]:
                        _session_tag_tasks[task_session_tag].remove(prompt_id)
                
                if url:
                    extra = (" Text: " + "; ".join(texts)) if texts else ""
                    if ftype == "image":
                        if url:
                            _session_image_url_queue.setdefault(task_session_key, []).append(url)
                            results.append({
                                "task_id": prompt_id,
                                "status": "completed",
                                "type": "image",
                                "url": url,
                                "description": extra.strip()
                            })
                    elif ftype == "video":
                        _session_video_url_queue.setdefault(task_session_key, []).append(url)
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": "video",
                            "auto_sent": True,
                            "delivery": "queued_by_plugin",
                            "message": "Video is queued for automatic sending. Do NOT call send_message_to_user. Reply with normal text only.",
                            "description": extra.strip()
                        })
                    else:
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": ftype,
                            "url": url,
                            "description": extra.strip()
                        })
                else:
                    results.append({
                        "task_id": prompt_id,
                        "status": "completed",
                        "message": "no output file"
                    })
            elif remaining < wait_threshold:
                # 等待时间不长，直接等待完成
                client_id = pending.get("client_id", "")
                url, ftype, texts = await _wait_for_completion(task_server_ip, client_id, prompt_id, timeout=remaining + 120, output_rules=output_rules)
                # 清理
                for k in list(_session_pending.keys()):
                    if _session_pending.get(k) and _session_pending.get(k).get("prompt_id") == prompt_id:
                        _session_pending.pop(k, None)
                _task_registry.pop(prompt_id, None)
                # 从 session_tag_tasks 中移除
                if task_session_tag and task_session_tag in _session_tag_tasks:
                    if prompt_id in _session_tag_tasks[task_session_tag]:
                        _session_tag_tasks[task_session_tag].remove(prompt_id)
                
                if url:
                    extra = (" Text: " + "; ".join(texts)) if texts else ""
                    if ftype == "image":
                        if url:
                            _session_image_url_queue.setdefault(task_session_key, []).append(url)
                            results.append({
                                "task_id": prompt_id,
                                "status": "completed",
                                "type": "image",
                                "url": url,
                                "description": extra.strip()
                            })
                    elif ftype == "video":
                        _session_video_url_queue.setdefault(task_session_key, []).append(url)
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": "video",
                            "auto_sent": True,
                            "delivery": "queued_by_plugin",
                            "message": "Video is queued for automatic sending. Do NOT call send_message_to_user. Reply with normal text only.",
                            "description": extra.strip()
                        })
                    else:
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": ftype,
                            "url": url,
                            "description": extra.strip()
                        })
                else:
                    results.append({
                        "task_id": prompt_id,
                        "status": "completed",
                        "message": "no output file"
                    })
            else:
                # 仍在队列中
                results.append({
                    "task_id": prompt_id,
                    "status": "pending",
                    "message": f"still in queue, estimated ~{remaining} seconds"
                })
        
        # 收集所有已完成任务的图片URL，准备下载到本地
        completed_tasks = []
        pending_count = 0
        for r in results:
            if isinstance(r, dict):
                if r.get("status") == "completed" and r.get("type") == "image" and r.get("url", "").startswith("http"):
                    completed_tasks.append(r)
                elif r.get("status") == "pending":
                    pending_count += 1
            else:
                if "still in queue" in str(r):
                    pending_count += 1
        
        # 下载所有远程图片到本地
        for task in completed_tasks:
            url = task.get("url", "")
            if url and url.startswith("http"):
                local_path = await _download_url_to_local(url)
                if local_path and local_path != url:
                    task["local_path"] = local_path
                    task["url"] = local_path  # 替换为本地路径
        
        # 返回 JSON 格式
        response = {
            "results": results,
            "summary": {
                "total": len(results),
                "completed": len(results) - pending_count,
                "pending": pending_count
            }
        }
        if pending_count > 0:
            response["message"] = f"{pending_count} task(s) still in queue. Call comfyui_query_wait again to check."
        
        return json.dumps(response, ensure_ascii=False, indent=2)


@dataclass
class ComfyUIExecuteTool(FunctionTool[AstrAgentContext]):
    """
    执行指定的 ComfyUI 工作流。工作流名称需与 list_workflows 返回的 name 一致。
    文本参数通过 texts 传入；图片从当前会话消息中自动提取；若工作流需要图而消息无图，可传 image_urls（占位符），插件会下载并转 base64 注入。
    ⚠️ 重要：如果需要生成多张图片（如 N 张），必须调用本工具 N 次（每次生成一张），所有任务会并行执行。
    每次调用会返回一个 task_id，之后用 comfyui_query_wait（传入 session_tag）批量查询所有任务的结果。
    """

    name: str = "comfyui_execute"
    description: str = "执行 ComfyUI 工作流（生成多张图需多次调用）。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "workflow_name": {
                    "type": "string",
                    "description": "Exact workflow name (e.g. from comfyui_list_workflows).",
                },
                "texts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Required when workflow needs text. Content must match workflow description and text_slots (see comfyui_list_workflows)—e.g. modification instruction like '根据图2的XX修改图1', not just image content description. Generate according to workflow requirement and user intent.",
                },
                "videos": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of video filenames (.mp4) on server for video workflows.",
                },
                "image_urls": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Image source(s) when message has none. Prefer HTTP URL or local path (plugin data dir or data/agent/comfyui/input). Do not paste raw base64.",
                },
                "session_tag": {
                    "type": "string",
                    "description": "REQUIRED. The sender's QQ number (the person who sent the command). This is used to track all tasks for this user. Example: '123456789'. Do not use your own QQ number, use the sender's QQ number.",
                },
            },
            "required": ["workflow_name", "session_tag"],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        # 日志脱敏：不输出 base64，避免进入 LLM 或日志留存
        logger.info(
            "[ComfyUI Tool] comfyui_execute called: workflow_name=%r, texts=%r, videos=%r, image_urls=%s",
            kwargs.get("workflow_name"),
            kwargs.get("texts"),
            kwargs.get("videos"),
            _sanitize_image_urls_for_log(kwargs.get("image_urls")),
        )
        workflow_name = (kwargs.get("workflow_name") or "").strip()
        texts = kwargs.get("texts") or []
        videos = list(kwargs.get("videos") or [])
        image_urls_arg = kwargs.get("image_urls") or []
        # 自动获取发送者的 QQ 号作为 session_tag
        sender_id = _get_sender_id_from_context(context.context)
        session_tag = (kwargs.get("session_tag") or "").strip()
        if not session_tag and sender_id:
            session_tag = sender_id
            logger.info("[ComfyUI Tool] Auto-filled session_tag with sender_id: %s", session_tag)
        if isinstance(image_urls_arg, str):
            image_urls_arg = [image_urls_arg]
        image_urls_arg = [u for u in image_urls_arg if u and isinstance(u, str)]
        if not workflow_name:
            return "缺少工作流名称。"
        if not session_tag:
            return "无法识别发送者标识，无法登记 ComfyUI 任务。"
        config = _plugin_config
        if not config:
            return "插件配置不可用。"
        server_ip, client_id = _get_server_config(config)
        wf_dir = _get_workflow_dir()
        ctx = getattr(context.context, "context", None)
        event = getattr(ctx, "event", None) if ctx else None
        submit = await _submit_comfyui_workflow(
            context.context,
            workflow_name,
            texts,
            videos,
            image_urls_arg,
            session_tag,
            event,
        )
        if not submit.get("ok"):
            return submit.get("message", "执行失败。")
        all_uuids = submit.get("all_task_ids", [])
        uuid_list_str = ", ".join(f'"{u}"' for u in all_uuids)
        prompt_id = submit["prompt_id"]
        return (
            f"Workflow '{workflow_name}' submitted. Task ID (prompt_id): {prompt_id}. "
            f"You have {len(all_uuids)} task(s) with session_tag '{session_tag}'. All task IDs: [{uuid_list_str}]. "
            f"IMPORTANT: You MUST immediately call comfyui_query_wait with session_tag='{session_tag}' and task_ids=['{prompt_id}'] to wait for the result. "
            "Do not reply to the user before calling comfyui_query_wait."
            + submit.get("desc_reminder", "")
        )
        images_b64 = await _extract_images_from_event_async(event) if event else []
        if image_urls_arg:
            from_sources = await _image_sources_to_base64(image_urls_arg)
            images_b64.extend(from_sources)
            if from_sources:
                logger.info("[ComfyUI Tool] Injected %d image(s) from image_urls placeholder (URL or local path).", len(from_sources))
        workflow_file = find_workflow_file(
            workflow_name, len(texts), len(images_b64), len(videos), wf_dir, _load_workflow_params()
        )
        # 获取工作流列表供错误提示使用
        workflows = _list_workflows_in_configured_dir(wf_dir)
        
        if not workflow_file:
            # 检查是否有同名工作流但参数不匹配
            matching_names = [w for w in workflows if w["name"] == workflow_name]
            
            if matching_names:
                # 同名工作流存在，检查参数需求
                required = []
                for w in matching_names:
                    required.append(f"'{w['filename']}' ({_format_workflow_required_params(w)})")
                
                if required:
                    return (
                        f"工作流 '{workflow_name}' 存在，但参数不匹配。你传了 texts={len(texts)}, images={len(images_b64)}, videos={len(videos)}。\n"
                        f"该工作流有以下版本可用：\n" + "\n".join(f"- {r}" for r in required) + "\n"
                        f"请选择正确的版本（参数数量匹配的工作流），或提供正确数量的参数。"
                    )
            
            hint = ""
            if len(images_b64) == 0:
                hint = (
                    " Current message has no image (images=0). image_urls accepted: (1) HTTP URL—plugin will download; "
                    "(2) local path under plugin data dir or under data/agent/comfyui/input (use absolute path e.g. /path/to/AstrBot/data/agent/comfyui/input/xxx.jpg). "
                    "If you have a local file, copy it to data/agent/comfyui/input/ then pass that path in image_urls."
                )
            return (
                f"没有找到匹配的工作流「{workflow_name}」（当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}）。"
                "请使用 comfyui_list_workflows 查看可用工作流和所需参数。"
                "Possible reasons: (1) workflow name typo—use exact name from list; (2) too few texts/images/videos—check required counts; (3) image_urls rejected (URL failed or path outside allowed dir)."
                + hint
            )
        info = _get_configured_workflow_info(wf_dir, Path(workflow_file).name)
        if not info:
            return "工作流配置不可用，无法解析输入输出参数。请在工作流管理页保存该工作流的参数配置。"
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
                f"\n\n[工作流「{workflow_name}」说明 (下次调用请按此生成 texts): {workflow_desc}"
                "\n文本须按上述说明填写（如「根据图2的XX修改图1」），不要只传图片内容描述。]"
            )
        ok_inputs, texts, images_b64, videos, input_error = _apply_workflow_input_rules(info, texts, images_b64, videos)
        if not ok_inputs:
            return (
                f"工作流「{workflow_name}」参数数量不匹配。"
                f"当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}。"
                + (" " + input_error if input_error else "")
                + desc_reminder
            )
        try:
            debug = bool(getattr(config, "debug_mode", False) if not isinstance(config, dict) else config.get("debug_mode", False))
            workflow = ComfyUIWorkflow(server_ip, client_id)
            workflow.load_workflow_api(workflow_file)
            prompt_id = await workflow.submit_only(images_b64, texts, videos, debug=debug)
            session_key = _get_session_key(context.context)
            output_rules = (info.get("params") or {}).get("outputs") or {}
            pending_data = {
                "prompt_id": prompt_id,
                "server_ip": server_ip,
                "client_id": client_id,
                "session_key": session_key,
                "session_tag": session_tag,
                "output_rules": output_rules,
            }
            _session_pending[session_key] = pending_data
            if session_key != "default":
                _session_pending["default"] = pending_data
            _task_registry[prompt_id] = pending_data
            
            # 注册到 session_tag_tasks
            if session_tag not in _session_tag_tasks:
                _session_tag_tasks[session_tag] = []
            if prompt_id not in _session_tag_tasks[session_tag]:
                _session_tag_tasks[session_tag].append(prompt_id)
            
            # 获取该 session_tag 下所有任务 UUID
            all_uuids = _session_tag_tasks.get(session_tag, [])
            uuid_list_str = ", ".join(f'"{u}"' for u in all_uuids)
            
            return (
                f"Workflow '{workflow_name}' submitted. Task ID (prompt_id): {prompt_id}. "
                f"You have {len(all_uuids)} task(s) with session_tag '{session_tag}'. All task IDs: [{uuid_list_str}]. "
                f"IMPORTANT: You MUST immediately call comfyui_query_wait with session_tag='{session_tag}' and task_ids=['{prompt_id}'] to wait for the result. "
                "Do not reply to the user before calling comfyui_query_wait."
                + desc_reminder
            )
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
            return msg + (" 建议修复工作流，或换用当前 ComfyUI 服务器可运行的工作流。" if summary else " 可能原因：工作流节点/输入不匹配、图片格式无效，或服务器错误。") + desc_reminder
        except Exception as e:
            logger.exception("comfyui_execute failed")
            return (
                f"执行失败：{e}。"
                "可能原因：ComfyUI 服务器不可达或超时、工作流节点错误、输入无效。"
                "请检查服务器地址和工作流 JSON 是否有效。"
                + desc_reminder
            )


# --------------- Plugin ---------------


@register(
    "comfyui_pro",
    "ComfyUI Pro",
    "ComfyUI Pro：执行/查询工作流、WebSocket 等待、手动命令、多来源切换与工作流 WebUI 管理",
    "1.0.2",
    "",
)
class ComfyUIPlugin(Star):
    def __init__(self, context: Context, config: Any = None):
        super().__init__(context)
        global _plugin_config, _plugin_context
        _plugin_config = self.config = config or {}
        _plugin_context = self.context
        self.context.add_llm_tools(
            ComfyUIListWorkflowsTool(),
            ComfyUIStatusTool(),
            ComfyUIQueryWaitTool(),
            ComfyUIExecuteTool(),
        )
        self._web_server = None  # ManagementServer 实例，在 initialize 中启动

    async def initialize(self) -> None:
        """插件加载完成后启动工作流管理页（若启用）。"""
        config = self.config or {}
        enabled = bool(getattr(config, "webui_enabled", True))
        if not enabled:
            logger.info("ComfyUI 工作流管理页已禁用")
            return
        try:
            from .management_server import ManagementServer
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
                ports_config_path=PORTS_CONFIG_PATH,
                active_port_state_path=ACTIVE_PORT_STATE_PATH,
                load_ports_func=lambda: _get_comfyui_ports(self.config or {}),
                save_ports_func=_save_ports_config_file,
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
        # 取本会话图片 URL（FIFO）
        iq = _session_image_url_queue.get(session_key) or _session_image_url_queue.get("default")
        image_url = iq.pop(0) if (iq and len(iq) > 0) else None
        ik = session_key if (session_key and _session_image_url_queue.get(session_key)) else "default"
        if iq is not None and len(iq) == 0:
            _session_image_url_queue.pop(ik, None)
        # 取本会话视频 URL（FIFO）
        vq = _session_video_url_queue.get(session_key) or _session_video_url_queue.get("default")
        video_urls = list(vq or [])
        vk = session_key if (session_key and _session_video_url_queue.get(session_key)) else "default"
        if vq is not None:
            _session_video_url_queue.pop(vk, None)
        video_url = video_urls[0] if video_urls else None
        if not image_url and not video_urls:
            return
        temp_path = await _download_image_to_temp(image_url) if image_url else None
        if image_url and (not temp_path or not Path(temp_path).exists()):
            temp_path = None
        video_temp_path = await _download_media_to_temp(video_url, ".mp4") if video_url else None
        if video_url and (not video_temp_path or not Path(video_temp_path).exists()):
            video_temp_path = None
        try:
            result = event.get_result()
        except Exception:
            return
        if result is None:
            return
        chain = getattr(result, "chain", None)
        if not chain or not isinstance(chain, list):
            return
        try:
            from astrbot.api.message_components import Image, Plain, Video
        except ImportError:
            from astrbot.api.message_components import Image, Plain
            Video = None  # 部分版本可能无 Video 组件
        new_chain: List[Any] = []
        # 将图片另存到持久化路径，消息中带出路径，便于 qts_get_recent_messages 返回的 content 被 Bot 解析后用于下一轮 image_urls
        image_path_for_send = temp_path
        persistent_image_path: Optional[str] = None
        if temp_path:
            persistent_image_path = await _save_image_to_persistent_path(temp_path, session_key or "")
            if persistent_image_path:
                image_path_for_send = persistent_image_path
        first_image_path_for_placeholder = image_path_for_send if temp_path else None
        # 先替换图片占位符
        for seg in chain:
            text = getattr(seg, "text", None) if seg is not None else None
            if text is not None and COMFYUI_IMAGE_PLACEHOLDER in (text if isinstance(text, str) else ""):
                parts = (text or "").split(COMFYUI_IMAGE_PLACEHOLDER)
                current_iq = _session_image_url_queue.get(session_key) or _session_image_url_queue.get("default")
                # 重新从队列获取图片（每次占位符对应一张图）
                current_iq = _session_image_url_queue.get(session_key) or _session_image_url_queue.get("default")
                for i, p in enumerate(parts):
                    if p:
                        new_chain.append(Plain(p))
                    if i < len(parts) - 1:
                        img_path = None
                        # 取一张图片
                        img_path = None
                        if first_image_path_for_placeholder:
                            img_path = first_image_path_for_placeholder
                            first_image_path_for_placeholder = None
                        elif current_iq and len(current_iq) > 0:
                            img_url = current_iq.pop(0)
                            # 下载并保存
                            temp_img = await _download_image_to_temp(img_url) if img_url else None
                            if temp_img and Path(temp_img).exists():
                                perm_img = await _save_image_to_persistent_path(temp_img, session_key or "")
                                if perm_img:
                                    img_path = perm_img
                                else:
                                    img_path = temp_img
                            else:
                                # 尝试直接用 URL
                                img_path = img_url
                            # 更新队列
                            if current_iq is not None and len(current_iq) == 0:
                                ik = session_key if (session_key and _session_image_url_queue.get(session_key)) else "default"
                                _session_image_url_queue.pop(ik, None)
                        if img_path:
                            try:
                                new_chain.append(Image.fromFileSystem(img_path))
                            except AttributeError:
                                new_chain.append(Image.from_file_system(img_path))
                            # 在消息中追加路径
            else:
                new_chain.append(seg)
        # 视频不与文本混在同一条消息：另存到持久化路径，消息中带出路径，再单独发一条视频
        video_path_for_send = video_temp_path
        persistent_video_path: Optional[str] = None
        if video_temp_path:
            persistent_video_path = await _save_video_to_persistent_path(video_temp_path, session_key or "")
            if persistent_video_path:
                video_path_for_send = persistent_video_path
        if video_temp_path:
            # send_message 需要 unified_msg_origin 格式（platform:MessageType:id），不能只用 get_session_id
            session_id = getattr(event, "unified_msg_origin", None) or ""
            if not session_id and hasattr(event, "get_session_id"):
                session_id = str(event.get_session_id() or "")
            # 从 chain 中移除 [COMFYUI_VIDEO] 占位符，并在消息中追加视频路径（便于 qts 返回的 content 被 Bot 解析）
            chain_2: List[Any] = []
            for seg in new_chain:
                text = getattr(seg, "text", None) if seg is not None else None
                if text is not None and COMFYUI_VIDEO_PLACEHOLDER in (text if isinstance(text, str) else ""):
                    new_text = (text or "").replace(COMFYUI_VIDEO_PLACEHOLDER, "").strip()
                    if new_text:
                        chain_2.append(Plain(new_text))
                else:
                    chain_2.append(seg)
            new_chain = chain_2
            # 先让本条消息发出，再单独发视频（视频只能独立一条）
            if session_id and ":" in session_id:
                _sid = session_id
                _vpath = video_path_for_send

                async def _send_video_later() -> None:
                    await asyncio.sleep(0.3)
                    await _send_video_to_session(_sid, _vpath)

                asyncio.create_task(_send_video_later())
            elif session_id:
                logger.warning(
                    "ComfyUI: skip sending video - session_id must be unified_msg_origin (e.g. napcat:GroupMessage:123), got: %s",
                    session_id[:50] if len(session_id) > 50 else session_id,
                )
            if session_id and ":" in session_id and len(video_urls) > 1:
                remaining_urls = video_urls[1:]

                async def _send_remaining_videos_later() -> None:
                    await asyncio.sleep(0.6)
                    for next_url in remaining_urls:
                        next_temp = await _download_media_to_temp(next_url, ".mp4")
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

    @filter.command("comfyuiport")
    async def cmd_comfyuiport(self, event: AstrMessageEvent):
        msg = _normalize_prefixed_command_text(event.message_str or "", "comfyuiport")
        config = self.config or {}
        ports = _get_comfyui_ports(config)
        active_port = _get_active_comfyui_port(config)
        if not msg:
            lines = [f"当前 ComfyUI：{active_port['name']} ({active_port['http']})", "", "可用来源："]
            for port in ports:
                marker = "*" if port["name"] == active_port["name"] else "-"
                workflows = port.get("workflows") or []
                workflow_text = "全部工作流" if not workflows else "、".join(workflows)
                lines.append(f"{marker} {port['name']} ({port['http']})：{workflow_text}")
            lines.append("")
            lines.append("使用 /comfyuiport <name> 切换来源。")
            yield event.plain_result("\n".join(lines))
            return

        target = None
        for port in ports:
            if port["name"] == msg:
                target = port
                break
        if not target:
            names = "、".join(port["name"] for port in ports) or "无"
            yield event.plain_result(f"没有找到 ComfyUI 来源「{msg}」。可用来源：{names}")
            return

        try:
            _write_active_port_name(target["name"])
        except Exception as e:
            logger.warning("ComfyUI write active port state failed: %s", e)
            yield event.plain_result(f"切换失败：无法保存当前来源配置。{e}")
            return

        workflows = _filter_workflows_for_port(_list_workflows_in_configured_dir(_get_workflow_dir()), target)
        yield event.plain_result(
            f"已切换 ComfyUI 来源：{target['name']} ({target['http']})\n"
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
            descriptions = await _load_workflow_descriptions(self.config)
            if not workflows:
                yield event.plain_result(f"当前 ComfyUI 来源「{active_port['name']}」没有可用工作流。请使用 /comfyuiport 切换来源，或调整该来源的可用工作流配置。")
                return
            lines = []
            for idx, w in enumerate(workflows, start=1):
                desc_data = descriptions.get(w["filename"])
                if isinstance(desc_data, dict):
                    desc = desc_data.get("short", "") or "（未填写说明）"
                else:
                    desc = str(desc_data) if desc_data else "（未填写说明）"
                if idx > 1:
                    lines.append("")
                lines.append(f"『{idx}』")
                lines.append(f"> {w['name']}")
                lines.append("")
                lines.append("```")
                lines.extend(_escape_telegram_code_block_text(desc).splitlines())
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
        workflow_name = _resolve_workflow_selector(selector, workflows)
        if not workflow_name:
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
