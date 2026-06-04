# -*- coding: utf-8 -*-
"""Helpers used by the /comfyui and /comfyui_port commands."""

from pathlib import Path
from typing import Any, Dict, List, Optional

from astrbot.api.event import AstrMessageEvent

from ..core import plugin as runtime


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
            videos.append(runtime.Path(source).name if not source.startswith(("http://", "https://")) else source)
        elif ctype_text in ("file",) and source:
            if name_text.endswith((".png", ".jpg", ".jpeg", ".webp", ".bmp")):
                image_urls.append(source)
            elif name_text.endswith((".mp4", ".mov", ".webm", ".mkv", ".avi")):
                videos.append(runtime.Path(source).name if not source.startswith(("http://", "https://")) else source)
    return image_urls, videos


async def _extract_command_media_sources_async(event: AstrMessageEvent) -> tuple[List[str], List[str]]:
    image_urls, videos = _extract_command_media_sources(event)
    try:
        quoted_images = await runtime.extract_quoted_message_images(event)
        if quoted_images:
            runtime.logger.info("ComfyUI command extracted %d quoted image(s).", len(quoted_images))
            image_urls.extend(quoted_images)
    except Exception as e:
        runtime.logger.warning("ComfyUI command extract quoted images failed: %s", e)

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
    url, ftype, texts = await runtime._get_result_for_prompt(server_ip, prompt_id, output_rules)
    if ftype == "error":
        return {"status": "error", "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"}
    if url:
        runtime._cleanup_completed_task(prompt_id, session_tag)
        await runtime._append_completed_task_result(results, context, prompt_id, server_ip, session_key, url, ftype, texts)
        elapsed = await runtime._get_prompt_elapsed_seconds(server_ip, prompt_id)
        return {"status": "completed", "results": results, "elapsed_seconds": elapsed}

    wait_result = await runtime._wait_for_comfyui_ws_completion_many(server_ip, client_id, [prompt_id], timeout)
    status_info = wait_result.get(prompt_id) or {"status": "timeout", "message": f"wait timed out after {timeout} seconds"}
    status = status_info.get("status")
    if status == "completed":
        url, ftype, texts = await runtime._get_result_for_prompt(server_ip, prompt_id, output_rules)
        if ftype == "error":
            runtime._cleanup_completed_task(prompt_id, session_tag)
            return {"status": "error", "message": "\n".join(texts) if texts else "ComfyUI 输出数量不匹配。"}
        runtime._cleanup_completed_task(prompt_id, session_tag)
        await runtime._append_completed_task_result(results, context, prompt_id, server_ip, session_key, url, ftype, texts)
        elapsed = await runtime._get_prompt_elapsed_seconds(server_ip, prompt_id)
        return {"status": "completed", "results": results, "elapsed_seconds": elapsed}
    if status in ("error", "interrupted"):
        runtime._cleanup_completed_task(prompt_id, session_tag)
        return {"status": status, "message": status_info.get("message", status)}
    if status == "ws_unavailable":
        return {"status": "error", "message": status_info.get("message", runtime.COMFYUI_WS_UNAVAILABLE_MESSAGE)}
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
        image_placeholders = runtime.COMFYUI_IMAGE_PLACEHOLDER * max(0, image_count)
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


__all__ = ['_split_comfyui_command_args', '_normalize_comfyui_command_text', '_normalize_prefixed_command_text', '_resolve_workflow_selector', '_format_workflow_required_params', '_escape_markdown_table_cell', '_escape_telegram_code_block_text', '_extract_command_media_sources', '_extract_command_media_sources_async', '_wait_for_command_result', '_format_command_result']
