# -*- coding: utf-8 -*-
"""Online debug and task-center routes."""

import base64
import io
import json
import re
import time
import uuid
import zipfile
from pathlib import Path

from aiohttp import web

from .context import ManagementContext
from .utils import maybe_await as _maybe_await, safe_basename as _safe_basename


def register_debug_routes(app: web.Application, ctx: ManagementContext) -> None:
    debug_submit_func = ctx.debug_submit_func
    debug_tasks_func = ctx.debug_tasks_func
    debug_history_func = ctx.debug_history_func
    debug_task_func = ctx.debug_task_func
    debug_delete_func = ctx.debug_delete_func
    debug_stop_func = ctx.debug_stop_func
    output_media_dir = ctx.output_media_dir
    media_history_dir = ctx.media_history_dir
    async def debug_submit_handler(request: web.Request) -> web.Response:
        if not debug_submit_func:
            return web.json_response({"ok": False, "error": "debug api unavailable"}, status=501)
        port_name = ""
        workflow_name = ""
        texts: list[str] = []
        images: list[str] = []
        videos: list[str] = []
        image_inputs: list[dict[str, str]] = []
        video_inputs: list[dict[str, object]] = []
        try:
            reader = await request.multipart()
            while True:
                part = await reader.next()
                if part is None:
                    break
                if part.name in ("port_name", "portName"):
                    port_name = (await part.text()).strip()
                elif part.name in ("workflow_name", "workflowName"):
                    workflow_name = (await part.text()).strip()
                elif part.name == "texts":
                    raw = await part.text()
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, list):
                            texts.extend(str(item) for item in parsed if str(item).strip())
                        elif str(parsed).strip():
                            texts.append(str(parsed))
                    except Exception:
                        if raw.strip():
                            texts.append(raw.strip())
                elif part.name in ("text", "texts[]"):
                    text = (await part.text()).strip()
                    if text:
                        texts.append(text)
                elif part.name in ("image", "images", "images[]"):
                    data = bytearray()
                    while True:
                        chunk = await part.read_chunk()
                        if not chunk:
                            break
                        data.extend(chunk)
                    if data:
                        encoded = base64.b64encode(bytes(data)).decode("utf-8")
                        images.append(encoded)
                        content_type = part.headers.get("content-type", "image/png")
                        image_inputs.append(
                            {
                                "name": _safe_basename(part.filename or "image.png"),
                                "data_url": f"data:{content_type};base64,{encoded}",
                            }
                        )
                elif part.name in ("video", "videos", "videos[]"):
                    original_name = _safe_basename(part.filename or "video.mp4")
                    suffix = Path(original_name).suffix.lower() or ".mp4"
                    stem = Path(original_name).stem or "video"
                    safe_stem = re.sub(r"[^a-zA-Z0-9_\-\+=\.\u4e00-\u9fff]+", "_", stem)[:60] or "video"
                    content_type = part.headers.get("content-type", "")
                    if not (content_type.startswith("video/") or suffix in {".mp4", ".webm", ".mov", ".avi", ".mkv"}):
                        while await part.read_chunk():
                            pass
                        continue
                    output_media_dir.mkdir(parents=True, exist_ok=True)
                    filename = f"webui_{uuid.uuid4().hex}_{safe_stem}{suffix}"
                    path = output_media_dir / filename
                    size = 0
                    with path.open("wb") as f:
                        while True:
                            chunk = await part.read_chunk()
                            if not chunk:
                                break
                            f.write(chunk)
                            size += len(chunk)
                    if size:
                        videos.append(filename)
                        video_inputs.append({"name": original_name, "filename": filename, "size": size})
                    else:
                        try:
                            path.unlink(missing_ok=True)
                        except Exception:
                            pass
        except Exception as e:
            return web.json_response({"ok": False, "error": f"invalid form: {e}"}, status=400)
        result = await _maybe_await(
            debug_submit_func(
                {
                    "port_name": port_name,
                    "workflow_name": workflow_name,
                    "texts": texts,
                    "images": images,
                    "videos": videos,
                    "image_inputs": image_inputs,
                    "video_inputs": video_inputs,
                }
            )
        )
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 400
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid debug result"}, status=status)

    async def debug_tasks_handler(request: web.Request) -> web.Response:
        if not debug_tasks_func:
            return web.json_response({"ok": True, "tasks": []})
        origin = request.query.get("origin", "")
        try:
            result = await _maybe_await(debug_tasks_func(origin))
        except TypeError:
            result = await _maybe_await(debug_tasks_func())
        return web.json_response(result if isinstance(result, dict) else {"ok": True, "tasks": []})

    async def debug_history_handler(request: web.Request) -> web.Response:
        if not debug_history_func:
            return web.json_response({"ok": True, "tasks": []})
        origin = request.query.get("origin", "")
        try:
            result = await _maybe_await(debug_history_func(origin))
        except TypeError:
            result = await _maybe_await(debug_history_func())
        return web.json_response(result if isinstance(result, dict) else {"ok": True, "tasks": []})

    async def debug_output_file_handler(request: web.Request) -> web.Response:
        filename = _safe_basename(request.match_info.get("filename", ""))
        if not filename:
            return web.Response(status=404)
        path = media_history_dir / filename
        if path.exists() and path.is_file():
            return web.FileResponse(path)
        return web.Response(status=404)

    async def media_history_file_handler(request: web.Request) -> web.Response:
        filename = _safe_basename(request.match_info.get("filename", ""))
        path = media_history_dir / filename
        if not filename or not path.exists() or not path.is_file():
            return web.Response(status=404)
        return web.FileResponse(path)

    async def debug_task_handler(request: web.Request) -> web.Response:
        if not debug_task_func:
            return web.json_response({"ok": False, "error": "debug api unavailable"}, status=501)
        result = await _maybe_await(debug_task_func(request.match_info.get("task_id", "")))
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 404
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid debug result"}, status=status)

    async def debug_delete_handler(request: web.Request) -> web.Response:
        if not debug_delete_func:
            return web.json_response({"ok": False, "error": "debug api unavailable"}, status=501)
        try:
            data = await request.json()
        except Exception:
            data = {}
        result = await _maybe_await(debug_delete_func(str(data.get("task_id") or "")))
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 404
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid debug result"}, status=status)

    async def debug_bulk_delete_handler(request: web.Request) -> web.Response:
        if not debug_delete_func:
            return web.json_response({"ok": False, "error": "debug api unavailable"}, status=501)
        try:
            data = await request.json()
        except Exception:
            data = {}
        task_ids = data.get("task_ids") if isinstance(data, dict) else []
        if not isinstance(task_ids, list) or not task_ids:
            return web.json_response({"ok": False, "error": "task_ids required"}, status=400)
        deleted = skipped = failed = 0
        errors: list[str] = []
        for raw_id in task_ids:
            task_id = str(raw_id or "").strip()
            if not task_id:
                skipped += 1
                continue
            if not _task_history_json_path(task_id).exists():
                skipped += 1
                continue
            result = await _maybe_await(debug_delete_func(task_id))
            if isinstance(result, dict) and result.get("ok", True):
                if result.get("scope") == "history":
                    deleted += int(result.get("deleted") or 1)
                else:
                    skipped += 1
            else:
                failed += 1
                if isinstance(result, dict) and result.get("error"):
                    errors.append(str(result.get("error")))
        return web.json_response({"ok": True, "deleted": deleted, "skipped": skipped, "failed": failed, "errors": errors[:5]})

    def _task_history_json_path(task_id: str) -> Path:
        safe_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(task_id or ""))
        return media_history_dir / f"{safe_id}.json"

    def _zip_safe_segment(value: object, fallback: str) -> str:
        text = str(value or "").strip() or fallback
        text = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", text)
        text = re.sub(r"\s+", "_", text)
        return text[:80] or fallback

    async def debug_export_media_handler(request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            data = {}
        task_ids = data.get("task_ids") if isinstance(data, dict) else []
        if not isinstance(task_ids, list) or not task_ids:
            return web.json_response({"ok": False, "error": "task_ids required"}, status=400)

        buf = io.BytesIO()
        exported_files = 0
        exported_tasks = 0
        skipped_tasks: list[str] = []
        seen_names: set[str] = set()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for raw_id in task_ids:
                task_id = str(raw_id or "").strip()
                path = _task_history_json_path(task_id)
                if not task_id or not path.exists() or not path.is_file():
                    skipped_tasks.append(task_id or "(empty)")
                    continue
                try:
                    task = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    skipped_tasks.append(task_id)
                    continue
                if not isinstance(task, dict):
                    skipped_tasks.append(task_id)
                    continue
                media_files = [item for item in (task.get("media_files") or []) if isinstance(item, dict)]
                created = task.get("created_at")
                try:
                    created_part = time.strftime("%Y%m%d_%H%M%S", time.localtime(float(created or path.stat().st_mtime)))
                except Exception:
                    created_part = time.strftime("%Y%m%d_%H%M%S", time.localtime(path.stat().st_mtime))
                workflow_part = _zip_safe_segment(task.get("workflow_name") or task.get("workflow_file"), "workflow")
                folder = f"{created_part}_{workflow_part}_{_zip_safe_segment(task_id, 'task')}"
                task_files = 0
                for item in media_files:
                    kind = str(item.get("kind") or item.get("type") or "").lower()
                    filename = Path(str(item.get("filename") or item.get("url", "").rsplit("/", 1)[-1])).name
                    if kind not in {"image", "video"} or not filename:
                        continue
                    media_path = media_history_dir / filename
                    if not media_path.exists() or not media_path.is_file():
                        continue
                    arcname = f"{folder}/{filename}"
                    if arcname in seen_names:
                        stem, suffix = Path(filename).stem, Path(filename).suffix
                        arcname = f"{folder}/{stem}_{task_files + 1}{suffix}"
                    seen_names.add(arcname)
                    zf.write(media_path, arcname)
                    task_files += 1
                    exported_files += 1
                if task_files:
                    exported_tasks += 1
                else:
                    skipped_tasks.append(task_id)
            summary = [
                "ComfyUI Bubble history media export",
                f"Exported tasks: {exported_tasks}",
                f"Exported files: {exported_files}",
                f"Skipped tasks: {len(skipped_tasks)}",
            ]
            if skipped_tasks:
                summary.append("Skipped task ids:")
                summary.extend(f"- {item}" for item in skipped_tasks[:200])
            zf.writestr("export_summary.txt", "\n".join(summary) + "\n")

        if exported_files <= 0:
            return web.json_response({"ok": False, "error": "选中任务没有可导出的本地图片/视频"}, status=400)
        buf.seek(0)
        filename = f"comfyui_history_media_{time.strftime('%Y%m%d_%H%M%S')}.zip"
        return web.Response(
            body=buf.getvalue(),
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            content_type="application/zip",
        )

    async def debug_stop_handler(request: web.Request) -> web.Response:
        if not debug_stop_func:
            return web.json_response({"ok": False, "error": "debug api unavailable"}, status=501)
        try:
            data = await request.json()
        except Exception:
            data = {}
        result = await _maybe_await(debug_stop_func(str(data.get("task_id") or "")))
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 400
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid debug result"}, status=status)

    app.router.add_post("/api/debug/submit", debug_submit_handler)
    app.router.add_get("/api/debug/tasks", debug_tasks_handler)
    app.router.add_get("/api/debug/history", debug_history_handler)
    app.router.add_get("/api/debug/output/{filename}", debug_output_file_handler)
    app.router.add_get("/api/media/history/{filename}", media_history_file_handler)
    app.router.add_get("/api/debug/tasks/{task_id}", debug_task_handler)
    app.router.add_post("/api/debug/delete", debug_delete_handler)
    app.router.add_post("/api/debug/bulk_delete", debug_bulk_delete_handler)
    app.router.add_post("/api/debug/export_media", debug_export_media_handler)
    app.router.add_post("/api/debug/stop", debug_stop_handler)
