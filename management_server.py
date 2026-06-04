"""ComfyUI Bubble management web server."""
import base64
import io
import json
import re
import time
import uuid
import zipfile
from collections.abc import Callable
from pathlib import Path

from aiohttp import web

from .workflow_engine import parse_workflow_filename

# 瀹夊叏鏂囦欢鍚嶏細鍙繚鐣欏畨鍏ㄥ瓧绗?
SAFE_FILENAME_RE = re.compile(r"^[a-zA-Z0-9_\-\+=\.\u4e00-\u9fff]+$")


def _safe_basename(name: str) -> str:
    """Return a safe basename to prevent path traversal."""
    return Path(name).name.strip()


def create_app(
    workflows_dir: Path,
    meta_path: Path,
    load_meta: Callable[[], dict[str, str]],
    save_meta: Callable[[dict[str, str]], None],
    plugin_data_dir: Path | None = None,
    cleanup_history_func: Callable[[], int] | None = None,
    ports_config_path: Path | None = None,
    active_port_state_path: Path | None = None,
    load_ports_func: Callable[[], list[dict[str, object]]] | None = None,
    save_ports_func: Callable[[list[dict[str, object]]], None] | None = None,
    active_port_changed_func: Callable[[], None] | None = None,
    debug_submit_func: Callable[[dict[str, object]], object] | None = None,
    debug_tasks_func: Callable[[], object] | None = None,
    debug_history_func: Callable[[], object] | None = None,
    debug_task_func: Callable[[str], object] | None = None,
    debug_delete_func: Callable[[str], object] | None = None,
    debug_stop_func: Callable[[str], object] | None = None,
) -> web.Application:
    app = web.Application()
    if plugin_data_dir is None:
        plugin_data_dir = workflows_dir.parent
    output_media_dir = (
        plugin_data_dir.resolve().parent.parent / "agent" / "comfyui" / "input"
    )
    tmp_dir = plugin_data_dir / "tmp"
    media_history_dir = plugin_data_dir / "media" / "history"
    if ports_config_path is None:
        ports_config_path = plugin_data_dir / "ports_config.json"
    if active_port_state_path is None:
        active_port_state_path = plugin_data_dir / "active_port.json"
    logo_path = Path(__file__).with_name("webui_logo.jpg")
    webui_output_dir = plugin_data_dir / "webui" / "output"

    # 淇濆瓨娓呯悊鍘嗗彶璁板綍鐨勫嚱鏁板紩鐢?
    _cleanup_history = cleanup_history_func

    async def _maybe_await(value):
        if hasattr(value, "__await__"):
            return await value
        return value

    def _load_workflow_params() -> dict[str, object]:
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("workflow_params"), dict):
                return data["workflow_params"]
        except Exception:
            pass
        return {}

    def _save_workflow_params(params: dict[str, object]) -> None:
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        existing: dict[str, object] = {}
        if meta_path.exists():
            try:
                data = json.loads(meta_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    existing = dict(data)
            except Exception:
                pass
        existing["workflow_params"] = params
        meta_path.write_text(
            json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _normalize_workflow_payload(
        body: dict[str, object],
    ) -> tuple[str, dict[str, str], dict[str, object]]:
        filename = _safe_basename(body.get("filename") or "")
        if not filename or not filename.endswith(".json"):
            raise ValueError("invalid filename")
        if not (workflows_dir / filename).exists():
            raise FileNotFoundError("file not found in workflows")

        raw_description = body.get("description")
        if isinstance(raw_description, dict):
            short = str(raw_description.get("short") or "").strip()
            detailed = str(raw_description.get("detailed") or "").strip()
        else:
            short = str(raw_description or "").strip()
            detailed = ""

        params = body.get("params")
        if not isinstance(params, dict):
            raise TypeError("invalid params")
        return filename, {"short": short, "detailed": detailed}, params

    def _save_workflow_payload(
        filename: str, description: dict[str, str], params: dict[str, object]
    ) -> None:
        meta = load_meta()
        current = meta.get(filename)
        if isinstance(current, dict):
            current["short"] = description.get("short", "")
            current["detailed"] = description.get("detailed", "")
            meta[filename] = current
        else:
            meta[filename] = {
                "short": description.get("short", ""),
                "detailed": description.get("detailed", "") or str(current or ""),
            }
        save_meta(meta)

        all_params = _load_workflow_params()
        all_params[filename] = params
        _save_workflow_params(all_params)

    def _validate_unique_workflow_names(
        updates: list[tuple[str, dict[str, str], dict[str, object]]]
    ) -> None:
        all_params = _load_workflow_params()
        merged = dict(all_params) if isinstance(all_params, dict) else {}
        for filename, _, params in updates:
            merged[filename] = params
        seen: dict[str, str] = {}
        if workflows_dir.exists():
            for f in sorted(workflows_dir.glob("*.json")):
                params = merged.get(f.name, {})
                name = ""
                if isinstance(params, dict):
                    name = str(params.get("name") or "").strip()
                if not name:
                    continue
                if name in seen and seen[name] != f.name:
                    raise ValueError(
                        f"工作流调用名称「{name}」重复：{seen[name]} 与 {f.name}"
                    )
                seen[name] = f.name

    async def list_handler(_: web.Request) -> web.Response:
        """List workflow JSON files and metadata."""
        meta = load_meta()
        workflow_params = _load_workflow_params()
        files = []
        if workflows_dir.exists():
            for f in sorted(workflows_dir.glob("*.json")):
                name = f.name
                params = (
                    workflow_params.get(name, {})
                    if isinstance(workflow_params, dict)
                    else {}
                )
                display_name = params.get("name") if isinstance(params, dict) else ""
                files.append(
                    {
                        "filename": name,
                        "name": display_name
                        or (parse_workflow_filename(name) or {}).get(
                            "name", name.removesuffix(".json")
                        ),
                        "description": meta.get(name, ""),
                        "params": params,
                    }
                )
        return web.json_response({"files": files})

    async def upload_handler(request: web.Request) -> web.Response:
        """Upload a workflow JSON file."""
        reader = await request.multipart()
        field = None
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                field = part
                break
        if field is None:
            return web.json_response(
                {"ok": False, "error": "missing field: file"}, status=400
            )
        filename = _safe_basename(field.filename or "workflow.json")
        if not filename.lower().endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "only .json allowed"}, status=400
            )
        if not SAFE_FILENAME_RE.match(filename):
            return web.json_response(
                {"ok": False, "error": "invalid filename"}, status=400
            )
        workflows_dir.mkdir(parents=True, exist_ok=True)
        path = workflows_dir / filename
        size = 0
        with open(path, "wb") as out:
            while True:
                chunk = await field.read_chunk()
                if not chunk:
                    break
                size += len(chunk)
                out.write(chunk)
        return web.json_response({"ok": True, "filename": filename, "size": size})

    async def description_handler(request: web.Request) -> web.Response:
        """Save short workflow description."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        description = (body.get("description") or "").strip()
        if not filename or not filename.endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "invalid filename"}, status=400
            )
        if not (workflows_dir / filename).exists():
            return web.json_response(
                {"ok": False, "error": "file not found in workflows"}, status=404
            )
        meta = load_meta()
        current = meta.get(filename)
        if isinstance(current, dict):
            current["short"] = description
            meta[filename] = current
        else:
            meta[filename] = {"short": description, "detailed": str(current or "")}
        save_meta(meta)
        return web.json_response({"ok": True})

    async def workflow_params_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        params = body.get("params")
        if not filename or not filename.endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "invalid filename"}, status=400
            )
        if not (workflows_dir / filename).exists():
            return web.json_response(
                {"ok": False, "error": "file not found in workflows"}, status=404
            )
        if not isinstance(params, dict):
            return web.json_response(
                {"ok": False, "error": "invalid params"}, status=400
            )
        try:
            _validate_unique_workflow_names([(filename, {"short": "", "detailed": ""}, params)])
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        all_params = _load_workflow_params()
        all_params[filename] = params
        _save_workflow_params(all_params)
        return web.json_response({"ok": True})

    async def description_detailed_handler(request: web.Request) -> web.Response:
        """Save detailed workflow description."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        description = (body.get("description") or "").strip()
        if not filename or not filename.endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "invalid filename"}, status=400
            )
        if not (workflows_dir / filename).exists():
            return web.json_response(
                {"ok": False, "error": "file not found in workflows"}, status=404
            )
        meta = load_meta()
        if filename not in meta:
            meta[filename] = {"short": "", "detailed": ""}
        meta[filename]["detailed"] = description
        save_meta(meta)
        return web.json_response({"ok": True})

    async def workflow_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        if not isinstance(body, dict):
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        try:
            filename, description, params = _normalize_workflow_payload(body)
            _validate_unique_workflow_names([(filename, description, params)])
            _save_workflow_payload(filename, description, params)
        except FileNotFoundError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=404)
        except TypeError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
        return web.json_response({"ok": True, "filename": filename})

    async def workflows_bulk_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        items = body.get("items") if isinstance(body, dict) else None
        if not isinstance(items, list):
            return web.json_response(
                {"ok": False, "error": "items must be a list"}, status=400
            )

        normalized: list[tuple[str, dict[str, str], dict[str, object]]] = []
        for item in items:
            if not isinstance(item, dict):
                return web.json_response(
                    {"ok": False, "error": "invalid workflow item"}, status=400
                )
            try:
                normalized.append(_normalize_workflow_payload(item))
            except FileNotFoundError as e:
                return web.json_response({"ok": False, "error": str(e)}, status=404)
            except (TypeError, ValueError) as e:
                return web.json_response({"ok": False, "error": str(e)}, status=400)

        try:
            _validate_unique_workflow_names(normalized)
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

        for filename, description, params in normalized:
            _save_workflow_payload(filename, description, params)
        return web.json_response({"ok": True, "saved": len(normalized)})

    async def rename_handler(request: web.Request) -> web.Response:
        """Rename a workflow file."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        old_name = _safe_basename(body.get("old_name") or "")
        new_name = _safe_basename(body.get("new_name") or "")
        if not old_name.endswith(".json") or not new_name.endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "only .json allowed"}, status=400
            )
        if not SAFE_FILENAME_RE.match(new_name):
            return web.json_response(
                {"ok": False, "error": "invalid new filename"}, status=400
            )
        old_path = workflows_dir / old_name
        new_path = workflows_dir / new_name
        if not old_path.exists():
            return web.json_response(
                {"ok": False, "error": "file not found"}, status=404
            )
        if new_path.exists():
            return web.json_response(
                {"ok": False, "error": "target already exists"}, status=400
            )
        old_path.rename(new_path)
        meta = load_meta()
        if old_name in meta:
            meta[new_name] = meta.pop(old_name)
            save_meta(meta)
        workflow_params = _load_workflow_params()
        if old_name in workflow_params:
            workflow_params[new_name] = workflow_params.pop(old_name)
            _save_workflow_params(workflow_params)
        return web.json_response({"ok": True, "filename": new_name})

    async def delete_handler(request: web.Request) -> web.Response:
        """Delete a workflow file."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        if not filename.endswith(".json"):
            return web.json_response(
                {"ok": False, "error": "invalid filename"}, status=400
            )
        path = workflows_dir / filename
        if not path.exists():
            return web.json_response(
                {"ok": False, "error": "file not found"}, status=404
            )
        path.unlink()
        meta = load_meta()
        meta.pop(filename, None)
        save_meta(meta)
        workflow_params = _load_workflow_params()
        workflow_params.pop(filename, None)
        _save_workflow_params(workflow_params)
        return web.json_response({"ok": True})

    async def clear_cache_handler(request: web.Request) -> web.Response:
        """Clear local ComfyUI cache files."""
        await request.read()
        deleted = 0
        for d in (tmp_dir,):
            if d.exists() and d.is_dir():
                for f in d.iterdir():
                    if f.is_file():
                        try:
                            f.unlink()
                            deleted += 1
                        except Exception:
                            pass
        return web.json_response(
            {
                "ok": True,
                "deleted": deleted,
                "dirs": [str(tmp_dir)],
            }
        )

    async def clear_history_handler(request: web.Request) -> web.Response:
        """Clear generated history records."""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        hours = body.get("hours", 48)
        try:
            hours = int(hours)
        except (TypeError, ValueError):
            return web.json_response(
                {"ok": False, "error": "hours must be an integer"}, status=400
            )

        if hours < 0:
            hours = 0

        # 璋冪敤娓呯悊鍑芥暟
        if _cleanup_history:
            try:
                try:
                    deleted_count = _cleanup_history(hours)
                except TypeError:
                    deleted_count = _cleanup_history()
                message = f"宸叉竻鐞?{deleted_count} 鏉″巻鍙茶褰?(淇濈暀{hours}灏忔椂鍐?"
            except Exception as e:
                return web.json_response({"ok": False, "error": str(e)}, status=500)
        else:
            return web.json_response(
                {"ok": False, "error": "cleanup function not available"}, status=500
            )

        return web.json_response({"ok": True, "message": message, "hours": hours})

    def _normalize_http(value: str) -> str:
        raw = str(value or "").strip().rstrip("/")
        if not raw:
            return ""
        if raw.startswith(("http://", "https://")):
            return raw
        return f"http://{raw}"

    def _normalize_workflow_names(value) -> list[str]:
        raw_items = value if isinstance(value, list) else []
        names: list[str] = []
        seen = set()
        for item in raw_items:
            name = str(item or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)
        return names

    def _normalize_port_entry(entry, idx: int) -> dict[str, object] | None:
        if not isinstance(entry, dict):
            return None
        name = str(entry.get("name") or "").strip()
        http = _normalize_http(entry.get("http") or "")
        workflows = _normalize_workflow_names(entry.get("workflows"))
        if not name and not http:
            return None
        if not name:
            name = f"port{idx}"
        if not http:
            return None
        return {"name": name, "http": http, "workflows": workflows}

    def _load_ports() -> list[dict[str, object]]:
        if load_ports_func:
            return list(load_ports_func() or [])
        try:
            if ports_config_path and ports_config_path.exists():
                data = json.loads(ports_config_path.read_text(encoding="utf-8"))
                raw_ports = data.get("ports") if isinstance(data, dict) else data
                if isinstance(raw_ports, list):
                    ports = []
                    for idx, item in enumerate(raw_ports, start=1):
                        port = _normalize_port_entry(item, idx)
                        if port:
                            ports.append(port)
                    return ports
        except Exception:
            pass
        return []

    def _save_ports(ports: list[dict[str, object]]) -> None:
        if save_ports_func:
            save_ports_func(ports)
            return
        if ports_config_path:
            ports_config_path.parent.mkdir(parents=True, exist_ok=True)
            ports_config_path.write_text(
                json.dumps({"ports": ports}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def _ensure_active_port_valid(ports: list[dict[str, object]]) -> str:
        active = _read_active_port()
        names = [str(port.get("name") or "").strip() for port in ports]
        if active and active in names:
            return active
        next_active = names[0] if names else ""
        _write_active_port(next_active)
        return next_active

    def _read_active_port() -> str:
        try:
            if active_port_state_path and active_port_state_path.exists():
                data = json.loads(active_port_state_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return str(data.get("name") or "").strip()
        except Exception:
            pass
        return ""

    def _write_active_port(name: str) -> None:
        if not active_port_state_path:
            return
        active_port_state_path.parent.mkdir(parents=True, exist_ok=True)
        active_port_state_path.write_text(
            json.dumps({"name": name}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _workflow_options() -> list[dict[str, str]]:
        options: list[dict[str, str]] = []
        seen = set()
        workflow_params = _load_workflow_params()
        if workflows_dir.exists():
            for f in sorted(workflows_dir.glob("*.json")):
                params = (
                    workflow_params.get(f.name, {})
                    if isinstance(workflow_params, dict)
                    else {}
                )
                name = (
                    (params.get("name") if isinstance(params, dict) else "")
                    or (parse_workflow_filename(f.name) or {}).get("name")
                    or f.stem
                )
                if name in seen:
                    continue
                seen.add(name)
                options.append({"name": name, "filename": f.name})
        return options

    async def ports_handler(_: web.Request) -> web.Response:
        ports = _load_ports()
        active = _ensure_active_port_valid(ports)
        return web.json_response(
            {
                "ok": True,
                "ports": ports,
                "active": active,
                "workflows": _workflow_options(),
            }
        )

    async def save_ports_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        raw_ports = body.get("ports") if isinstance(body, dict) else None
        if not isinstance(raw_ports, list):
            return web.json_response(
                {"ok": False, "error": "ports must be a list"}, status=400
            )
        ports: list[dict[str, object]] = []
        for idx, item in enumerate(raw_ports, start=1):
            port = _normalize_port_entry(item, idx)
            if port:
                ports.append(port)
        _save_ports(ports)
        active = _ensure_active_port_valid(ports)
        if active_port_changed_func:
            active_port_changed_func()
        return web.json_response({"ok": True, "ports": ports, "active": active})

    async def active_port_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        name = str(body.get("name") or "").strip() if isinstance(body, dict) else ""
        if not name:
            return web.json_response({"ok": False, "error": "missing name"}, status=400)
        ports = _load_ports()
        if not any(str(port.get("name") or "") == name for port in ports):
            return web.json_response({"ok": False, "error": "接口不存在"}, status=404)
        _write_active_port(name)
        if active_port_changed_func:
            active_port_changed_func()
        return web.json_response({"ok": True, "active": name})

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

    async def index_handler(_: web.Request) -> web.Response:
        """Return the management page HTML."""
        html = _INDEX_HTML
        return web.Response(text=html, content_type="text/html")

    async def logo_handler(_: web.Request) -> web.Response:
        if not logo_path.exists():
            return web.Response(status=404)
        return web.FileResponse(logo_path)

    app.router.add_get("/", index_handler)
    app.router.add_get("/webui_logo.jpg", logo_handler)
    app.router.add_get("/api/list", list_handler)
    app.router.add_post("/api/upload", upload_handler)
    app.router.add_post("/api/description", description_handler)
    app.router.add_post("/api/description_detailed", description_detailed_handler)
    app.router.add_post("/api/workflow_params", workflow_params_handler)
    app.router.add_post("/api/workflow", workflow_handler)
    app.router.add_post("/api/workflows/bulk", workflows_bulk_handler)
    app.router.add_post("/api/rename", rename_handler)
    app.router.add_post("/api/delete", delete_handler)
    app.router.add_post("/api/clear_cache", clear_cache_handler)
    app.router.add_post("/api/clear_history", clear_history_handler)
    app.router.add_get("/api/ports", ports_handler)
    app.router.add_post("/api/ports", save_ports_handler)
    app.router.add_post("/api/active_port", active_port_handler)
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
    return app


_INDEX_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Comfyui 泡泡版</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f8ff;
      --ink: #111827;
      --text: #1f2937;
      --muted: #667085;
      --subtle: #98a2b3;
      --line: rgba(94, 114, 144, .18);
      --line-strong: rgba(94, 114, 144, .28);
      --glass: rgba(255, 255, 255, .68);
      --glass-strong: rgba(255, 255, 255, .82);
      --glass-soft: rgba(255, 255, 255, .46);
      --brand: #087cff;
      --brand-2: #16b8ff;
      --brand-soft: rgba(8, 124, 255, .12);
      --cyan: #14b8d4;
      --green: #21c55d;
      --pink: #ec4899;
      --orange: #f97316;
      --success: #17b26a;
      --danger: #ef4444;
      --warn: #f59e0b;
      --success-soft: rgba(23, 178, 106, .13);
      --danger-soft: rgba(239, 68, 68, .13);
      --warn-soft: rgba(245, 158, 11, .16);
      --shadow: 0 22px 58px rgba(42, 58, 88, .18), 0 4px 12px rgba(42, 58, 88, .08);
      --shadow-float: 0 30px 80px rgba(35, 46, 70, .22), 0 8px 22px rgba(35, 46, 70, .12);
      --highlight: inset 0 1px 0 rgba(255, 255, 255, .85), inset 1px 0 0 rgba(255, 255, 255, .44);
      --blur: blur(24px) saturate(1.22);
      --radius: 22px;
      --radius-sm: 16px;
      --sidebar: 246px;
      --rail: 78px;
    }
    :root[data-theme="dark"] {
      color-scheme: dark;
      --bg: #101624;
      --ink: #f8fafc;
      --text: #edf2f7;
      --muted: #b5c0cf;
      --subtle: #8290a4;
      --line: rgba(226, 232, 240, .13);
      --line-strong: rgba(226, 232, 240, .24);
      --glass: rgba(24, 32, 47, .66);
      --glass-strong: rgba(30, 40, 58, .82);
      --glass-soft: rgba(255, 255, 255, .08);
      --brand: #60a5fa;
      --brand-2: #22d3ee;
      --brand-soft: rgba(96, 165, 250, .18);
      --shadow: 0 24px 70px rgba(0, 0, 0, .36), 0 6px 16px rgba(0, 0, 0, .25);
      --shadow-float: 0 36px 90px rgba(0, 0, 0, .46), 0 10px 28px rgba(0, 0, 0, .3);
      --highlight: inset 0 1px 0 rgba(255, 255, 255, .16), inset 1px 0 0 rgba(255, 255, 255, .08);
    }
    *{box-sizing:border-box} html,body{min-width:0;min-height:100%}
    body{
      margin:0;color:var(--text);font-family:"SF Pro Text","SF Pro Display","PingFang SC","HarmonyOS Sans SC","MiSans","Alibaba PuHuiTi 3.0","Microsoft YaHei UI",-apple-system,BlinkMacSystemFont,"Segoe UI",ui-sans-serif,system-ui,sans-serif;line-height:1.45;letter-spacing:0;-webkit-font-smoothing:antialiased;text-rendering:geometricPrecision;
      background:
        radial-gradient(circle at 0% 0%, rgba(201, 241, 255, .95), transparent 25rem),
        radial-gradient(circle at 88% 2%, rgba(228, 220, 255, .98), transparent 30rem),
        radial-gradient(circle at 18% 96%, rgba(216, 250, 242, .88), transparent 27rem),
        linear-gradient(135deg, #f7fbff 0%, #eef5ff 45%, #f6f1ff 100%);
      background-attachment:fixed;overflow-x:hidden;
    }
    :root[data-theme="dark"] body{background:radial-gradient(circle at 8% 0%, rgba(37,99,235,.28), transparent 26rem),radial-gradient(circle at 86% 6%, rgba(20,184,166,.18), transparent 28rem),linear-gradient(135deg,#0f172a,#111827 52%,#171329)}
    body::before{content:"";position:fixed;inset:0;pointer-events:none;background:linear-gradient(90deg,rgba(255,255,255,.28),transparent 28%,rgba(255,255,255,.18));mix-blend-mode:soft-light}
    button,input,textarea,select{font:inherit} button{min-height:38px;border:1px solid var(--line);border-radius:12px;padding:8px 13px;color:var(--text);background:var(--glass-strong);cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:8px;white-space:nowrap;box-shadow:var(--highlight);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease,background .18s ease}button:hover:not(:disabled){transform:translateY(-2px);border-color:rgba(8,124,255,.35);box-shadow:0 14px 32px rgba(42,58,88,.14),0 0 0 4px var(--brand-soft),var(--highlight)}button:disabled{opacity:.55;cursor:default;transform:none;box-shadow:var(--highlight)}
    input,textarea,select{width:100%;min-width:0;border:1px solid var(--line-strong);border-radius:13px;background:rgba(255,255,255,.62);color:var(--text);outline:none;box-shadow:var(--highlight);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur)}:root[data-theme="dark"] input,:root[data-theme="dark"] textarea,:root[data-theme="dark"] select{background:rgba(15,23,42,.44)}input,select{min-height:40px;padding:8px 11px}textarea{min-height:102px;padding:10px 12px;resize:vertical}input:focus,textarea:focus,select:focus{border-color:var(--brand);box-shadow:0 0 0 4px var(--brand-soft),var(--highlight)}
    svg.icon{width:17px;height:17px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;flex:0 0 auto}
    .app{min-height:100vh;display:grid;grid-template-columns:var(--sidebar) minmax(0,1fr);gap:22px;padding:14px}
    .sidebar{position:sticky;top:14px;height:calc(100vh - 28px);display:flex;flex-direction:column;gap:14px;padding:18px;border:1px solid rgba(255,255,255,.78);border-radius:22px;background:linear-gradient(160deg,rgba(222,245,255,.70),rgba(255,255,255,.54) 42%,rgba(238,247,255,.68));box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);overflow:hidden}
    .sidebar::after{content:"";position:absolute;inset:auto -80px -80px 35%;height:180px;background:radial-gradient(circle,rgba(255,255,255,.75),transparent 62%);pointer-events:none}
    .brand{position:relative;display:grid;grid-template-columns:46px minmax(0,1fr);gap:12px;align-items:center}
    .brand-mark{width:46px;height:46px;border-radius:16px;display:grid;place-items:center;color:#087cff;background:rgba(255,255,255,.72);box-shadow:0 12px 26px rgba(8,124,255,.16),var(--highlight);overflow:hidden}
    .brand-logo{width:100%;height:100%;display:block;object-fit:cover}
    .brand strong{display:block;color:#405064;font-size:1.06rem;font-weight:760;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .brand span{display:block;color:var(--muted);font-size:.78rem}
    .nav-label{margin:8px 10px 0;color:var(--subtle);font-size:.78rem}
    .nav{position:relative;display:grid;gap:7px}
    .nav button{width:100%;justify-content:flex-start;border-color:transparent;background:transparent;color:#344054;font-size:.95rem;box-shadow:none}
    .nav button.active{color:var(--brand);background:rgba(8,124,255,.10);box-shadow:inset 4px 0 0 var(--brand)}
    .nav button.active svg{filter:drop-shadow(0 4px 8px rgba(8,124,255,.28))}
    .nav-text{min-width:0;overflow:hidden;text-overflow:ellipsis}
    .sidebar-footer{position:relative;margin-top:auto}
    .app.collapsed{grid-template-columns:var(--rail) minmax(0,1fr)}
    .app.collapsed .sidebar{align-items:center}
    .app.collapsed .brand{grid-template-columns:46px}
    .app.collapsed .brand-text,.app.collapsed .nav-label,.app.collapsed .nav-text,.app.collapsed .collapse-text{display:none}
    .app.collapsed .nav button{width:46px;padding-inline:0;justify-content:center}
    .main{min-width:0;display:grid;grid-template-rows:auto auto minmax(0,1fr);gap:14px}
    .topbar{position:sticky;top:14px;z-index:40;min-height:72px;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:12px 18px;border:1px solid rgba(255,255,255,.8);border-radius:22px;background:linear-gradient(100deg,rgba(216,243,255,.64),rgba(255,255,255,.56) 50%,rgba(234,226,255,.72));box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur)}
    .top-left{min-width:0;display:flex;align-items:center;gap:12px}
    .menu-btn{display:none;width:42px;padding:0}
    .title-block{min-width:0}
    .title-block h1{margin:0;color:#101828;font-size:1.15rem;line-height:1.2;font-weight:780;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .title-block p{margin:4px 0 0;color:var(--muted);font-size:.85rem}
    .statusbar{min-height:38px;display:flex;align-items:center}
    .toast{display:none;max-width:780px;padding:9px 13px;border:1px solid rgba(255,255,255,.76);border-radius:14px;background:var(--glass-strong);box-shadow:var(--shadow);backdrop-filter:var(--blur);font-size:.9rem}
    .toast.show{display:inline-flex}.toast.ok{color:var(--success)}.toast.err{color:var(--danger)}
    .module{display:none;min-width:0}.module.active{display:block}
    .panel,.card{border:1px solid rgba(255,255,255,.78);border-radius:var(--radius);background:var(--glass);box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);overflow:hidden;transition:transform .18s ease,box-shadow .18s ease,border-color .18s ease}
    .panel:hover,.card:hover{transform:translateY(-2px);box-shadow:var(--shadow-float);border-color:rgba(255,255,255,.95)}
    .panel-head,.card-head{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:18px 22px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.35)}
    .panel-title h2,.card-title h3{margin:0;color:#202633;font-size:1rem;font-weight:720}
    .panel-title p,.card-title p{margin:5px 0 0;color:var(--muted);font-size:.84rem}
    .panel-body,.card-body{padding:18px 22px}
    .btn-primary{color:#fff;border-color:transparent;background:linear-gradient(135deg,var(--brand),var(--brand-2));box-shadow:0 12px 24px rgba(8,124,255,.24)}
    .btn-danger{color:#fff;border-color:transparent;background:linear-gradient(135deg,#ff5b6b,#ef4444);box-shadow:0 12px 24px rgba(239,68,68,.18)}
    .btn-soft{background:rgba(255,255,255,.54)}
    .chip{min-height:28px;display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;background:rgba(255,255,255,.56);color:var(--muted);font-size:.78rem;box-shadow:var(--highlight)}
    .chip.active{color:var(--success);background:rgba(220,252,231,.68)}.chip.warn{color:var(--warn);background:rgba(255,244,229,.78)}
    .source-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
    .source-card{padding:16px;display:grid;gap:14px;background:linear-gradient(145deg,rgba(255,255,255,.72),rgba(255,255,255,.48))}
    .source-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
    .source-name{min-width:0;font-weight:760;color:#1d2735;overflow-wrap:anywhere}
    .source-http{color:var(--muted);font-size:.82rem;overflow-wrap:anywhere}
    .field-grid{display:grid;grid-template-columns:1fr 1.25fr;gap:12px}
    .field-label{display:block;margin:0 0 6px;color:var(--muted);font-size:.78rem}
    .whitelist-head{display:flex;align-items:center;justify-content:space-between;gap:10px}.port-actions{margin-left:auto;justify-content:flex-end;flex-wrap:nowrap}
    .checkline{display:inline-flex;align-items:center;gap:8px;color:var(--text);font-size:.88rem}.checkline input{width:auto;min-height:0}
    .workflow-checks{max-height:238px;overflow:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(215px,1fr));gap:9px;padding-right:4px}
    .workflow-check{min-width:0;display:grid;grid-template-columns:auto minmax(0,1fr);gap:3px 8px;align-items:center;padding:10px;border:1px solid rgba(255,255,255,.7);border-radius:14px;background:rgba(255,255,255,.48);box-shadow:var(--highlight)}
    .workflow-check input{width:auto;min-height:0}.workflow-check span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .workflow-file{grid-column:2;color:var(--subtle);font-size:.74rem}
    .workflow-shell{display:grid;grid-template-columns:minmax(270px,350px) minmax(0,1fr);gap:16px;min-height:calc(100vh - 154px)}
    .workflow-list-panel{min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr) auto}
    .workflow-toolbar{display:grid;gap:10px;padding:16px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.30)}
    .upload-inline{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center}.upload-inline input{min-width:0}
    .filter-row{display:grid;grid-template-columns:1fr 1fr;gap:9px}
    .workflow-list{min-height:0;overflow:auto;display:grid;align-content:start;gap:9px;padding:12px}
    .workflow-item{width:100%;min-height:74px;justify-content:flex-start;align-items:flex-start;text-align:left;border-radius:16px;padding:11px;background:rgba(255,255,255,.50);border-color:rgba(255,255,255,.68)}
    .workflow-item.active{border-color:rgba(8,124,255,.32);background:rgba(232,242,255,.72);box-shadow:0 12px 30px rgba(8,124,255,.14),0 0 0 4px var(--brand-soft)}
    .workflow-item-main{min-width:0;display:grid;gap:2px}.workflow-item-title{min-width:0;font-weight:720;color:#1d2735;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .workflow-item-meta{color:var(--muted);font-size:.77rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .dirty-dot{width:8px;height:8px;border-radius:999px;background:var(--warn);margin-top:7px;flex:0 0 auto;box-shadow:0 0 0 4px var(--warn-soft)}
    .list-footer{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:12px 16px;border-top:1px solid rgba(94,114,144,.12);color:var(--muted);font-size:.84rem;background:rgba(255,255,255,.28)}
    .editor{min-width:0;min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr)}
    .editor-head{display:flex;justify-content:space-between;gap:14px;padding:18px 22px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.32)}
    .editor-name{min-width:0}.editor-name h2{margin:0;color:#1d2735;font-size:1.08rem;overflow-wrap:anywhere}.editor-name p{margin:4px 0 6px;color:var(--muted);font-size:.82rem;overflow-wrap:anywhere}
    .editor-actions{display:flex;gap:8px;align-items:flex-start;flex-wrap:wrap;justify-content:flex-end}
    .editor-body{min-height:0;overflow:auto;padding:18px 22px;display:grid;gap:14px;align-content:start}
    .form-row{display:grid;gap:6px}
    .param-panel{border:1px solid rgba(255,255,255,.74);border-radius:18px;background:rgba(255,255,255,.42);overflow:hidden;box-shadow:var(--highlight)}
    .param-grid{display:grid;grid-template-columns:76px repeat(3,minmax(120px,1fr));gap:1px;background:rgba(94,114,144,.12)}
    .param-cell{min-width:0;padding:10px;background:rgba(255,255,255,.50)}.param-label{color:var(--muted);font-size:.78rem;font-weight:680}
    .param-control{display:grid;grid-template-columns:minmax(0,1fr) 56px;gap:7px;align-items:center}.param-control input{min-height:36px}
    .mode-toggle{min-height:36px;padding-inline:8px;color:var(--muted)}.mode-toggle.strict{color:#fff;border-color:transparent;background:linear-gradient(135deg,#fbbf24,#f97316)}
    .tools-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}.tool-card{min-height:216px;display:grid;grid-template-rows:auto minmax(0,1fr) auto}
    .file-box{display:grid;gap:10px}.danger-copy{color:var(--muted);font-size:.88rem}
    .history-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center}
    .empty{padding:28px;color:var(--muted);text-align:center}.drawer-backdrop{display:none}
    .modal-backdrop{position:fixed;inset:0;z-index:80;display:none;place-items:center;padding:20px;background:rgba(16,24,40,.38);backdrop-filter:blur(12px)}
    .modal-backdrop.show{display:grid}.modal{width:min(460px,100%);border:1px solid rgba(255,255,255,.8);border-radius:22px;background:var(--glass-strong);box-shadow:var(--shadow-float);backdrop-filter:var(--blur)}
    .modal-body{padding:20px}.modal-body h3{margin:0 0 8px;font-size:1.06rem}.modal-body p{margin:0;color:var(--muted)}
    .modal-actions{display:flex;justify-content:flex-end;gap:8px;padding:14px 20px;border-top:1px solid rgba(94,114,144,.12)}
    .brand strong,.title-block h1{font-weight:700}.nav button,.workflow-item-title,.source-name{font-weight:650}
    .btn-success{color:#fff;border-color:transparent;background:linear-gradient(135deg,#34d399,#22c55e);box-shadow:0 14px 28px rgba(34,197,94,.22)}.btn-success:hover:not(:disabled){border-color:rgba(34,197,94,.34);box-shadow:0 16px 34px rgba(34,197,94,.22),0 0 0 4px rgba(34,197,94,.14),var(--highlight)}.btn-delete-port{color:#fff!important;border-color:transparent!important;background:linear-gradient(135deg,#fb7185,#ef4444)!important;box-shadow:0 12px 24px rgba(239,68,68,.18)!important}.btn-active-port.current{color:#065f46!important;border-color:rgba(34,197,94,.35)!important;background:linear-gradient(135deg,#bbf7d0,#4ade80)!important;box-shadow:0 12px 24px rgba(34,197,94,.18),var(--highlight)!important}.source-title{display:flex;align-items:center;gap:8px;min-width:0}.source-dot{width:9px;height:9px;border-radius:999px;flex:0 0 auto;background:transparent}.source-dot.active{background:#22c55e;box-shadow:0 0 0 5px rgba(34,197,94,.16),0 6px 14px rgba(34,197,94,.30)}
    #collapseBtn svg{transition:transform .18s ease}.app.collapsed #collapseBtn svg{transform:rotate(180deg)}
    .switch-row{display:flex;align-items:center;gap:8px;color:var(--text);font-size:.9rem;font-weight:650;cursor:pointer;user-select:none}.switch-row input{position:absolute;opacity:0;pointer-events:none}.switch{position:relative;width:48px;height:28px;flex:0 0 auto;border-radius:999px;background:rgba(148,163,184,.20);border:1px solid rgba(148,163,184,.26);box-shadow:inset 0 1px 3px rgba(15,23,42,.18),var(--highlight);transition:background .18s ease,border-color .18s ease,box-shadow .18s ease}.switch::after{content:"";position:absolute;left:3px;top:3px;width:22px;height:22px;border-radius:999px;background:rgba(255,255,255,.94);box-shadow:0 5px 14px rgba(15,23,42,.24);transition:transform .18s ease,background .18s ease}.switch-row input:checked+.switch{border-color:rgba(34,197,94,.42);background:linear-gradient(135deg,#34d399,#22c55e);box-shadow:0 10px 24px rgba(34,197,94,.18),inset 0 1px 0 rgba(255,255,255,.36)}.switch-row input:checked+.switch::after{transform:translateX(20px)}.workflow-check{position:relative;grid-template-columns:minmax(0,1fr);min-height:48px;padding:13px 15px;cursor:pointer}.workflow-check-input{position:absolute!important;opacity:0!important;pointer-events:none!important}.workflow-check .workflow-title{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text);font-weight:650}.workflow-check.selected{border-color:rgba(34,197,94,.48);background:linear-gradient(135deg,rgba(34,197,94,.24),rgba(20,184,166,.16));box-shadow:0 14px 30px rgba(34,197,94,.14),inset 0 1px 0 rgba(255,255,255,.34)}.workflow-check.selected .workflow-title{color:#047857}.workflow-check.disabled{cursor:default;opacity:.58;background:rgba(148,163,184,.10);border-color:rgba(148,163,184,.18);box-shadow:var(--highlight)}
    .file-upload-row{grid-template-columns:auto minmax(0,1fr) auto}.file-input-hidden{position:absolute!important;width:1px!important;height:1px!important;opacity:0!important;pointer-events:none!important}.upload-button-row{grid-template-columns:1fr}.upload-button-row #uploadBtn{width:100%;justify-content:center}.file-picker{min-height:40px;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:8px 13px;border:1px solid rgba(148,163,184,.28);border-radius:13px;background:rgba(255,255,255,.60);box-shadow:var(--highlight);backdrop-filter:var(--blur);cursor:pointer;white-space:nowrap;font-weight:650;color:var(--text);transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease}.file-picker:hover{transform:translateY(-2px);border-color:rgba(8,124,255,.35);box-shadow:0 14px 32px rgba(42,58,88,.14),0 0 0 4px var(--brand-soft),var(--highlight)}.file-picker-name{min-width:0;color:var(--muted);font-size:.84rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.field-error{border-color:var(--danger)!important;box-shadow:0 0 0 4px var(--danger-soft),var(--highlight)!important}.label-help{display:flex;align-items:center;gap:8px}.help-wrap{position:relative;display:inline-flex}.help-dot{width:22px;height:22px;min-height:22px;padding:0;border-radius:999px;font-size:.82rem;font-weight:760;color:var(--brand);background:rgba(8,124,255,.10);border-color:rgba(8,124,255,.22)}.help-popover{position:absolute;left:50%;bottom:calc(100% + 10px);z-index:30;width:min(360px,80vw);transform:translateX(-50%) translateY(4px);padding:12px 14px;border:1px solid rgba(255,255,255,.72);border-radius:14px;background:var(--glass-strong);box-shadow:var(--shadow);backdrop-filter:var(--blur);color:var(--text);font-size:.84rem;line-height:1.5;opacity:0;pointer-events:none;transition:opacity .16s ease,transform .16s ease}.help-wrap:hover .help-popover,.help-wrap:focus-within .help-popover,.help-wrap.open .help-popover{opacity:1;transform:translateX(-50%) translateY(0);pointer-events:auto}.param-cell.header-cell,.param-grid>.param-cell:first-child,.param-grid>.param-cell:nth-child(5),.param-grid>.param-cell:nth-child(9){display:grid;place-items:center;text-align:center}.param-label{text-align:center}.mode-toggle{min-width:78px}.param-control{grid-template-columns:minmax(0,1fr) 86px}
    .statusbar{min-height:0;height:0;overflow:visible}.toast{position:fixed;top:18px;left:50%;z-index:120;display:flex;max-width:min(720px,calc(100vw - 32px));padding:12px 16px;border-radius:16px;align-items:center;justify-content:center;transform:translate(-50%,-18px);opacity:0;pointer-events:none;transition:opacity .22s ease,transform .22s ease;border:1px solid rgba(255,255,255,.72);background:var(--glass-strong);box-shadow:var(--shadow-float);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);font-size:.92rem;line-height:1.45}.toast.show{display:flex;opacity:1;transform:translate(-50%,0)}.toast.ok{color:var(--success);border-color:rgba(34,197,94,.30);background:rgba(240,253,244,.86)}.toast.err{color:var(--danger);border-color:rgba(239,68,68,.35);background:rgba(255,241,242,.88)}:root[data-theme="dark"] .toast.ok{color:#bbf7d0;background:rgba(20,83,45,.88);border-color:rgba(74,222,128,.28)}:root[data-theme="dark"] .toast.err{color:#fecdd3;background:rgba(76,29,42,.90);border-color:rgba(251,113,133,.34)}.workflow-shell{grid-template-columns:minmax(320px,410px) minmax(620px,1fr)}.editor,.editor-body,.editor-head,.form-row,.param-panel,textarea{min-width:0}.editor-actions{min-width:0;flex-wrap:wrap}.param-panel{overflow-x:auto}.param-grid{min-width:640px;grid-template-columns:72px repeat(3,minmax(165px,1fr))}.param-control{grid-template-columns:minmax(72px,1fr) 70px}.mode-toggle{min-width:64px;padding-inline:10px;background:rgba(255,255,255,.46);box-shadow:var(--highlight)}.mode-toggle.strict{color:#713f12;border-color:rgba(245,158,11,.48);background:linear-gradient(135deg,#fde68a,#f59e0b);box-shadow:inset 0 1px 0 rgba(255,255,255,.45)}.help-wrap{position:relative}.help-popover{position:fixed;left:50%;top:88px;bottom:auto;width:min(420px,calc(100vw - 32px));transform:translateX(-50%) translateY(-8px);white-space:normal;overflow-wrap:anywhere}.help-wrap:hover .help-popover,.help-wrap:focus-within .help-popover,.help-wrap.open .help-popover{transform:translateX(-50%) translateY(0)}
    .debug-shell{display:grid;grid-template-columns:minmax(360px,.92fr) minmax(360px,1fr);grid-template-areas:"form tasks" "output output";gap:16px}.debug-form-panel{grid-area:form}.debug-tasks-panel{grid-area:tasks}.debug-output-panel{grid-area:output}.debug-form{display:grid;gap:15px}.debug-section-head{display:flex;align-items:center;justify-content:space-between;gap:10px;color:var(--text);font-weight:720}.debug-texts{display:grid;gap:9px}.debug-text-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:start}.debug-text-row textarea{min-height:74px}.debug-text-row button{width:40px;padding:0}.debug-image-preview,.debug-video-preview{min-height:118px;display:grid;grid-template-columns:repeat(auto-fill,minmax(92px,1fr));gap:10px;padding:12px;border:1px dashed rgba(8,124,255,.24);border-radius:18px;background:rgba(255,255,255,.30);box-shadow:var(--highlight);transition:border-color .18s ease,background .18s ease,box-shadow .18s ease}.debug-video-preview{grid-template-columns:1fr;min-height:86px}.debug-image-preview.dragging,.debug-video-preview.dragging{border-color:rgba(34,197,94,.62);background:rgba(34,197,94,.10);box-shadow:0 0 0 4px rgba(34,197,94,.12),var(--highlight)}.debug-image-preview .empty,.debug-video-preview .empty{grid-column:1/-1;padding:24px 12px}.debug-image-tile{position:relative;aspect-ratio:1;border:1px solid rgba(255,255,255,.72);border-radius:16px;overflow:hidden;background:rgba(255,255,255,.46);box-shadow:var(--highlight)}.debug-image-tile img{width:100%;height:100%;display:block;object-fit:cover}.debug-image-tile button{position:absolute;top:6px;right:6px;width:28px;height:28px;min-height:28px;padding:0;border-radius:999px;background:rgba(239,68,68,.90);color:#fff;border-color:transparent}.debug-image-name{position:absolute;left:0;right:0;bottom:0;padding:4px 6px;color:#fff;font-size:.68rem;background:linear-gradient(180deg,transparent,rgba(15,23,42,.72));overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.debug-video-item{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center;padding:10px 12px;border-radius:14px;background:rgba(255,255,255,.44);border:1px solid rgba(255,255,255,.70);box-shadow:var(--highlight)}.debug-video-name{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text);font-weight:680}.debug-video-size{color:var(--muted);font-size:.78rem}.debug-video-item button{width:32px;height:32px;min-height:32px;padding:0;border-radius:999px;background:rgba(239,68,68,.90);color:#fff;border-color:transparent}.debug-task-tabs{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:12px 14px 0}.debug-task-tabs button{min-height:36px}.debug-task-tabs button.active{color:#065f46;border-color:rgba(34,197,94,.32);background:linear-gradient(135deg,rgba(187,247,208,.88),rgba(74,222,128,.72))}.debug-task-list{min-height:320px;max-height:520px;overflow:auto;display:grid;align-content:start;gap:10px;padding:14px}.debug-task{width:100%;display:grid;grid-template-columns:auto minmax(0,1fr);gap:10px;align-items:start;text-align:left;padding:12px;border-radius:16px;background:rgba(255,255,255,.50);border:1px solid rgba(255,255,255,.68);box-shadow:var(--highlight)}.debug-task.active{border-color:rgba(8,124,255,.34);background:rgba(232,242,255,.74);box-shadow:0 12px 30px rgba(8,124,255,.14),0 0 0 4px var(--brand-soft)}.debug-task-thumb{width:58px;height:58px;border-radius:14px;overflow:hidden;background:rgba(148,163,184,.18);display:grid;place-items:center;color:var(--muted);font-size:.72rem}.debug-task-thumb img{width:100%;height:100%;object-fit:cover;display:block}.debug-task-content{min-width:0;display:grid;gap:8px}.debug-task-title{display:grid;grid-template-columns:minmax(0,1fr) auto auto auto;align-items:center;gap:8px;min-width:0}.debug-task-title strong{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.debug-task-copy,.debug-task-delete{width:32px;height:32px;min-height:32px;padding:0;border-radius:10px;display:grid;place-items:center}.debug-task-delete{color:var(--danger);background:var(--danger-soft);border:1px solid rgba(239,68,68,.22)}.debug-task-meta{display:flex;gap:7px;flex-wrap:wrap;color:var(--muted);font-size:.78rem}.debug-status{min-height:24px;padding:3px 8px;border-radius:999px;font-size:.75rem;background:rgba(148,163,184,.18);color:var(--muted)}.debug-status.completed{background:rgba(34,197,94,.16);color:var(--success)}.debug-status.running{background:rgba(8,124,255,.14);color:var(--brand)}.debug-status.queued{background:rgba(148,163,184,.18);color:var(--muted)}.debug-status.failed,.debug-status.timeout{background:var(--danger-soft);color:var(--danger)}.debug-detail{display:grid;gap:16px}.debug-detail-section{display:grid;gap:10px}.debug-detail-section h3{margin:0;color:var(--text);font-size:.95rem}.debug-input-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}.debug-input-chip{padding:10px 12px;border-radius:14px;background:rgba(255,255,255,.45);border:1px solid rgba(255,255,255,.70);box-shadow:var(--highlight);overflow-wrap:anywhere}.debug-output-texts{display:grid;gap:10px}.debug-output-text{white-space:pre-wrap;overflow-wrap:anywhere;padding:12px;border:1px solid rgba(255,255,255,.72);border-radius:16px;background:rgba(255,255,255,.45);box-shadow:var(--highlight)}.debug-output-images,.debug-output-videos{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px;margin-top:14px}.debug-output-images a,.debug-input-images a,.debug-video-card{display:block;border-radius:18px;overflow:hidden;border:1px solid rgba(255,255,255,.74);background:rgba(255,255,255,.42);box-shadow:var(--highlight)}.debug-output-images img,.debug-input-images img{width:100%;display:block;aspect-ratio:1;object-fit:cover}.debug-output-videos video{width:100%;display:block;aspect-ratio:16/9;background:#020617}.debug-video-link{display:block;padding:8px 10px;color:var(--brand);font-size:.82rem}.debug-input-images{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:10px}.debug-input-videos{display:grid;gap:8px}.debug-error{padding:12px;border-radius:16px;color:var(--danger);background:var(--danger-soft);border:1px solid rgba(239,68,68,.24);white-space:pre-wrap;overflow-wrap:anywhere}
    .module[data-module-panel="tasks"] .debug-shell{grid-template-columns:minmax(280px,.72fr) minmax(320px,.8fr) minmax(420px,1.18fr);grid-template-areas:"manage tasks output"}
    .task-manage-panel{grid-area:manage;display:block;min-width:0}.task-manage-body{display:grid;gap:12px}.task-manage-controls{display:grid;gap:12px}.task-manage-panel.active-only .task-manage-controls,.task-manage-panel.collapsed .task-manage-controls{display:none}.task-manage-panel.active-only #taskManageToggleBtn{display:none}#taskManageToggleBtn .icon{transition:transform .2s ease;transform:rotate(-90deg)}#taskManageToggleBtn.open .icon{transform:rotate(90deg)}.task-manage-grid{display:grid;grid-template-columns:1fr;gap:10px}.task-manage-actions{display:grid;grid-template-columns:1fr 1fr;gap:8px}.task-manage-actions button{min-width:0;padding-inline:10px}.task-select{width:32px;height:32px;min-height:32px;padding:0;border-radius:10px;display:grid;place-items:center;color:var(--muted);background:rgba(148,163,184,.13);border:1px solid rgba(148,163,184,.20)}.debug-task.selected{border-color:rgba(34,197,94,.42);background:rgba(220,252,231,.62);box-shadow:0 12px 30px rgba(34,197,94,.13),0 0 0 4px rgba(34,197,94,.10)}.debug-task.selected .task-select{color:#065f46;background:linear-gradient(135deg,rgba(187,247,208,.95),rgba(74,222,128,.76));border-color:rgba(34,197,94,.35)}.task-count-chip{display:inline-flex;align-items:center;min-height:26px;padding:4px 9px;border-radius:999px;background:rgba(148,163,184,.14);color:var(--muted);font-size:.78rem}
    .task-info-card{position:relative;padding-bottom:18px}.debug-detail-prompt{justify-self:end;max-width:100%;color:var(--subtle);font-size:.72rem;line-height:1.35;overflow-wrap:anywhere;text-align:right}
    @media(max-width:820px){.topbar{top:12px;z-index:45}}
    .debug-task-stop{width:32px;height:32px;min-height:32px;padding:0;border-radius:10px;display:grid;place-items:center;color:#b45309;background:rgba(245,158,11,.16);border:1px solid rgba(245,158,11,.28)}.debug-status.canceled{background:rgba(245,158,11,.16);color:#b45309}
    :root[data-theme="dark"] body::before{background:linear-gradient(90deg,rgba(96,165,250,.10),transparent 30%,rgba(34,211,238,.08));mix-blend-mode:screen}
    :root[data-theme="dark"] .sidebar{border-color:rgba(148,163,184,.20);background:linear-gradient(160deg,rgba(30,41,59,.78),rgba(15,23,42,.60) 44%,rgba(30,27,55,.70));box-shadow:var(--shadow)}
    :root[data-theme="dark"] .sidebar::after{background:radial-gradient(circle,rgba(96,165,250,.22),transparent 64%)}
    :root[data-theme="dark"] .topbar{border-color:rgba(148,163,184,.20);background:linear-gradient(100deg,rgba(30,41,59,.72),rgba(15,23,42,.60) 52%,rgba(39,32,74,.70))}
    :root[data-theme="dark"] .panel,:root[data-theme="dark"] .card{border-color:rgba(148,163,184,.20);background:rgba(17,24,39,.64)}
    :root[data-theme="dark"] .panel-head,:root[data-theme="dark"] .card-head,:root[data-theme="dark"] .editor-head,:root[data-theme="dark"] .workflow-toolbar,:root[data-theme="dark"] .list-footer{border-color:rgba(148,163,184,.14);background:rgba(15,23,42,.36)}
    :root[data-theme="dark"] .source-card{background:linear-gradient(145deg,rgba(30,41,59,.72),rgba(15,23,42,.52))}
    :root[data-theme="dark"] .workflow-item,:root[data-theme="dark"] .workflow-check,:root[data-theme="dark"] .param-cell{border-color:rgba(148,163,184,.16);background:rgba(15,23,42,.42)}
    :root[data-theme="dark"] .workflow-item.active{border-color:rgba(96,165,250,.42);background:rgba(37,99,235,.18);box-shadow:0 12px 30px rgba(37,99,235,.20),0 0 0 4px var(--brand-soft)}
    :root[data-theme="dark"] .param-panel{border-color:rgba(148,163,184,.18);background:rgba(15,23,42,.30)}
    :root[data-theme="dark"] .param-grid{background:rgba(148,163,184,.12)}
    :root[data-theme="dark"] .brand-mark{color:#dbeafe;background:rgba(96,165,250,.20);box-shadow:0 12px 28px rgba(96,165,250,.16),var(--highlight)}
    :root[data-theme="dark"] .brand strong,:root[data-theme="dark"] .title-block h1,:root[data-theme="dark"] .panel-title h2,:root[data-theme="dark"] .card-title h3,:root[data-theme="dark"] .source-name,:root[data-theme="dark"] .workflow-item-title,:root[data-theme="dark"] .editor-name h2{color:var(--ink)}
    :root[data-theme="dark"] .nav button{color:#d1d9e6;background:transparent}
    :root[data-theme="dark"] .nav button.active{color:#e0f2fe;background:rgba(96,165,250,.18);box-shadow:inset 4px 0 0 var(--brand)}
    :root[data-theme="dark"] .btn-soft,:root[data-theme="dark"] button{background:rgba(30,41,59,.64);border-color:rgba(148,163,184,.20);color:var(--text)}
    :root[data-theme="dark"] .btn-primary{color:#fff;border-color:transparent;background:linear-gradient(135deg,var(--brand),var(--brand-2));box-shadow:0 14px 28px rgba(96,165,250,.20)}
    :root[data-theme="dark"] .btn-danger{color:#fff;border-color:transparent;background:linear-gradient(135deg,#fb7185,#ef4444)}
    :root[data-theme="dark"] .btn-success{color:#ecfdf5;background:linear-gradient(135deg,#10b981,#22c55e);box-shadow:0 14px 28px rgba(34,197,94,.20)}:root[data-theme="dark"] .btn-active-port.current{color:#052e16!important;background:linear-gradient(135deg,#86efac,#22c55e)!important}
    :root[data-theme="dark"] .chip{background:rgba(30,41,59,.64);color:var(--muted)}
    :root[data-theme="dark"] .chip.active{color:#bbf7d0;background:rgba(34,197,94,.16)}
    :root[data-theme="dark"] .chip.warn{color:#fde68a;background:rgba(245,158,11,.16)}
    :root[data-theme="dark"] .workflow-check.selected{border-color:rgba(52,211,153,.48);background:linear-gradient(135deg,rgba(34,197,94,.30),rgba(20,184,166,.20));box-shadow:0 14px 34px rgba(34,197,94,.14),inset 0 1px 0 rgba(255,255,255,.14)}
    :root[data-theme="dark"] .mode-toggle{background:rgba(30,41,59,.64);border-color:rgba(148,163,184,.20);color:var(--text);box-shadow:var(--highlight)}
    :root[data-theme="dark"] .mode-toggle.strict{color:#713f12;border-color:rgba(245,158,11,.52);background:linear-gradient(135deg,#fde68a,#f59e0b);box-shadow:inset 0 1px 0 rgba(255,255,255,.42)}
    :root[data-theme="dark"] .workflow-check.selected .workflow-title{color:#d1fae5}
    :root[data-theme="dark"] .workflow-check.disabled{border-color:rgba(148,163,184,.14);background:rgba(15,23,42,.30);box-shadow:var(--highlight)}
    :root[data-theme="dark"] .debug-image-preview,:root[data-theme="dark"] .debug-video-preview{background:rgba(15,23,42,.50);border-color:rgba(148,163,184,.22);box-shadow:inset 0 1px 0 rgba(255,255,255,.04),var(--highlight)}
    :root[data-theme="dark"] .debug-image-preview.dragging,:root[data-theme="dark"] .debug-video-preview.dragging{border-color:rgba(34,197,94,.58);background:rgba(21,128,61,.18);box-shadow:0 0 0 4px rgba(34,197,94,.10),inset 0 1px 0 rgba(255,255,255,.05),var(--highlight)}
    :root[data-theme="dark"] .debug-image-preview .empty,:root[data-theme="dark"] .debug-video-preview .empty{color:#94a3b8}
    :root[data-theme="dark"] .debug-image-tile,:root[data-theme="dark"] .debug-output-images a,:root[data-theme="dark"] .debug-input-images a{background:rgba(15,23,42,.62);border-color:rgba(148,163,184,.22);box-shadow:inset 0 1px 0 rgba(255,255,255,.05),var(--highlight)}
    :root[data-theme="dark"] .debug-input-chip,:root[data-theme="dark"] .debug-output-text,:root[data-theme="dark"] .debug-video-item,:root[data-theme="dark"] .debug-video-card{background:rgba(15,23,42,.68);border-color:rgba(148,163,184,.24);box-shadow:inset 0 1px 0 rgba(255,255,255,.05),var(--highlight)}
    :root[data-theme="dark"] .debug-input-chip,:root[data-theme="dark"] .debug-output-text,:root[data-theme="dark"] .debug-video-name{color:#e5eefb}
    :root[data-theme="dark"] .debug-video-size{color:#94a3b8}
    :root[data-theme="dark"] .debug-task.selected{border-color:rgba(52,211,153,.40);background:rgba(22,101,52,.24);box-shadow:0 12px 30px rgba(34,197,94,.10),0 0 0 4px rgba(34,197,94,.08)}:root[data-theme="dark"] .task-select{background:rgba(15,23,42,.58);border-color:rgba(148,163,184,.20);color:#94a3b8}:root[data-theme="dark"] .debug-task.selected .task-select{color:#bbf7d0;background:rgba(34,197,94,.24);border-color:rgba(74,222,128,.34)}:root[data-theme="dark"] .task-count-chip{background:rgba(15,23,42,.58);color:#94a3b8}
    @media(max-width:1100px){.source-grid,.tools-grid{grid-template-columns:1fr}.workflow-shell{grid-template-columns:minmax(250px,320px) minmax(0,1fr)}.debug-shell{grid-template-columns:1fr;grid-template-areas:"form" "tasks" "output"}.module[data-module-panel="tasks"] .debug-shell{grid-template-columns:1fr;grid-template-areas:"manage" "tasks" "output"}}
    @media(max-width:820px){.app,.app.collapsed{grid-template-columns:1fr;padding:12px}.main,.module,.panel,.debug-shell,.debug-tasks-panel,.debug-output-panel,.task-manage-panel{min-width:0}.sidebar{position:fixed;inset:12px auto 12px 12px;z-index:60;width:min(84vw,320px);height:auto;transform:translateX(calc(-100% - 18px));transition:transform .2s}.drawer-backdrop{position:fixed;inset:0;z-index:50;background:rgba(16,24,40,.32);backdrop-filter:blur(10px)}body.drawer-open .sidebar{transform:translateX(0)}body.drawer-open .drawer-backdrop{display:block}.app.collapsed #collapseBtn svg{transform:none}.app.collapsed .brand-text,.app.collapsed .nav-label,.app.collapsed .nav-text,.app.collapsed .collapse-text{display:block}.app.collapsed .sidebar{align-items:stretch}.app.collapsed .brand{grid-template-columns:46px minmax(0,1fr)}.app.collapsed .nav button{width:100%;padding-inline:12px;justify-content:flex-start}.menu-btn{display:inline-flex}.theme-text{display:none}.workflow-shell{grid-template-columns:1fr;min-height:0}.workflow-list-panel{max-height:440px}.field-grid,.filter-row,.upload-inline{grid-template-columns:1fr}.editor-head,.panel-head,.card-head{align-items:flex-start;flex-direction:column}.editor-actions{width:100%;display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}.editor-actions button{padding-inline:8px}.param-panel{overflow:visible}.param-grid{min-width:0;grid-template-columns:1fr}.param-cell{padding:10px 12px}.param-cell.empty-cell,.param-cell.header-cell{display:none}.param-cell[data-label]::before{content:attr(data-label);display:block;margin-bottom:6px;color:var(--muted);font-size:.78rem;font-weight:680}.param-control{grid-template-columns:minmax(0,1fr) 68px;gap:8px}.param-control input{min-width:0;padding-inline:10px}.mode-toggle{min-width:68px;width:68px;padding-inline:0;justify-content:center;white-space:nowrap}.workflow-checks{grid-template-columns:1fr}.history-row{grid-template-columns:1fr}.debug-shell{grid-template-columns:minmax(0,1fr);grid-template-areas:"form" "tasks" "output"}.module[data-module-panel="tasks"] .debug-shell{grid-template-columns:minmax(0,1fr);grid-template-areas:"manage" "tasks" "output"}.debug-task-list{min-height:220px;max-height:none}.debug-task{grid-template-columns:46px minmax(0,1fr);padding:10px}.debug-task-thumb{width:46px;height:46px;border-radius:12px}.debug-task-title{grid-template-columns:minmax(0,1fr) auto auto auto;gap:6px}.module[data-module-panel="tasks"] .debug-task-title{grid-template-columns:minmax(0,1fr) auto auto auto}.debug-task-copy,.debug-task-delete,.debug-task-stop,.task-select{width:30px;height:30px;min-height:30px}.debug-task-meta{gap:6px;font-size:.72rem}.debug-input-grid{grid-template-columns:1fr}.debug-output-images,.debug-output-videos{grid-template-columns:repeat(auto-fill,minmax(132px,1fr))}.debug-input-images{grid-template-columns:repeat(auto-fill,minmax(104px,1fr))}.task-manage-actions{grid-template-columns:1fr}.task-manage-grid{grid-template-columns:1fr}}
    @media(max-width:820px){.module[data-module-panel="tasks"] .task-manage-panel .panel-head,.module[data-module-panel="tasks"] .debug-tasks-panel .panel-head{align-items:center;flex-direction:row}.module[data-module-panel="tasks"] .task-manage-panel .panel-title,.module[data-module-panel="tasks"] .debug-tasks-panel .panel-title{min-width:0}.module[data-module-panel="tasks"] .task-manage-panel .panel-title h2,.module[data-module-panel="tasks"] .debug-tasks-panel .panel-title h2,.module[data-module-panel="tasks"] .task-manage-panel .panel-title p,.module[data-module-panel="tasks"] .debug-tasks-panel .panel-title p{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}#taskCenterRefreshBtn,#taskManageToggleBtn{width:42px;min-width:42px;padding-inline:0;font-size:0;gap:0}#taskCenterRefreshBtn .icon,#taskManageToggleBtn .icon{width:18px;height:18px}.task-manage-toggle-text{display:none}}
    @media(max-width:820px){.whitelist-head{display:grid;grid-template-columns:minmax(88px,auto) minmax(0,1fr);align-items:center}.whitelist-head .switch-row{min-width:0}.whitelist-head .port-actions{width:auto!important;margin-left:auto;display:flex!important;grid-template-columns:none!important;justify-content:flex-end;gap:8px}.whitelist-head .port-actions button{min-width:72px;padding-inline:12px}}
    @media(min-width:821px){.workflow-shell{grid-template-columns:minmax(320px,410px) minmax(620px,1fr)}.param-grid{min-width:640px;grid-template-columns:72px repeat(3,minmax(165px,1fr))}.param-control{grid-template-columns:minmax(72px,1fr) 70px}}
  </style>
</head>
<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true"><symbol id="i-bubble" viewBox="0 0 24 24"><circle cx="8" cy="9" r="4"></circle><circle cx="15" cy="14" r="5"></circle><path d="M5 17c2 2 5 3 9 3"></path></symbol><symbol id="i-source" viewBox="0 0 24 24"><path d="M4 7h16"></path><path d="M4 17h16"></path><circle cx="8" cy="7" r="3"></circle><circle cx="16" cy="17" r="3"></circle></symbol><symbol id="i-workflow" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="2"></rect><rect x="14" y="3" width="7" height="7" rx="2"></rect><rect x="8" y="14" width="8" height="7" rx="2"></rect><path d="M10 6h4"></path><path d="M12 10v4"></path></symbol><symbol id="i-tools" viewBox="0 0 24 24"><path d="M14 6l4 4"></path><path d="M4 20l8-8"></path><path d="M15 5l4-1-1 4L7 19H4v-3z"></path></symbol><symbol id="i-menu" viewBox="0 0 24 24"><path d="M4 6h16"></path><path d="M4 12h16"></path><path d="M4 18h16"></path></symbol><symbol id="i-sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2"></path><path d="M12 20v2"></path><path d="M4.93 4.93l1.41 1.41"></path><path d="M17.66 17.66l1.41 1.41"></path><path d="M2 12h2"></path><path d="M20 12h2"></path><path d="M4.93 19.07l1.41-1.41"></path><path d="M17.66 6.34l1.41-1.41"></path></symbol><symbol id="i-save" viewBox="0 0 24 24"><path d="M5 3h14l2 2v16H3V5z"></path><path d="M8 3v6h8V3"></path><path d="M8 21v-7h8v7"></path></symbol><symbol id="i-upload" viewBox="0 0 24 24"><path d="M12 16V4"></path><path d="M7 9l5-5 5 5"></path><path d="M4 20h16"></path></symbol><symbol id="i-trash" viewBox="0 0 24 24"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 14h10l1-14"></path><path d="M9 7V4h6v3"></path></symbol><symbol id="i-stop" viewBox="0 0 24 24"><rect x="6" y="6" width="12" height="12" rx="2"></rect></symbol><symbol id="i-edit" viewBox="0 0 24 24"><path d="M4 20h4l11-11-4-4L4 16z"></path><path d="M13 6l4 4"></path></symbol><symbol id="i-plus" viewBox="0 0 24 24"><path d="M12 5v14"></path><path d="M5 12h14"></path></symbol><symbol id="i-copy" viewBox="0 0 24 24"><rect x="8" y="8" width="12" height="12" rx="2"></rect><path d="M4 16V6a2 2 0 0 1 2-2h10"></path></symbol><symbol id="i-play" viewBox="0 0 24 24"><path d="M8 5v14l11-7z"></path></symbol><symbol id="i-refresh" viewBox="0 0 24 24"><path d="M21 12a9 9 0 0 1-15 6.7L3 16"></path><path d="M3 21v-5h5"></path><path d="M3 12a9 9 0 0 1 15-6.7L21 8"></path><path d="M21 3v5h-5"></path></symbol><symbol id="i-chevron" viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"></path></symbol></svg>
<div class="drawer-backdrop" id="drawerBackdrop"></div><div class="app" id="appShell"><aside class="sidebar"><div class="brand"><div class="brand-mark"><img class="brand-logo" src="/webui_logo.jpg" alt="Comfyui 泡泡版"></div><div class="brand-text"><strong>Comfyui 泡泡版</strong><span>Workflow Console</span></div></div><div class="nav-label">控制台</div><nav class="nav"><button type="button" data-module="sources"><svg class="icon"><use href="#i-source"></use></svg><span class="nav-text">接口配置</span></button><button type="button" data-module="workflows"><svg class="icon"><use href="#i-workflow"></use></svg><span class="nav-text">工作流管理</span></button><button type="button" data-module="debug"><svg class="icon"><use href="#i-play"></use></svg><span class="nav-text">在线调试</span></button><button type="button" data-module="tasks"><svg class="icon"><use href="#i-tools"></use></svg><span class="nav-text">任务中心</span></button><button type="button" data-module="tools"><svg class="icon"><use href="#i-trash"></use></svg><span class="nav-text">缓存清理</span></button></nav><div class="sidebar-footer"><button type="button" id="collapseBtn" class="btn-soft"><svg class="icon"><use href="#i-chevron"></use></svg><span class="collapse-text">收起侧栏</span></button></div></aside><main class="main"><header class="topbar"><div class="top-left"><button type="button" id="menuBtn" class="menu-btn"><svg class="icon"><use href="#i-menu"></use></svg></button><div class="title-block"><h1 id="moduleTitle">接口配置</h1><p id="moduleSubtitle">管理 ComfyUI 接口与工作流白名单</p></div></div><div class="top-actions"><button type="button" id="themeBtn" class="btn-soft"><svg class="icon"><use href="#i-sun"></use></svg><span class="theme-text">深色</span></button></div></header><div class="statusbar"><div id="toast" class="toast"></div></div><section class="module" data-module-panel="sources"><article class="panel"><div class="panel-head"><div class="panel-title"><h2>ComfyUI 接口</h2><p>支持动态添加接口、切换当前接口，并限制可用工作流。</p></div><div class="editor-actions"><button type="button" id="addPortBtn" class="btn-success"><svg class="icon"><use href="#i-plus"></use></svg>添加接口</button><button type="button" id="savePortsBtn" class="btn-primary"><svg class="icon"><use href="#i-save"></use></svg>保存</button><button type="button" id="refreshPortsBtn" class="btn-soft"><svg class="icon"><use href="#i-refresh"></use></svg>刷新</button></div></div><div class="panel-body"><div class="source-grid" id="portsList"></div></div></article></section><section class="module" data-module-panel="workflows"><div class="workflow-shell"><aside class="workflow-list-panel panel"><div class="workflow-toolbar"><div class="upload-inline upload-button-row"><input class="file-input-hidden" type="file" id="fileInput" accept=".json"><button type="button" id="uploadBtn" class="btn-primary"><svg class="icon"><use href="#i-upload"></use></svg>上传 .json</button></div><input id="workflowSearch" type="search" placeholder="搜索文件名、显示名称或说明"></div><div class="workflow-list" id="workflowList"></div><div class="list-footer"><span id="workflowCount">0 个工作流</span><button type="button" id="saveAllBtn" class="btn-primary"><svg class="icon"><use href="#i-save"></use></svg>保存全部</button></div></aside><article class="editor panel" id="workflowEditor"></article></div></section><section class="module" data-module-panel="debug"><div class="debug-shell"><article class="panel debug-form-panel"><div class="panel-head"><div class="panel-title"><h2>在线调用</h2><p>直接提交到 ComfyUI，用于调试接口和工作流。</p></div><button type="button" id="debugRunBtn" class="btn-success"><svg class="icon"><use href="#i-play"></use></svg>开始生成</button></div><div class="panel-body debug-form"><div class="field-grid"><div><label class="field-label">接口</label><select id="debugPort"></select></div><div><label class="field-label">工作流</label><select id="debugWorkflow"></select></div></div><div class="debug-section-head"><span>文本输入</span><button type="button" id="debugAddTextBtn" class="btn-soft"><svg class="icon"><use href="#i-plus"></use></svg>添加文本</button></div><div id="debugTexts" class="debug-texts"></div><div class="debug-section-head"><span>图片输入</span><button type="button" id="debugImageBtn" class="btn-soft"><svg class="icon"><use href="#i-upload"></use></svg>选择图片</button><input id="debugImageInput" type="file" accept="image/*" multiple class="file-input-hidden"></div><div id="debugImagePreview" class="debug-image-preview"></div><div class="debug-section-head"><span>视频输入</span><button type="button" id="debugVideoBtn" class="btn-soft"><svg class="icon"><use href="#i-upload"></use></svg>选择视频</button><input id="debugVideoInput" type="file" accept="video/*,.mp4,.webm,.mov,.avi,.mkv" multiple class="file-input-hidden"></div><div id="debugVideoPreview" class="debug-video-preview"></div></div></article><article class="panel debug-tasks-panel"><div class="panel-head"><div class="panel-title"><h2>任务列表</h2><p>仅显示 WebUI 在线调试提交的任务。</p></div><button type="button" id="debugRefreshBtn" class="btn-soft"><svg class="icon"><use href="#i-refresh"></use></svg>刷新</button></div><div class="debug-task-tabs"><button type="button" id="debugActiveTab" class="btn-soft active">正在进行</button><button type="button" id="debugHistoryTab" class="btn-soft">历史任务</button></div><div class="debug-task-list" id="debugTaskList"></div></article><article class="panel debug-output-panel"><div class="panel-head"><div class="panel-title"><h2>输出结果</h2><p id="debugOutputSub">选择一个任务查看输出。</p></div></div><div class="panel-body" id="debugOutput"><div class="empty">暂无输出</div></div></article></div></section><section class="module" data-module-panel="tasks"><div class="debug-shell task-center-shell"><article class="panel task-manage-panel" id="taskManagePanel"><div class="panel-head"><div class="panel-title"><h2>历史管理</h2><p id="taskManageSummary">切换到历史任务后可手动展开管理工具。</p></div><button type="button" id="taskManageToggleBtn" class="btn-soft"><svg class="icon"><use href="#i-chevron"></use></svg><span class="task-manage-toggle-text">展开</span></button></div><div class="panel-body task-manage-body"><div id="taskManageControls" class="task-manage-controls"><input id="taskSearch" type="search" placeholder="搜索工作流、接口、会话 ID、Prompt ID"><div class="task-manage-grid"><select id="taskFilterOrigin"></select><select id="taskFilterStatus"></select><select id="taskFilterPort"></select><select id="taskFilterSession"></select><select id="taskSort"><option value="created_desc">创建时间：新到旧</option><option value="created_asc">创建时间：旧到新</option><option value="elapsed_desc">耗时：长到短</option><option value="elapsed_asc">耗时：短到长</option><option value="workflow_asc">工作流名称</option></select></div><div class="task-manage-actions"><button type="button" id="taskSelectAllBtn" class="btn-soft">全选结果</button><button type="button" id="taskClearSelectionBtn" class="btn-soft">清空选择</button><button type="button" id="taskExportBtn" class="btn-primary" disabled>导出媒体 ZIP</button><button type="button" id="taskBulkDeleteBtn" class="btn-danger" disabled>批量删除</button></div></div></div></article><article class="panel debug-tasks-panel"><div class="panel-head"><div class="panel-title"><h2>全局任务</h2><p>汇总 WebUI、命令和 LLM 工具提交的任务。</p></div><button type="button" id="taskCenterRefreshBtn" class="btn-soft"><svg class="icon"><use href="#i-refresh"></use></svg>刷新</button></div><div class="debug-task-tabs"><button type="button" id="taskCenterActiveTab" class="btn-soft active">正在进行</button><button type="button" id="taskCenterHistoryTab" class="btn-soft">历史任务</button></div><div class="debug-task-list" id="taskCenterList"></div></article><article class="panel debug-output-panel"><div class="panel-head"><div class="panel-title"><h2>任务详情</h2><p id="taskCenterOutputSub">选择一个任务查看详情。</p></div></div><div class="panel-body" id="taskCenterOutput"><div class="empty">暂无任务</div></div></article></div></section><section class="module" data-module-panel="tools"><div class="tools-grid"><article class="card tool-card"><div class="card-head"><div class="card-title"><h3>清理缓存</h3><p>删除插件临时文件，不影响历史输出。</p></div></div><div class="card-body"><p class="danger-copy">会清理插件 tmp 下的临时中转文件。</p></div><div class="card-body"><button type="button" id="clearCacheBtn" class="btn-danger"><svg class="icon"><use href="#i-trash"></use></svg>清理缓存</button></div></article><article class="card tool-card"><div class="card-head"><div class="card-title"><h3>清理历史</h3><p>删除历史任务记录和本地历史媒体。</p></div></div><div class="card-body"><label class="field-label">保留最近小时数</label><div class="history-row"><input type="number" id="historyHours" value="48" min="0"><span class="chip">0 表示全部清理</span></div></div><div class="card-body"><button type="button" id="clearHistoryBtn" class="btn-danger"><svg class="icon"><use href="#i-trash"></use></svg>清理历史</button></div></article></div></section></main></div><div class="modal-backdrop" id="modalBackdrop"><div class="modal"><div class="modal-body"><h3 id="modalTitle"></h3><p id="modalMessage"></p></div><div class="modal-actions"><button type="button" id="modalCancel" class="btn-soft">取消</button><button type="button" id="modalConfirm" class="btn-danger">确认</button></div></div></div>
  <script>
const moduleInfo={sources:['接口配置','管理 ComfyUI 接口与工作流白名单'],workflows:['工作流管理','上传、编辑参数、说明与文件'],debug:['在线调试','直接调用 ComfyUI 并查看输出结果'],tasks:['任务中心','监控全部 ComfyUI 生成任务'],tools:['缓存清理','清理缓存与历史记录']};
const storageKeys={theme:'comfyui_bubble_theme',module:'comfyui_bubble_module',sidebar:'comfyui_bubble_sidebar_collapsed',selected:'comfyui_bubble_selected_workflow',debugPort:'comfyui_bubble_debug_port',debugWorkflow:'comfyui_bubble_debug_workflow'};
const state={files:[],ports:[],active:'',workflowOptions:[],module:localStorage.getItem(storageKeys.module)||'sources',theme:localStorage.getItem(storageKeys.theme)||'light',collapsed:localStorage.getItem(storageKeys.sidebar)==='1',selected:localStorage.getItem(storageKeys.selected)||'',query:'',dirty:new Set(),debugImages:[],debugVideos:[],debugTasks:[],debugHistory:[],debugTaskTab:'active',debugSelected:'',debugRenderKey:'',debugOutputKey:'',taskCenterTasks:[],taskCenterHistory:[],taskCenterTab:'active',taskCenterSelected:'',taskCenterRenderKey:'',taskCenterOutputKey:'',taskCenterSelectedIds:new Set(),taskManageOpen:false,taskFilters:{query:'',origin:'all',status:'all',port:'all',session:'all',sort:'created_desc'},debugTimer:null};
const $=(s,r)=>(r||document).querySelector(s);const $$=(s,r)=>Array.from((r||document).querySelectorAll(s));
const icon=n=>'<svg class="icon"><use href="#'+n+'"></use></svg>';const esc=v=>String(v??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const workflowNamePattern=/^[\u4e00-\u9fff\u3040-\u30ffA-Za-z0-9_.:-]+$/;const workflowNameHint='工作流名称只能使用中文、日文、英文、数字、下划线 _、中划线 -、英文句号 . 和冒号 :';const isValidWorkflowName=name=>!!String(name||'').trim()&&workflowNamePattern.test(String(name).trim());const validateWorkflowNameField=(show=true)=>{const input=$('#workflowName');if(!input)return true;const ok=isValidWorkflowName(input.value);input.classList.toggle('field-error',!ok);if(!ok&&show)toast(workflowNameHint,false);return ok};const workflowObjectName=f=>((f&&f.params&&f.params.name)||f.name||'').trim();function findDuplicateWorkflowName(){const seen=new Map();for(const f of state.files){const name=workflowObjectName(f);if(!name)continue;if(seen.has(name))return{name,first:seen.get(name),second:f.filename};seen.set(name,f.filename)}return null}function validateUniqueWorkflowNames(show=true){const dup=findDuplicateWorkflowName();if(!dup)return true;if(show)toast('工作流调用名称「'+dup.name+'」重复：'+dup.first+' 与 '+dup.second,false);state.selected=dup.second;renderWorkflowList();renderWorkflowEditor();return false}
const desc=(f,k)=>f&&f.description&&typeof f.description==='object'?(f.description[k]||''):(k==='short'?(f&&f.description)||'':'');const setDesc=(f,s,d)=>{f.description={short:s||'',detailed:d||''}};
const getFile=fn=>state.files.find(f=>f.filename===fn);let toastTimer=null;const toast=(t,ok=true)=>{const e=$('#toast');if(toastTimer)clearTimeout(toastTimer);e.textContent=t||'';e.className=t?'toast show '+(ok?'ok':'err'):'toast';if(t)toastTimer=setTimeout(()=>{e.className='toast'},3000)};
async function api(path,body){const res=await fetch(path,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{});const data=await res.json().catch(()=>({}));if(!res.ok||data.ok===false)throw new Error(data.error||res.statusText);return data}
function setLoading(btn,on,label='处理中...'){if(!btn)return;if(on){btn.dataset.prev=btn.innerHTML;btn.innerHTML=label;btn.disabled=true}else{btn.innerHTML=btn.dataset.prev||btn.innerHTML;btn.disabled=false;delete btn.dataset.prev}}
function payload(f){return{filename:f.filename,description:{short:desc(f,'short'),detailed:desc(f,'detailed')},params:f.params||{}}}
function applyTheme(){document.documentElement.dataset.theme=state.theme==='dark'?'dark':'light';$('#themeBtn').innerHTML=icon('i-sun')+'<span class="theme-text">'+(state.theme==='dark'?'浅色':'深色')+'</span>';localStorage.setItem(storageKeys.theme,state.theme)}
function applySidebar(){$('#appShell').classList.toggle('collapsed',state.collapsed);localStorage.setItem(storageKeys.sidebar,state.collapsed?'1':'0')}
function closeSidebar(){document.body.classList.remove('drawer-open')}
function setModule(name){state.module=moduleInfo[name]?name:'sources';localStorage.setItem(storageKeys.module,state.module);$$('.nav button').forEach(b=>b.classList.toggle('active',b.dataset.module===state.module));$$('.module').forEach(p=>p.classList.toggle('active',p.dataset.modulePanel===state.module));$('#moduleTitle').textContent=moduleInfo[state.module][0];$('#moduleSubtitle').textContent=moduleInfo[state.module][1];closeSidebar()}
function confirmAction(title,msg,ok='确认',danger=true){return new Promise(resolve=>{const back=$('#modalBackdrop');$('#modalTitle').textContent=title;$('#modalMessage').textContent=msg;$('#modalConfirm').textContent=ok;$('#modalConfirm').className=danger?'btn-danger':'btn-primary';back.classList.add('show');const done=v=>{back.classList.remove('show');resolve(v)};$('#modalCancel').onclick=()=>done(false);$('#modalConfirm').onclick=()=>done(true)})}
function textInputAction(title,label,value,ok='确认'){return new Promise(resolve=>{const back=$('#modalBackdrop');$('#modalTitle').textContent=title;$('#modalMessage').innerHTML='<label class="field-label">'+esc(label)+'</label><input id="modalTextInput" value="'+esc(value||'')+'">';$('#modalConfirm').textContent=ok;$('#modalConfirm').className='btn-primary';back.classList.add('show');setTimeout(()=>{$('#modalTextInput').focus();$('#modalTextInput').select()},0);const done=v=>{const input=$('#modalTextInput');const value=input?input.value.trim():'';back.classList.remove('show');$('#modalMessage').textContent='';resolve(v?value:null)};$('#modalCancel').onclick=()=>done(false);$('#modalConfirm').onclick=()=>done(true);$('#modalTextInput').onkeydown=e=>{if(e.key==='Enter')done(true)}})}
function interfaceInputAction(){return new Promise(resolve=>{const back=$('#modalBackdrop');$('#modalTitle').textContent='添加接口';$('#modalMessage').innerHTML='<div class="form-row"><label class="field-label">接口名称</label><input id="modalPortName" placeholder="例如：高性能机"></div><div class="form-row" style="margin-top:12px"><label class="field-label">HTTP 地址</label><input id="modalPortHttp" placeholder="例如：http://127.0.0.1:8188"></div>';$('#modalConfirm').textContent='添加';$('#modalConfirm').className='btn-primary';back.classList.add('show');setTimeout(()=>$('#modalPortName').focus(),0);const done=v=>{const name=$('#modalPortName')?$('#modalPortName').value.trim():'';const http=$('#modalPortHttp')?$('#modalPortHttp').value.trim():'';back.classList.remove('show');$('#modalMessage').textContent='';resolve(v?{name,http}:null)};$('#modalCancel').onclick=()=>done(false);$('#modalConfirm').onclick=()=>done(true);$$('#modalPortName,#modalPortHttp').forEach(input=>input.onkeydown=e=>{if(e.key==='Enter')done(true)})})}
function markDirty(fn){state.dirty.add(fn);renderWorkflowList();renderEditorHeader()}function clearDirty(fn){state.dirty.delete(fn);renderWorkflowList();renderEditorHeader()}
function allWorkflows(p){return !p.workflows||p.workflows.length===0}
function renderPorts(){const root=$('#portsList');root.innerHTML=state.ports.map((p,i)=>{const all=allWorkflows(p), active=p.name&&p.name===state.active, count=all?state.workflowOptions.length:(p.workflows||[]).length;const checks=state.workflowOptions.map(w=>{const selected=!all&&(p.workflows||[]).includes(w.name);return '<label class="workflow-check '+(selected?'selected ':'')+(all?'disabled':'')+'"><input type="checkbox" class="workflow-check-input" data-port="'+i+'" value="'+esc(w.name)+'" '+(selected?'checked':'')+' '+(all?'disabled':'')+'><span class="workflow-title">'+esc(w.name)+'</span></label>'}).join('')||'<div class="empty">暂无工作流</div>';return '<article class="source-card card" data-port="'+i+'"><div class="source-head"><div><div class="source-title"><span class="source-dot '+(active?'active':'')+'"></span><div class="source-name">'+esc(p.name||'接口 '+(i+1))+'</div></div><div class="source-http">'+esc(p.http||'未配置地址')+'</div></div><span class="chip">'+count+' 个工作流</span></div><div class="field-grid"><div><label class="field-label">名称</label><input class="port-name" value="'+esc(p.name||'')+'" placeholder="例如：高性能机"></div><div><label class="field-label">HTTP 地址</label><input class="port-http" value="'+esc(p.http||'')+'" placeholder="例如：http://127.0.0.1:8188"></div></div><div class="whitelist-head"><label class="switch-row"><input type="checkbox" class="all-workflows-check" data-port="'+i+'" '+(all?'checked':'')+'><span class="switch"></span><span>全选</span></label><div class="editor-actions port-actions"><button type="button" class="btn-soft btn-active-port '+(active?'current':'')+'" data-port="'+i+'" '+(!p.name?'disabled':'')+'>'+(active?'当前':'设为当前')+'</button><button type="button" class="btn-delete-port" data-port="'+i+'" '+(state.ports.length<=1?'disabled':'')+'>删除</button></div></div><div class="workflow-checks">'+checks+'</div></article>'}).join('')||'<div class="empty">暂无接口，请点击添加接口。</div>';$$('.all-workflows-check').forEach(input=>input.onchange=()=>{const i=+input.dataset.port;state.ports[i].workflows=input.checked?[]:state.workflowOptions.map(w=>w.name);renderPorts()});$$('.workflow-check-input').forEach(input=>input.onchange=()=>{input.closest('.workflow-check').classList.toggle('selected',input.checked)});$$('.btn-active-port').forEach(btn=>btn.onclick=async()=>{try{const p=state.ports[+btn.dataset.port];if(!p||p.name===state.active)return;const data=await api('/api/active_port',{name:p.name});state.active=data.active||p.name;toast('当前接口已切换为 '+state.active);renderPorts()}catch(e){toast(e.message,false)}});$$('.btn-delete-port').forEach(btn=>btn.onclick=async()=>{const i=+btn.dataset.port,p=state.ports[i];if(!p)return;if(!await confirmAction('删除接口','将删除接口 '+(p.name||('接口 '+(i+1)))+'。','删除',true))return;state.ports.splice(i,1);if(p.name===state.active)state.active=(state.ports[0]&&state.ports[0].name)||'';renderPorts()})}
async function addInterface(){const item=await interfaceInputAction();if(!item)return;if(!item.name||!item.http){toast('接口名称和 HTTP 地址不能为空',false);return}if(state.ports.some(p=>p.name===item.name)){toast('接口名称不能重复：'+item.name,false);return}state.ports.push({name:item.name,http:item.http,workflows:[]});if(!state.active)state.active=item.name;toast('已添加接口，请保存');renderPorts()}
function collectPorts(){const ports=$$('.source-card').map(card=>{const all=$('.all-workflows-check',card).checked;return{name:$('.port-name',card).value.trim(),http:$('.port-http',card).value.trim(),workflows:all?[]:$$('.workflow-check-input:checked',card).map(i=>i.value)}});const names=new Set();for(const p of ports){if(!p.name||!p.http)throw new Error('接口名称和 HTTP 地址不能为空');if(names.has(p.name))throw new Error('接口名称不能重复：'+p.name);names.add(p.name)}return ports}
function hasKind(f,k){if(k==='all')return true;const p=f.params||{}, input=p.inputs&&p.inputs[k];if(input&&input.limit!==undefined&&input.limit!==null&&input.limit!=='')return Number(input.limit)>0||input.mode==='strict';return [f.filename,f.name,desc(f,'short')].join(' ').toLowerCase().includes({text:'文本',image:'图片',video:'视频'}[k])}
function renderWorkflowList(){const q=state.query.toLowerCase();const items=state.files.filter(f=>{const text=[f.filename,f.name,desc(f,'short'),desc(f,'detailed')].join(' ').toLowerCase();return !q||text.includes(q)});if(!state.selected||!getFile(state.selected))state.selected=(items[0]||state.files[0]||{}).filename||'';localStorage.setItem(storageKeys.selected,state.selected||'');$('#workflowList').innerHTML=items.map(f=>'<button type="button" class="workflow-item '+(f.filename===state.selected?'active':'')+'" data-filename="'+esc(f.filename)+'">'+(state.dirty.has(f.filename)?'<span class="dirty-dot"></span>':'')+'<span class="workflow-item-main"><span class="workflow-item-title">'+esc(f.name||f.filename)+'</span><span class="workflow-item-meta">'+esc(f.filename)+'</span></span></button>').join('')||'<div class="empty">没有匹配的工作流</div>';$('#workflowCount').textContent=items.length+'/'+state.files.length+' 个工作流';$$('.workflow-item').forEach(b=>b.onclick=()=>{state.selected=b.dataset.filename;renderWorkflowList();renderWorkflowEditor()})}
function debugPort(){const name=$('#debugPort')?$('#debugPort').value:'';return state.ports.find(p=>p.name===name)||state.ports[0]||null}
function debugWorkflows(){const p=debugPort();if(!p)return[];const allowed=p.workflows||[];return state.files.filter(f=>!allowed.length||allowed.includes(f.name))}
function debugWorkflow(){const name=$('#debugWorkflow')?$('#debugWorkflow').value:'';return debugWorkflows().find(f=>(f.name||f.filename)===name)||debugWorkflows()[0]||null}
function debugRuleLimit(f,key){const p=f&&f.params&&f.params.inputs&&f.params.inputs[key];const n=p&&p.limit!==null&&p.limit!==undefined&&p.limit!==''?parseInt(p.limit,10):null;return Number.isFinite(n)?Math.max(0,n):null}
function renderDebugSelectors(){const portSel=$('#debugPort'), wfSel=$('#debugWorkflow');if(!portSel||!wfSel)return;const savedPort=localStorage.getItem(storageKeys.debugPort)||state.active;portSel.innerHTML=state.ports.map(p=>'<option value="'+esc(p.name)+'">'+esc(p.name||'未命名接口')+'</option>').join('');portSel.value=state.ports.some(p=>p.name===savedPort)?savedPort:((state.ports[0]||{}).name||'');const workflows=debugWorkflows();const savedWorkflow=localStorage.getItem(storageKeys.debugWorkflow)||'';wfSel.innerHTML=workflows.map(f=>'<option value="'+esc(f.name||f.filename)+'">'+esc(f.name||f.filename)+'</option>').join('');wfSel.value=workflows.some(f=>(f.name||f.filename)===savedWorkflow)?savedWorkflow:((workflows[0]||{}).name||(workflows[0]||{}).filename||'');if(!workflows.length)wfSel.innerHTML='<option value="">当前接口暂无可用工作流</option>';renderDebugTextDefaults(false)}
function debugTextValues(){return $$('.debug-text-input').map(t=>t.value)}
function addDebugText(value=''){const root=$('#debugTexts');if(!root)return;const index=$$('.debug-text-row',root).length+1;const row=document.createElement('div');row.className='debug-text-row';row.innerHTML='<textarea class="debug-text-input" rows="3" placeholder="文本 '+index+'">'+esc(value)+'</textarea><button type="button" class="btn-soft" title="移除">×</button>';row.querySelector('button').onclick=()=>{row.remove();if(!$$('.debug-text-row',root).length)addDebugText('')};root.appendChild(row)}
function renderDebugTextDefaults(force=true){const root=$('#debugTexts');if(!root)return;const existing=debugTextValues().filter(v=>v.trim());if(!force&&$$('.debug-text-row',root).length)return;root.innerHTML='';const f=debugWorkflow();const limit=debugRuleLimit(f,'text');const count=Math.max(1,limit||1);for(let i=0;i<count;i++)addDebugText(existing[i]||'')}
function dataUrlToFile(dataUrl,name){const parts=String(dataUrl||'').split(','),meta=parts[0]||'',b64=parts[1]||'',mime=(meta.match(/data:(.*?);base64/)||[])[1]||'image/png';const bin=atob(b64),bytes=new Uint8Array(bin.length);for(let i=0;i<bin.length;i++)bytes[i]=bin.charCodeAt(i);return new File([bytes],name||'image.png',{type:mime})}
function addDebugImageFiles(files){let rejected=0;Array.from(files||[]).forEach(file=>{if(!file.type||!file.type.startsWith('image/')){rejected++;return}state.debugImages.push({file,url:URL.createObjectURL(file)})});if(rejected)toast('已忽略非图片文件',false);renderDebugImages()}
function clearDebugImages(){state.debugImages.forEach(item=>{if(item.url&&item.url.startsWith('blob:'))URL.revokeObjectURL(item.url)});state.debugImages=[]}
function renderDebugImages(){const root=$('#debugImagePreview');if(!root)return;root.innerHTML=state.debugImages.map((item,i)=>'<div class="debug-image-tile"><img src="'+esc(item.url)+'" alt="'+esc(item.file.name)+'"><span class="debug-image-name">'+esc(item.file.name)+'</span><button type="button" data-i="'+i+'">×</button></div>').join('')||'<div class="empty">点击选择或拖拽图片到这里</div>';$$('button',root).forEach(b=>b.onclick=()=>{const i=+b.dataset.i;const item=state.debugImages[i];if(item&&item.url&&item.url.startsWith('blob:'))URL.revokeObjectURL(item.url);state.debugImages.splice(i,1);renderDebugImages()})}
function formatBytes(n){n=Number(n)||0;if(n<1024)return n+' B';if(n<1048576)return (n/1024).toFixed(1)+' KB';return (n/1048576).toFixed(1)+' MB'}
function formatTaskTime(ts){const n=Number(ts)||0;if(!n)return'未知时间';const d=new Date(n*1000);const pad=v=>String(v).padStart(2,'0');return d.getFullYear()+' '+pad(d.getMonth()+1)+'-'+pad(d.getDate())+' '+pad(d.getHours())+':'+pad(d.getMinutes())}
function taskElapsed(t){const n=Number(t&&t.elapsed);return Number.isFinite(n)&&n>0?n.toFixed(1)+'s':'-'}
function isVideoFile(file){return file&&(file.type&&file.type.startsWith('video/')||/\.(mp4|webm|mov|avi|mkv)$/i.test(file.name||''))}
function addDebugVideoFiles(files){let rejected=0;Array.from(files||[]).forEach(file=>{if(!isVideoFile(file)){rejected++;return}state.debugVideos.push({file})});if(rejected)toast('已忽略非视频文件',false);renderDebugVideos()}
function clearDebugVideos(){state.debugVideos=[]}
function renderDebugVideos(){const root=$('#debugVideoPreview');if(!root)return;root.innerHTML=state.debugVideos.map((item,i)=>'<div class="debug-video-item"><div><div class="debug-video-name">'+esc(item.file.name)+'</div><div class="debug-video-size">'+formatBytes(item.file.size)+'</div></div><button type="button" data-i="'+i+'">×</button></div>').join('')||'<div class="empty">点击选择或拖拽视频到这里</div>';$$('button',root).forEach(b=>b.onclick=()=>{state.debugVideos.splice(+b.dataset.i,1);renderDebugVideos()})}
const statusText=s=>({queued:'排队中',running:'运行中',completed:'完成',failed:'失败',timeout:'超时',canceled:'已停止'}[s]||s||'未知');
function currentDebugTasks(){return state.debugTaskTab==='history'?state.debugHistory:state.debugTasks}
function taskThumb(t){const rv=t.result&&t.result.videos?t.result.videos.length:0,sv=t.result_summary&&t.result_summary.videos?t.result_summary.videos:0;return t.thumbnail?'<img src="'+esc(t.thumbnail)+'" alt="缩略图">':(rv||sv?'视频':'无图')}
function taskRenderKey(items,selected){return selected+'|'+items.map(t=>[t.task_id,t.status,t.elapsed,t.thumbnail,t.prompt_id,t.workflow_name,t.port_name,t.origin,t.session_label,t.created_at].join('~')).join('|')}
function taskDetailKey(t){if(!t)return'';return [t.task_id,t.status,t.elapsed,t.error,t.summary,t.thumbnail,JSON.stringify(t.input||{}),JSON.stringify(t.result||{})].join('|')}
const isLiveTask=t=>['queued','running'].includes(String(t&&t.status||''));
function taskActionHtml(t){return isLiveTask(t)?'<span class="debug-task-stop" data-stop="'+esc(t.task_id)+'" title="停止任务">'+icon('i-stop')+'</span>':'<span class="debug-task-delete" data-delete="'+esc(t.task_id)+'" title="删除任务">'+icon('i-trash')+'</span>'}
function renderDebugTaskTabs(){const a=$('#debugActiveTab'),h=$('#debugHistoryTab');if(a)a.classList.toggle('active',state.debugTaskTab==='active');if(h)h.classList.toggle('active',state.debugTaskTab==='history')}
function mergeDebugTaskDetail(task){const lists=[state.debugTasks,state.debugHistory,state.taskCenterTasks,state.taskCenterHistory];for(const list of lists){const i=list.findIndex(x=>x.task_id===task.task_id);if(i>=0){list[i]={...list[i],...task,summary:false};return list[i]}}return task}
async function ensureDebugTaskDetail(t){if(!t||!t.summary)return t;const data=await api('/api/debug/tasks/'+encodeURIComponent(t.task_id));return mergeDebugTaskDetail(data.task||t)}
function renderDebugTasks(force=false){const root=$('#debugTaskList');if(!root)return;renderDebugTaskTabs();const items=currentDebugTasks();const key=state.debugTaskTab+'|'+taskRenderKey(items,state.debugSelected);if(!force&&key===state.debugRenderKey)return;state.debugRenderKey=key;const scrollTop=root.scrollTop;root.innerHTML=items.map(t=>'<button type="button" class="debug-task '+(t.task_id===state.debugSelected?'active':'')+'" data-id="'+esc(t.task_id)+'"><span class="debug-task-thumb">'+taskThumb(t)+'</span><span class="debug-task-content"><span class="debug-task-title"><strong>'+esc(t.workflow_name||t.prompt_id||t.task_id)+'</strong><span class="debug-status '+esc(t.status)+'">'+esc(statusText(t.status))+'</span><span class="debug-task-copy" data-copy="'+esc(t.task_id)+'" title="还原本次输入">'+icon('i-copy')+'</span>'+taskActionHtml(t)+'</span><span class="debug-task-meta"><span>'+esc(t.port_name||'')+'</span><span>'+esc(t.prompt_id||'未提交')+'</span><span>'+esc((t.elapsed||0)+'s')+'</span></span></span></button>').join('')||'<div class="empty">'+(state.debugTaskTab==='history'?'暂无历史任务':'暂无正在进行的任务')+'</div>';root.scrollTop=scrollTop;$$('.debug-task',root).forEach(b=>b.onclick=()=>{state.debugSelected=b.dataset.id;renderDebugTasks();renderDebugOutput(true)});$$('.debug-task-copy',root).forEach(b=>b.onclick=async e=>{e.stopPropagation();let t=[...state.debugTasks,...state.debugHistory].find(x=>x.task_id===b.dataset.copy);if(t){try{t=await ensureDebugTaskDetail(t);restoreDebugTaskInput(t)}catch(err){toast(err.message,false)}}});$$('.debug-task-stop',root).forEach(b=>b.onclick=async e=>{e.stopPropagation();await stopDebugTask(b.dataset.stop)});$$('.debug-task-delete',root).forEach(b=>b.onclick=async e=>{e.stopPropagation();await deleteDebugTask(b.dataset.delete)})}
function renderDebugInputSection(t){const input=t.input||{},texts=input.texts||[],imgs=input.images||[],videos=input.videos||[];let html='<div class="debug-detail-section"><h3>本次输入</h3><div class="debug-input-grid"><div class="debug-input-chip">接口：'+esc(input.port_name||t.port_name||'')+'</div><div class="debug-input-chip">工作流：'+esc(input.workflow_name||t.workflow_name||'')+'</div></div>';html+=texts.length?'<div class="debug-output-texts">'+texts.map((x,i)=>'<div class="debug-output-text">文本 '+(i+1)+'：\n'+esc(x)+'</div>').join('')+'</div>':'<div class="empty">本次没有文本输入</div>';if(imgs.length)html+='<div class="debug-input-images">'+imgs.map(img=>'<a href="'+esc(img.data_url)+'" target="_blank" rel="noopener"><img src="'+esc(img.data_url)+'" alt="'+esc(img.name||'输入图片')+'"><span class="debug-image-name">'+esc(img.name||'输入图片')+'</span></a>').join('')+'</div>';if(videos.length)html+='<div class="debug-input-videos">'+videos.map(v=>'<div class="debug-video-item"><div><div class="debug-video-name">'+esc(v.name||v.filename||'输入视频')+'</div><div class="debug-video-size">'+formatBytes(v.size||0)+'</div></div></div>').join('')+'</div>';html+='</div>';return html}
function renderDebugOutput(force=false){const root=$('#debugOutput');if(!root)return;const items=currentDebugTasks();const t=items.find(x=>x.task_id===state.debugSelected)||items[0];if(!t){state.debugOutputKey='';$('#debugOutputSub').textContent='选择一个任务查看输出。';root.innerHTML='<div class="empty">暂无输出</div>';return}state.debugSelected=t.task_id;const key=state.debugTaskTab+'|'+taskDetailKey(t);if(!force&&key===state.debugOutputKey)return;state.debugOutputKey=key;$('#debugOutputSub').textContent=(t.workflow_name||'任务')+' · '+statusText(t.status)+' · '+(t.prompt_id||'');if(t.summary){root.innerHTML='<div class="empty">正在加载任务详情...</div>';ensureDebugTaskDetail(t).then(()=>{renderDebugTasks(true);renderDebugOutput(true)}).catch(e=>toast(e.message,false));return}const r=t.result||{}, texts=r.texts||[], images=r.images||[], videos=r.videos||[];let out='';if(t.error)out='<div class="debug-error">'+esc(t.error)+'</div>';else{if(texts.length)out+='<div class="debug-output-texts">'+texts.map(x=>'<div class="debug-output-text">'+esc(x)+'</div>').join('')+'</div>';if(images.length)out+='<div class="debug-output-images">'+images.map(u=>'<a href="'+esc(u)+'" target="_blank" rel="noopener"><img src="'+esc(u)+'" alt="ComfyUI 输出图片"></a>').join('')+'</div>';if(videos.length)out+='<div class="debug-output-videos">'+videos.map(u=>'<div class="debug-video-card"><video controls preload="metadata" src="'+esc(u)+'"></video><a class="debug-video-link" href="'+esc(u)+'" target="_blank" rel="noopener">打开/下载视频</a></div>').join('')+'</div>';if(!out)out='<div class="empty">'+(t.status==='completed'?'任务已完成，但没有文本、图片或视频输出':'等待输出结果...')+'</div>'}root.innerHTML='<div class="debug-detail">'+renderDebugInputSection(t)+'<div class="debug-detail-section"><h3>输出结果</h3>'+out+'</div></div>'}
function restoreDebugTaskInput(t){const input=t.input||{};if(input.port_name){$('#debugPort').value=input.port_name;localStorage.setItem(storageKeys.debugPort,input.port_name);renderDebugSelectors()}if(input.workflow_name){$('#debugWorkflow').value=input.workflow_name;localStorage.setItem(storageKeys.debugWorkflow,input.workflow_name)}const root=$('#debugTexts');root.innerHTML='';(input.texts&&input.texts.length?input.texts:['']).forEach(text=>addDebugText(text));clearDebugImages();(input.images||[]).forEach(img=>{try{const file=dataUrlToFile(img.data_url,img.name);state.debugImages.push({file,url:img.data_url})}catch(e){}});renderDebugImages();clearDebugVideos();renderDebugVideos();toast((input.videos&&input.videos.length)?'已还原文本和图片，视频输入需重新选择':'已还原本次输入')}
async function deleteDebugTask(taskId){const t=[...state.debugTasks,...state.debugHistory,...state.taskCenterTasks,...state.taskCenterHistory].find(x=>x.task_id===taskId);if(!t)return;if(isLiveTask(t)){await stopDebugTask(taskId);return}const msg='将删除该任务的本地历史记录、缩略图和关联媒体。';if(!await confirmAction('删除任务',msg,'删除',true))return;try{await api('/api/debug/delete',{task_id:taskId});toast('任务已删除');if(state.debugSelected===taskId)state.debugSelected='';if(state.taskCenterSelected===taskId)state.taskCenterSelected='';await Promise.all([loadDebugTasks(),loadTaskCenter()])}catch(e){toast(e.message,false)}}
async function stopDebugTask(taskId){const t=[...state.debugTasks,...state.taskCenterTasks].find(x=>x.task_id===taskId);if(!t)return;if(!await confirmAction('停止任务','将尝试从 ComfyUI 队列中移除该任务，或中断正在运行的生成。','停止',true))return;try{await api('/api/debug/stop',{task_id:taskId});toast('任务已停止');if(state.debugSelected===taskId)state.debugSelected='';if(state.taskCenterSelected===taskId)state.taskCenterSelected='';await Promise.all([loadDebugTasks(),loadTaskCenter()])}catch(e){toast(e.message,false);await Promise.all([loadDebugTasks(),loadTaskCenter()])}}
async function loadDebugTasks(){try{const [active,history]=await Promise.all([api('/api/debug/tasks?origin=webui'),api('/api/debug/history?origin=webui')]);state.debugTasks=active.tasks||[];const oldHistory=new Map(state.debugHistory.map(t=>[t.task_id,t]));state.debugHistory=(history.tasks||[]).map(t=>{const old=oldHistory.get(t.task_id);return old&&!old.summary?{...old,...t,summary:false}:t});const items=currentDebugTasks();if(!items.some(t=>t.task_id===state.debugSelected))state.debugSelected=(items[0]||{}).task_id||'';renderDebugTasks();renderDebugOutput()}catch(e){toast(e.message,false)}}
function selectedTaskIds(){return [...state.taskCenterSelectedIds].filter(id=>state.taskCenterHistory.some(t=>t.task_id===id))}
async function bulkDeleteTaskHistory(){const ids=selectedTaskIds();if(!ids.length){toast('请先选择历史任务',false);return}if(!await confirmAction('批量删除','将删除选中的 '+ids.length+' 条历史任务、本地缩略图和关联媒体。','删除',true))return;try{const data=await api('/api/debug/bulk_delete',{task_ids:ids});state.taskCenterSelectedIds.clear();if(state.taskCenterSelected&&ids.includes(state.taskCenterSelected))state.taskCenterSelected='';toast('已删除 '+(data.deleted??0)+' 条历史任务'+((data.skipped??0)?'，跳过 '+data.skipped+' 条':''));await Promise.all([loadDebugTasks(),loadTaskCenter()])}catch(e){toast(e.message,false);await loadTaskCenter()}}
async function exportTaskHistoryMedia(){const ids=selectedTaskIds();if(!ids.length){toast('请先选择历史任务',false);return}try{const res=await fetch('/api/debug/export_media',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({task_ids:ids})});if(!res.ok){const data=await res.json().catch(()=>({}));throw new Error(data.error||res.statusText)}const blob=await res.blob();const url=URL.createObjectURL(blob);const a=document.createElement('a');const cd=res.headers.get('Content-Disposition')||'';const m=/filename="?([^"]+)"?/i.exec(cd);a.href=url;a.download=m?m[1]:'comfyui_history_media.zip';document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1200);toast('媒体 ZIP 已开始下载')}catch(e){toast(e.message,false)}}
async function submitDebugTask(){const port=$('#debugPort').value,wf=$('#debugWorkflow').value,texts=debugTextValues().map(v=>v.trim()).filter(Boolean);if(!port){toast('请选择接口',false);return}if(!wf){toast('请选择工作流',false);return}const fd=new FormData();fd.append('port_name',port);fd.append('workflow_name',wf);fd.append('texts',JSON.stringify(texts));state.debugImages.forEach(item=>fd.append('images[]',item.file,item.file.name));state.debugVideos.forEach(item=>fd.append('videos[]',item.file,item.file.name));const btn=$('#debugRunBtn');try{setLoading(btn,true,'提交中...');const res=await fetch('/api/debug/submit',{method:'POST',body:fd});const data=await res.json();if(!res.ok||data.ok===false)throw new Error(data.error||res.statusText);toast('任务已提交');state.debugTaskTab='active';state.debugSelected=data.task&&data.task.task_id||'';await loadDebugTasks()}catch(e){toast(e.message,false);await loadDebugTasks()}finally{setLoading(btn,false)}}
function originText(t){if(t&&t.origin==='command')return'command';return t.origin_label||({webui:'WebUI',llm_tool:'LLM 工具'}[t.origin]||t.origin||'任务')}
function sessionLabel(t){const s=String(t&&t.session_label||'').trim();return s&&s!=='****ault'?s:''}
function currentTaskCenterTasks(){return state.taskCenterTab==='history'?filteredTaskCenterHistory():state.taskCenterTasks}
function renderTaskCenterTabs(){const a=$('#taskCenterActiveTab'),h=$('#taskCenterHistoryTab'),m=$('#taskManagePanel'),b=$('#taskManageToggleBtn');if(a)a.classList.toggle('active',state.taskCenterTab==='active');if(h)h.classList.toggle('active',state.taskCenterTab==='history');if(m){m.classList.toggle('active-only',state.taskCenterTab!=='history');m.classList.toggle('collapsed',state.taskCenterTab==='history'&&!state.taskManageOpen)}if(b){b.classList.toggle('open',state.taskManageOpen);const text=$('.task-manage-toggle-text',b);if(text)text.textContent=state.taskManageOpen?'收起':'展开'}}
function taskFilterText(t){return [t.workflow_name,t.workflow_file,t.prompt_id,t.task_id,t.origin,t.origin_label,t.port_name,t.status,t.error,t.session_label,sessionLabel(t)].join(' ').toLowerCase()}
function uniqueTaskValues(field,fn){const values=[...new Set(state.taskCenterHistory.map(t=>String(fn?fn(t):(t[field]||'')).trim()).filter(Boolean))];return values.sort((a,b)=>a.localeCompare(b,'zh-CN'))}
function selectOptions(el,items,allLabel,current){if(!el)return;const value=current||'all';el.innerHTML='<option value="all">'+allLabel+'</option>'+items.map(v=>'<option value="'+esc(v)+'">'+esc(v)+'</option>').join('');el.value=items.includes(value)?value:'all';return el.value}
function renderTaskManage(){renderTaskCenterTabs();const panel=$('#taskManagePanel');if(!panel)return;if(state.taskCenterTab!=='history'){const summary=$('#taskManageSummary');if(summary)summary.textContent='切换到历史任务后可手动展开管理工具。';return}const f=state.taskFilters;f.origin=selectOptions($('#taskFilterOrigin'),uniqueTaskValues('origin',t=>originText(t)),'全部来源',f.origin);f.status=selectOptions($('#taskFilterStatus'),uniqueTaskValues('status',t=>statusText(t.status)),'全部状态',f.status);f.port=selectOptions($('#taskFilterPort'),uniqueTaskValues('port_name'),'全部接口',f.port);f.session=selectOptions($('#taskFilterSession'),uniqueTaskValues('session_label',t=>sessionLabel(t)),'全部会话',f.session);const q=$('#taskSearch');if(q&&q.value!==f.query)q.value=f.query;const sort=$('#taskSort');if(sort)sort.value=f.sort||'created_desc';const items=filteredTaskCenterHistory();for(const id of [...state.taskCenterSelectedIds]){if(!state.taskCenterHistory.some(t=>t.task_id===id))state.taskCenterSelectedIds.delete(id)}const selected=state.taskCenterSelectedIds.size;const summary=$('#taskManageSummary');if(summary)summary.textContent=state.taskManageOpen?('当前结果 '+items.length+' 条，已选 '+selected+' 条'):('历史任务 '+items.length+' 条，点击展开进行管理。');const exportBtn=$('#taskExportBtn'),deleteBtn=$('#taskBulkDeleteBtn');if(exportBtn)exportBtn.disabled=selected===0;if(deleteBtn)deleteBtn.disabled=selected===0}
function filteredTaskCenterHistory(){const f=state.taskFilters,q=String(f.query||'').trim().toLowerCase();let items=state.taskCenterHistory.filter(t=>{if(q&&!taskFilterText(t).includes(q))return false;if(f.origin!=='all'&&originText(t)!==f.origin)return false;if(f.status!=='all'&&statusText(t.status)!==f.status)return false;if(f.port!=='all'&&String(t.port_name||'')!==f.port)return false;if(f.session!=='all'&&sessionLabel(t)!==f.session)return false;return true});const n=v=>Number(v||0);items=[...items].sort((a,b)=>{switch(f.sort){case'created_asc':return n(a.created_at)-n(b.created_at);case'elapsed_desc':return n(b.elapsed)-n(a.elapsed);case'elapsed_asc':return n(a.elapsed)-n(b.elapsed);case'workflow_asc':return String(a.workflow_name||'').localeCompare(String(b.workflow_name||''),'zh-CN');default:return n(b.created_at)-n(a.created_at)}});return items}
function renderTaskCenterTasks(force=false){const root=$('#taskCenterList');if(!root)return;renderTaskManage();const items=currentTaskCenterTasks();const selectedKey=[...state.taskCenterSelectedIds].sort().join(',');const key=state.taskCenterTab+'|'+taskRenderKey(items,state.taskCenterSelected)+'|'+selectedKey+'|'+JSON.stringify(state.taskFilters);if(!force&&key===state.taskCenterRenderKey)return;state.taskCenterRenderKey=key;const scrollTop=root.scrollTop;root.innerHTML=items.map(t=>{const multi=state.taskCenterTab==='history';const selected=state.taskCenterSelectedIds.has(t.task_id);return '<button type="button" class="debug-task '+(t.task_id===state.taskCenterSelected?'active ':'')+(selected?'selected':'')+'" data-id="'+esc(t.task_id)+'"><span class="debug-task-thumb">'+taskThumb(t)+'</span><span class="debug-task-content"><span class="debug-task-title"><strong>'+esc(t.workflow_name||t.prompt_id||t.task_id)+'</strong><span class="debug-status '+esc(t.status)+'">'+esc(statusText(t.status))+'</span>'+(multi?'<span class="task-select" data-select="'+esc(t.task_id)+'" title="选择任务">'+(selected?'✓':'')+'</span>':taskActionHtml(t))+'</span><span class="debug-task-meta">'+(sessionLabel(t)?'<span>'+esc(sessionLabel(t))+'</span>':'')+'<span>'+esc(formatTaskTime(t.created_at))+'</span></span></span></button>'}).join('')||'<div class="empty">'+(state.taskCenterTab==='history'?'暂无历史任务':'暂无正在进行的任务')+'</div>';root.scrollTop=scrollTop;$$('.debug-task',root).forEach(b=>b.onclick=()=>{state.taskCenterSelected=b.dataset.id;renderTaskCenterTasks();renderTaskCenterOutput(true)});$$('.task-select',root).forEach(b=>b.onclick=e=>{e.stopPropagation();const id=b.dataset.select;if(state.taskCenterSelectedIds.has(id))state.taskCenterSelectedIds.delete(id);else state.taskCenterSelectedIds.add(id);renderTaskManage();renderTaskCenterTasks(true)});$$('.debug-task-stop',root).forEach(b=>b.onclick=async e=>{e.stopPropagation();await stopDebugTask(b.dataset.stop);await loadTaskCenter()});$$('.debug-task-delete',root).forEach(b=>b.onclick=async e=>{e.stopPropagation();await deleteDebugTask(b.dataset.delete);await loadTaskCenter()})}
function renderTaskCenterOutput(force=false){const root=$('#taskCenterOutput');if(!root)return;const items=currentTaskCenterTasks();const t=items.find(x=>x.task_id===state.taskCenterSelected)||items[0];if(!t){state.taskCenterOutputKey='';$('#taskCenterOutputSub').textContent='选择一个任务查看详情。';root.innerHTML='<div class="empty">暂无任务</div>';return}state.taskCenterSelected=t.task_id;const key=state.taskCenterTab+'|'+taskDetailKey(t);if(!force&&key===state.taskCenterOutputKey)return;state.taskCenterOutputKey=key;$('#taskCenterOutputSub').textContent=originText(t)+' · '+(t.workflow_name||'任务')+' · '+statusText(t.status)+' · '+formatTaskTime(t.created_at)+' · 耗时 '+taskElapsed(t);if(t.summary){root.innerHTML='<div class="empty">正在加载任务详情...</div>';ensureDebugTaskDetail(t).then(()=>{renderTaskCenterOutput(true)}).catch(e=>toast(e.message,false));return}const r=t.result||{},texts=r.texts||[],images=r.images||[],videos=r.videos||[];let out='';if(t.error)out='<div class="debug-error">'+esc(t.error)+'</div>';else{if(texts.length)out+='<div class="debug-output-texts">'+texts.map(x=>'<div class="debug-output-text">'+esc(x)+'</div>').join('')+'</div>';if(images.length)out+='<div class="debug-output-images">'+images.map(u=>'<a href="'+esc(u)+'" target="_blank" rel="noopener"><img src="'+esc(u)+'" alt="ComfyUI 输出图片"></a>').join('')+'</div>';if(videos.length)out+='<div class="debug-output-videos">'+videos.map(u=>'<div class="debug-video-card"><video controls preload="metadata" src="'+esc(u)+'"></video><a class="debug-video-link" href="'+esc(u)+'" target="_blank" rel="noopener">打开/下载视频</a></div>').join('')+'</div>';if(!out)out='<div class="empty">'+(t.status==='completed'?'任务已完成，但没有文本、图片或视频输出':'等待输出结果...')+'</div>'}const session=sessionLabel(t);root.innerHTML='<div class="debug-detail"><div class="debug-detail-section task-info-card"><h3>任务信息</h3><div class="debug-input-grid"><div class="debug-input-chip">来源：'+esc(originText(t))+'</div><div class="debug-input-chip">接口：'+esc(t.port_name||'')+'</div>'+(session?'<div class="debug-input-chip">会话：'+esc(session)+'</div>':'')+'</div><div class="debug-detail-prompt">Prompt ID: '+esc(t.prompt_id||'')+'</div></div>'+renderDebugInputSection(t)+'<div class="debug-detail-section"><h3>输出结果</h3>'+out+'</div></div>'}
async function loadTaskCenter(){try{const [active,history]=await Promise.all([api('/api/debug/tasks'),api('/api/debug/history')]);state.taskCenterTasks=active.tasks||[];const oldHistory=new Map(state.taskCenterHistory.map(t=>[t.task_id,t]));state.taskCenterHistory=(history.tasks||[]).map(t=>{const old=oldHistory.get(t.task_id);return old&&!old.summary?{...old,...t,summary:false}:t});for(const id of [...state.taskCenterSelectedIds]){if(!state.taskCenterHistory.some(t=>t.task_id===id))state.taskCenterSelectedIds.delete(id)}const items=currentTaskCenterTasks();if(!items.some(t=>t.task_id===state.taskCenterSelected))state.taskCenterSelected=(items[0]||{}).task_id||'';renderTaskCenterTasks();renderTaskCenterOutput()}catch(e){toast(e.message,false)}}
const ruleText=(g,k)=>(g==='inputs'?'输入':'输出')+' / '+({text:'文本',image:'图片',video:'视频'}[k]||k);function rule(p,g,k){const r=(p[g]&&p[g][k])||{}, strict=r.mode==='strict';return '<div class="param-cell" data-label="'+ruleText(g,k)+'"><div class="param-control" data-group="'+g+'" data-key="'+k+'"><input class="param-count" type="number" min="0" value="'+esc(r.limit??'')+'" placeholder="无限制"><button type="button" class="mode-toggle '+(strict?'strict':'')+'" data-mode="'+(strict?'strict':'loose')+'">严格</button></div></div>'}
function paramsHtml(f){const p=f.params||{};return '<div class="param-panel"><div class="param-grid"><div class="param-cell empty-cell"></div><div class="param-cell header-cell"><span class="param-label">文本</span></div><div class="param-cell header-cell"><span class="param-label">图片</span></div><div class="param-cell header-cell"><span class="param-label">视频</span></div><div class="param-cell"><span class="param-label">输入</span></div>'+rule(p,'inputs','text')+rule(p,'inputs','image')+rule(p,'inputs','video')+'<div class="param-cell"><span class="param-label">输出</span></div>'+rule(p,'outputs','text')+rule(p,'outputs','image')+rule(p,'outputs','video')+'</div></div>'}
function collectEditor(){const f=getFile(state.selected);if(!f)return;const p={name:$('#workflowName').value.trim(),inputs:{},outputs:{}};validateWorkflowNameField(false);$$('.param-control').forEach(c=>{const raw=$('.param-count',c).value;p[c.dataset.group][c.dataset.key]={limit:raw===''?null:Math.max(0,parseInt(raw,10)||0),mode:$('.mode-toggle',c).dataset.mode||'loose'}});f.name=p.name||f.filename.replace(/\.json$/i,'');f.params=p;setDesc(f,$('#shortDesc').value,$('#detailedDesc').value);markDirty(f.filename)}
function renderEditorHeader(){const b=$('#editorDirtyBadge');if(b)b.style.display=state.dirty.has(state.selected)?'inline-flex':'none'}
function renderWorkflowEditor(){const f=getFile(state.selected), root=$('#workflowEditor');if(!f){root.innerHTML='<div class="empty">暂无工作流，上传 JSON 后开始配置。</div>';return}root.innerHTML='<div class="editor-head"><div class="editor-name"><h2>'+esc(f.name||f.filename)+'</h2><p>'+esc(f.filename)+'</p><span id="editorDirtyBadge" class="chip warn" style="display:none">未保存</span></div><div class="editor-actions"><button type="button" id="saveOneBtn" class="btn-primary">'+icon('i-save')+'保存</button><button type="button" id="renameBtn" class="btn-soft">重命名</button><button type="button" id="deleteBtn" class="btn-danger">'+icon('i-trash')+'删除</button></div></div><div class="editor-body"><div class="form-row"><label class="field-label">工作流名称（命令调用使用的名称）</label><input id="workflowName" value="'+esc(f.name||'')+'"></div><div class="form-row"><div class="field-label label-help"><span>输入输出数量设置</span><span class="help-wrap"><button type="button" class="help-dot" aria-label="输入输出数量设置说明">?</button><span class="help-popover">设置该工作流对应类型输入输出源的数量上限，开启严格模式后，输入数量必须等于设置值，输出数量不小于设置值</span></span></div>'+paramsHtml(f)+'</div><div class="form-row"><label class="field-label">简要说明</label><textarea id="shortDesc" rows="3">'+esc(desc(f,'short'))+'</textarea></div><div class="form-row"><label class="field-label">详细说明</label><textarea id="detailedDesc" rows="7">'+esc(desc(f,'detailed'))+'</textarea></div></div>';renderEditorHeader();$$('#workflowName,#shortDesc,#detailedDesc').forEach(e=>e.oninput=collectEditor);$('#workflowName').onblur=()=>validateWorkflowNameField(false);$$('.param-count').forEach(e=>e.oninput=collectEditor);$$('.mode-toggle').forEach(b=>b.onclick=()=>{b.dataset.mode=b.dataset.mode==='strict'?'loose':'strict';b.textContent='严格';b.classList.toggle('strict',b.dataset.mode==='strict');collectEditor()});$$('.help-dot').forEach(b=>b.onclick=()=>b.closest('.help-wrap').classList.toggle('open'));$('#saveOneBtn').onclick=saveOne;$('#renameBtn').onclick=renameWorkflow;$('#deleteBtn').onclick=deleteWorkflow}
async function saveOne(){collectEditor();if(!validateWorkflowNameField(true))return;if(!validateUniqueWorkflowNames(true))return;const f=getFile(state.selected), btn=$('#saveOneBtn');if(!f)return;try{setLoading(btn,true,'保存中...');await api('/api/workflow',payload(f));clearDirty(f.filename);toast('已保存 '+f.filename)}catch(e){toast(e.message,false)}finally{setLoading(btn,false)}}
async function saveAll(){collectEditor();if(!validateUniqueWorkflowNames(true))return;const invalid=state.files.find(f=>state.dirty.has(f.filename)&&!isValidWorkflowName(workflowObjectName(f)));if(invalid){state.selected=invalid.filename;renderWorkflowList();renderWorkflowEditor();validateWorkflowNameField(true);return}const items=state.files.filter(f=>state.dirty.has(f.filename)).map(payload);if(!items.length){toast('没有未保存更改');return}const btn=$('#saveAllBtn');try{setLoading(btn,true,'保存中...');await api('/api/workflows/bulk',{items});items.forEach(i=>state.dirty.delete(i.filename));renderWorkflowList();renderEditorHeader();toast('已保存全部更改')}catch(e){toast(e.message,false)}finally{setLoading(btn,false)}}
async function renameWorkflow(){const f=getFile(state.selected);if(!f)return;let name=await textInputAction('重命名 JSON 文件','新的 JSON 文件名',f.filename,'重命名');if(!name)return;if(!name.toLowerCase().endsWith('.json'))name+='.json';if(name===f.filename)return;try{await api('/api/rename',{old_name:f.filename,new_name:name});toast('已重命名');state.selected=name;await loadAll();renderWorkflowList();renderWorkflowEditor()}catch(e){toast(e.message,false)}}
async function deleteWorkflow(){const f=getFile(state.selected);if(!f)return;if(!await confirmAction('删除工作流','将删除 '+f.filename+'，此操作不可撤销。','删除',true))return;try{await api('/api/delete',{filename:f.filename});toast('已删除');state.selected='';await loadAll()}catch(e){toast(e.message,false)}}
async function uploadWorkflow(){const input=$('#fileInput'), file=input.files&&input.files[0];if(!file)return;const btn=$('#uploadBtn'), fd=new FormData();fd.append('file',file);try{setLoading(btn,true,'上传中...');const res=await fetch('/api/upload',{method:'POST',body:fd});const data=await res.json();if(!res.ok||data.ok===false)throw new Error(data.error||res.statusText);toast('上传成功');input.value='';state.selected=data.filename;await loadAll()}catch(e){toast(e.message,false);input.value=''}finally{setLoading(btn,false)}}
async function loadAll(){const [list,ports]=await Promise.all([api('/api/list'),api('/api/ports')]);state.files=list.files||[];state.ports=ports.ports||[];state.active=ports.active||'';state.workflowOptions=state.files.map(f=>({name:f.name||f.filename.replace(/\.json$/i,''),filename:f.filename}));renderPorts();renderWorkflowList();renderWorkflowEditor();renderDebugSelectors();await Promise.all([loadDebugTasks(),loadTaskCenter()])}
function bindTaskManageControls(){const toggle=$('#taskManageToggleBtn');if(toggle)toggle.onclick=()=>{if(state.taskCenterTab!=='history'){state.taskCenterTab='history';state.taskCenterSelected='';state.taskManageOpen=true;loadTaskCenter();return}state.taskManageOpen=!state.taskManageOpen;renderTaskManage()};const search=$('#taskSearch');if(search)search.oninput=e=>{state.taskFilters.query=e.target.value;state.taskCenterSelected='';renderTaskCenterTasks(true);renderTaskCenterOutput(true)};[['#taskFilterOrigin','origin'],['#taskFilterStatus','status'],['#taskFilterPort','port'],['#taskFilterSession','session'],['#taskSort','sort']].forEach(([sel,key])=>{const el=$(sel);if(el)el.onchange=e=>{state.taskFilters[key]=e.target.value;state.taskCenterSelected='';renderTaskCenterTasks(true);renderTaskCenterOutput(true)}});const selectAll=$('#taskSelectAllBtn');if(selectAll)selectAll.onclick=()=>{filteredTaskCenterHistory().forEach(t=>state.taskCenterSelectedIds.add(t.task_id));renderTaskManage();renderTaskCenterTasks(true)};const clear=$('#taskClearSelectionBtn');if(clear)clear.onclick=()=>{state.taskCenterSelectedIds.clear();renderTaskManage();renderTaskCenterTasks(true)};const del=$('#taskBulkDeleteBtn');if(del)del.onclick=bulkDeleteTaskHistory;const exp=$('#taskExportBtn');if(exp)exp.onclick=exportTaskHistoryMedia}
function bind(){applyTheme();applySidebar();setModule(state.module);$$('.nav button').forEach(b=>b.onclick=()=>setModule(b.dataset.module));$('#themeBtn').onclick=()=>{state.theme=state.theme==='dark'?'light':'dark';applyTheme()};$('#collapseBtn').onclick=()=>{if(window.innerWidth<=820){closeSidebar();return}state.collapsed=!state.collapsed;applySidebar()};$('#menuBtn').onclick=()=>document.body.classList.add('drawer-open');$('#drawerBackdrop').onclick=closeSidebar;$('#workflowSearch').oninput=e=>{state.query=e.target.value;renderWorkflowList()};$('#fileInput').onchange=uploadWorkflow;$('#saveAllBtn').onclick=saveAll;$('#uploadBtn').onclick=()=>{const input=$('#fileInput');input.value='';input.click()};$('#addPortBtn').onclick=addInterface;$('#refreshPortsBtn').onclick=loadAll;$('#debugPort').onchange=e=>{localStorage.setItem(storageKeys.debugPort,e.target.value);renderDebugSelectors()};$('#debugWorkflow').onchange=e=>{localStorage.setItem(storageKeys.debugWorkflow,e.target.value);renderDebugTextDefaults(true)};$('#debugAddTextBtn').onclick=()=>addDebugText('');$('#debugImageBtn').onclick=()=>{$('#debugImageInput').value='';$('#debugImageInput').click()};$('#debugImageInput').onchange=e=>addDebugImageFiles(e.target.files);const drop=$('#debugImagePreview');['dragenter','dragover'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.add('dragging')}));['dragleave','drop'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.remove('dragging')}));drop.addEventListener('drop',e=>addDebugImageFiles(e.dataTransfer&&e.dataTransfer.files));drop.onclick=e=>{if(e.target===drop||e.target.classList.contains('empty'))$('#debugImageInput').click()};$('#debugVideoBtn').onclick=()=>{$('#debugVideoInput').value='';$('#debugVideoInput').click()};$('#debugVideoInput').onchange=e=>addDebugVideoFiles(e.target.files);const vdrop=$('#debugVideoPreview');['dragenter','dragover'].forEach(ev=>vdrop.addEventListener(ev,e=>{e.preventDefault();vdrop.classList.add('dragging')}));['dragleave','drop'].forEach(ev=>vdrop.addEventListener(ev,e=>{e.preventDefault();vdrop.classList.remove('dragging')}));vdrop.addEventListener('drop',e=>addDebugVideoFiles(e.dataTransfer&&e.dataTransfer.files));vdrop.onclick=e=>{if(e.target===vdrop||e.target.classList.contains('empty'))$('#debugVideoInput').click()};$('#debugRunBtn').onclick=submitDebugTask;$('#debugRefreshBtn').onclick=loadDebugTasks;$('#debugActiveTab').onclick=()=>{state.debugTaskTab='active';state.debugSelected='';renderDebugTasks();renderDebugOutput()};$('#debugHistoryTab').onclick=()=>{state.debugTaskTab='history';state.debugSelected='';renderDebugTasks();renderDebugOutput();loadDebugTasks()};$('#taskCenterRefreshBtn').onclick=loadTaskCenter;$('#taskCenterActiveTab').onclick=()=>{state.taskCenterTab='active';state.taskManageOpen=false;state.taskCenterSelected='';renderTaskCenterTasks();renderTaskCenterOutput();loadTaskCenter()};$('#taskCenterHistoryTab').onclick=()=>{state.taskCenterTab='history';state.taskCenterSelected='';renderTaskCenterTasks();renderTaskCenterOutput();loadTaskCenter()};state.debugTimer=setInterval(()=>{const active=state.debugTasks.some(t=>['queued','running'].includes(t.status))||state.taskCenterTasks.some(t=>['queued','running'].includes(t.status));if(state.module==='debug'||active)loadDebugTasks();if(state.module==='tasks'&&state.taskCenterTab==='active')loadTaskCenter()},2500);$('#savePortsBtn').onclick=async()=>{try{const data=await api('/api/ports',{ports:collectPorts()});state.active=data.active||state.active;toast('接口配置已保存');await loadAll()}catch(e){toast(e.message,false)}};$('#clearCacheBtn').onclick=async()=>{if(!await confirmAction('清理缓存','将删除插件临时文件。','清理',true))return;try{const d=await api('/api/clear_cache',{});toast('缓存清理完成，删除 '+(d.deleted??0)+' 个文件')}catch(e){toast(e.message,false)}};$('#clearHistoryBtn').onclick=async()=>{const hours=Math.max(0,parseInt($('#historyHours').value,10)||0);if(!await confirmAction('清理历史','将清理 '+(hours?hours+' 小时以前':'全部')+' 的历史任务与本地媒体。','清理',true))return;try{const d=await api('/api/clear_history',{hours});toast('历史清理完成，删除 '+(d.deleted??0)+' 条记录')}catch(e){toast(e.message,false)}};window.addEventListener('beforeunload',e=>{clearDebugImages();clearDebugVideos();if(state.dirty.size){e.preventDefault();e.returnValue=''}})}
bind();bindTaskManageControls();loadAll().catch(e=>toast(e.message,false));
</script>
</body>
</html>
"""


class ManagementServer:
    """
    Workflow management page server with async start/stop hooks for AstrBot.
    """

    def __init__(
        self,
        workflows_dir: Path,
        meta_path: Path,
        load_meta: Callable[[], dict[str, str]],
        save_meta: Callable[[dict[str, str]], None],
        plugin_data_dir: Path | None = None,
        cleanup_history_func: Callable[[], int] | None = None,
        ports_config_path: Path | None = None,
        active_port_state_path: Path | None = None,
        load_ports_func: Callable[[], list[dict[str, object]]] | None = None,
        save_ports_func: Callable[[list[dict[str, object]]], None] | None = None,
        active_port_changed_func: Callable[[], None] | None = None,
        debug_submit_func: Callable[[dict[str, object]], object] | None = None,
        debug_tasks_func: Callable[[], object] | None = None,
        debug_history_func: Callable[[], object] | None = None,
        debug_task_func: Callable[[str], object] | None = None,
        debug_delete_func: Callable[[str], object] | None = None,
        debug_stop_func: Callable[[str], object] | None = None,
    ):
        if plugin_data_dir is None:
            plugin_data_dir = workflows_dir.parent
        self.app = create_app(
            workflows_dir,
            meta_path,
            load_meta,
            save_meta,
            plugin_data_dir=plugin_data_dir,
            cleanup_history_func=cleanup_history_func,
            ports_config_path=ports_config_path,
            active_port_state_path=active_port_state_path,
            load_ports_func=load_ports_func,
            save_ports_func=save_ports_func,
            active_port_changed_func=active_port_changed_func,
            debug_submit_func=debug_submit_func,
            debug_tasks_func=debug_tasks_func,
            debug_history_func=debug_history_func,
            debug_task_func=debug_task_func,
            debug_delete_func=debug_delete_func,
            debug_stop_func=debug_stop_func,
        )
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._started = False

    async def start(self, host: str, port: int) -> bool:
        """Start the web server and return whether it succeeded."""
        try:
            self._runner = web.AppRunner(self.app, access_log=None)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, str(host).strip(), int(port))
            await self._site.start()
            self._started = True
            return True
        except OSError as e:
            if "Address already in use" in str(e) or getattr(e, "errno", None) in (
                98,
                10048,
            ):
                raise RuntimeError(
                    f"Port {port} is already in use. Please choose another port or stop the process using it."
                ) from e
            raise
        except Exception:
            raise

    async def stop(self) -> None:
        """Stop the web server."""
        if not self._started:
            return
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._site = None
        self._runner = None
        self._started = False























