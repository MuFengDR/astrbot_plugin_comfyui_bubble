"""ComfyUI Bubble management web server."""
import json
import re
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
) -> web.Application:
    app = web.Application()
    if plugin_data_dir is None:
        plugin_data_dir = workflows_dir.parent
    output_media_dir = (
        plugin_data_dir.resolve().parent.parent / "agent" / "comfyui" / "input"
    )
    tmp_dir = plugin_data_dir / "tmp"
    if ports_config_path is None:
        ports_config_path = plugin_data_dir / "ports_config.json"
    if active_port_state_path is None:
        active_port_state_path = plugin_data_dir / "active_port.json"
    logo_path = Path(__file__).with_name("webui_logo.jpg")

    # 淇濆瓨娓呯悊鍘嗗彶璁板綍鐨勫嚱鏁板紩鐢?
    _cleanup_history = cleanup_history_func

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
        for d in (output_media_dir, tmp_dir):
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
                "dirs": [str(output_media_dir), str(tmp_dir)],
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
                    for idx, item in enumerate(raw_ports[:4], start=1):
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
        while len(ports) < 4:
            ports.append({"name": "", "http": "", "workflows": []})
        return web.json_response(
            {
                "ok": True,
                "ports": ports[:4],
                "active": _read_active_port(),
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
        for idx, item in enumerate(raw_ports[:4], start=1):
            port = _normalize_port_entry(item, idx)
            if port:
                ports.append(port)
        _save_ports(ports)
        return web.json_response({"ok": True, "ports": ports})

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
            return web.json_response({"ok": False, "error": "port not found"}, status=404)
        _write_active_port(name)
        return web.json_response({"ok": True, "active": name})

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
    svg.icon{width:17px;height:17px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;flex:0 0 auto}.app{min-height:100vh;display:grid;grid-template-columns:var(--sidebar) minmax(0,1fr);gap:22px;padding:14px}.sidebar{position:sticky;top:14px;height:calc(100vh - 28px);display:flex;flex-direction:column;gap:14px;padding:18px;border:1px solid rgba(255,255,255,.78);border-radius:22px;background:linear-gradient(160deg,rgba(222,245,255,.70),rgba(255,255,255,.54) 42%,rgba(238,247,255,.68));box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);overflow:hidden}.sidebar::after{content:"";position:absolute;inset:auto -80px -80px 35%;height:180px;background:radial-gradient(circle,rgba(255,255,255,.75),transparent 62%);pointer-events:none}.brand{position:relative;display:grid;grid-template-columns:46px minmax(0,1fr);gap:12px;align-items:center}.brand-mark{width:46px;height:46px;border-radius:16px;display:grid;place-items:center;color:#087cff;background:rgba(255,255,255,.72);box-shadow:0 12px 26px rgba(8,124,255,.16),var(--highlight);overflow:hidden}.brand-logo{width:100%;height:100%;display:block;object-fit:cover}.brand strong{display:block;color:#405064;font-size:1.06rem;font-weight:760;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.brand span{display:block;color:var(--muted);font-size:.78rem}.nav-label{margin:8px 10px 0;color:var(--subtle);font-size:.78rem}.nav{position:relative;display:grid;gap:7px}.nav button{width:100%;justify-content:flex-start;border-color:transparent;background:transparent;color:#344054;font-size:.95rem;box-shadow:none}.nav button.active{color:var(--brand);background:rgba(8,124,255,.10);box-shadow:inset 4px 0 0 var(--brand)}.nav button.active svg{filter:drop-shadow(0 4px 8px rgba(8,124,255,.28))}.nav-text{min-width:0;overflow:hidden;text-overflow:ellipsis}.sidebar-footer{position:relative;margin-top:auto}.app.collapsed{grid-template-columns:var(--rail) minmax(0,1fr)}.app.collapsed .sidebar{align-items:center}.app.collapsed .brand{grid-template-columns:46px}.app.collapsed .brand-text,.app.collapsed .nav-label,.app.collapsed .nav-text,.app.collapsed .collapse-text{display:none}.app.collapsed .nav button{width:46px;padding-inline:0;justify-content:center}.main{min-width:0;display:grid;grid-template-rows:auto auto minmax(0,1fr);gap:14px}.topbar{min-height:72px;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:12px 18px;border:1px solid rgba(255,255,255,.8);border-radius:22px;background:linear-gradient(100deg,rgba(216,243,255,.64),rgba(255,255,255,.56) 50%,rgba(234,226,255,.72));box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur)}.top-left{min-width:0;display:flex;align-items:center;gap:12px}.menu-btn{display:none;width:42px;padding:0}.title-block{min-width:0}.title-block h1{margin:0;color:#101828;font-size:1.15rem;line-height:1.2;font-weight:780;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.title-block p{margin:4px 0 0;color:var(--muted);font-size:.85rem}.statusbar{min-height:38px;display:flex;align-items:center}.toast{display:none;max-width:780px;padding:9px 13px;border:1px solid rgba(255,255,255,.76);border-radius:14px;background:var(--glass-strong);box-shadow:var(--shadow);backdrop-filter:var(--blur);font-size:.9rem}.toast.show{display:inline-flex}.toast.ok{color:var(--success)}.toast.err{color:var(--danger)}.module{display:none;min-width:0}.module.active{display:block}.panel,.card{border:1px solid rgba(255,255,255,.78);border-radius:var(--radius);background:var(--glass);box-shadow:var(--shadow);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);overflow:hidden;transition:transform .18s ease,box-shadow .18s ease,border-color .18s ease}.panel:hover,.card:hover{transform:translateY(-2px);box-shadow:var(--shadow-float);border-color:rgba(255,255,255,.95)}.panel-head,.card-head{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:18px 22px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.35)}.panel-title h2,.card-title h3{margin:0;color:#202633;font-size:1rem;font-weight:720}.panel-title p,.card-title p{margin:5px 0 0;color:var(--muted);font-size:.84rem}.panel-body,.card-body{padding:18px 22px}.btn-primary{color:#fff;border-color:transparent;background:linear-gradient(135deg,var(--brand),var(--brand-2));box-shadow:0 12px 24px rgba(8,124,255,.24)}.btn-danger{color:#fff;border-color:transparent;background:linear-gradient(135deg,#ff5b6b,#ef4444);box-shadow:0 12px 24px rgba(239,68,68,.18)}.btn-soft{background:rgba(255,255,255,.54)}.chip{min-height:28px;display:inline-flex;align-items:center;gap:6px;padding:4px 9px;border-radius:999px;background:rgba(255,255,255,.56);color:var(--muted);font-size:.78rem;box-shadow:var(--highlight)}.chip.active{color:var(--success);background:rgba(220,252,231,.68)}.chip.warn{color:var(--warn);background:rgba(255,244,229,.78)}.source-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}.source-card{padding:16px;display:grid;gap:14px;background:linear-gradient(145deg,rgba(255,255,255,.72),rgba(255,255,255,.48))}.source-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}.source-name{min-width:0;font-weight:760;color:#1d2735;overflow-wrap:anywhere}.source-http{color:var(--muted);font-size:.82rem;overflow-wrap:anywhere}.field-grid{display:grid;grid-template-columns:1fr 1.25fr;gap:12px}.field-label{display:block;margin:0 0 6px;color:var(--muted);font-size:.78rem}.whitelist-head{display:flex;align-items:center;justify-content:space-between;gap:10px}.checkline{display:inline-flex;align-items:center;gap:8px;color:var(--text);font-size:.88rem}.checkline input{width:auto;min-height:0}.workflow-checks{max-height:238px;overflow:auto;display:grid;grid-template-columns:repeat(auto-fill,minmax(215px,1fr));gap:9px;padding-right:4px}.workflow-check{min-width:0;display:grid;grid-template-columns:auto minmax(0,1fr);gap:3px 8px;align-items:center;padding:10px;border:1px solid rgba(255,255,255,.7);border-radius:14px;background:rgba(255,255,255,.48);box-shadow:var(--highlight)}.workflow-check input{width:auto;min-height:0}.workflow-check span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.workflow-file{grid-column:2;color:var(--subtle);font-size:.74rem}.workflow-shell{display:grid;grid-template-columns:minmax(270px,350px) minmax(0,1fr);gap:16px;min-height:calc(100vh - 154px)}.workflow-list-panel{min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr) auto}.workflow-toolbar{display:grid;gap:10px;padding:16px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.30)}.upload-inline{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center}.upload-inline input{min-width:0}.filter-row{display:grid;grid-template-columns:1fr 1fr;gap:9px}.workflow-list{min-height:0;overflow:auto;display:grid;align-content:start;gap:9px;padding:12px}.workflow-item{width:100%;min-height:74px;justify-content:flex-start;align-items:flex-start;text-align:left;border-radius:16px;padding:11px;background:rgba(255,255,255,.50);border-color:rgba(255,255,255,.68)}.workflow-item.active{border-color:rgba(8,124,255,.32);background:rgba(232,242,255,.72);box-shadow:0 12px 30px rgba(8,124,255,.14),0 0 0 4px var(--brand-soft)}.workflow-item-main{min-width:0;display:grid;gap:2px}.workflow-item-title{min-width:0;font-weight:720;color:#1d2735;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.workflow-item-meta{color:var(--muted);font-size:.77rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.dirty-dot{width:8px;height:8px;border-radius:999px;background:var(--warn);margin-top:7px;flex:0 0 auto;box-shadow:0 0 0 4px var(--warn-soft)}.list-footer{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:12px 16px;border-top:1px solid rgba(94,114,144,.12);color:var(--muted);font-size:.84rem;background:rgba(255,255,255,.28)}.editor{min-width:0;min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr)}.editor-head{display:flex;justify-content:space-between;gap:14px;padding:18px 22px;border-bottom:1px solid rgba(94,114,144,.12);background:rgba(255,255,255,.32)}.editor-name{min-width:0}.editor-name h2{margin:0;color:#1d2735;font-size:1.08rem;overflow-wrap:anywhere}.editor-name p{margin:4px 0 6px;color:var(--muted);font-size:.82rem;overflow-wrap:anywhere}.editor-actions{display:flex;gap:8px;align-items:flex-start;flex-wrap:wrap;justify-content:flex-end}.editor-body{min-height:0;overflow:auto;padding:18px 22px;display:grid;gap:14px;align-content:start}.form-row{display:grid;gap:6px}.param-panel{border:1px solid rgba(255,255,255,.74);border-radius:18px;background:rgba(255,255,255,.42);overflow:hidden;box-shadow:var(--highlight)}.param-grid{display:grid;grid-template-columns:76px repeat(3,minmax(120px,1fr));gap:1px;background:rgba(94,114,144,.12)}.param-cell{min-width:0;padding:10px;background:rgba(255,255,255,.50)}.param-label{color:var(--muted);font-size:.78rem;font-weight:680}.param-control{display:grid;grid-template-columns:minmax(0,1fr) 56px;gap:7px;align-items:center}.param-control input{min-height:36px}.mode-toggle{min-height:36px;padding-inline:8px;color:var(--muted)}.mode-toggle.strict{color:#fff;border-color:transparent;background:linear-gradient(135deg,#fbbf24,#f97316)}.tools-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}.tool-card{min-height:216px;display:grid;grid-template-rows:auto minmax(0,1fr) auto}.file-box{display:grid;gap:10px}.danger-copy{color:var(--muted);font-size:.88rem}.history-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center}.empty{padding:28px;color:var(--muted);text-align:center}.drawer-backdrop{display:none}.modal-backdrop{position:fixed;inset:0;z-index:80;display:none;place-items:center;padding:20px;background:rgba(16,24,40,.38);backdrop-filter:blur(12px)}.modal-backdrop.show{display:grid}.modal{width:min(460px,100%);border:1px solid rgba(255,255,255,.8);border-radius:22px;background:var(--glass-strong);box-shadow:var(--shadow-float);backdrop-filter:var(--blur)}.modal-body{padding:20px}.modal-body h3{margin:0 0 8px;font-size:1.06rem}.modal-body p{margin:0;color:var(--muted)}.modal-actions{display:flex;justify-content:flex-end;gap:8px;padding:14px 20px;border-top:1px solid rgba(94,114,144,.12)}
    .brand strong,.title-block h1{font-weight:700}.nav button,.workflow-item-title,.source-name{font-weight:650}
    #collapseBtn svg{transition:transform .18s ease}.app.collapsed #collapseBtn svg{transform:rotate(180deg)}
    .switch-row{display:flex;align-items:center;gap:10px;color:var(--text);font-size:.9rem;font-weight:650;cursor:pointer;user-select:none}.switch-row input{position:absolute;opacity:0;pointer-events:none}.switch{position:relative;width:48px;height:28px;flex:0 0 auto;border-radius:999px;background:rgba(148,163,184,.20);border:1px solid rgba(148,163,184,.26);box-shadow:inset 0 1px 3px rgba(15,23,42,.18),var(--highlight);transition:background .18s ease,border-color .18s ease,box-shadow .18s ease}.switch::after{content:"";position:absolute;left:3px;top:3px;width:22px;height:22px;border-radius:999px;background:rgba(255,255,255,.94);box-shadow:0 5px 14px rgba(15,23,42,.24);transition:transform .18s ease,background .18s ease}.switch-row input:checked+.switch{border-color:rgba(34,197,94,.42);background:linear-gradient(135deg,#34d399,#22c55e);box-shadow:0 10px 24px rgba(34,197,94,.18),inset 0 1px 0 rgba(255,255,255,.36)}.switch-row input:checked+.switch::after{transform:translateX(20px)}.workflow-check{position:relative;grid-template-columns:minmax(0,1fr);min-height:48px;padding:13px 15px;cursor:pointer}.workflow-check-input{position:absolute!important;opacity:0!important;pointer-events:none!important}.workflow-check .workflow-title{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text);font-weight:650}.workflow-check.selected{border-color:rgba(34,197,94,.48);background:linear-gradient(135deg,rgba(34,197,94,.24),rgba(20,184,166,.16));box-shadow:0 14px 30px rgba(34,197,94,.14),inset 0 1px 0 rgba(255,255,255,.34)}.workflow-check.selected .workflow-title{color:#047857}.workflow-check.disabled{cursor:default;opacity:.58;background:rgba(148,163,184,.10);border-color:rgba(148,163,184,.18);box-shadow:var(--highlight)}
    .file-upload-row{grid-template-columns:auto minmax(0,1fr) auto}.file-input-hidden{position:absolute!important;width:1px!important;height:1px!important;opacity:0!important;pointer-events:none!important}.upload-button-row{grid-template-columns:1fr}.upload-button-row #uploadBtn{width:100%;justify-content:center}.file-picker{min-height:40px;display:inline-flex;align-items:center;justify-content:center;gap:8px;padding:8px 13px;border:1px solid rgba(148,163,184,.28);border-radius:13px;background:rgba(255,255,255,.60);box-shadow:var(--highlight);backdrop-filter:var(--blur);cursor:pointer;white-space:nowrap;font-weight:650;color:var(--text);transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease}.file-picker:hover{transform:translateY(-2px);border-color:rgba(8,124,255,.35);box-shadow:0 14px 32px rgba(42,58,88,.14),0 0 0 4px var(--brand-soft),var(--highlight)}.file-picker-name{min-width:0;color:var(--muted);font-size:.84rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.field-error{border-color:var(--danger)!important;box-shadow:0 0 0 4px var(--danger-soft),var(--highlight)!important}.label-help{display:flex;align-items:center;gap:8px}.help-wrap{position:relative;display:inline-flex}.help-dot{width:22px;height:22px;min-height:22px;padding:0;border-radius:999px;font-size:.82rem;font-weight:760;color:var(--brand);background:rgba(8,124,255,.10);border-color:rgba(8,124,255,.22)}.help-popover{position:absolute;left:50%;bottom:calc(100% + 10px);z-index:30;width:min(360px,80vw);transform:translateX(-50%) translateY(4px);padding:12px 14px;border:1px solid rgba(255,255,255,.72);border-radius:14px;background:var(--glass-strong);box-shadow:var(--shadow);backdrop-filter:var(--blur);color:var(--text);font-size:.84rem;line-height:1.5;opacity:0;pointer-events:none;transition:opacity .16s ease,transform .16s ease}.help-wrap:hover .help-popover,.help-wrap:focus-within .help-popover,.help-wrap.open .help-popover{opacity:1;transform:translateX(-50%) translateY(0);pointer-events:auto}.param-cell.header-cell,.param-grid>.param-cell:first-child,.param-grid>.param-cell:nth-child(5),.param-grid>.param-cell:nth-child(9){display:grid;place-items:center;text-align:center}.param-label{text-align:center}.mode-toggle{min-width:78px}.param-control{grid-template-columns:minmax(0,1fr) 86px}
    .statusbar{min-height:0;height:0;overflow:visible}.toast{position:fixed;top:18px;left:50%;z-index:120;display:flex;max-width:min(720px,calc(100vw - 32px));padding:12px 16px;border-radius:16px;align-items:center;justify-content:center;transform:translate(-50%,-18px);opacity:0;pointer-events:none;transition:opacity .22s ease,transform .22s ease;border:1px solid rgba(255,255,255,.72);background:var(--glass-strong);box-shadow:var(--shadow-float);backdrop-filter:var(--blur);-webkit-backdrop-filter:var(--blur);font-size:.92rem;line-height:1.45}.toast.show{display:flex;opacity:1;transform:translate(-50%,0)}.toast.ok{color:var(--success);border-color:rgba(34,197,94,.30);background:rgba(240,253,244,.86)}.toast.err{color:var(--danger);border-color:rgba(239,68,68,.35);background:rgba(255,241,242,.88)}:root[data-theme="dark"] .toast.ok{color:#bbf7d0;background:rgba(20,83,45,.88);border-color:rgba(74,222,128,.28)}:root[data-theme="dark"] .toast.err{color:#fecdd3;background:rgba(76,29,42,.90);border-color:rgba(251,113,133,.34)}.workflow-shell{grid-template-columns:minmax(320px,410px) minmax(620px,1fr)}.editor,.editor-body,.editor-head,.form-row,.param-panel,textarea{min-width:0}.editor-actions{min-width:0;flex-wrap:wrap}.param-panel{overflow-x:auto}.param-grid{min-width:640px;grid-template-columns:72px repeat(3,minmax(165px,1fr))}.param-control{grid-template-columns:minmax(72px,1fr) 70px}.mode-toggle{min-width:64px;padding-inline:10px;background:rgba(255,255,255,.46);box-shadow:var(--highlight)}.mode-toggle.strict{color:#713f12;border-color:rgba(245,158,11,.48);background:linear-gradient(135deg,#fde68a,#f59e0b);box-shadow:inset 0 1px 0 rgba(255,255,255,.45)}.help-wrap{position:relative}.help-popover{position:fixed;left:50%;top:88px;bottom:auto;width:min(420px,calc(100vw - 32px));transform:translateX(-50%) translateY(-8px);white-space:normal;overflow-wrap:anywhere}.help-wrap:hover .help-popover,.help-wrap:focus-within .help-popover,.help-wrap.open .help-popover{transform:translateX(-50%) translateY(0)}
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
    :root[data-theme="dark"] .chip{background:rgba(30,41,59,.64);color:var(--muted)}
    :root[data-theme="dark"] .chip.active{color:#bbf7d0;background:rgba(34,197,94,.16)}
    :root[data-theme="dark"] .chip.warn{color:#fde68a;background:rgba(245,158,11,.16)}
    :root[data-theme="dark"] .workflow-check.selected{border-color:rgba(52,211,153,.48);background:linear-gradient(135deg,rgba(34,197,94,.30),rgba(20,184,166,.20));box-shadow:0 14px 34px rgba(34,197,94,.14),inset 0 1px 0 rgba(255,255,255,.14)}
    :root[data-theme="dark"] .mode-toggle{background:rgba(30,41,59,.64);border-color:rgba(148,163,184,.20);color:var(--text);box-shadow:var(--highlight)}
    :root[data-theme="dark"] .mode-toggle.strict{color:#713f12;border-color:rgba(245,158,11,.52);background:linear-gradient(135deg,#fde68a,#f59e0b);box-shadow:inset 0 1px 0 rgba(255,255,255,.42)}
    :root[data-theme="dark"] .workflow-check.selected .workflow-title{color:#d1fae5}
    :root[data-theme="dark"] .workflow-check.disabled{border-color:rgba(148,163,184,.14);background:rgba(15,23,42,.30);box-shadow:var(--highlight)}
    @media(max-width:1100px){.source-grid,.tools-grid{grid-template-columns:1fr}.workflow-shell{grid-template-columns:minmax(250px,320px) minmax(0,1fr)}}
    @media(max-width:820px){.app,.app.collapsed{grid-template-columns:1fr;padding:12px}.sidebar{position:fixed;inset:12px auto 12px 12px;z-index:60;width:min(84vw,320px);height:auto;transform:translateX(calc(-100% - 18px));transition:transform .2s}.drawer-backdrop{position:fixed;inset:0;z-index:50;background:rgba(16,24,40,.32);backdrop-filter:blur(10px)}body.drawer-open .sidebar{transform:translateX(0)}body.drawer-open .drawer-backdrop{display:block}.app.collapsed #collapseBtn svg{transform:none}.app.collapsed .brand-text,.app.collapsed .nav-label,.app.collapsed .nav-text,.app.collapsed .collapse-text{display:block}.app.collapsed .sidebar{align-items:stretch}.app.collapsed .brand{grid-template-columns:46px minmax(0,1fr)}.app.collapsed .nav button{width:100%;padding-inline:12px;justify-content:flex-start}.menu-btn{display:inline-flex}.theme-text{display:none}.workflow-shell{grid-template-columns:1fr;min-height:0}.workflow-list-panel{max-height:440px}.field-grid,.filter-row,.upload-inline{grid-template-columns:1fr}.editor-head,.panel-head,.card-head{align-items:flex-start;flex-direction:column}.editor-actions{width:100%;display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}.editor-actions button{padding-inline:8px}.param-panel{overflow:visible}.param-grid{min-width:0;grid-template-columns:1fr}.param-cell{padding:10px 12px}.param-cell.empty-cell,.param-cell.header-cell{display:none}.param-cell[data-label]::before{content:attr(data-label);display:block;margin-bottom:6px;color:var(--muted);font-size:.78rem;font-weight:680}.param-control{grid-template-columns:minmax(0,1fr) 68px;gap:8px}.param-control input{min-width:0;padding-inline:10px}.mode-toggle{min-width:68px;width:68px;padding-inline:0;justify-content:center;white-space:nowrap}.workflow-checks{grid-template-columns:1fr}.history-row{grid-template-columns:1fr}}
    @media(min-width:821px){.workflow-shell{grid-template-columns:minmax(320px,410px) minmax(620px,1fr)}.param-grid{min-width:640px;grid-template-columns:72px repeat(3,minmax(165px,1fr))}.param-control{grid-template-columns:minmax(72px,1fr) 70px}}
  </style>
</head>
<body>
  <svg width="0" height="0" style="position:absolute" aria-hidden="true"><symbol id="i-bubble" viewBox="0 0 24 24"><circle cx="8" cy="9" r="4"></circle><circle cx="15" cy="14" r="5"></circle><path d="M5 17c2 2 5 3 9 3"></path></symbol><symbol id="i-source" viewBox="0 0 24 24"><path d="M4 7h16"></path><path d="M4 17h16"></path><circle cx="8" cy="7" r="3"></circle><circle cx="16" cy="17" r="3"></circle></symbol><symbol id="i-workflow" viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="2"></rect><rect x="14" y="3" width="7" height="7" rx="2"></rect><rect x="8" y="14" width="8" height="7" rx="2"></rect><path d="M10 6h4"></path><path d="M12 10v4"></path></symbol><symbol id="i-tools" viewBox="0 0 24 24"><path d="M14 6l4 4"></path><path d="M4 20l8-8"></path><path d="M15 5l4-1-1 4L7 19H4v-3z"></path></symbol><symbol id="i-menu" viewBox="0 0 24 24"><path d="M4 6h16"></path><path d="M4 12h16"></path><path d="M4 18h16"></path></symbol><symbol id="i-sun" viewBox="0 0 24 24"><circle cx="12" cy="12" r="4"></circle><path d="M12 2v2"></path><path d="M12 20v2"></path><path d="M4.93 4.93l1.41 1.41"></path><path d="M17.66 17.66l1.41 1.41"></path><path d="M2 12h2"></path><path d="M20 12h2"></path><path d="M4.93 19.07l1.41-1.41"></path><path d="M17.66 6.34l1.41-1.41"></path></symbol><symbol id="i-save" viewBox="0 0 24 24"><path d="M5 3h14l2 2v16H3V5z"></path><path d="M8 3v6h8V3"></path><path d="M8 21v-7h8v7"></path></symbol><symbol id="i-upload" viewBox="0 0 24 24"><path d="M12 16V4"></path><path d="M7 9l5-5 5 5"></path><path d="M4 20h16"></path></symbol><symbol id="i-trash" viewBox="0 0 24 24"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 14h10l1-14"></path><path d="M9 7V4h6v3"></path></symbol><symbol id="i-edit" viewBox="0 0 24 24"><path d="M4 20h4l11-11-4-4L4 16z"></path><path d="M13 6l4 4"></path></symbol><symbol id="i-chevron" viewBox="0 0 24 24"><path d="M15 18l-6-6 6-6"></path></symbol></svg>
  <div class="drawer-backdrop" id="drawerBackdrop"></div><div class="app" id="appShell"><aside class="sidebar"><div class="brand"><div class="brand-mark"><img class="brand-logo" src="/webui_logo.jpg" alt="Comfyui 泡泡版"></div><div class="brand-text"><strong>Comfyui 泡泡版</strong><span>Workflow Console</span></div></div><div class="nav-label">控制台</div><nav class="nav"><button type="button" data-module="sources"><svg class="icon"><use href="#i-source"></use></svg><span class="nav-text">来源配置</span></button><button type="button" data-module="workflows"><svg class="icon"><use href="#i-workflow"></use></svg><span class="nav-text">工作流管理</span></button><button type="button" data-module="tools"><svg class="icon"><use href="#i-trash"></use></svg><span class="nav-text">缓存清理</span></button></nav><div class="sidebar-footer"><button type="button" id="collapseBtn" class="btn-soft"><svg class="icon"><use href="#i-chevron"></use></svg><span class="collapse-text">收起侧栏</span></button></div></aside><main class="main"><header class="topbar"><div class="top-left"><button type="button" id="menuBtn" class="menu-btn"><svg class="icon"><use href="#i-menu"></use></svg></button><div class="title-block"><h1 id="moduleTitle">来源配置</h1><p id="moduleSubtitle">管理 ComfyUI 来源与工作流白名单</p></div></div><div class="top-actions"><button type="button" id="themeBtn" class="btn-soft"><svg class="icon"><use href="#i-sun"></use></svg><span class="theme-text">深色</span></button></div></header><div class="statusbar"><div id="toast" class="toast"></div></div><section class="module" data-module-panel="sources"><article class="panel"><div class="panel-head"><div class="panel-title"><h2>ComfyUI 来源</h2><p>最多 4 个来源，支持切换当前来源并限制可用工作流。</p></div><div class="editor-actions"><button type="button" id="savePortsBtn" class="btn-primary"><svg class="icon"><use href="#i-save"></use></svg>保存来源</button><button type="button" id="refreshPortsBtn" class="btn-soft">刷新列表</button></div></div><div class="panel-body"><div class="source-grid" id="portsList"></div></div></article></section><section class="module" data-module-panel="workflows"><div class="workflow-shell"><aside class="workflow-list-panel panel"><div class="workflow-toolbar"><div class="upload-inline upload-button-row"><input class="file-input-hidden" type="file" id="fileInput" accept=".json"><button type="button" id="uploadBtn" class="btn-primary"><svg class="icon"><use href="#i-upload"></use></svg>上传 .json</button></div><input id="workflowSearch" type="search" placeholder="搜索文件名、显示名称或说明"></div><div class="workflow-list" id="workflowList"></div><div class="list-footer"><span id="workflowCount">0 个工作流</span><button type="button" id="saveAllBtn" class="btn-primary"><svg class="icon"><use href="#i-save"></use></svg>保存全部</button></div></aside><article class="editor panel" id="workflowEditor"></article></div></section><section class="module" data-module-panel="tools"><div class="tools-grid"><article class="card tool-card"><div class="card-head"><div class="card-title"><h3>清理缓存</h3><p>删除输入缓存和插件临时文件。</p></div></div><div class="card-body"><p class="danger-copy">会清理 data/agent/comfyui/input 与插件 tmp 下的文件。</p></div><div class="card-body"><button type="button" id="clearCacheBtn" class="btn-danger"><svg class="icon"><use href="#i-trash"></use></svg>清理缓存</button></div></article><article class="card tool-card"><div class="card-head"><div class="card-title"><h3>清理历史</h3><p>释放历史生成记录占用的内存。</p></div></div><div class="card-body"><label class="field-label">保留最近小时数</label><div class="history-row"><input type="number" id="historyHours" value="48" min="0"><span class="chip">0 表示全部清理</span></div></div><div class="card-body"><button type="button" id="clearHistoryBtn" class="btn-danger"><svg class="icon"><use href="#i-trash"></use></svg>清理历史</button></div></article></div></section></main></div><div class="modal-backdrop" id="modalBackdrop"><div class="modal"><div class="modal-body"><h3 id="modalTitle"></h3><p id="modalMessage"></p></div><div class="modal-actions"><button type="button" id="modalCancel" class="btn-soft">取消</button><button type="button" id="modalConfirm" class="btn-danger">确认</button></div></div></div>
  <script>
const moduleInfo={sources:['来源配置','管理 ComfyUI 来源与工作流白名单'],workflows:['工作流管理','上传、编辑参数、说明与文件'],tools:['缓存清理','清理缓存与历史记录']};
const storageKeys={theme:'comfyui_bubble_theme',module:'comfyui_bubble_module',sidebar:'comfyui_bubble_sidebar_collapsed',selected:'comfyui_bubble_selected_workflow'};
const state={files:[],ports:[],active:'',workflowOptions:[],module:localStorage.getItem(storageKeys.module)||'sources',theme:localStorage.getItem(storageKeys.theme)||'light',collapsed:localStorage.getItem(storageKeys.sidebar)==='1',selected:localStorage.getItem(storageKeys.selected)||'',query:'',dirty:new Set()};
const $=(s,r)=>(r||document).querySelector(s);const $$=(s,r)=>Array.from((r||document).querySelectorAll(s));
const icon=n=>'<svg class="icon"><use href="#'+n+'"></use></svg>';const esc=v=>String(v??'').replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const workflowNamePattern=/^[\u4e00-\u9fff\u3040-\u30ffA-Za-z0-9_.:-]+$/;const workflowNameHint='工作流名称只能使用中文、日文、英文、数字、下划线 _、中划线 -、英文句号 . 和冒号 :';const isValidWorkflowName=name=>!!String(name||'').trim()&&workflowNamePattern.test(String(name).trim());const validateWorkflowNameField=(show=true)=>{const input=$('#workflowName');if(!input)return true;const ok=isValidWorkflowName(input.value);input.classList.toggle('field-error',!ok);if(!ok&&show)toast(workflowNameHint,false);return ok};const workflowObjectName=f=>((f&&f.params&&f.params.name)||f.name||'').trim();
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
function markDirty(fn){state.dirty.add(fn);renderWorkflowList();renderEditorHeader()}function clearDirty(fn){state.dirty.delete(fn);renderWorkflowList();renderEditorHeader()}
function allWorkflows(p){return !p.workflows||p.workflows.length===0}
function renderPorts(){const root=$('#portsList');root.innerHTML=state.ports.map((p,i)=>{const all=allWorkflows(p), active=p.name&&p.name===state.active;const checks=state.workflowOptions.map(w=>{const selected=!all&&(p.workflows||[]).includes(w.name);return '<label class="workflow-check '+(selected?'selected ':'')+(all?'disabled':'')+'"><input type="checkbox" class="workflow-check-input" data-port="'+i+'" value="'+esc(w.name)+'" '+(selected?'checked':'')+' '+(all?'disabled':'')+'><span class="workflow-title">'+esc(w.name)+'</span></label>'}).join('')||'<div class="empty">暂无工作流</div>';return '<article class="source-card card" data-port="'+i+'"><div class="source-head"><div><div class="source-name">'+esc(p.name||'来源 '+(i+1))+'</div><div class="source-http">'+esc(p.http||'未配置地址')+'</div></div><span class="chip '+(active?'active':'')+'">'+(active?'当前来源':(all?state.workflowOptions.length:(p.workflows||[]).length)+' 个工作流')+'</span></div><div class="field-grid"><div><label class="field-label">名称</label><input class="port-name" value="'+esc(p.name||'')+'" placeholder="例如：高性能机"></div><div><label class="field-label">HTTP 地址</label><input class="port-http" value="'+esc(p.http||'')+'" placeholder="例如：http://127.0.0.1:8188"></div></div><div class="whitelist-head"><label class="switch-row"><input type="checkbox" class="all-workflows-check" data-port="'+i+'" '+(all?'checked':'')+'><span class="switch"></span><span>允许全部工作流</span></label><button type="button" class="btn-soft btn-active-port" data-port="'+i+'" '+(!p.name||active?'disabled':'')+'>设为当前</button></div><div class="workflow-checks">'+checks+'</div></article>'}).join('');$$('.all-workflows-check').forEach(input=>input.onchange=()=>{const i=+input.dataset.port;state.ports[i].workflows=input.checked?[]:state.workflowOptions.map(w=>w.name);renderPorts()});$$('.workflow-check-input').forEach(input=>input.onchange=()=>{input.closest('.workflow-check').classList.toggle('selected',input.checked)});$$('.btn-active-port').forEach(btn=>btn.onclick=async()=>{try{const p=state.ports[+btn.dataset.port];const data=await api('/api/active_port',{name:p.name});state.active=data.active||p.name;toast('当前来源已切换为 '+state.active);renderPorts()}catch(e){toast(e.message,false)}})}
function collectPorts(){return $$('.source-card').map(card=>{const all=$('.all-workflows-check',card).checked;return{name:$('.port-name',card).value.trim(),http:$('.port-http',card).value.trim(),workflows:all?[]:$$('.workflow-check-input:checked',card).map(i=>i.value)}})}
function hasKind(f,k){if(k==='all')return true;const p=f.params||{}, input=p.inputs&&p.inputs[k];if(input&&input.limit!==undefined&&input.limit!==null&&input.limit!=='')return Number(input.limit)>0||input.mode==='strict';return [f.filename,f.name,desc(f,'short')].join(' ').toLowerCase().includes({text:'文本',image:'图片',video:'视频'}[k])}
function renderWorkflowList(){const q=state.query.toLowerCase();const items=state.files.filter(f=>{const text=[f.filename,f.name,desc(f,'short'),desc(f,'detailed')].join(' ').toLowerCase();return !q||text.includes(q)});if(!state.selected||!getFile(state.selected))state.selected=(items[0]||state.files[0]||{}).filename||'';localStorage.setItem(storageKeys.selected,state.selected||'');$('#workflowList').innerHTML=items.map(f=>'<button type="button" class="workflow-item '+(f.filename===state.selected?'active':'')+'" data-filename="'+esc(f.filename)+'">'+(state.dirty.has(f.filename)?'<span class="dirty-dot"></span>':'')+'<span class="workflow-item-main"><span class="workflow-item-title">'+esc(f.name||f.filename)+'</span><span class="workflow-item-meta">'+esc(f.filename)+'</span></span></button>').join('')||'<div class="empty">没有匹配的工作流</div>';$('#workflowCount').textContent=items.length+'/'+state.files.length+' 个工作流';$$('.workflow-item').forEach(b=>b.onclick=()=>{state.selected=b.dataset.filename;renderWorkflowList();renderWorkflowEditor()})}
const ruleText=(g,k)=>(g==='inputs'?'输入':'输出')+' / '+({text:'文本',image:'图片',video:'视频'}[k]||k);function rule(p,g,k){const r=(p[g]&&p[g][k])||{}, strict=r.mode==='strict';return '<div class="param-cell" data-label="'+ruleText(g,k)+'"><div class="param-control" data-group="'+g+'" data-key="'+k+'"><input class="param-count" type="number" min="0" value="'+esc(r.limit??'')+'" placeholder="无限制"><button type="button" class="mode-toggle '+(strict?'strict':'')+'" data-mode="'+(strict?'strict':'loose')+'">严格</button></div></div>'}
function paramsHtml(f){const p=f.params||{};return '<div class="param-panel"><div class="param-grid"><div class="param-cell empty-cell"></div><div class="param-cell header-cell"><span class="param-label">文本</span></div><div class="param-cell header-cell"><span class="param-label">图片</span></div><div class="param-cell header-cell"><span class="param-label">视频</span></div><div class="param-cell"><span class="param-label">输入</span></div>'+rule(p,'inputs','text')+rule(p,'inputs','image')+rule(p,'inputs','video')+'<div class="param-cell"><span class="param-label">输出</span></div>'+rule(p,'outputs','text')+rule(p,'outputs','image')+rule(p,'outputs','video')+'</div></div>'}
function collectEditor(){const f=getFile(state.selected);if(!f)return;const p={name:$('#workflowName').value.trim(),inputs:{},outputs:{}};validateWorkflowNameField(false);$$('.param-control').forEach(c=>{const raw=$('.param-count',c).value;p[c.dataset.group][c.dataset.key]={limit:raw===''?null:Math.max(0,parseInt(raw,10)||0),mode:$('.mode-toggle',c).dataset.mode||'loose'}});f.name=p.name||f.filename.replace(/\.json$/i,'');f.params=p;setDesc(f,$('#shortDesc').value,$('#detailedDesc').value);markDirty(f.filename)}
function renderEditorHeader(){const b=$('#editorDirtyBadge');if(b)b.style.display=state.dirty.has(state.selected)?'inline-flex':'none'}
function renderWorkflowEditor(){const f=getFile(state.selected), root=$('#workflowEditor');if(!f){root.innerHTML='<div class="empty">暂无工作流，上传 JSON 后开始配置。</div>';return}root.innerHTML='<div class="editor-head"><div class="editor-name"><h2>'+esc(f.name||f.filename)+'</h2><p>'+esc(f.filename)+'</p><span id="editorDirtyBadge" class="chip warn" style="display:none">未保存</span></div><div class="editor-actions"><button type="button" id="saveOneBtn" class="btn-primary">'+icon('i-save')+'保存</button><button type="button" id="renameBtn" class="btn-soft">重命名</button><button type="button" id="deleteBtn" class="btn-danger">'+icon('i-trash')+'删除</button></div></div><div class="editor-body"><div class="form-row"><label class="field-label">工作流名称（命令调用使用的名称）</label><input id="workflowName" value="'+esc(f.name||'')+'"></div><div class="form-row"><div class="field-label label-help"><span>输入输出数量设置</span><span class="help-wrap"><button type="button" class="help-dot" aria-label="输入输出数量设置说明">?</button><span class="help-popover">设置该工作流对应类型输入输出源的数量上限，开启严格模式后，输入数量必须等于设置值，输出数量不小于设置值</span></span></div>'+paramsHtml(f)+'</div><div class="form-row"><label class="field-label">简要说明</label><textarea id="shortDesc" rows="3">'+esc(desc(f,'short'))+'</textarea></div><div class="form-row"><label class="field-label">详细说明</label><textarea id="detailedDesc" rows="7">'+esc(desc(f,'detailed'))+'</textarea></div></div>';renderEditorHeader();$$('#workflowName,#shortDesc,#detailedDesc').forEach(e=>e.oninput=collectEditor);$('#workflowName').onblur=()=>validateWorkflowNameField(false);$$('.param-count').forEach(e=>e.oninput=collectEditor);$$('.mode-toggle').forEach(b=>b.onclick=()=>{b.dataset.mode=b.dataset.mode==='strict'?'loose':'strict';b.textContent='严格';b.classList.toggle('strict',b.dataset.mode==='strict');collectEditor()});$$('.help-dot').forEach(b=>b.onclick=()=>b.closest('.help-wrap').classList.toggle('open'));$('#saveOneBtn').onclick=saveOne;$('#renameBtn').onclick=renameWorkflow;$('#deleteBtn').onclick=deleteWorkflow}
async function saveOne(){collectEditor();if(!validateWorkflowNameField(true))return;const f=getFile(state.selected), btn=$('#saveOneBtn');if(!f)return;try{setLoading(btn,true,'保存中...');await api('/api/workflow',payload(f));clearDirty(f.filename);toast('已保存 '+f.filename)}catch(e){toast(e.message,false)}finally{setLoading(btn,false)}}
async function saveAll(){collectEditor();const invalid=state.files.find(f=>state.dirty.has(f.filename)&&!isValidWorkflowName(workflowObjectName(f)));if(invalid){state.selected=invalid.filename;renderWorkflowList();renderWorkflowEditor();validateWorkflowNameField(true);return}const items=state.files.filter(f=>state.dirty.has(f.filename)).map(payload);if(!items.length){toast('没有未保存更改');return}const btn=$('#saveAllBtn');try{setLoading(btn,true,'保存中...');await api('/api/workflows/bulk',{items});items.forEach(i=>state.dirty.delete(i.filename));renderWorkflowList();renderEditorHeader();toast('已保存全部更改')}catch(e){toast(e.message,false)}finally{setLoading(btn,false)}}
async function renameWorkflow(){const f=getFile(state.selected);if(!f)return;let name=await textInputAction('重命名 JSON 文件','新的 JSON 文件名',f.filename,'重命名');if(!name)return;if(!name.toLowerCase().endsWith('.json'))name+='.json';if(name===f.filename)return;try{await api('/api/rename',{old_name:f.filename,new_name:name});toast('已重命名');state.selected=name;await loadAll();renderWorkflowList();renderWorkflowEditor()}catch(e){toast(e.message,false)}}
async function deleteWorkflow(){const f=getFile(state.selected);if(!f)return;if(!await confirmAction('删除工作流','将删除 '+f.filename+'，此操作不可撤销。','删除',true))return;try{await api('/api/delete',{filename:f.filename});toast('已删除');state.selected='';await loadAll()}catch(e){toast(e.message,false)}}
async function uploadWorkflow(){const input=$('#fileInput'), file=input.files&&input.files[0];if(!file)return;const btn=$('#uploadBtn'), fd=new FormData();fd.append('file',file);try{setLoading(btn,true,'上传中...');const res=await fetch('/api/upload',{method:'POST',body:fd});const data=await res.json();if(!res.ok||data.ok===false)throw new Error(data.error||res.statusText);toast('上传成功');input.value='';state.selected=data.filename;await loadAll()}catch(e){toast(e.message,false);input.value=''}finally{setLoading(btn,false)}}
async function loadAll(){const [list,ports]=await Promise.all([api('/api/list'),api('/api/ports')]);state.files=list.files||[];state.ports=ports.ports||[];state.active=ports.active||'';state.workflowOptions=state.files.map(f=>({name:f.name||f.filename.replace(/\.json$/i,''),filename:f.filename}));renderPorts();renderWorkflowList();renderWorkflowEditor()}
function bind(){applyTheme();applySidebar();setModule(state.module);$$('.nav button').forEach(b=>b.onclick=()=>setModule(b.dataset.module));$('#themeBtn').onclick=()=>{state.theme=state.theme==='dark'?'light':'dark';applyTheme()};$('#collapseBtn').onclick=()=>{if(window.innerWidth<=820){closeSidebar();return}state.collapsed=!state.collapsed;applySidebar()};$('#menuBtn').onclick=()=>document.body.classList.add('drawer-open');$('#drawerBackdrop').onclick=closeSidebar;$('#workflowSearch').oninput=e=>{state.query=e.target.value;renderWorkflowList()};$('#fileInput').onchange=uploadWorkflow;$('#saveAllBtn').onclick=saveAll;$('#uploadBtn').onclick=()=>{const input=$('#fileInput');input.value='';input.click()};$('#refreshPortsBtn').onclick=loadAll;$('#savePortsBtn').onclick=async()=>{try{await api('/api/ports',{ports:collectPorts()});toast('来源配置已保存');await loadAll()}catch(e){toast(e.message,false)}};$('#clearCacheBtn').onclick=async()=>{if(!await confirmAction('清理缓存','将删除输入缓存和插件临时文件。','清理',true))return;try{const d=await api('/api/clear_cache',{});toast('缓存清理完成，删除 '+(d.deleted??0)+' 个文件')}catch(e){toast(e.message,false)}};$('#clearHistoryBtn').onclick=async()=>{const hours=Math.max(0,parseInt($('#historyHours').value,10)||0);if(!await confirmAction('清理历史','将清理 '+(hours?hours+' 小时以前':'全部')+' 的历史记录。','清理',true))return;try{const d=await api('/api/clear_history',{hours});toast('历史清理完成，删除 '+(d.deleted??0)+' 条记录')}catch(e){toast(e.message,false)}};window.addEventListener('beforeunload',e=>{if(state.dirty.size){e.preventDefault();e.returnValue=''}})}
bind();loadAll().catch(e=>toast(e.message,false));
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

