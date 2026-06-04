# -*- coding: utf-8 -*-
"""Workflow management routes."""

import json

from aiohttp import web

from ..workflow_engine import parse_workflow_filename
from .context import ManagementContext
from .utils import SAFE_FILENAME_RE, safe_basename as _safe_basename


def register_workflow_routes(app: web.Application, ctx: ManagementContext) -> None:
    workflows_dir = ctx.workflows_dir
    meta_path = ctx.meta_path
    load_meta = ctx.load_meta
    save_meta = ctx.save_meta
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

    app.router.add_get("/api/list", list_handler)
    app.router.add_post("/api/upload", upload_handler)
    app.router.add_post("/api/description", description_handler)
    app.router.add_post("/api/description_detailed", description_detailed_handler)
    app.router.add_post("/api/workflow_params", workflow_params_handler)
    app.router.add_post("/api/workflow", workflow_handler)
    app.router.add_post("/api/workflows/bulk", workflows_bulk_handler)
    app.router.add_post("/api/rename", rename_handler)
    app.router.add_post("/api/delete", delete_handler)
