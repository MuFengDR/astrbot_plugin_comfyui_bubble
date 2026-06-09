# -*- coding: utf-8 -*-
"""Content audit management routes."""

from aiohttp import web

from .context import ManagementContext
from .utils import maybe_await as _maybe_await


def register_audit_routes(app: web.Application, ctx: ManagementContext) -> None:
    audit_records_func = ctx.audit_records_func
    audit_stats_func = ctx.audit_stats_func
    audit_get_settings_func = ctx.audit_get_settings_func
    audit_save_settings_func = ctx.audit_save_settings_func
    audit_test_func = ctx.audit_test_func

    async def audit_records_handler(request: web.Request) -> web.Response:
        if not audit_records_func:
            return web.json_response({"ok": True, "records": []})
        filters = {
            "status": request.query.get("status", ""),
            "origin": request.query.get("origin", ""),
            "workflow": request.query.get("workflow", ""),
            "port": request.query.get("port", ""),
            "limit": request.query.get("limit", "200"),
        }
        result = await _maybe_await(audit_records_func(filters))
        return web.json_response(result if isinstance(result, dict) else {"ok": True, "records": []})

    async def audit_stats_handler(_: web.Request) -> web.Response:
        if not audit_stats_func:
            return web.json_response({"ok": True, "stats": {"total": 0, "unknown": 0, "pass": 0, "block": 0, "error": 0}})
        result = await _maybe_await(audit_stats_func())
        return web.json_response(result if isinstance(result, dict) else {"ok": True, "stats": {}})

    async def audit_settings_handler(request: web.Request) -> web.Response:
        if request.method == "GET":
            if not audit_get_settings_func:
                return web.json_response(
                    {
                        "ok": True,
                        "settings": {
                            "enabled": True,
                            "provider": "baidu_icr",
                            "providers": {"image": "baidu_icr", "video": "", "text": "baidu_text_censor"},
                            "baidu_text_censor": {"credential_mode": "reuse_image", "configured": False},
                            "fail_policy": "allow",
                        },
                    }
                )
            result = await _maybe_await(audit_get_settings_func())
            return web.json_response(result if isinstance(result, dict) else {"ok": True, "settings": {}})
        if not audit_save_settings_func:
            return web.json_response({"ok": False, "error": "audit api unavailable"}, status=501)
        try:
            data = await request.json()
        except Exception:
            data = {}
        result = await _maybe_await(audit_save_settings_func(data if isinstance(data, dict) else {}))
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 400
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid audit result"}, status=status)

    async def audit_test_handler(request: web.Request) -> web.Response:
        if not audit_test_func:
            return web.json_response({"ok": False, "error": "audit api unavailable"}, status=501)
        try:
            data = await request.json()
        except Exception:
            data = {}
        result = await _maybe_await(audit_test_func(data if isinstance(data, dict) else {}))
        status = 200 if isinstance(result, dict) and result.get("ok", True) else 400
        return web.json_response(result if isinstance(result, dict) else {"ok": False, "error": "invalid audit result"}, status=status)

    app.router.add_get("/api/audit/records", audit_records_handler)
    app.router.add_get("/api/audit/stats", audit_stats_handler)
    app.router.add_get("/api/audit/settings", audit_settings_handler)
    app.router.add_post("/api/audit/settings", audit_settings_handler)
    app.router.add_post("/api/audit/test", audit_test_handler)
