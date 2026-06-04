# -*- coding: utf-8 -*-
"""Cache and history maintenance routes."""

from aiohttp import web

from .context import ManagementContext


def register_maintenance_routes(app: web.Application, ctx: ManagementContext) -> None:
    tmp_dir = ctx.tmp_dir
    _cleanup_history = ctx.cleanup_history_func
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

    app.router.add_post("/api/clear_cache", clear_cache_handler)
    app.router.add_post("/api/clear_history", clear_history_handler)
