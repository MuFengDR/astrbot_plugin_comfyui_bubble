# -*- coding: utf-8 -*-
"""aiohttp application factory for the management WebUI."""

from collections.abc import Callable
from pathlib import Path

from aiohttp import web

from .context import ManagementContext
from .debug import register_debug_routes
from .interfaces import register_interface_routes
from .maintenance import register_maintenance_routes
from .ui import register_ui_routes
from .workflows import register_workflow_routes


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
    ctx = ManagementContext(
        workflows_dir=workflows_dir,
        meta_path=meta_path,
        load_meta=load_meta,
        save_meta=save_meta,
        plugin_data_dir=plugin_data_dir,
        cleanup_history_func=cleanup_history_func,
        ports_config_path=ports_config_path or plugin_data_dir / "ports_config.json",
        active_port_state_path=active_port_state_path or plugin_data_dir / "active_port.json",
        load_ports_func=load_ports_func,
        save_ports_func=save_ports_func,
        active_port_changed_func=active_port_changed_func,
        debug_submit_func=debug_submit_func,
        debug_tasks_func=debug_tasks_func,
        debug_history_func=debug_history_func,
        debug_task_func=debug_task_func,
        debug_delete_func=debug_delete_func,
        debug_stop_func=debug_stop_func,
        output_media_dir=plugin_data_dir.resolve().parent.parent / "agent" / "comfyui" / "input",
        tmp_dir=plugin_data_dir / "tmp",
        media_history_dir=plugin_data_dir / "media" / "history",
        logo_path=Path(__file__).resolve().parent.parent / "webui_logo.jpg",
        webui_output_dir=plugin_data_dir / "webui" / "output",
    )
    register_ui_routes(app, ctx)
    register_workflow_routes(app, ctx)
    register_maintenance_routes(app, ctx)
    register_interface_routes(app, ctx)
    register_debug_routes(app, ctx)
    return app
