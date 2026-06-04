# -*- coding: utf-8 -*-
"""Management WebUI server lifecycle."""

from collections.abc import Callable
from pathlib import Path

from aiohttp import web

from .app import create_app

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
