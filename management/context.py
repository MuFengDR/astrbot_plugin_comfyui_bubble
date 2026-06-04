# -*- coding: utf-8 -*-
"""Dependency container for the management WebUI server."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ManagementContext:
    workflows_dir: Path
    meta_path: Path
    load_meta: Callable[[], dict[str, str]]
    save_meta: Callable[[dict[str, str]], None]
    plugin_data_dir: Path
    cleanup_history_func: Callable[..., int] | None = None
    ports_config_path: Path | None = None
    active_port_state_path: Path | None = None
    load_ports_func: Callable[[], list[dict[str, object]]] | None = None
    save_ports_func: Callable[[list[dict[str, object]]], None] | None = None
    active_port_changed_func: Callable[[], None] | None = None
    debug_submit_func: Callable[[dict[str, object]], object] | None = None
    debug_tasks_func: Callable[..., object] | None = None
    debug_history_func: Callable[..., object] | None = None
    debug_task_func: Callable[[str], object] | None = None
    debug_delete_func: Callable[[str], object] | None = None
    debug_stop_func: Callable[[str], object] | None = None
    output_media_dir: Path | None = None
    tmp_dir: Path | None = None
    media_history_dir: Path | None = None
    logo_path: Path | None = None
    webui_output_dir: Path | None = None
