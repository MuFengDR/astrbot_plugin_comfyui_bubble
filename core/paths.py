# -*- coding: utf-8 -*-
"""Filesystem paths used by the plugin."""

from pathlib import Path


def _resolve_plugin_data_dir() -> Path:
    try:
        from astrbot.api.star import StarTools

        return Path(StarTools.get_data_dir("astrbot_plugin_comfyui_bubble"))
    except Exception:
        return Path("data/plugin_data/astrbot_plugin_comfyui_bubble").resolve()


PLUGIN_DATA_DIR = _resolve_plugin_data_dir()
WORKFLOWS_DIR = PLUGIN_DATA_DIR / "workflows"
META_PATH = PLUGIN_DATA_DIR / "workflow_meta.json"
ACTIVE_PORT_STATE_PATH = PLUGIN_DATA_DIR / "active_port.json"
PORTS_CONFIG_PATH = PLUGIN_DATA_DIR / "ports_config.json"


__all__ = [
    "ACTIVE_PORT_STATE_PATH",
    "META_PATH",
    "PLUGIN_DATA_DIR",
    "PORTS_CONFIG_PATH",
    "WORKFLOWS_DIR",
]
