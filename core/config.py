# -*- coding: utf-8 -*-
"""ComfyUI interface configuration helpers."""

import json
from typing import Any, Dict, List, Optional

from astrbot.api import logger

from .paths import ACTIVE_PORT_STATE_PATH, PLUGIN_DATA_DIR, PORTS_CONFIG_PATH


def _config_get(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def _normalize_comfyui_http(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    return f"http://{raw}"


def _get_comfyui_http_base(server_ip: str) -> str:
    return _normalize_comfyui_http(server_ip or "127.0.0.1:8188")


def _get_comfyui_host(server_ip: str) -> str:
    raw = _get_comfyui_http_base(server_ip)
    try:
        from urllib.parse import urlparse

        parsed = urlparse(raw)
        return (parsed.hostname or "").lower()
    except Exception:
        return raw.replace("http://", "").replace("https://", "").split(":")[0].lower()


def _split_workflow_names(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_items = [str(v) for v in value]
    else:
        text = str(value).replace("，", ",").replace("\n", ",")
        raw_items = text.split(",")
    names: List[str] = []
    seen = set()
    for item in raw_items:
        name = item.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        names.append(name)
    return names


def _normalize_comfyui_port_entry(entry: Any, idx: int) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    name = str(entry.get("name") or "").strip()
    http = _normalize_comfyui_http(entry.get("http") or entry.get("server_ip") or "")
    workflows = _split_workflow_names(entry.get("workflows", []))
    if not name and not http:
        return None
    if not name:
        name = f"port{idx}"
    if not http:
        return None
    return {"name": name, "http": http, "workflows": workflows}


def _load_ports_config_file() -> Optional[List[Dict[str, Any]]]:
    try:
        if not PORTS_CONFIG_PATH.exists():
            return None
        data = json.loads(PORTS_CONFIG_PATH.read_text(encoding="utf-8"))
        raw_ports = data.get("ports") if isinstance(data, dict) else data
        if not isinstance(raw_ports, list):
            return None
        ports: List[Dict[str, Any]] = []
        for idx, item in enumerate(raw_ports, start=1):
            port = _normalize_comfyui_port_entry(item, idx)
            if port:
                ports.append(port)
        return ports
    except Exception as e:
        logger.warning("ComfyUI read ports config failed: %s", e)
        return None


def _save_ports_config_file(ports: List[Dict[str, Any]]) -> None:
    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate((ports or []), start=1):
        port = _normalize_comfyui_port_entry(item, idx)
        if port:
            normalized.append(port)
    PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    PORTS_CONFIG_PATH.write_text(
        json.dumps({"ports": normalized}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_schema_comfyui_ports(config: Any) -> List[Dict[str, Any]]:
    """Read legacy 1-4 interface fields for first-run migration only."""
    ports: List[Dict[str, Any]] = []
    for idx in range(1, 5):
        name = str(_config_get(config, f"comfyui_port_{idx}_name", "") or "").strip()
        http = _normalize_comfyui_http(_config_get(config, f"comfyui_port_{idx}_http", "") or "")
        workflows = _split_workflow_names(_config_get(config, f"comfyui_port_{idx}_workflows", ""))
        if not name and not http:
            continue
        if not name:
            name = f"port{idx}"
        if not http:
            continue
        ports.append({"name": name, "http": http, "workflows": workflows})
    if not ports:
        ports.append({"name": "default", "http": "http://127.0.0.1:8188", "workflows": []})
    return ports


def _get_comfyui_ports(config: Any) -> List[Dict[str, Any]]:
    ports = _load_ports_config_file()
    return ports if ports is not None else _get_schema_comfyui_ports(config)


def _read_active_port_name() -> str:
    try:
        if ACTIVE_PORT_STATE_PATH.exists():
            data = json.loads(ACTIVE_PORT_STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return str(data.get("name") or "").strip()
    except Exception as e:
        logger.warning("ComfyUI read active port state failed: %s", e)
    return ""


def _write_active_port_name(name: str) -> None:
    PLUGIN_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ACTIVE_PORT_STATE_PATH.write_text(
        json.dumps({"name": name}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_active_comfyui_port(config: Any) -> Dict[str, Any]:
    ports = _get_comfyui_ports(config)
    if not ports:
        return {"name": "", "http": "", "workflows": []}
    preferred = _read_active_port_name()
    if preferred:
        for port in ports:
            if port["name"] == preferred:
                return port
    return ports[0]


def _sync_active_interface_config(config: Any, persist: bool = False) -> None:
    """Best-effort sync for read-only fields shown by AstrBot's config page."""
    try:
        active = _get_active_comfyui_port(config or {})
        if isinstance(config, dict):
            config["comfyui_active_interface_name"] = active.get("name", "")
            config["comfyui_active_interface_http"] = active.get("http", "")
        else:
            setattr(config, "comfyui_active_interface_name", active.get("name", ""))
            setattr(config, "comfyui_active_interface_http", active.get("http", ""))
        if persist and hasattr(config, "save_config"):
            config.save_config()
    except Exception:
        pass


def _workflow_allowed_for_port(workflow: Dict[str, Any], port: Dict[str, Any]) -> bool:
    allowed = port.get("workflows") or []
    return not allowed or workflow.get("name") in allowed


def _filter_workflows_for_port(workflows: List[Dict[str, Any]], port: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not (port.get("name") and port.get("http")):
        return []
    return [w for w in workflows if _workflow_allowed_for_port(w, port)]


def _get_server_config(config: Any) -> tuple:
    port = _get_active_comfyui_port(config)
    server_ip = port["http"]
    client_id = str(_config_get(config, "client_id", "astrbot-comfyui-bubble-1") or "astrbot-comfyui-bubble-1").strip()
    return server_ip, client_id


__all__ = ['_config_get', '_normalize_comfyui_http', '_get_comfyui_http_base', '_get_comfyui_host', '_split_workflow_names', '_normalize_comfyui_port_entry', '_load_ports_config_file', '_save_ports_config_file', '_get_schema_comfyui_ports', '_get_comfyui_ports', '_read_active_port_name', '_write_active_port_name', '_get_active_comfyui_port', '_sync_active_interface_config', '_workflow_allowed_for_port', '_filter_workflows_for_port', '_get_server_config']
