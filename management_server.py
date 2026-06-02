# -*- coding: utf-8 -*-
"""
工作流管理小站：提供 .json 文件列表、上传、备注、重命名、删除。
在浏览器打开 http://localhost:{management_port} 使用。
"""
import re
import json
from pathlib import Path
from typing import Callable, Dict, List, Optional

from aiohttp import web

from .workflow_engine import parse_workflow_filename

# 安全文件名：只保留安全字符
SAFE_FILENAME_RE = re.compile(r"^[a-zA-Z0-9_\-\+=\.\u4e00-\u9fff]+$")


def _safe_basename(name: str) -> str:
    """防止路径穿越，只取 basename。"""
    return Path(name).name.strip()


def create_app(
    workflows_dir: Path,
    meta_path: Path,
    load_meta: Callable[[], Dict[str, str]],
    save_meta: Callable[[Dict[str, str]], None],
    plugin_data_dir: Optional[Path] = None,
    cleanup_history_func: Optional[Callable[[], int]] = None,
    ports_config_path: Optional[Path] = None,
    active_port_state_path: Optional[Path] = None,
    load_ports_func: Optional[Callable[[], List[Dict[str, object]]]] = None,
    save_ports_func: Optional[Callable[[List[Dict[str, object]]], None]] = None,
) -> web.Application:
    app = web.Application()
    if plugin_data_dir is None:
        plugin_data_dir = workflows_dir.parent
    output_media_dir = plugin_data_dir.resolve().parent.parent / "agent" / "comfyui" / "input"
    tmp_dir = plugin_data_dir / "tmp"
    if ports_config_path is None:
        ports_config_path = plugin_data_dir / "ports_config.json"
    if active_port_state_path is None:
        active_port_state_path = plugin_data_dir / "active_port.json"
    
    # 保存清理历史记录的函数引用
    _cleanup_history = cleanup_history_func

    async def list_handler(_: web.Request) -> web.Response:
        """GET /api/list：列出 workflows 目录下所有 .json 及备注。"""
        meta = load_meta()
        files = []
        if workflows_dir.exists():
            for f in sorted(workflows_dir.glob("*.json")):
                name = f.name
                files.append({
                    "filename": name,
                    "name": (parse_workflow_filename(name) or {}).get("name", name.removesuffix(".json")),
                    "description": meta.get(name, ""),
                })
        return web.json_response({"files": files})

    async def upload_handler(request: web.Request) -> web.Response:
        """POST /api/upload：上传一个 .json 文件。"""
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
            return web.json_response({"ok": False, "error": "missing field: file"}, status=400)
        filename = _safe_basename(field.filename or "workflow.json")
        if not filename.lower().endswith(".json"):
            return web.json_response({"ok": False, "error": "only .json allowed"}, status=400)
        if not SAFE_FILENAME_RE.match(filename):
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
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
        """POST /api/description：保存某个文件的备注。body: {"filename":"x.json","description":"..."}"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        description = (body.get("description") or "").strip()
        if not filename or not filename.endswith(".json"):
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
        if not (workflows_dir / filename).exists():
            return web.json_response({"ok": False, "error": "file not found in workflows"}, status=404)
        meta = load_meta()
        meta[filename] = description
        save_meta(meta)
        return web.json_response({"ok": True})

    async def description_detailed_handler(request: web.Request) -> web.Response:
        """POST /api/description_detailed：保存某个文件的详细说明。body: {"filename":"x.json","description":"..."}"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        description = (body.get("description") or "").strip()
        if not filename or not filename.endswith(".json"):
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
        if not (workflows_dir / filename).exists():
            return web.json_response({"ok": False, "error": "file not found in workflows"}, status=404)
        meta = load_meta()
        if filename not in meta:
            meta[filename] = {"short": "", "detailed": ""}
        meta[filename]["detailed"] = description
        save_meta(meta)
        return web.json_response({"ok": True})

    async def rename_handler(request: web.Request) -> web.Response:
        """POST /api/rename：重命名文件。body: {"old_name":"a.json","new_name":"b.json"}"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        old_name = _safe_basename(body.get("old_name") or "")
        new_name = _safe_basename(body.get("new_name") or "")
        if not old_name.endswith(".json") or not new_name.endswith(".json"):
            return web.json_response({"ok": False, "error": "only .json allowed"}, status=400)
        if not SAFE_FILENAME_RE.match(new_name):
            return web.json_response({"ok": False, "error": "invalid new filename"}, status=400)
        old_path = workflows_dir / old_name
        new_path = workflows_dir / new_name
        if not old_path.exists():
            return web.json_response({"ok": False, "error": "file not found"}, status=404)
        if new_path.exists():
            return web.json_response({"ok": False, "error": "target already exists"}, status=400)
        old_path.rename(new_path)
        meta = load_meta()
        if old_name in meta:
            meta[new_name] = meta.pop(old_name)
            save_meta(meta)
        return web.json_response({"ok": True, "filename": new_name})

    async def delete_handler(request: web.Request) -> web.Response:
        """POST /api/delete：删除文件。body: {"filename":"x.json"}"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        filename = _safe_basename(body.get("filename") or "")
        if not filename.endswith(".json"):
            return web.json_response({"ok": False, "error": "invalid filename"}, status=400)
        path = workflows_dir / filename
        if not path.exists():
            return web.json_response({"ok": False, "error": "file not found"}, status=404)
        path.unlink()
        meta = load_meta()
        meta.pop(filename, None)
        save_meta(meta)
        return web.json_response({"ok": True})

    async def clear_cache_handler(request: web.Request) -> web.Response:
        """POST /api/clear_cache：清理本地缓存（data/agent/comfyui/input 与插件 tmp），防止图片/视频过多占用空间。"""
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
        return web.json_response({"ok": True, "deleted": deleted, "dirs": [str(output_media_dir), str(tmp_dir)]})
    async def clear_history_handler(request: web.Request) -> web.Response:
        """POST /api/clear_history：清理历史生成记录，释放内存。body: {"hours": 48}"""
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        
        hours = body.get("hours", 48)
        try:
            hours = int(hours)
        except (TypeError, ValueError):
            return web.json_response({"ok": False, "error": "hours must be an integer"}, status=400)
        
        if hours < 0:
            hours = 0
        
        # 调用清理函数
        if _cleanup_history:
            try:
                deleted_count = _cleanup_history()
                message = f"已清理 {deleted_count} 条历史记录 (保留{hours}小时内)"
            except Exception as e:
                return web.json_response({"ok": False, "error": str(e)}, status=500)
        else:
            return web.json_response({"ok": False, "error": "cleanup function not available"}, status=500)
        
        return web.json_response({"ok": True, "message": message, "hours": hours})


    def _normalize_http(value: str) -> str:
        raw = str(value or "").strip().rstrip("/")
        if not raw:
            return ""
        if raw.startswith(("http://", "https://")):
            return raw
        return f"http://{raw}"

    def _normalize_workflow_names(value) -> List[str]:
        raw_items = value if isinstance(value, list) else []
        names: List[str] = []
        seen = set()
        for item in raw_items:
            name = str(item or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            names.append(name)
        return names

    def _normalize_port_entry(entry, idx: int) -> Optional[Dict[str, object]]:
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

    def _load_ports() -> List[Dict[str, object]]:
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

    def _save_ports(ports: List[Dict[str, object]]) -> None:
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

    def _workflow_options() -> List[Dict[str, str]]:
        options: List[Dict[str, str]] = []
        seen = set()
        if workflows_dir.exists():
            for f in sorted(workflows_dir.glob("*.json")):
                parsed = parse_workflow_filename(f.name)
                name = (parsed or {}).get("name") or f.stem
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
            return web.json_response({"ok": False, "error": "ports must be a list"}, status=400)
        ports: List[Dict[str, object]] = []
        for idx, item in enumerate(raw_ports[:4], start=1):
            port = _normalize_port_entry(item, idx)
            if port:
                ports.append(port)
        _save_ports(ports)
        return web.json_response({"ok": True, "ports": ports})

    async def index_handler(_: web.Request) -> web.Response:
        """GET /：返回管理页 HTML。"""
        html = _INDEX_HTML
        return web.Response(text=html, content_type="text/html")

    app.router.add_get("/", index_handler)
    app.router.add_get("/api/list", list_handler)
    app.router.add_post("/api/upload", upload_handler)
    app.router.add_post("/api/description", description_handler)
    app.router.add_post("/api/description_detailed", description_detailed_handler)
    app.router.add_post("/api/rename", rename_handler)
    app.router.add_post("/api/delete", delete_handler)
    app.router.add_post("/api/clear_cache", clear_cache_handler)
    app.router.add_post("/api/clear_history", clear_history_handler)
    app.router.add_get("/api/ports", ports_handler)
    app.router.add_post("/api/ports", save_ports_handler)
    return app


_INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ComfyUI 工作流管理</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; padding: 16px; background: #1e1e1e; color: #d4d4d4; }
    h1 { font-size: 1.25rem; margin-bottom: 12px; }
    .upload { margin-bottom: 16px; padding: 12px; background: #252526; border-radius: 8px; }
    .upload input[type=file] { margin-right: 8px; }
    .upload button { padding: 6px 12px; cursor: pointer; background: #0e639c; color: #fff; border: none; border-radius: 4px; }
    .upload button:hover { background: #1177bb; }
    table { width: 100%; border-collapse: collapse; background: #252526; border-radius: 8px; overflow: hidden; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #333; }
    th { background: #2d2d30; }
    .desc { width: 40%; }
    .desc textarea { width: 100%; min-height: 48px; padding: 6px; background: #3c3c3c; color: #d4d4d4; border: 1px solid #555; border-radius: 4px; resize: vertical; }
    .act { white-space: nowrap; }
    .act button { margin-right: 6px; padding: 4px 10px; cursor: pointer; border: none; border-radius: 4px; font-size: 12px; }
    .btn-save { background: #0e639c; color: #fff; }
    .btn-rename { background: #5a5a5a; color: #fff; }
    .btn-del { background: #a1260d; color: #fff; }
    .ports { margin-bottom: 16px; padding: 12px; background: #252526; border-radius: 8px; }
    .ports h2 { font-size: 1rem; margin: 0 0 10px; }
    .port-card { border: 1px solid #333; border-radius: 6px; margin-bottom: 8px; overflow: hidden; }
    .port-card summary { cursor: pointer; padding: 10px 12px; background: #2d2d30; font-weight: 600; }
    .port-body { padding: 12px; }
    .port-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-bottom: 10px; }
    .port-grid label { display: block; font-size: 12px; color: #aaa; margin-bottom: 4px; }
    .port-grid input { width: 100%; padding: 6px; background: #3c3c3c; color: #d4d4d4; border: 1px solid #555; border-radius: 4px; }
    .workflow-checks { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 6px 12px; max-height: 220px; overflow: auto; padding: 8px; background: #1e1e1e; border: 1px solid #333; border-radius: 6px; }
    .workflow-checks label, .all-workflows { display: flex; align-items: center; gap: 6px; font-size: 13px; }
    .workflow-file { color: #888; font-size: 11px; margin-left: 4px; }
    .ports-actions { display: flex; gap: 8px; margin-top: 10px; }
    .ports-actions button { padding: 6px 12px; cursor: pointer; border: none; border-radius: 4px; }
    .btn-secondary { background: #5a5a5a; color: #fff; }
    .msg { margin-top: 8px; padding: 8px; border-radius: 4px; }
    .msg.ok { background: #1e3a1e; color: #8bc34a; }
    .msg.err { background: #3a1e1e; color: #f44336; }
  </style>
</head>
<body>
  <h1>ComfyUI 工作流管理</h1>
  <section class="ports">
    <h2>ComfyUI 来源配置</h2>
    <div id="portsList"></div>
    <div class="ports-actions">
      <button type="button" id="savePortsBtn" class="btn-save">保存来源配置</button>
      <button type="button" id="refreshPortsBtn" class="btn-secondary">刷新工作流列表</button>
    </div>
  </section>
  <div class="upload">
    <input type="file" id="fileInput" accept=".json">
    <button type="button" id="uploadBtn">上传 .json</button>
    <span style="margin-left: 1rem;"></span>
    <button type="button" id="clearCacheBtn" class="btn-del" title="删除 data/agent/comfyui/input 与插件 tmp 下的图片/视频缓存，释放空间">清理本地缓存</button>
    <span style="margin-left: 1rem;"></span>
    <button type="button" id="clearHistoryBtn" class="btn-del" title="清理历史生成记录，释放内存">清理历史记录</button>
    <input type="number" id="historyHours" value="48" min="0" style="width: 60px; padding: 4px; background: #3c3c3c; color: #d4d4d4; border: 1px solid #555; border-radius: 4px;" title="保留最近几小时的记录（0表示全部清理）"> 小时
  </div>
  <div id="msg"></div>
  <table>
    <thead><tr><th>文件名</th><th class="desc">说明（供 LLM 选择工作流）</th><th class="act">操作</th></tr></thead>
    <tbody id="list"></tbody>
  </table>
  <script>
    const msg = (text, ok) => {
      const el = document.getElementById('msg');
      el.textContent = text;
      el.className = 'msg ' + (ok ? 'ok' : 'err');
    };
    const api = async (path, body) => {
      const res = await fetch(path, body ? { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {});
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || res.statusText);
      return data;
    };
    let workflowOptions = [];
    let portState = [];
    const escapeHtml = (value) => String(value || '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    const isAllWorkflows = (port) => !port.workflows || port.workflows.length === 0;
    const renderPorts = () => {
      const root = document.getElementById('portsList');
      root.innerHTML = portState.map((port, idx) => {
        const allChecked = isAllWorkflows(port);
        const checks = workflowOptions.map(w => {
          const checked = !allChecked && (port.workflows || []).includes(w.name);
          return `
            <label>
              <input type="checkbox" class="workflow-check" data-port="${idx}" value="${escapeHtml(w.name)}" ${checked ? 'checked' : ''} ${allChecked ? 'disabled' : ''}>
              <span>${escapeHtml(w.name)}</span>
              <span class="workflow-file">${escapeHtml(w.filename)}</span>
            </label>
          `;
        }).join('') || '<div style="color:#888;">暂无工作流，请先上传 JSON。</div>';
        const title = port.name || `来源 ${idx + 1}`;
        return `
          <details class="port-card" ${idx === 0 ? 'open' : ''}>
            <summary>${escapeHtml(title)}${port.http ? ` - ${escapeHtml(port.http)}` : ''}</summary>
            <div class="port-body" data-port="${idx}">
              <div class="port-grid">
                <div>
                  <label>名称</label>
                  <input class="port-name" value="${escapeHtml(port.name || '')}" placeholder="例如：高性能机">
                </div>
                <div>
                  <label>HTTP 地址</label>
                  <input class="port-http" value="${escapeHtml(port.http || '')}" placeholder="例如：http://127.0.0.1:8188">
                </div>
              </div>
              <label class="all-workflows">
                <input type="checkbox" class="all-workflows-check" data-port="${idx}" ${allChecked ? 'checked' : ''}>
                允许全部工作流
              </label>
              <div class="workflow-checks">${checks}</div>
            </div>
          </details>
        `;
      }).join('');
      root.querySelectorAll('.all-workflows-check').forEach(input => {
        input.onchange = () => {
          const idx = Number(input.dataset.port);
          portState[idx].workflows = input.checked ? [] : workflowOptions.map(w => w.name);
          renderPorts();
        };
      });
    };
    const collectPorts = () => {
      const cards = document.querySelectorAll('.port-body');
      return Array.from(cards).map(card => {
        const workflows = Array.from(card.querySelectorAll('.workflow-check:checked')).map(input => input.value);
        const allChecked = card.querySelector('.all-workflows-check')?.checked;
        return {
          name: card.querySelector('.port-name')?.value.trim() || '',
          http: card.querySelector('.port-http')?.value.trim() || '',
          workflows: allChecked ? [] : workflows,
        };
      });
    };
    const loadPorts = async () => {
      const data = await api('/api/ports');
      workflowOptions = data.workflows || [];
      portState = data.ports || [];
      while (portState.length < 4) portState.push({ name: '', http: '', workflows: [] });
      renderPorts();
    };
    const savePorts = async () => {
      try {
        portState = collectPorts();
        await api('/api/ports', { ports: portState });
        msg('来源配置已保存', true);
        await loadPorts();
      } catch (e) { msg(e.message, false); }
    };
    const loadList = async () => {
      const { files } = await api('/api/list');
      const tbody = document.getElementById('list');
      tbody.innerHTML = files.map(f => `
        <tr data-filename="${f.filename.replace(/"/g, '&quot;')}">
          <td>${f.filename}</td>
          <td class="desc"><textarea rows="2" placeholder="简要说明（50字内），供 LLM 选择" maxlength="50">${(f.description?.short || '').replace(/</g, '&lt;')}</textarea></td>
          <td class="desc-detailed"><textarea rows="4" placeholder="详细说明，不限制字数，供 comfyui_get_workflow_detail 查询">${(f.description?.detailed || '').replace(/</g, '&lt;')}</textarea></td>
          <td class="act">
            <button class="btn-save">保存说明</button>
            <button class="btn-rename">重命名</button>
            <button class="btn-del">删除</button>
          </td>
        </tr>
      `).join('');
      tbody.querySelectorAll('.btn-save').forEach(btn => {
        btn.onclick = async () => {
          const row = btn.closest('tr');
          const filename = row.dataset.filename;
          const description = row.querySelector('td.desc textarea').value;
          const detailed = row.querySelector('td.desc-detailed textarea').value;
          try { 
            await api('/api/description', { filename, description }); 
            await api('/api/description_detailed', { filename, description: detailed }); 
            msg('已保存说明', true); 
          } catch (e) { msg(e.message, false); }
        };
      });
      tbody.querySelectorAll('.btn-rename').forEach(btn => {
        btn.onclick = async () => {
          const row = btn.closest('tr');
          const old_name = row.dataset.filename;
          const new_name = prompt('新文件名（.json 结尾）', old_name);
          if (!new_name || new_name === old_name) return;
          try { await api('/api/rename', { old_name, new_name }); msg('已重命名', true); loadList(); loadPorts(); } catch (e) { msg(e.message, false); }
        };
      });
      tbody.querySelectorAll('.btn-del').forEach(btn => {
        btn.onclick = async () => {
          if (!confirm('确定删除该工作流文件？')) return;
          const row = btn.closest('tr');
          const filename = row.dataset.filename;
          try { await api('/api/delete', { filename }); msg('已删除', true); loadList(); loadPorts(); } catch (e) { msg(e.message, false); }
        };
      });
    };
    document.getElementById('uploadBtn').onclick = async () => {
      const input = document.getElementById('fileInput');
      if (!input.files.length) { msg('请选择文件', false); return; }
      const form = new FormData();
      form.append('file', input.files[0]);
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: form });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) { msg(data.error || res.statusText || '上传失败', false); return; }
        msg('上传成功', true);
        input.value = '';
        loadList();
        loadPorts();
      } catch (e) { msg(e.message, false); }
    };
    document.getElementById('clearCacheBtn').onclick = async () => {
      if (!confirm('确定清理本地缓存？将删除 data/agent/comfyui/input 与插件 tmp 下的所有文件，释放磁盘空间。')) return;
      try {
        const data = await api('/api/clear_cache', {});
        msg('已删除 ' + (data.deleted || 0) + ' 个缓存文件', true);
      } catch (e) { msg(e.message, false); }
    };
    document.getElementById('clearHistoryBtn').onclick = async () => {
      const hours = prompt('请输入要保留的小时数（删除该时间之前的记录）：', '48');
      if (!hours) return;
      try {
        const data = await api('/api/clear_history', { hours: parseInt(hours) });
        msg(data.message || '历史记录已清理', true);
      } catch (e) { msg(e.message, false); }
    };
    document.getElementById('savePortsBtn').onclick = savePorts;
    document.getElementById('refreshPortsBtn').onclick = async () => {
      try {
        await loadPorts();
        msg('工作流列表已刷新', true);
      } catch (e) { msg(e.message, false); }
    };
    loadPorts();
    loadList();
  </script>
</body>
</html>
"""


class ManagementServer:
    """
    工作流管理页服务器，支持 async start/stop，与 AstrBot 事件循环协同。
    参考 astrbot_plugin_stealer 的 WebServer 模式。
    """

    def __init__(
        self,
        workflows_dir: Path,
        meta_path: Path,
        load_meta: Callable[[], Dict[str, str]],
        save_meta: Callable[[Dict[str, str]], None],
        plugin_data_dir: Optional[Path] = None,
        cleanup_history_func: Optional[Callable[[], int]] = None,
        ports_config_path: Optional[Path] = None,
        active_port_state_path: Optional[Path] = None,
        load_ports_func: Optional[Callable[[], List[Dict[str, object]]]] = None,
        save_ports_func: Optional[Callable[[List[Dict[str, object]]], None]] = None,
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
        """启动 Web 服务器。返回是否启动成功。"""
        try:
            self._runner = web.AppRunner(self.app, access_log=None)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, str(host).strip(), int(port))
            await self._site.start()
            self._started = True
            return True
        except OSError as e:
            if "Address already in use" in str(e) or getattr(e, "errno", None) in (98, 10048):
                raise RuntimeError(f"端口 {port} 已被占用，请更换端口或关闭占用程序") from e
            raise
        except Exception:
            raise

    async def stop(self) -> None:
        """停止 Web 服务器。"""
        if not self._started:
            return
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._site = None
        self._runner = None
        self._started = False
