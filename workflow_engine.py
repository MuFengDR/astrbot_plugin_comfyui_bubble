# -*- coding: utf-8 -*-
"""
ComfyUI 工作流解析与执行引擎。
复用 nonebot_plugin_novelai 的工作流识别模式，使用 httpx 异步请求。
"""
import asyncio
import json
import random
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from astrbot.api import logger


ASTR_BUBBLE_INPUT_NODES = {
    "AstrBubble_TextInput": ("text", "text"),
    "AstrBubble_ImageInput": ("image", "image_base64"),
    "AstrBubble_VideoInput": ("video", "video"),
}
ASTR_BUBBLE_OUTPUT_NODES = {
    "AstrBubble_TextOutput": ("text", None),
    "AstrBubble_ImageOutput": ("image", None),
    "AstrBubble_VideoOutput": ("video", None),
    "AstrBubble_VideoCombine": ("video", None),
}
ASTR_BUBBLE_NODE_CLASSES = set(ASTR_BUBBLE_INPUT_NODES) | set(ASTR_BUBBLE_OUTPUT_NODES)
SLOT_KIND_LABELS = {"text": "文本", "image": "图片", "video": "视频"}
HIDDEN_TITLE_PREFIX = "[hide]"


def _empty_rule(limit: Optional[int] = None) -> Dict[str, Any]:
    return {"limit": limit, "mode": "strict"}


def _empty_auto_params(name: str = "") -> Dict[str, Any]:
    return {
        "name": name,
        "inputs": {
            "text": _empty_rule(0),
            "image": _empty_rule(0),
            "video": _empty_rule(0),
        },
        "outputs": {
            "text": _empty_rule(0),
            "image": _empty_rule(0),
            "video": _empty_rule(0),
        },
        "allow_other_outputs": False,
        "slots": [],
        "inspection": {"ok": False, "error": "未识别到 AstrBubble 专属输入/输出节点。"},
    }


def _iter_workflow_nodes(workflow_data: Any):
    if isinstance(workflow_data, dict):
        api_nodes = [
            (str(node_id), node)
            for node_id, node in workflow_data.items()
            if isinstance(node, dict) and "class_type" in node
        ]
        if api_nodes:
            yield from api_nodes
            return
        raw_nodes = workflow_data.get("nodes")
        if isinstance(raw_nodes, list):
            for index, node in enumerate(raw_nodes):
                if not isinstance(node, dict):
                    continue
                node_id = node.get("id", index)
                yield str(node_id), node


def _slot_index(value: Any) -> Optional[int]:
    try:
        index = int(value)
    except (TypeError, ValueError):
        return None
    return index if index > 0 else None


def _validate_slot_indexes(slots: List[Dict[str, Any]]) -> List[str]:
    errors: List[str] = []
    groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for slot in slots:
        if slot.get("hidden"):
            continue
        groups.setdefault((slot["direction"], slot["kind"]), []).append(slot)
    for (direction, kind), items in groups.items():
        indexes = [item["index"] for item in items]
        duplicates = sorted({idx for idx in indexes if indexes.count(idx) > 1})
        if duplicates:
            errors.append(f"{direction}/{kind} index 重复：{duplicates}")
            continue
        expected = list(range(1, len(indexes) + 1))
        actual = sorted(indexes)
        if actual != expected:
            errors.append(f"{direction}/{kind} index 必须从 1 连续编号，当前为 {actual}")
    return errors


def _slot_default_value(inputs: Dict[str, Any], kind: str, input_key: Optional[str]) -> str:
    if kind == "text":
        return str(inputs.get("text") or "")
    if kind == "image":
        return str(inputs.get("image_base64") or "")
    if kind == "video":
        return str(inputs.get("video") or "")
    if input_key:
        return str(inputs.get(input_key) or "")
    return ""


def _node_title(node: Dict[str, Any], node_id: str, class_type: str) -> str:
    meta = node.get("_meta") if isinstance(node.get("_meta"), dict) else {}
    title = str(meta.get("title") or node.get("title") or "").strip()
    return title or f"{class_type} #{node_id}"


def _is_hidden_title(title: str) -> bool:
    return str(title or "").strip().lower().startswith(HIDDEN_TITLE_PREFIX)


def _media_command_incompatibilities(slots: List[Dict[str, Any]]) -> List[str]:
    warnings: List[str] = []
    for kind in ("image", "video"):
        media_slots = sorted(
            [
                slot
                for slot in slots
                if slot.get("direction") == "input" and slot.get("kind") == kind
                and not slot.get("hidden")
            ],
            key=lambda slot: int(slot.get("index") or 0),
        )
        seen_optional = False
        for slot in media_slots:
            if slot.get("optional"):
                seen_optional = True
            elif seen_optional:
                warnings.append(
                    f"{SLOT_KIND_LABELS.get(kind, kind)}输入存在可缺省项位于必填项之前，/comfyui 命令无法可靠跳过媒体槽位。"
                )
                break
    return warnings


def inspect_workflow_slots(workflow_data: Any, name: str = "") -> Dict[str, Any]:
    """Read AstrBubble node slots from a ComfyUI API workflow."""
    slots: List[Dict[str, Any]] = []
    errors: List[str] = []
    for node_id, node in _iter_workflow_nodes(workflow_data):
        class_type = str(node.get("class_type") or "")
        if class_type not in ASTR_BUBBLE_NODE_CLASSES:
            continue
        inputs = node.get("inputs") if isinstance(node.get("inputs"), dict) else {}
        index = _slot_index(inputs.get("index"))
        if index is None:
            errors.append(f"节点 {node_id}({class_type}) 缺少正整数 index")
            continue
        explain = str(inputs.get("explain") or "").strip()
        if class_type in ASTR_BUBBLE_INPUT_NODES:
            kind, input_key = ASTR_BUBBLE_INPUT_NODES[class_type]
            direction = "input"
        else:
            kind, input_key = ASTR_BUBBLE_OUTPUT_NODES[class_type]
            direction = "output"
        title = _node_title(node, node_id, class_type)
        slots.append(
            {
                "direction": direction,
                "kind": kind,
                "index": index,
                "node_id": node_id,
                "class_type": class_type,
                "title": title,
                "hidden": _is_hidden_title(title),
                "explain": explain,
                "input_key": input_key,
                "optional": bool(inputs.get("optional", False)),
                "enabled": bool(inputs.get("enabled", True)) if direction == "output" else True,
                "default": _slot_default_value(inputs, kind, input_key) if direction == "input" else "",
            }
        )
    errors.extend(_validate_slot_indexes(slots))
    command_warnings = _media_command_incompatibilities(slots)
    params = _empty_auto_params(name)
    params["slots"] = sorted(
        slots,
        key=lambda item: (
            item["direction"],
            item["kind"],
            bool(item.get("hidden")),
            item["index"],
            item["node_id"],
        ),
    )
    for direction, group_name in (("input", "inputs"), ("output", "outputs")):
        for kind in ("text", "image", "video"):
            count = len(
                [
                    slot
                    for slot in slots
                    if slot["direction"] == direction and slot["kind"] == kind
                    and not slot.get("hidden")
                ]
            )
            params[group_name][kind] = _empty_rule(count)
    if not slots:
        errors.append("未识别到 AstrBubble 专属输入/输出节点。")
    params["inspection"] = {
        "ok": not errors,
        "error": "；".join(errors),
        "command_compatible": not command_warnings,
        "command_warning": "；".join(command_warnings),
    }
    return params


def apply_workflow_slot_edits(workflow_data: Any, slot_edits: List[Dict[str, Any]]) -> Any:
    """Apply safe AstrBubble slot edits to a ComfyUI workflow JSON object."""
    if not slot_edits:
        return workflow_data
    edits_by_node = {
        str(edit.get("node_id") or ""): edit
        for edit in slot_edits
        if isinstance(edit, dict) and str(edit.get("node_id") or "")
    }
    if not edits_by_node:
        return workflow_data
    for node_id, node in _iter_workflow_nodes(workflow_data):
        edit = edits_by_node.get(str(node_id))
        if not edit or not isinstance(node, dict):
            continue
        class_type = str(node.get("class_type") or "")
        if class_type in ASTR_BUBBLE_INPUT_NODES:
            direction = "input"
            kind, input_key = ASTR_BUBBLE_INPUT_NODES[class_type]
        elif class_type in ASTR_BUBBLE_OUTPUT_NODES:
            direction = "output"
            kind, input_key = ASTR_BUBBLE_OUTPUT_NODES[class_type]
        else:
            continue
        inputs = node.get("inputs")
        if not isinstance(inputs, dict):
            inputs = {}
            node["inputs"] = inputs
        if "index" in edit:
            index = _slot_index(edit.get("index"))
            if index is None:
                raise ValueError(f"节点 {node_id}({class_type}) index 必须是正整数")
            inputs["index"] = index
        if "explain" in edit:
            inputs["explain"] = str(edit.get("explain") or "").strip()
            inputs.pop("label", None)
        inputs["optional"] = bool(edit.get("optional", False))
        if direction == "output":
            inputs["enabled"] = bool(edit.get("enabled", True))
            continue
        if kind == "text" and "default" in edit:
            inputs["text"] = str(edit.get("default") or "")
        elif kind == "image" and "default" in edit:
            inputs["image_base64"] = str(edit.get("default") or "")
        elif kind == "video" and "default" in edit:
            inputs["video"] = str(edit.get("default") or "")
        elif input_key and "default" in edit and kind not in ("image", "video"):
            inputs[input_key] = str(edit.get("default") or "")
    inspected = inspect_workflow_slots(workflow_data)
    if not inspected.get("inspection", {}).get("ok", False):
        raise ValueError(inspected.get("inspection", {}).get("error") or "输入槽位配置无效")
    return workflow_data


def inspect_workflow_file(filepath: Path, name: str = "") -> Dict[str, Any]:
    try:
        data = json.loads(filepath.read_text(encoding="utf-8"))
    except Exception as e:
        params = _empty_auto_params(name)
        params["inspection"] = {"ok": False, "error": f"工作流 JSON 读取失败：{e}"}
        return params
    return inspect_workflow_slots(data, name)


def apply_workflow_slot_edits_file(filepath: Path, slot_edits: List[Dict[str, Any]]) -> None:
    data = json.loads(filepath.read_text(encoding="utf-8"))
    apply_workflow_slot_edits(data, slot_edits)
    filepath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_workflow_filename(filename: str) -> Optional[Dict[str, Any]]:
    """
    解析工作流文件名，仅提取默认工作流名称。
    输入/输出参数由 WebUI 的 workflow_params 配置，不再从文件名解析。
    """
    if not filename.endswith(".json"):
        return None
    return _build_workflow_info(filename, None)


def _normalize_limit(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, number)


def _normalize_mode(value: Any) -> str:
    return "strict" if value == "strict" else "loose"


def _normalize_rule(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    return {
        "limit": _normalize_limit(raw.get("limit")),
        "mode": _normalize_mode(raw.get("mode")),
    }


def _normalize_workflow_params(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    inputs = raw.get("inputs") if isinstance(raw.get("inputs"), dict) else {}
    outputs = raw.get("outputs") if isinstance(raw.get("outputs"), dict) else {}
    normalized = {
        "name": str(raw.get("name") or "").strip(),
        "inputs": {
            "text": _normalize_rule(inputs.get("text")),
            "image": _normalize_rule(inputs.get("image")),
            "video": _normalize_rule(inputs.get("video")),
        },
        "outputs": {
            "text": _normalize_rule(outputs.get("text")),
            "image": _normalize_rule(outputs.get("image")),
            "video": _normalize_rule(outputs.get("video")),
        },
        "allow_other_outputs": bool(raw.get("allow_other_outputs", False)),
    }
    normalized["slots"] = raw.get("slots") if isinstance(raw.get("slots"), list) else []
    inspection = raw.get("inspection") if isinstance(raw.get("inspection"), dict) else {}
    normalized["inspection"] = {
        "ok": bool(inspection.get("ok", True)),
        "error": str(inspection.get("error") or "").strip(),
        "command_compatible": bool(inspection.get("command_compatible", True)),
        "command_warning": str(inspection.get("command_warning") or "").strip(),
    }
    return normalized


def _build_workflow_info(filename: str, params: Any = None) -> Dict[str, Any]:
    normalized = _normalize_workflow_params(params)
    name = normalized.get("name") or Path(filename).stem
    return {
        "name": name,
        "texts": normalized["inputs"]["text"]["limit"],
        "images": normalized["inputs"]["image"]["limit"],
        "videos": normalized["inputs"]["video"]["limit"],
        "output_texts": normalized["outputs"]["text"]["limit"],
        "output_images": normalized["outputs"]["image"]["limit"],
        "output_videos": normalized["outputs"]["video"]["limit"],
        "filename": filename,
        "params": normalized,
    }


def _input_rule_matches(count: int, rule: Dict[str, Any]) -> bool:
    limit = rule.get("limit")
    if limit is None:
        return True
    if rule.get("mode") == "strict":
        return count == limit
    return True


def _input_slots_for_kind(info: Dict[str, Any], kind: str) -> List[Dict[str, Any]]:
    params = info.get("params") if isinstance(info.get("params"), dict) else {}
    slots = params.get("slots") if isinstance(params.get("slots"), list) else []
    return sorted(
        [
            slot
            for slot in slots
            if isinstance(slot, dict)
            and slot.get("direction") == "input"
            and slot.get("kind") == kind
            and not slot.get("hidden")
        ],
        key=lambda slot: int(slot.get("index") or 0),
    )


def _slot_values_match(values: List[Any], slots: List[Dict[str, Any]]) -> bool:
    if len(values) > len(slots):
        return False
    for idx, slot in enumerate(slots):
        provided = idx < len(values) and str(values[idx] or "").strip() != ""
        if not provided and not bool(slot.get("optional", False)):
            return False
    return True


def _input_match_score(count: int, rule: Dict[str, Any]) -> int:
    limit = rule.get("limit")
    if limit is None:
        return 0
    return abs(count - limit)


def _params_for_workflow_file(
    filepath: Path, workflow_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    saved = (workflow_params or {}).get(filepath.name)
    saved_name = ""
    if isinstance(saved, dict):
        saved_name = str(saved.get("name") or "").strip()
    scanned = inspect_workflow_file(filepath, saved_name)
    if isinstance(saved, dict) and saved_name:
        scanned["name"] = saved_name
    if isinstance(saved, dict) and "allow_other_outputs" in saved:
        scanned["allow_other_outputs"] = bool(saved.get("allow_other_outputs"))
    return scanned


def list_workflows_in_dir(workflow_dir: Path, workflow_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """扫描指定目录下的 .json 工作流，返回可解析的工作流信息列表。"""
    workflows = []
    if not workflow_dir.exists():
        return workflows
    for f in workflow_dir.glob("*.json"):
        workflows.append(_build_workflow_info(f.name, _params_for_workflow_file(f, workflow_params)))
    return workflows


def find_workflow_file(
    workflow_name: str,
    text_count: int,
    image_count: int,
    video_count: int,
    workflow_dir: Path,
    workflow_params: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """根据工作流名称和参数数量在指定目录中查找最佳匹配的工作流文件路径。"""
    if not workflow_dir.exists():
        return None
    candidates = []
    for f in workflow_dir.glob("*.json"):
        info = _build_workflow_info(f.name, _params_for_workflow_file(f, workflow_params))
        if not info or info["name"] != workflow_name:
            continue
        params = info.get("params") or {}
        inspection = params.get("inspection") if isinstance(params.get("inspection"), dict) else {}
        if not inspection.get("ok", True):
            continue
        inputs = params.get("inputs") or {}
        text_slots = _input_slots_for_kind(info, "text")
        image_slots = _input_slots_for_kind(info, "image")
        video_slots = _input_slots_for_kind(info, "video")
        if (
            (text_slots and _slot_values_match(["x"] * text_count, text_slots) or not text_slots and _input_rule_matches(text_count, inputs.get("text", {})))
            and (image_slots and _slot_values_match(["x"] * image_count, image_slots) or not image_slots and _input_rule_matches(image_count, inputs.get("image", {})))
            and (video_slots and _slot_values_match(["x"] * video_count, video_slots) or not video_slots and _input_rule_matches(video_count, inputs.get("video", {})))
        ):
            score = (
                _input_match_score(text_count, inputs.get("text", {}))
                + _input_match_score(image_count, inputs.get("image", {}))
                + _input_match_score(video_count, inputs.get("video", {}))
            )
            candidates.append({"file": str(f), "score": score})
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["score"])
    return candidates[0]["file"]


class ComfyUIWorkflow:
    """异步执行 ComfyUI 工作流（使用 httpx）。"""

    def __init__(self, server_ip: str, client_id: str):
        raw_server_ip = (server_ip or "127.0.0.1:8188").strip().rstrip("/")
        if raw_server_ip.startswith(("http://", "https://")):
            self._base = raw_server_ip
            self.server_ip = raw_server_ip.replace("http://", "", 1).replace("https://", "", 1)
        else:
            self.server_ip = raw_server_ip
            self._base = f"http://{self.server_ip}"
        self.client_id = client_id
        self._queue: deque = deque()
        self._processing = False

    def load_workflow_api(self, filepath: str) -> None:
        with open(filepath, "r", encoding="utf-8") as f:
            self.workflow_api = json.load(f)

    async def enqueue_workflow(
        self,
        base64_images: Optional[List[str]] = None,
        texts: Optional[List[str]] = None,
        videos: Optional[List[str]] = None,
        extract_text: bool = False,
    ) -> Tuple[Optional[str], str, List[str]]:
        """将任务加入队列并等待执行完成，返回 (文件URL, 文件类型, 文本输出列表)。"""
        base64_images = base64_images or []
        texts = texts or []
        videos = videos or []
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._queue.append((base64_images, texts, videos, extract_text, future))
        if not self._processing:
            asyncio.create_task(self._process_queue())
        return await future

    async def _process_queue(self) -> None:
        """单消费者：整段循环期间持锁，避免并发执行导致任务丢失/串线。"""
        self._processing = True
        try:
            while self._queue:
                base64_images, texts, videos, extract_text, future = self._queue.popleft()
                try:
                    result = await self._run_workflow(base64_images, texts, videos, extract_text)
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
        finally:
            self._processing = False

    def _replace_base64_images(self, data: Any, base64_images: List[str]) -> Tuple[Any, int]:
        """Replace AstrBubble image input nodes by their explicit index."""
        replaced = {"count": 0}
        inspected = inspect_workflow_slots(data)
        slots = _input_slots_for_kind({"params": inspected}, "image")

        def replace(d: Any) -> Any:
            if isinstance(d, dict):
                new_data = dict(d)
                inputs_modified = False
                if new_data.get("class_type") == "AstrBubble_ImageInput" and not _is_hidden_title(
                    _node_title(new_data, "", "AstrBubble_ImageInput")
                ):
                    inputs = new_data.get("inputs") if isinstance(new_data.get("inputs"), dict) else {}
                    index = _slot_index(inputs.get("index"))
                    if index is not None and index <= len(base64_images):
                        value = str(base64_images[index - 1] or "")
                        if not value.strip():
                            slot = next((s for s in slots if s.get("index") == index), {})
                            if slot.get("optional"):
                                return new_data
                            raise ValueError(f"缺少必填输入：[图片{index}] {inputs.get('explain') or ''}".strip())
                        new_data["inputs"] = dict(inputs)
                        new_data["inputs"]["image_base64"] = value
                        replaced["count"] += 1
                        inputs_modified = True
                    elif index is not None:
                        slot = next((s for s in slots if s.get("index") == index), {})
                        if not slot.get("optional"):
                            raise ValueError(f"缺少必填输入：[图片{index}] {inputs.get('explain') or ''}".strip())
                for k, v in d.items():
                    if k == "inputs" and inputs_modified:
                        continue
                    new_data[k] = replace(v)
                return new_data
            if isinstance(d, list):
                return [replace(x) for x in d]
            return d

        return replace(data), replaced["count"]

    def _replace_video_nodes(self, data: Any, video_filenames: List[str]) -> Tuple[Any, int]:
        replaced = {"count": 0}
        inspected = inspect_workflow_slots(data)
        slots = _input_slots_for_kind({"params": inspected}, "video")

        def replace(d: Any) -> Any:
            if isinstance(d, dict):
                new_data = dict(d)
                inputs_modified = False
                if new_data.get("class_type") == "AstrBubble_VideoInput" and not _is_hidden_title(
                    _node_title(new_data, "", "AstrBubble_VideoInput")
                ):
                    inputs = new_data.get("inputs") if isinstance(new_data.get("inputs"), dict) else {}
                    index = _slot_index(inputs.get("index"))
                    if index is not None and index <= len(video_filenames):
                        value = str(video_filenames[index - 1] or "")
                        if not value.strip():
                            slot = next((s for s in slots if s.get("index") == index), {})
                            if slot.get("optional"):
                                return new_data
                            raise ValueError(f"缺少必填输入：[视频{index}] {inputs.get('explain') or ''}".strip())
                        new_data["inputs"] = dict(inputs)
                        new_data["inputs"]["video"] = value
                        replaced["count"] += 1
                        inputs_modified = True
                    elif index is not None:
                        slot = next((s for s in slots if s.get("index") == index), {})
                        if not slot.get("optional"):
                            raise ValueError(f"缺少必填输入：[视频{index}] {inputs.get('explain') or ''}".strip())
                for k, v in d.items():
                    if k == "inputs" and inputs_modified:
                        continue
                    new_data[k] = replace(v)
                return new_data
            if isinstance(d, list):
                return [replace(x) for x in d]
            return d

        return replace(data), replaced["count"]

    def _count_text_nodes(self, data: Any) -> int:
        """Count AstrBubble text input nodes."""
        count = 0

        def walk(d: Any) -> None:
            nonlocal count
            if isinstance(d, dict):
                if (
                    d.get("class_type") == "AstrBubble_TextInput"
                    and isinstance(d.get("inputs"), dict)
                    and not _is_hidden_title(_node_title(d, "", "AstrBubble_TextInput"))
                ):
                    count += 1
                for v in d.values():
                    walk(v)
            elif isinstance(d, list):
                for x in d:
                    walk(x)

        walk(data)
        return count

    def _smart_merge_texts(self, texts: List[str], slots: int) -> List[str]:
        if not texts or slots <= 0:
            return []
        if slots >= len(texts):
            return texts
        if slots == 1:
            return [" ".join(texts)]
        result = texts[: slots - 1]
        result.append(" ".join(texts[slots - 1 :]))
        return result

    def _update_text_nodes(self, data: Any, texts: List[str]) -> Tuple[Any, int]:
        """Replace AstrBubble text input nodes by their explicit index."""
        replaced = {"count": 0}
        inspected = inspect_workflow_slots(data)
        slots = _input_slots_for_kind({"params": inspected}, "text")

        def replace(d: Any) -> Any:
            if isinstance(d, dict):
                new_data = dict(d)
                inputs_modified = False
                if new_data.get("class_type") == "AstrBubble_TextInput" and not _is_hidden_title(
                    _node_title(new_data, "", "AstrBubble_TextInput")
                ):
                    inputs = new_data.get("inputs") if isinstance(new_data.get("inputs"), dict) else {}
                    index = _slot_index(inputs.get("index"))
                    if index is not None and index <= len(texts):
                        value = str(texts[index - 1] or "")
                        if not value.strip():
                            slot = next((s for s in slots if s.get("index") == index), {})
                            if slot.get("optional"):
                                return new_data
                            raise ValueError(f"缺少必填输入：[文本{index}] {inputs.get('explain') or ''}".strip())
                        new_data["inputs"] = dict(inputs)
                        new_data["inputs"]["text"] = value
                        replaced["count"] += 1
                        inputs_modified = True
                    elif index is not None:
                        slot = next((s for s in slots if s.get("index") == index), {})
                        if not slot.get("optional"):
                            raise ValueError(f"缺少必填输入：[文本{index}] {inputs.get('explain') or ''}".strip())
                for k, v in d.items():
                    if k == "inputs" and inputs_modified:
                        continue
                    new_data[k] = replace(v)
                return new_data
            if isinstance(d, list):
                return [replace(x) for x in d]
            return d

        result = replace(data)
        if replaced["count"] > 0 and texts:
            logger.info(
                "[ComfyUI] Replaced %d AstrBubble text input node(s) with prompt: %s",
                replaced["count"],
                texts[0][:80] + ("..." if len(texts[0]) > 80 else ""),
            )
        return result, replaced["count"]

    def _randomize_seeds(self, data: Any) -> Any:
        if isinstance(data, dict):
            return {
                k: random.randint(1, 1000000000) if k in ("seed", "noise_seed") else self._randomize_seeds(v)
                for k, v in data.items()
            }
        if isinstance(data, list):
            return [self._randomize_seeds(x) for x in data]
        return data

    def _extract_text_outputs(
        self, workflow_data: Dict, history_data: Dict, prompt_id: str
    ) -> List[str]:
        text_outputs = []
        showtext_nodes = {
            nid: nd
            for nid, nd in workflow_data.items()
            if isinstance(nd, dict) and nd.get("class_type") == "ShowText|pysssss"
        }
        if not showtext_nodes:
            return text_outputs
        history_entry = history_data.get(prompt_id) if isinstance(history_data, dict) else None
        for node_id in showtext_nodes:
            text_content = None
            if history_entry and isinstance(history_entry, dict) and "prompts" in history_entry:
                prompts = history_entry["prompts"]
                if isinstance(prompts, list):
                    for item in prompts:
                        if isinstance(item, (list, tuple)) and len(item) >= 2 and str(item[0]) == node_id:
                            inp = item[1].get("inputs", {}) if isinstance(item[1], dict) else {}
                            text_content = inp.get("text_0")
                            break
            if text_content is None:
                nd = showtext_nodes[node_id]
                text_content = (nd.get("inputs") or {}).get("text_0")
            if isinstance(text_content, str):
                if "</think>" in text_content:
                    text_content = text_content.split("</think>", 1)[1]
                t = text_content.strip()
                if t:
                    text_outputs.append(t)
        return text_outputs

    async def _run_workflow(
        self,
        base64_images: List[str],
        texts: List[str],
        videos: List[str],
        extract_text: bool,
    ) -> Tuple[Optional[str], str, List[str]]:
        workflow_api_modified = json.loads(json.dumps(self.workflow_api))
        workflow_api_modified, _ = self._replace_base64_images(workflow_api_modified, base64_images)
        workflow_api_modified, _ = self._replace_video_nodes(workflow_api_modified, videos)
        workflow_api_modified, _ = self._update_text_nodes(workflow_api_modified, texts)
        workflow_api_modified = self._randomize_seeds(workflow_api_modified)

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{self._base}/prompt",
                json={"client_id": self.client_id, "prompt": workflow_api_modified},
            )
            resp.raise_for_status()
            prompt_id = resp.json()["prompt_id"]

        return await self._wait_and_collect_result(prompt_id, workflow_api_modified, extract_text)

    async def submit_only(
        self,
        base64_images: List[str],
        texts: List[str],
        videos: List[str],
        debug: bool = False,
    ) -> str:
        """
        仅提交工作流到 ComfyUI 队列，不等待完成。返回 prompt_id。
        用于由外部（如 query_wait）控制等待策略。
        debug=True 时在终端打印完整发送给 ComfyUI 的工作流 JSON 及文本替换信息。
        """
        workflow_api_modified = json.loads(json.dumps(self.workflow_api))
        text_slots = self._count_text_nodes(workflow_api_modified)
        workflow_api_modified, img_count = self._replace_base64_images(workflow_api_modified, base64_images)
        if debug:
            logger.info("[ComfyUI Debug] Replaced %d AstrBubble image input node(s) with %d image(s)", img_count, len(base64_images))
        workflow_api_modified, _ = self._replace_video_nodes(workflow_api_modified, videos)
        workflow_api_modified, replaced = self._update_text_nodes(workflow_api_modified, texts)
        if debug:
            logger.info(
                "[ComfyUI Debug] AstrBubble text slots in workflow: %d, replaced: %d, texts passed: %s",
                text_slots,
                replaced,
                texts,
            )
        workflow_api_modified = self._randomize_seeds(workflow_api_modified)
        if debug:
            try:
                payload = json.dumps(workflow_api_modified, ensure_ascii=False, indent=2)
                logger.info("[ComfyUI Debug] Full workflow JSON sent to ComfyUI (first 50k chars):\n%s", payload[:50000])
                if len(payload) > 50000:
                    logger.info("[ComfyUI Debug] ... (truncated, total %d chars)", len(payload))
            except Exception as e:
                logger.warning("[ComfyUI Debug] Failed to dump workflow: %s", e)
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{self._base}/prompt",
                json={"client_id": self.client_id, "prompt": workflow_api_modified},
            )
            if resp.status_code >= 400:
                try:
                    body = resp.text
                    if body:
                        logger.warning("[ComfyUI] /prompt %s response body: %s", resp.status_code, body[:2000])
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()["prompt_id"]

    async def _wait_and_collect_result(
        self, prompt_id: str, workflow_api_modified: Dict, extract_text: bool
    ) -> Tuple[Optional[str], str, List[str]]:
        while True:
            async with httpx.AsyncClient(timeout=10.0) as client:
                queue_resp = await client.get(f"{self._base}/queue")
                queue_data = queue_resp.json()
            running = queue_data.get("queue_running", [])
            pending = queue_data.get("queue_pending", [])
            if not any(item[1] == prompt_id for item in running + pending):
                break
            await asyncio.sleep(1)
        async with httpx.AsyncClient(timeout=10.0) as client:
            history_resp = await client.get(f"{self._base}/history/{prompt_id}")
            image_info = history_resp.json()
        text_outputs = []
        if extract_text:
            text_outputs = self._extract_text_outputs(workflow_api_modified, image_info, prompt_id)
            text_outputs = [t.split("</think>", 1)[-1].strip() for t in text_outputs if t.strip()]
        if prompt_id not in image_info or "outputs" not in image_info[prompt_id]:
            return None, "unknown", text_outputs
        outputs = image_info[prompt_id]["outputs"]
        for key in outputs:
            out = outputs[key]
            if isinstance(out, dict) and "audio" in out:
                for audio in out["audio"]:
                    if audio.get("type") == "output":
                        fn = audio["filename"]
                        sub = audio.get("subfolder", "")
                        url = f"{self._base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{self._base}/view?filename={fn}&type=output"
                        return url, "audio", text_outputs
            if isinstance(out, dict) and "gifs" in out:
                for video in out["gifs"]:
                    if video.get("type") == "output":
                        fn = video["filename"]
                        sub = video.get("subfolder", "")
                        url = f"{self._base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{self._base}/view?filename={fn}&type=output"
                        return url, "video", text_outputs
            if isinstance(out, dict) and "images" in out:
                for img in out["images"]:
                    if img.get("type") == "output":
                        fn = img["filename"]
                        sub = img.get("subfolder", "")
                        url = f"{self._base}/view?filename={fn}&subfolder={sub}&type=output" if sub else f"{self._base}/view?filename={fn}&type=output"
                        return url, "image", text_outputs
        return None, "unknown", text_outputs
