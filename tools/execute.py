# -*- coding: utf-8 -*-
"""LLM tool for submitting ComfyUI workflow tasks."""

from pydantic import Field
from pydantic.dataclasses import dataclass

from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext

from ..core import plugin as runtime


@dataclass
class ComfyUIExecuteTool(FunctionTool[AstrAgentContext]):
    """
    执行指定的 ComfyUI 工作流。工作流名称需与 list_workflows 返回的 name 一致。
    文本参数通过 texts 传入；图片从当前会话消息中自动提取；若工作流需要图而消息无图，可传 image_urls（占位符），插件会下载并转 base64 注入。
    ⚠️ 重要：如果需要生成多张图片（如 N 张），必须调用本工具 N 次（每次生成一张），所有任务会并行执行。
    每次调用会返回一个 task_id，之后用 comfyui_query_wait（传入 session_tag）批量查询所有任务的结果。
    """

    name: str = "comfyui_execute"
    description: str = "执行 ComfyUI 工作流（生成多张图需多次调用）。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "workflow_name": {
                    "type": "string",
                    "description": "Exact workflow name (e.g. from comfyui_list_workflows).",
                },
                "texts": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Text inputs for the workflow. Content must follow the workflow description from comfyui_list_workflows.",
                },
                "videos": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of video filenames (.mp4) on server for video workflows.",
                },
                "image_urls": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Image source(s) when message has none. Prefer HTTP URL or local path (plugin data dir or data/agent/comfyui/input). Do not paste raw base64.",
                },
                "session_tag": {
                    "type": "string",
                    "description": "REQUIRED. The sender's QQ number (the person who sent the command). This is used to track all tasks for this user. Example: '123456789'. Do not use your own QQ number, use the sender's QQ number.",
                },
            },
            "required": ["workflow_name", "session_tag"],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        # 日志脱敏：不输出 base64，避免进入 LLM 或日志留存
        runtime.logger.info(
            "[ComfyUI Tool] comfyui_execute called: workflow_name=%r, texts=%r, videos=%r, image_urls=%s",
            kwargs.get("workflow_name"),
            kwargs.get("texts"),
            kwargs.get("videos"),
            runtime._sanitize_image_urls_for_log(kwargs.get("image_urls")),
        )
        workflow_name = (kwargs.get("workflow_name") or "").strip()
        texts = kwargs.get("texts") or []
        videos = list(kwargs.get("videos") or [])
        image_urls_arg = kwargs.get("image_urls") or []
        # 自动获取发送者的 QQ 号作为 session_tag
        sender_id = runtime._get_sender_id_from_context(context.context)
        session_tag = (kwargs.get("session_tag") or "").strip()
        if not session_tag and sender_id:
            session_tag = sender_id
            runtime.logger.info("[ComfyUI Tool] Auto-filled session_tag with sender_id: %s", session_tag)
        if isinstance(image_urls_arg, str):
            image_urls_arg = [image_urls_arg]
        image_urls_arg = [u for u in image_urls_arg if u and isinstance(u, str)]
        if not workflow_name:
            return "缺少工作流名称。"
        if not session_tag:
            return "无法识别发送者标识，无法登记 ComfyUI 任务。"
        config = runtime._plugin_config
        if not config:
            return "插件配置不可用。"
        server_ip, client_id = runtime._get_server_config(config)
        wf_dir = runtime._get_workflow_dir()
        ctx = getattr(context.context, "context", None)
        event = getattr(ctx, "event", None) if ctx else None
        submit = await runtime._submit_comfyui_workflow(
            context.context,
            workflow_name,
            texts,
            videos,
            image_urls_arg,
            session_tag,
            event,
        )
        if not submit.get("ok"):
            return submit.get("message", "执行失败。")
        all_uuids = submit.get("all_task_ids", [])
        uuid_list_str = ", ".join(f'"{u}"' for u in all_uuids)
        prompt_id = submit["prompt_id"]
        return (
            f"Workflow '{workflow_name}' submitted. Task ID (prompt_id): {prompt_id}. "
            f"You have {len(all_uuids)} task(s) with session_tag '{session_tag}'. All task IDs: [{uuid_list_str}]. "
            f"IMPORTANT: You MUST immediately call comfyui_query_wait with session_tag='{session_tag}' and task_ids=['{prompt_id}'] to wait for the result. "
            "Do not reply to the user before calling comfyui_query_wait."
            + submit.get("desc_reminder", "")
        )
        images_b64 = await runtime._extract_images_from_event_async(event) if event else []
        if image_urls_arg:
            from_sources = await runtime._image_sources_to_base64(image_urls_arg)
            images_b64.extend(from_sources)
            if from_sources:
                runtime.logger.info("[ComfyUI Tool] Injected %d image(s) from image_urls placeholder (URL or local path).", len(from_sources))
        workflow_file = runtime.find_workflow_file(
            workflow_name, len(texts), len(images_b64), len(videos), wf_dir, runtime._load_workflow_params()
        )
        # 获取工作流列表供错误提示使用
        workflows = runtime._list_workflows_in_configured_dir(wf_dir)
        
        if not workflow_file:
            # 检查是否有同名工作流但参数不匹配
            matching_names = [w for w in workflows if w["name"] == workflow_name]
            
            if matching_names:
                # 同名工作流存在，检查参数需求
                required = []
                for w in matching_names:
                    required.append(f"'{w['filename']}'")
                
                if required:
                    return (
                        f"工作流 '{workflow_name}' 存在，但参数不匹配。你传了 texts={len(texts)}, images={len(images_b64)}, videos={len(videos)}。\n"
                        f"同名工作流文件：\n" + "\n".join(f"- {r}" for r in required) + "\n"
                        f"请检查工作流说明，或在管理页调整参数配置。"
                    )
            
            hint = ""
            if len(images_b64) == 0:
                hint = (
                    " Current message has no image (images=0). image_urls accepted: (1) HTTP URL—plugin will download; "
                    "(2) local path under plugin data dir or under data/agent/comfyui/input (use absolute path e.g. /path/to/AstrBot/data/agent/comfyui/input/xxx.jpg). "
                    "If you have a local file, copy it to data/agent/comfyui/input/ then pass that path in image_urls."
                )
            return (
                f"没有找到匹配的工作流「{workflow_name}」（当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}）。"
                "请使用 comfyui_list_workflows 查看可用工作流说明。"
                "可能原因：工作流名称不准确、输入数量不符合管理页配置，或 image_urls 无法读取。"
                + hint
            )
        info = runtime._get_configured_workflow_info(wf_dir, runtime.Path(workflow_file).name)
        if not info:
            return "工作流配置不可用，无法解析输入输出参数。请在工作流管理页保存该工作流的参数配置。"
        wf_filename = runtime.Path(workflow_file).name
        descriptions = await runtime._load_workflow_descriptions(config)
        workflow_desc_data = descriptions.get(wf_filename)
        if isinstance(workflow_desc_data, dict):
            workflow_desc = workflow_desc_data.get("detailed", "") or workflow_desc_data.get("short", "")
        else:
            workflow_desc = str(workflow_desc_data) if workflow_desc_data else ""
        desc_reminder = ""
        if workflow_desc:
            desc_reminder = (
                f"\n\n[工作流「{workflow_name}」说明 (下次调用请按此生成 texts): {workflow_desc}"
                "\n文本须按上述说明填写（如「根据图2的XX修改图1」），不要只传图片内容描述。]"
            )
        ok_inputs, texts, images_b64, videos, input_error = runtime._apply_workflow_input_rules(info, texts, images_b64, videos)
        if not ok_inputs:
            return (
                f"工作流「{workflow_name}」参数数量不匹配。"
                f"当前提供：文本{len(texts)}，图片{len(images_b64)}，视频{len(videos)}。"
                + (" " + input_error if input_error else "")
                + desc_reminder
            )
        try:
            debug = bool(getattr(config, "debug_mode", False) if not isinstance(config, dict) else config.get("debug_mode", False))
            workflow = runtime.ComfyUIWorkflow(server_ip, client_id)
            workflow.load_workflow_api(workflow_file)
            prompt_id = await workflow.submit_only(images_b64, texts, videos, debug=debug)
            session_key = runtime._get_session_key(context.context)
            output_rules = (info.get("params") or {}).get("outputs") or {}
            pending_data = {
                "prompt_id": prompt_id,
                "server_ip": server_ip,
                "client_id": client_id,
                "session_key": session_key,
                "session_tag": session_tag,
                "output_rules": output_rules,
                "workflow_name": workflow_name,
                "workflow_file": wf_filename,
            }
            runtime._session_pending[session_key] = pending_data
            if session_key != "default":
                runtime._session_pending["default"] = pending_data
            runtime._task_registry[prompt_id] = pending_data
            
            # 注册到 session_tag_tasks
            if session_tag not in runtime._session_tag_tasks:
                runtime._session_tag_tasks[session_tag] = []
            if prompt_id not in runtime._session_tag_tasks[session_tag]:
                runtime._session_tag_tasks[session_tag].append(prompt_id)
            if runtime._task_service:
                try:
                    runtime._task_service.remember_external_task(
                        "llm_tool",
                        pending_data,
                        workflow_name,
                        texts=texts,
                        images=images_b64,
                        videos=videos,
                        session_tag=session_tag,
                    )
                except Exception as e:
                    runtime.logger.warning("ComfyUI task center register failed: %s", e)
            
            # 获取该 session_tag 下所有任务 UUID
            all_uuids = runtime._session_tag_tasks.get(session_tag, [])
            uuid_list_str = ", ".join(f'"{u}"' for u in all_uuids)
            
            return (
                f"Workflow '{workflow_name}' submitted. Task ID (prompt_id): {prompt_id}. "
                f"You have {len(all_uuids)} task(s) with session_tag '{session_tag}'. All task IDs: [{uuid_list_str}]. "
                f"IMPORTANT: You MUST immediately call comfyui_query_wait with session_tag='{session_tag}' and task_ids=['{prompt_id}'] to wait for the result. "
                "Do not reply to the user before calling comfyui_query_wait."
                + desc_reminder
            )
        except runtime.httpx.HTTPStatusError as e:
            body = ""
            try:
                if e.response is not None:
                    body = e.response.text
            except Exception:
                pass
            summary = runtime._parse_comfyui_400_summary(body)
            msg = (
                f"执行失败：ComfyUI 返回 {e.response.status_code if e.response else '?'}。"
                + (summary if summary else (f"服务端信息：{body[:1500]}" if body else str(e)))
            )
            runtime.logger.exception("comfyui_execute failed: %s", msg)
            return msg + (" 建议修复工作流，或换用当前 ComfyUI 服务器可运行的工作流。" if summary else " 可能原因：工作流节点/输入不匹配、图片格式无效，或服务器错误。") + desc_reminder
        except Exception as e:
            runtime.logger.exception("comfyui_execute failed")
            return (
                f"执行失败：{e}。"
                "可能原因：ComfyUI 服务器不可达或超时、工作流节点错误、输入无效。"
                "请检查服务器地址和工作流 JSON 是否有效。"
                + desc_reminder
            )


__all__ = ["ComfyUIExecuteTool"]
