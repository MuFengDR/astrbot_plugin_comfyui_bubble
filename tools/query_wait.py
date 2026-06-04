# -*- coding: utf-8 -*-
"""LLM tool for waiting on submitted ComfyUI workflow tasks."""

from pydantic import Field
from pydantic.dataclasses import dataclass

from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext

from ..core import plugin as runtime


@dataclass
class ComfyUIQueryWaitTool(FunctionTool[AstrAgentContext]):
    """
    查询 ComfyUI 任务状态并等待完成。
    ⚠️ 重要：查询时传入 session_tag（发送者的 QQ 号），会自动返回该用户提交的所有任务结果。
    如果需要生成 N 张图，先用 comfyui_execute 调用 N 次（每次返回不同 task_id），
    然后调用本工具一次（带 session_tag），批量获取所有任务结果。
    """

    name: str = "comfyui_query_wait"
    description: str = "批量查询所有任务状态（传入 session_tag）。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "session_tag": {
                    "type": "string",
                    "description": "REQUIRED. The sender's QQ number (the person who sent the command). Example: '123456789'. Use this to query all tasks submitted by this user.",
                },
                "task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional. List of specific task IDs (prompt_id) to query. Example: ['uuid1', 'uuid2'].",
                },
                "count": {
                    "type": "integer",
                    "description": "Optional. Query the most recent N tasks. Default: 20.",
                },
            },
            "required": ["session_tag"],
        }
    )

    description = (
        "Wait for ComfyUI WebSocket completion events and return task results. "
        "Pass session_tag and optionally task_ids/count. Do not pass a wait time; "
        "timeout is configured by websocket_wait_timeout_seconds. Images and videos are handled by the plugin. "
        "If a video result is returned as queued_by_plugin/auto_sent, do NOT call send_message_to_user; reply with normal text only."
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        runtime.logger.info("[ComfyUI Tool] comfyui_query_wait called with args: %s", kwargs)
        config = runtime._plugin_config or {}
        server_ip, client_id_cfg = runtime._get_server_config(config)
        session_key = runtime._get_session_key(context.context)
        
        # 支持的查询方式：
        # 1. session_tag: 查询该标识下所有任务（默认自动填充为发送者的 QQ 号）
        # 2. task_ids: 精确查询指定任务列表
        # 3. 兼容旧版: task_id (单个)
        
        # 自动获取发送者的 QQ 号作为 session_tag
        sender_id = runtime._get_sender_id_from_context(context.context)
        session_tag = (kwargs.get("session_tag") or "").strip()
        if not session_tag and sender_id:
            session_tag = sender_id
            runtime.logger.info("[ComfyUI Tool] Auto-filled session_tag with sender_id: %s", session_tag)
        task_ids_arg = kwargs.get("task_ids") or []
        if isinstance(task_ids_arg, str):
            task_ids_arg = [task_ids_arg]
        task_ids_arg = [tid.strip() for tid in task_ids_arg if tid and isinstance(tid, str)]
        
        # 兼容旧版
        old_task_id = (kwargs.get("task_id") or "").strip()
        if old_task_id and old_task_id not in task_ids_arg:
            task_ids_arg.append(old_task_id)
        
        # 等待一段时间后再查询（避免频繁轮询）
        # 默认等待 30 秒，最小 30 秒，最大 900 秒（15 分钟）
        wait_seconds = runtime._get_websocket_wait_timeout(config)
        
        runtime.logger.info("[ComfyUI Tool] Will wait up to %d seconds for ComfyUI WebSocket events", wait_seconds)
        
        # 如果提供了 session_tag，从 session_tag_tasks 获取任务列表
        if session_tag:
            all_task_ids = runtime._session_tag_tasks.get(session_tag, [])
            # 支持 count 参数限制数量，默认最多 20 个
            count = kwargs.get("count")
            if count and isinstance(count, int) and count > 0:
                all_task_ids = all_task_ids[-count:]
            else:
                all_task_ids = all_task_ids[-20:]  # 默认最多返回 20 个
            task_ids_arg = all_task_ids
            runtime.logger.info("[ComfyUI Tool] Query by session_tag '%s', got %d tasks", session_tag, len(task_ids_arg))
        
        # 如果仍然没有任务，尝试从队列恢复
        if not task_ids_arg:
            running_n, pending_n = await runtime._get_queue_status(server_ip)
            if running_n >= 0 and (running_n + pending_n) == 1:
                first = await runtime._get_first_task_from_queue(server_ip)
                if first:
                    prompt_id_first, client_id_first = first
                    task_ids_arg = [prompt_id_first]
                    pending = {
                        "prompt_id": prompt_id_first,
                        "server_ip": server_ip,
                        "client_id": client_id_first or client_id_cfg,
                        "session_key": session_key,
                    }
                    runtime._session_pending[session_key] = pending
                    if session_key != "default":
                        runtime._session_pending["default"] = pending
                    runtime._task_registry[prompt_id_first] = pending
                    runtime.logger.info("[ComfyUI Tool] Recovered pending from queue: %s", prompt_id_first)
        
        if not task_ids_arg:
            return "No pending ComfyUI task found. Submit a workflow with comfyui_execute first."

        results = []
        wait_targets = []
        for task_id in task_ids_arg:
            pending = runtime._task_registry.get(task_id)
            if not pending:
                results.append({"task_id": task_id, "status": "error", "message": "not found in registry"})
                continue

            pending = dict(pending)
            task_session_key = pending.get("session_key") or session_key
            task_session_tag = pending.get("session_tag", "")
            prompt_id = pending.get("prompt_id")
            task_server_ip = pending.get("server_ip") or server_ip
            output_rules = pending.get("output_rules")
            task_client_id = pending.get("client_id") or client_id_cfg
            output_rules = pending.get("output_rules")

            if pending.get("status") == "canceled":
                runtime._cleanup_completed_task(prompt_id, task_session_tag)
                results.append(
                    {
                        "task_id": prompt_id,
                        "status": "canceled",
                        "message": pending.get("message") or "ComfyUI task was manually stopped from WebUI.",
                    }
                )
                continue

            if not prompt_id or not task_server_ip:
                results.append({"task_id": task_id, "status": "error", "message": "invalid task data"})
                continue

            url, ftype, texts = await runtime._get_result_for_prompt(task_server_ip, prompt_id, output_rules)
            if url or ftype in ("text", "error"):
                runtime._cleanup_completed_task(prompt_id, task_session_tag)
                await runtime._append_completed_task_result(
                    results,
                    context.context,
                    prompt_id,
                    task_server_ip,
                    task_session_key,
                    url,
                    ftype,
                    texts,
                )
                continue

            history_state = await runtime._get_prompt_history_state(task_server_ip, prompt_id)
            history_status = history_state.get("status_str", "")
            if history_state.get("exists") and history_status in ("error", "failed"):
                runtime._cleanup_completed_task(prompt_id, task_session_tag)
                results.append(
                    {
                        "task_id": prompt_id,
                        "status": "error",
                        "message": history_state.get("message") or "ComfyUI execution failed",
                    }
                )
                continue
            if history_state.get("exists") and history_state.get("completed"):
                runtime._cleanup_completed_task(prompt_id, task_session_tag)
                results.append({"task_id": prompt_id, "status": "completed", "message": "no output file"})
                continue

            if wait_seconds <= 0:
                results.append({"task_id": prompt_id, "status": "pending", "message": "not completed yet"})
                continue

            wait_targets.append(
                {
                    "prompt_id": prompt_id,
                    "server_ip": task_server_ip,
                    "client_id": task_client_id,
                    "session_key": task_session_key,
                    "session_tag": task_session_tag,
                    "output_rules": output_rules,
                }
            )

        if wait_targets:
            grouped_wait_targets = {}
            for item in wait_targets:
                grouped_wait_targets.setdefault((item["server_ip"], item["client_id"]), []).append(item)
            grouped_wait_results = await runtime.asyncio.gather(
                *[
                    runtime._wait_for_comfyui_ws_completion_many(
                        server_ip,
                        client_id,
                        [item["prompt_id"] for item in items],
                        wait_seconds,
                    )
                    for (server_ip, client_id), items in grouped_wait_targets.items()
                ]
            )
            wait_results_by_prompt = {}
            for group_result in grouped_wait_results:
                wait_results_by_prompt.update(group_result)
            wait_results = [
                wait_results_by_prompt.get(
                    item["prompt_id"],
                    {"status": "timeout", "message": f"wait timed out after {wait_seconds} seconds"},
                )
                for item in wait_targets
            ]
            for item, wait_result in zip(wait_targets, wait_results):
                prompt_id = item["prompt_id"]
                status = wait_result.get("status")
                if status == "completed":
                    url, ftype, texts = await runtime._get_result_for_prompt(item["server_ip"], prompt_id, item.get("output_rules"))
                    runtime._cleanup_completed_task(prompt_id, item["session_tag"])
                    await runtime._append_completed_task_result(
                        results,
                        context.context,
                        prompt_id,
                        item["server_ip"],
                        item["session_key"],
                        url,
                        ftype,
                        texts,
                    )
                elif status in ("error", "interrupted"):
                    runtime._cleanup_completed_task(prompt_id, item["session_tag"])
                    results.append(
                        {
                            "task_id": prompt_id,
                            "status": status,
                            "message": wait_result.get("message", status),
                        }
                    )
                elif status == "ws_unavailable":
                    results.append(
                        {
                            "task_id": prompt_id,
                            "status": "error",
                            "message": wait_result.get("message", runtime.COMFYUI_WS_UNAVAILABLE_MESSAGE),
                        }
                    )
                else:
                    url, ftype, texts = await runtime._get_result_for_prompt(item["server_ip"], prompt_id, item.get("output_rules"))
                    if url or ftype in ("text", "error"):
                        runtime._cleanup_completed_task(prompt_id, item["session_tag"])
                        await runtime._append_completed_task_result(
                            results,
                            context.context,
                            prompt_id,
                            item["server_ip"],
                            item["session_key"],
                            url,
                            ftype,
                            texts,
                        )
                    else:
                        history_state = await runtime._get_prompt_history_state(item["server_ip"], prompt_id)
                        history_status = history_state.get("status_str", "")
                        if history_state.get("exists") and history_status in ("error", "failed"):
                            runtime._cleanup_completed_task(prompt_id, item["session_tag"])
                            results.append(
                                {
                                    "task_id": prompt_id,
                                    "status": "error",
                                    "message": history_state.get("message") or "ComfyUI execution failed",
                                }
                            )
                        elif history_state.get("exists") and history_state.get("completed"):
                            runtime._cleanup_completed_task(prompt_id, item["session_tag"])
                            results.append({"task_id": prompt_id, "status": "completed", "message": "no output file"})
                        else:
                            results.append(
                                {
                                    "task_id": prompt_id,
                                    "status": "pending",
                                    "message": wait_result.get("message", "not completed yet"),
                                }
                            )

        completed_tasks = []
        pending_count = 0
        canceled_count = 0
        for r in results:
            if isinstance(r, dict):
                if r.get("status") == "completed" and r.get("type") == "image" and r.get("url", "").startswith("http"):
                    completed_tasks.append(r)
                elif r.get("status") == "pending":
                    pending_count += 1
                elif r.get("status") == "canceled":
                    canceled_count += 1

        for task in completed_tasks:
            url = task.get("url", "")
            if url and url.startswith("http"):
                local_path = await runtime._download_url_to_local(url)
                if local_path and local_path != url:
                    task["local_path"] = local_path
                    task["url"] = local_path

        response = {
            "results": results,
            "summary": {
                "total": len(results),
                "completed": sum(1 for r in results if isinstance(r, dict) and r.get("status") == "completed"),
                "pending": pending_count,
                "canceled": canceled_count,
            },
        }
        if pending_count > 0:
            response["message"] = f"{pending_count} task(s) still pending. Call comfyui_query_wait again to check."

        return runtime.json.dumps(response, ensure_ascii=False, indent=2)
        
        # 批量查询多个任务
        results = []
        completed_tasks = []
        for task_id in task_ids_arg:
            pending = runtime._task_registry.get(task_id)
            if not pending:
                results.append({"task_id": task_id, "status": "error", "message": "not found in registry"})
                continue
            
            pending = dict(pending)
            task_session_key = pending.get("session_key") or session_key
            task_session_tag = pending.get("session_tag", "")
            prompt_id = pending.get("prompt_id")
            task_server_ip = pending.get("server_ip") or server_ip
            
            if not prompt_id or not task_server_ip:
                results.append({"task_id": task_id, "status": "error", "message": "invalid task data"})
                continue
            
            remaining = await runtime._estimate_remaining_seconds(task_server_ip, prompt_id)
            
            if remaining == 0:
                # 任务完成
                url, ftype, texts = await runtime._get_result_for_prompt(task_server_ip, prompt_id, output_rules)
                # 清理
                for k in list(runtime._session_pending.keys()):
                    if runtime._session_pending.get(k) and runtime._session_pending.get(k).get("prompt_id") == prompt_id:
                        runtime._session_pending.pop(k, None)
                runtime._task_registry.pop(prompt_id, None)
                # 从 session_tag_tasks 中移除
                if task_session_tag and task_session_tag in runtime._session_tag_tasks:
                    if prompt_id in runtime._session_tag_tasks[task_session_tag]:
                        runtime._session_tag_tasks[task_session_tag].remove(prompt_id)
                
                if url:
                    extra = (" Text: " + "; ".join(texts)) if texts else ""
                    if ftype == "image":
                        if url:
                            runtime._session_image_url_queue.setdefault(task_session_key, []).append(url)
                            results.append({
                                "task_id": prompt_id,
                                "status": "completed",
                                "type": "image",
                                "url": url,
                                "description": extra.strip()
                            })
                    elif ftype == "video":
                        runtime._session_video_url_queue.setdefault(task_session_key, []).append(url)
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": "video",
                            "auto_sent": True,
                            "delivery": "queued_by_plugin",
                            "message": "Video is queued for automatic sending. Do NOT call send_message_to_user. Reply with normal text only.",
                            "description": extra.strip()
                        })
                    else:
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": ftype,
                            "url": url,
                            "description": extra.strip()
                        })
                else:
                    results.append({
                        "task_id": prompt_id,
                        "status": "completed",
                        "message": "no output file"
                    })
            elif remaining < wait_threshold:
                # 等待时间不长，直接等待完成
                client_id = pending.get("client_id", "")
                url, ftype, texts = await runtime._wait_for_completion(task_server_ip, client_id, prompt_id, timeout=remaining + 120, output_rules=output_rules)
                # 清理
                for k in list(runtime._session_pending.keys()):
                    if runtime._session_pending.get(k) and runtime._session_pending.get(k).get("prompt_id") == prompt_id:
                        runtime._session_pending.pop(k, None)
                runtime._task_registry.pop(prompt_id, None)
                # 从 session_tag_tasks 中移除
                if task_session_tag and task_session_tag in runtime._session_tag_tasks:
                    if prompt_id in runtime._session_tag_tasks[task_session_tag]:
                        runtime._session_tag_tasks[task_session_tag].remove(prompt_id)
                
                if url:
                    extra = (" Text: " + "; ".join(texts)) if texts else ""
                    if ftype == "image":
                        if url:
                            runtime._session_image_url_queue.setdefault(task_session_key, []).append(url)
                            results.append({
                                "task_id": prompt_id,
                                "status": "completed",
                                "type": "image",
                                "url": url,
                                "description": extra.strip()
                            })
                    elif ftype == "video":
                        runtime._session_video_url_queue.setdefault(task_session_key, []).append(url)
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": "video",
                            "auto_sent": True,
                            "delivery": "queued_by_plugin",
                            "message": "Video is queued for automatic sending. Do NOT call send_message_to_user. Reply with normal text only.",
                            "description": extra.strip()
                        })
                    else:
                        results.append({
                            "task_id": prompt_id,
                            "status": "completed",
                            "type": ftype,
                            "url": url,
                            "description": extra.strip()
                        })
                else:
                    results.append({
                        "task_id": prompt_id,
                        "status": "completed",
                        "message": "no output file"
                    })
            else:
                # 仍在队列中
                results.append({
                    "task_id": prompt_id,
                    "status": "pending",
                    "message": f"still in queue, estimated ~{remaining} seconds"
                })
        
        # 收集所有已完成任务的图片URL，准备下载到本地
        completed_tasks = []
        pending_count = 0
        for r in results:
            if isinstance(r, dict):
                if r.get("status") == "completed" and r.get("type") == "image" and r.get("url", "").startswith("http"):
                    completed_tasks.append(r)
                elif r.get("status") == "pending":
                    pending_count += 1
            else:
                if "still in queue" in str(r):
                    pending_count += 1
        
        # 下载所有远程图片到本地
        for task in completed_tasks:
            url = task.get("url", "")
            if url and url.startswith("http"):
                local_path = await runtime._download_url_to_local(url)
                if local_path and local_path != url:
                    task["local_path"] = local_path
                    task["url"] = local_path  # 替换为本地路径
        
        # 返回 JSON 格式
        response = {
            "results": results,
            "summary": {
                "total": len(results),
                "completed": len(results) - pending_count,
                "pending": pending_count
            }
        }
        if pending_count > 0:
            response["message"] = f"{pending_count} task(s) still in queue. Call comfyui_query_wait again to check."
        
        return runtime.json.dumps(response, ensure_ascii=False, indent=2)


__all__ = ["ComfyUIQueryWaitTool"]
