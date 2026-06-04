# -*- coding: utf-8 -*-
"""LLM tool for reading workflow details."""

from pydantic import Field
from pydantic.dataclasses import dataclass

from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.tool import FunctionTool, ToolExecResult
from astrbot.core.astr_agent_context import AstrAgentContext

from ..core import plugin as runtime


class ComfyUIGetWorkflowDetailTool(FunctionTool[AstrAgentContext]):
    """
    获取指定工作流的详细说明。
    当需要了解某个工作流的详细用途和参数说明时使用。
    """

    name: str = "comfyui_get_workflow_detail"
    description: str = "获取指定工作流的详细说明。"
    parameters: dict = Field(
        default_factory=lambda: {
            "type": "object",
            "properties": {
                "workflow_name": {
                    "type": "string",
                    "description": "工作流名称（从 comfyui_list_workflows 获取）。",
                },
            },
            "required": ["workflow_name"],
        }
    )

    async def call(self, context: ContextWrapper[AstrAgentContext], **kwargs) -> ToolExecResult:
        runtime.logger.info("[ComfyUI Tool] comfyui_get_workflow_detail called with args: %s", kwargs)
        workflow_name = (kwargs.get("workflow_name") or "").strip()
        if not workflow_name:
            return "缺少工作流名称。"
        
        config = runtime._plugin_config
        if not config:
            return "插件配置不可用。"
        
        active_port = runtime._get_active_comfyui_port(config)
        descriptions = await runtime._load_workflow_descriptions(config)
        wf_dir = runtime._get_workflow_dir()
        workflows = runtime._filter_workflows_for_port(runtime._list_workflows_in_configured_dir(wf_dir), active_port)
        
        # 查找对应的工作流
        target_wf = None
        for w in workflows:
            if w["name"] == workflow_name:
                target_wf = w
                break
        
        if not target_wf:
            return f"未找到工作流「{workflow_name}」。"
        
        filename = target_wf.get("filename", "")
        desc_data = descriptions.get(filename, {})
        
        if isinstance(desc_data, dict):
            short_desc = desc_data.get("short", "")
            detailed_desc = desc_data.get("detailed", "")
        else:
            short_desc = str(desc_data) if desc_data else ""
            detailed_desc = short_desc
        
        result = f"Workflow: {workflow_name}\n"
        result += f"Filename: {filename}\n"
        result += f"Short description: {short_desc or '(无)'}\n"
        result += f"Detailed description: {detailed_desc or '(无)'}"
        
        return result


__all__ = ["ComfyUIGetWorkflowDetailTool"]
