# -*- coding: utf-8 -*-
"""AstrBot entrypoint for ComfyUI Bubble.

The implementation lives in ``core.plugin`` while this module stays as the
stable AstrBot plugin entrypoint.
"""

from astrbot.core.provider.register import llm_tools
from astrbot.core.star.star import star_map
from astrbot.core.star.star_handler import star_handlers_registry

from .core.plugin import ComfyUIPlugin


def _attach_core_handlers_to_entrypoint() -> None:
    """Keep AstrBot's command/plugin discovery pointed at this entry module.

    Decorated handlers are created in ``core.plugin`` during import, but the
    AstrBot plugin manager binds handlers by the entry module path
    (``...main``). Re-homing the metadata here preserves the split
    implementation without hiding commands from the behavior manager.
    """

    core_module = ComfyUIPlugin.__module__
    entry_module = __name__
    if core_module == entry_module:
        return

    metadata = star_map.pop(core_module, None)
    if metadata is not None:
        metadata.module_path = entry_module
        metadata.star_cls_type = ComfyUIPlugin
        star_map[entry_module] = metadata

    for handler in list(star_handlers_registry):
        if handler.handler_module_path != core_module:
            continue
        star_handlers_registry.star_handlers_map.pop(handler.handler_full_name, None)
        handler.handler_module_path = entry_module
        handler.handler_full_name = f"{entry_module}_{handler.handler_name}"
        star_handlers_registry.star_handlers_map[handler.handler_full_name] = handler

    for tool in llm_tools.func_list:
        if getattr(tool, "handler_module_path", None) == core_module:
            tool.handler_module_path = entry_module


_attach_core_handlers_to_entrypoint()

__all__ = ["ComfyUIPlugin"]


