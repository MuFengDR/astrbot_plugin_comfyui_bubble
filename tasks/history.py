"""Task history cleanup facade."""

from ..core.plugin import ComfyUIPlugin

cleanup_task_history = ComfyUIPlugin.cleanup_task_history

__all__ = ["cleanup_task_history"]

