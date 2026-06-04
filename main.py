# -*- coding: utf-8 -*-
"""AstrBot entrypoint for ComfyUI Bubble.

The implementation lives in ``core.plugin`` while this module stays as the
stable AstrBot plugin entrypoint.
"""

from .core.plugin import ComfyUIPlugin

__all__ = ["ComfyUIPlugin"]


