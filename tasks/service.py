"""Unified task service facade.

Task service behavior is currently implemented by ``ComfyUIPlugin`` methods and
is exposed here as a stable migration target for the next split.
"""

from ..core.plugin import ComfyUIPlugin

__all__ = ["ComfyUIPlugin"]

