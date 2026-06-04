# -*- coding: utf-8 -*-
"""Compatibility entrypoint for the ComfyUI Bubble management WebUI."""

from .management.app import create_app
from .management.server import ManagementServer

__all__ = ["ManagementServer", "create_app"]
