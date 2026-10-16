"""Warm IPC Pool: persistent Python processes for jobs that cannot run in subinterpreters."""

from .client import WarmClient
from .supervisor import DEFAULTS, WarmPool, normalize

__all__ = ["DEFAULTS", "WarmClient", "WarmPool", "normalize"]
