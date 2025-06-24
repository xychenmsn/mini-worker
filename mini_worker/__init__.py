"""
Mini-Worker: A simple, parameter-driven worker framework
"""

from .base_worker import BaseMiniWorker
from .manager import MiniWorkerManager
from .monitoring import FileMonitoring, MonitoringBackend
from .utils import import_worker_class, setup_signal_handlers

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

__all__ = [
    "BaseMiniWorker",
    "MiniWorkerManager",
    "FileMonitoring",
    "MonitoringBackend",
    "import_worker_class",
    "setup_signal_handlers",
]
