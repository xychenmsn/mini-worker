"""
Utility functions for mini-worker framework
"""

import importlib
import signal
import sys
from typing import Type, Any, Dict


def import_worker_class(class_path: str) -> Type:
    """
    Import a worker class from a string path.
    
    Args:
        class_path: Full path to class, e.g., 'mymodule.MyWorker' or just 'MyWorker'
        
    Returns:
        The imported class
        
    Raises:
        ImportError: If the class cannot be imported
        AttributeError: If the class doesn't exist in the module
    """
    if '.' in class_path:
        # Full module path provided
        module_path, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    else:
        # Just class name provided, try to find it in __main__ or current globals
        # This handles cases where the worker is defined in the same file
        import __main__
        if hasattr(__main__, class_path):
            return getattr(__main__, class_path)
        
        # Try to find in current frame's globals
        frame = sys._getframe(1)
        if class_path in frame.f_globals:
            return frame.f_globals[class_path]
            
        raise ImportError(f"Cannot find worker class '{class_path}'. "
                         f"Use full module path like 'mymodule.{class_path}'")


def setup_signal_handlers(shutdown_callback):
    """
    Set up signal handlers for graceful shutdown.
    
    Args:
        shutdown_callback: Function to call when shutdown signal is received
    """
    def signal_handler(signum, frame):
        print(f"Received signal {signum}. Shutting down gracefully...")
        shutdown_callback()
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def parse_worker_params(params_str: str) -> Dict[str, Any]:
    """
    Parse worker parameters from JSON string.
    
    Args:
        params_str: JSON string containing parameters
        
    Returns:
        Dictionary of parsed parameters
        
    Raises:
        ValueError: If JSON is invalid
    """
    import json
    
    if not params_str:
        return {}
        
    try:
        return json.loads(params_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in worker parameters: {e}")


def validate_worker_class(worker_class: Type) -> bool:
    """
    Validate that a class is a proper mini-worker.
    
    Args:
        worker_class: Class to validate
        
    Returns:
        True if valid, False otherwise
    """
    from .base_worker import BaseMiniWorker
    
    # Check if it's a subclass of BaseMiniWorker
    if not issubclass(worker_class, BaseMiniWorker):
        return False
        
    # Check if required abstract methods are implemented
    required_methods = ['get_worker_id', 'do_work']
    for method_name in required_methods:
        if not hasattr(worker_class, method_name):
            return False
        method = getattr(worker_class, method_name)
        if getattr(method, '__isabstractmethod__', False):
            return False
            
    return True


def get_worker_status(worker_id: str, stats_dir: str) -> Dict[str, Any]:
    """
    Get worker status from files.
    
    Args:
        worker_id: Worker identifier
        stats_dir: Directory containing stats files
        
    Returns:
        Status dictionary or empty dict if not found
    """
    import json
    import os
    
    json_file = os.path.join(stats_dir, f"{worker_id}.json")
    try:
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def is_worker_running(worker_id: str, stats_dir: str) -> bool:
    """
    Check if a worker is currently running by checking PID file.
    
    Args:
        worker_id: Worker identifier
        stats_dir: Directory containing PID files
        
    Returns:
        True if worker appears to be running, False otherwise
    """
    import os
    import psutil
    
    pid_file = os.path.join(stats_dir, f"{worker_id}.pid")
    
    if not os.path.exists(pid_file):
        return False
        
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
            
        # Check if process with this PID exists and is running
        return psutil.pid_exists(pid)
    except (ValueError, FileNotFoundError, PermissionError):
        return False


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string like "1h 23m 45s"
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.0f}s"
