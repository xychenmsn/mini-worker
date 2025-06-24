"""
Worker manager for mini-worker framework
"""

import os
import sys
import json
import subprocess
import psutil
import time
from typing import Dict, Any, List, Optional
from pathlib import Path

from .utils import import_worker_class, validate_worker_class, is_worker_running, get_worker_status


class MiniWorkerManager:
    """
    Manages worker processes - starting, stopping, and monitoring.
    
    Provides the same interface as the original WorkerManager for compatibility
    with existing REST APIs and frontend systems.
    """
    
    def __init__(self, 
                 log_dir: Optional[str] = None,
                 stats_dir: Optional[str] = None,
                 python_executable: Optional[str] = None):
        """
        Initialize the worker manager.
        
        Args:
            log_dir: Directory for worker log files (default: ./logs)
            stats_dir: Directory for worker stats files (default: same as log_dir)
            python_executable: Python executable to use (default: sys.executable)
        """
        self.log_dir = log_dir or os.path.join(os.getcwd(), "logs")
        self.stats_dir = stats_dir or self.log_dir
        self.python_executable = python_executable or sys.executable
        
        # Ensure directories exist
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.stats_dir, exist_ok=True)
        
        # Track available workers (can be populated dynamically)
        self.available_workers = {}
        
    def register_worker(self, worker_name: str, worker_class_path: str):
        """
        Register a worker class for management.
        
        Args:
            worker_name: Name identifier for the worker
            worker_class_path: Full import path to worker class (e.g., 'mymodule.MyWorker')
        """
        try:
            worker_class = import_worker_class(worker_class_path)
            if validate_worker_class(worker_class):
                self.available_workers[worker_name] = worker_class_path
            else:
                raise ValueError(f"Invalid worker class: {worker_class_path}")
        except Exception as e:
            raise ValueError(f"Cannot register worker {worker_name}: {e}")
    
    def get_unique_id(self, worker_name: str) -> str:
        """Generate unique worker ID (compatible with original system)"""
        return f"worker_manager_{worker_name}"
    
    def start_worker(self, worker_name: str):
        """
        Start a worker without parameters.
        
        Args:
            worker_name: Name of the worker to start
            
        Raises:
            ValueError: If worker is unknown or already running
        """
        self.start_worker_with_params(worker_name, {})
    
    def start_worker_with_params(self, worker_name: str, parameters: Dict[str, Any]):
        """
        Start a worker with optional runtime parameters.
        
        Args:
            worker_name: Name of the worker to start
            parameters: Dictionary of parameters to pass to the worker
            
        Raises:
            ValueError: If worker is unknown or already running
        """
        if worker_name not in self.available_workers:
            raise ValueError(f"Unknown worker: {worker_name}. Available workers: {list(self.available_workers.keys())}")
        
        if self.is_worker_running(worker_name):
            raise ValueError(f"Worker {worker_name} is already running")
        
        worker_class_path = self.available_workers[worker_name]
        unique_id = self.get_unique_id(worker_name)
        
        # Build command to start worker
        cmd = [
            self.python_executable,
            "-m", "mini_worker.cli",
            "run",
            "--worker-class", worker_class_path,
            "--worker-id", unique_id,
            "--log-dir", self.log_dir,
            "--stats-dir", self.stats_dir
        ]
        
        # Add parameters if provided
        if parameters:
            cmd.extend(["--worker-params", json.dumps(parameters)])
        
        # Start worker as subprocess
        try:
            subprocess.Popen(
                cmd,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        except Exception as e:
            raise RuntimeError(f"Failed to start worker {worker_name}: {e}")
    
    def stop_worker(self, worker_name: str):
        """
        Stop a running worker.
        
        Args:
            worker_name: Name of the worker to stop
            
        Raises:
            ValueError: If worker is not running
        """
        if not self.is_worker_running(worker_name):
            raise ValueError(f"Worker {worker_name} is not running")
        
        unique_id = self.get_unique_id(worker_name)
        
        try:
            # Find and terminate the worker process
            for proc in psutil.process_iter(['pid', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if unique_id in cmdline and 'mini_worker' in cmdline:
                        proc.terminate()
                        # Wait a bit for graceful shutdown
                        try:
                            proc.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            proc.kill()
                        return
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            raise ValueError(f"Could not find process for worker {worker_name}")
            
        except Exception as e:
            raise ValueError(f"Failed to stop worker {worker_name}: {e}")
    
    def is_worker_running(self, worker_name: str) -> bool:
        """
        Check if a worker is currently running.
        
        Args:
            worker_name: Name of the worker to check
            
        Returns:
            True if worker is running, False otherwise
        """
        unique_id = self.get_unique_id(worker_name)
        return is_worker_running(unique_id, self.stats_dir)
    
    def get_worker_status(self, worker_name: str) -> Dict[str, Any]:
        """
        Get status information for a worker.
        
        Args:
            worker_name: Name of the worker
            
        Returns:
            Dictionary containing worker status information
        """
        unique_id = self.get_unique_id(worker_name)
        is_running = self.is_worker_running(worker_name)
        
        # Get stats from files
        stats = get_worker_status(unique_id, self.stats_dir)
        
        if is_running:
            # Try to get process information
            for proc in psutil.process_iter(['pid', 'cmdline', 'create_time']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if unique_id in cmdline and 'mini_worker' in cmdline:
                        return {
                            "name": worker_name,
                            "status": "running",
                            "pid": proc.info['pid'],
                            "start_time": proc.info['create_time'],
                            "stats": stats
                        }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        
        return {
            "name": worker_name,
            "status": "stopped",
            "stats": stats
        }
    
    def get_worker_statuses(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status information for all registered workers.
        
        Returns:
            Dictionary mapping worker names to their status information
        """
        return {worker: self.get_worker_status(worker) for worker in self.available_workers}
    
    def get_available_workers(self) -> List[str]:
        """
        Get list of available workers.
        
        Returns:
            List of worker names
        """
        return list(self.available_workers.keys())
    
    def reload_worker_config(self):
        """
        Reload worker configuration.
        
        Note: In mini-worker, workers are registered programmatically,
        so this is a no-op for compatibility.
        """
        pass
