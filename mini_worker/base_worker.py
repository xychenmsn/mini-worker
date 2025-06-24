"""
Base worker class for mini-worker framework
"""

import os
import sys
import time
import signal
import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler

from .monitoring import FileMonitoring, MonitoringBackend


class BaseMiniWorker(ABC):
    """
    Base class for mini-worker framework.
    
    All configuration is parameter-driven with no external dependencies.
    Workers run in internal loops with configurable wait times.
    """
    
    def __init__(self, 
                 worker_id: Optional[str] = None,
                 log_dir: Optional[str] = None,
                 stats_dir: Optional[str] = None,
                 wait_seconds: int = 600,
                 max_cycles: Optional[int] = None,
                 monitoring: Optional[MonitoringBackend] = None,
                 **kwargs):
        """
        Initialize the worker with all parameters.
        
        Args:
            worker_id: Unique identifier for this worker instance
            log_dir: Directory for log files (default: current directory)
            stats_dir: Directory for stats files (default: same as log_dir)
            wait_seconds: Seconds to wait between work cycles (default: 600)
            max_cycles: Maximum number of cycles before stopping (default: unlimited)
            monitoring: Custom monitoring backend (default: FileMonitoring)
            **kwargs: Additional worker-specific parameters
        """
        self.worker_id = worker_id or self.get_worker_id()
        self.log_dir = log_dir or os.getcwd()
        self.stats_dir = stats_dir or self.log_dir
        self.wait_seconds = wait_seconds
        self.max_cycles = max_cycles
        self.worker_params = kwargs
        
        # Ensure directories exist
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.stats_dir, exist_ok=True)
        
        # Initialize monitoring
        self.monitoring = monitoring or FileMonitoring(self.stats_dir)
        
        # Initialize statistics
        self.stats = {
            'total_work_cycles': 0,
            'total_processing_time': 0,
            'last_work_cycle_time': 0,
            'last_work_cycle_start': 0,
            'last_work_cycle_end': 0,
            'start_time': None,
            'worker_id': self.worker_id,
            'status': 'initializing'
        }
        
        # Initialize operation-specific stats
        self.stats_dict = {}
        
        # Logger will be set up in setup_logging
        self.logger = None
        
        # Signal handling
        self._shutdown_requested = False
        
    def setup_logging(self):
        """Set up logging for this worker instance"""
        log_file = os.path.join(self.log_dir, f"{self.worker_id}.log")
        
        # Create logger for this worker
        self.logger = logging.getLogger(f"mini_worker.{self.worker_id}")
        self.logger.setLevel(logging.INFO)
        
        # Remove any existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
        
        self.logger.info(f"Logging initialized for worker {self.worker_id}")
        self.logger.info(f"Log file: {log_file}")
        
    def setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown"""
        def signal_handler(signum, _):
            self.logger.info(f"Received signal {signum}. Requesting shutdown...")
            self._shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def write_pid_file(self):
        """Write process ID to file for monitoring"""
        pid_file = os.path.join(self.stats_dir, f"{self.worker_id}.pid")
        try:
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            self.logger.debug(f"PID file written: {pid_file}")
        except Exception as e:
            self.logger.error(f"Error writing PID file: {e}")
            
    def remove_pid_file(self):
        """Remove PID file on shutdown"""
        pid_file = os.path.join(self.stats_dir, f"{self.worker_id}.pid")
        try:
            if os.path.exists(pid_file):
                os.unlink(pid_file)
                self.logger.debug(f"PID file removed: {pid_file}")
        except Exception as e:
            self.logger.error(f"Error removing PID file: {e}")
            
    @contextmanager
    def track_operation(self, operation_name: str):
        """
        Context manager for tracking statistics for a specific operation.

        Usage:
            with self.track_operation('process_articles'):
                # Do some work...
        """
        if operation_name not in self.stats_dict:
            self.stats_dict[operation_name] = {
                'count': 0,
                'total_duration': 0,
                'start_time': time.time(),
                'rate_per_hour': 0
            }

        start_time = time.time()
        try:
            yield
            # Only count successful operations
            end_time = time.time()
            duration = end_time - start_time

            # Update operation stats
            op_stats = self.stats_dict[operation_name]
            op_stats['count'] += 1
            op_stats['total_duration'] += duration

            # Calculate rate (per hour)
            total_time = end_time - op_stats['start_time']
            if total_time > 0:
                op_stats['rate_per_hour'] = op_stats['count'] / (total_time / 3600)

            # Update monitoring
            self.monitoring.report_status(self.worker_id, self.get_status_dict())

        except Exception as e:
            self.logger.error(f"Error in operation {operation_name}: {e}", exc_info=True)
            raise

    @contextmanager
    def calc_one(self, operation_name: str):
        """
        Compatibility method for existing workers that use calc_one().

        This is an alias for track_operation() to maintain compatibility
        with workers migrating from the original BaseWorker.

        Usage:
            with self.calc_one('process_articles'):
                # Do some work...
        """
        with self.track_operation(operation_name):
            yield
            
    def get_status_dict(self) -> Dict[str, Any]:
        """Get current worker status as dictionary"""
        status = self.stats.copy()
        status['operations'] = self.stats_dict.copy()
        status['timestamp'] = time.time()
        return status
        
    def get_status_string(self) -> str:
        """Get human-readable status string"""
        if not self.stats_dict:
            return "No operations completed yet"

        lines = []
        for op_name, op_stats in sorted(self.stats_dict.items()):
            rate = op_stats.get('rate_per_hour', 0)
            count = op_stats.get('count', 0)
            lines.append(f"{op_name}: {rate:.1f}/hour ({count} total)")

        return "\n".join(lines)

    def _update_cycle_stats(self, start_time: float, end_time: float):
        """Update statistics after a work cycle"""
        processing_time = end_time - start_time
        self.stats['total_work_cycles'] += 1
        self.stats['total_processing_time'] += processing_time
        self.stats['last_work_cycle_time'] = processing_time
        self.stats['last_work_cycle_start'] = start_time
        self.stats['last_work_cycle_end'] = end_time
        if not self.stats['start_time']:
            self.stats['start_time'] = start_time

    def run(self):
        """Main worker loop"""
        self.setup_logging()
        self.setup_signal_handlers()
        self.write_pid_file()

        self.logger.info(f"Starting {self.__class__.__name__} worker")
        self.logger.info(f"Worker ID: {self.worker_id}")
        self.logger.info(f"Wait time: {self.wait_seconds} seconds")
        self.logger.info(f"Max cycles: {self.max_cycles or 'unlimited'}")

        try:
            self.stats['status'] = 'running'
            self.setup()

            cycle_count = 0
            while not self._shutdown_requested:
                # Check max cycles limit
                if self.max_cycles and cycle_count >= self.max_cycles:
                    self.logger.info(f"Reached max cycles limit ({self.max_cycles})")
                    break

                start_time = time.time()
                try:
                    self.logger.info(f"Starting work cycle {cycle_count + 1}")
                    self.do_work()
                    self.logger.info("Work cycle completed successfully")
                except Exception as e:
                    self.logger.error(f"Error in work cycle: {e}", exc_info=True)

                end_time = time.time()
                self._update_cycle_stats(start_time, end_time)

                # Update status
                self.stats['status'] = 'waiting'
                self.monitoring.report_status(self.worker_id, self.get_status_dict())

                cycle_count += 1

                # Wait before next cycle (unless shutdown requested)
                if not self._shutdown_requested and self.wait_seconds > 0:
                    elapsed = end_time - start_time
                    sleep_time = max(0, self.wait_seconds - elapsed)
                    self.logger.info(f"Waiting {sleep_time:.1f} seconds before next cycle")

                    # Sleep in small chunks to respond to shutdown signals
                    sleep_start = time.time()
                    while (time.time() - sleep_start < sleep_time and
                           not self._shutdown_requested):
                        time.sleep(min(1.0, sleep_time - (time.time() - sleep_start)))

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Unexpected error in worker: {e}", exc_info=True)
        finally:
            self.stats['status'] = 'stopped'
            self.monitoring.report_status(self.worker_id, self.get_status_dict())
            self.cleanup()
            self.remove_pid_file()
            self.logger.info(f"Worker {self.worker_id} stopped")

    def setup(self):
        """Override this method to perform worker-specific setup"""
        self.logger.info("Worker setup completed")

    def cleanup(self):
        """Override this method to perform worker-specific cleanup"""
        self.logger.info("Worker cleanup completed")

    @abstractmethod
    def get_worker_id(self) -> str:
        """Return a unique identifier for this worker type"""
        pass

    @abstractmethod
    def do_work(self):
        """Implement the main work logic here"""
        pass
