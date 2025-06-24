#!/usr/bin/env python3
"""
Tests for BaseMiniWorker
"""

import os
import time
import tempfile
import shutil
import json
from unittest.mock import patch
import pytest

from mini_worker import BaseMiniWorker


class TestWorker(BaseMiniWorker):
    """Test worker implementation"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.work_calls = 0
        self.setup_called = False
        self.cleanup_called = False
        
    def get_worker_id(self):
        return "test_worker"
        
    def do_work(self):
        self.work_calls += 1
        time.sleep(0.1)  # Simulate some work
        
    def setup(self):
        super().setup()
        self.setup_called = True
        
    def cleanup(self):
        super().cleanup()
        self.cleanup_called = True


class TestBaseMiniWorker:
    """Test cases for BaseMiniWorker"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_worker_initialization(self):
        """Test worker initialization with parameters"""
        worker = TestWorker(
            worker_id="test_001",
            log_dir=self.temp_dir,
            wait_seconds=30,
            max_cycles=5,
            custom_param="test_value"
        )
        
        assert worker.worker_id == "test_001"
        assert worker.log_dir == self.temp_dir
        assert worker.wait_seconds == 30
        assert worker.max_cycles == 5
        assert worker.worker_params["custom_param"] == "test_value"
        
    def test_default_worker_id(self):
        """Test that worker uses get_worker_id() when no worker_id provided"""
        worker = TestWorker(log_dir=self.temp_dir)
        assert worker.worker_id == "test_worker"
        
    def test_directory_creation(self):
        """Test that log and stats directories are created"""
        log_dir = os.path.join(self.temp_dir, "logs")
        stats_dir = os.path.join(self.temp_dir, "stats")
        
        worker = TestWorker(
            log_dir=log_dir,
            stats_dir=stats_dir
        )
        
        assert os.path.exists(log_dir)
        assert os.path.exists(stats_dir)
        
    def test_pid_file_creation(self):
        """Test PID file creation and removal"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()
        
        # Test PID file creation
        worker.write_pid_file()
        pid_file = os.path.join(self.temp_dir, "test_worker.pid")
        assert os.path.exists(pid_file)
        
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        assert pid == os.getpid()
        
        # Test PID file removal
        worker.remove_pid_file()
        assert not os.path.exists(pid_file)
        
    def test_stats_tracking(self):
        """Test operation statistics tracking"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()
        
        # Track some operations
        with worker.track_operation("test_op"):
            time.sleep(0.1)
            
        with worker.track_operation("test_op"):
            time.sleep(0.1)
            
        # Check stats
        assert "test_op" in worker.stats_dict
        op_stats = worker.stats_dict["test_op"]
        assert op_stats["count"] == 2
        assert op_stats["total_duration"] > 0.2
        assert op_stats["rate_per_hour"] > 0
        
    def test_status_files(self):
        """Test status file creation"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()
        
        # Generate some stats
        with worker.track_operation("test_op"):
            time.sleep(0.1)
            
        # Check that files are created
        stats_file = os.path.join(self.temp_dir, "test_worker.stats")
        json_file = os.path.join(self.temp_dir, "test_worker.json")
        
        assert os.path.exists(stats_file)
        assert os.path.exists(json_file)
        
        # Check file contents
        with open(stats_file, 'r') as f:
            stats_content = f.read()
        assert "test_op" in stats_content
        
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        assert "operations" in json_data
        assert "test_op" in json_data["operations"]
        
    def test_limited_cycles(self):
        """Test worker stops after max_cycles"""
        worker = TestWorker(
            log_dir=self.temp_dir,
            wait_seconds=0.1,  # Very short wait
            max_cycles=3
        )
        
        # Mock the setup and cleanup to avoid signal handlers in tests
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        assert worker.work_calls == 3
        assert worker.setup_called
        assert worker.cleanup_called
        
    def test_shutdown_signal(self):
        """Test graceful shutdown on signal"""
        worker = TestWorker(
            log_dir=self.temp_dir,
            wait_seconds=10  # Long wait to test interruption
        )
        
        # Start worker in a way that we can interrupt it
        with patch.object(worker, 'setup_signal_handlers'):
            # Simulate shutdown request after first cycle
            def mock_do_work():
                worker.work_calls += 1
                if worker.work_calls == 1:
                    worker._shutdown_requested = True
                    
            worker.do_work = mock_do_work
            worker.run()
            
        assert worker.work_calls == 1
        assert worker.cleanup_called
        
    def test_error_handling_in_work(self):
        """Test that errors in do_work don't crash the worker"""
        class ErrorWorker(TestWorker):
            def do_work(self):
                self.work_calls += 1
                if self.work_calls == 1:
                    raise ValueError("Test error")
                    
        worker = ErrorWorker(
            log_dir=self.temp_dir,
            wait_seconds=0.1,
            max_cycles=2
        )
        
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Should complete both cycles despite error in first
        assert worker.work_calls == 2
        
    def test_status_dict_format(self):
        """Test status dictionary format"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()
        
        status = worker.get_status_dict()
        
        # Check required fields
        assert "worker_id" in status
        assert "status" in status
        assert "total_work_cycles" in status
        assert "operations" in status
        assert "timestamp" in status
        
    def test_status_string_format(self):
        """Test human-readable status string"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()

        # No operations yet
        status_str = worker.get_status_string()
        assert "No operations completed yet" in status_str

        # Add some operations
        with worker.track_operation("test_op"):
            time.sleep(0.1)

        status_str = worker.get_status_string()
        assert "test_op" in status_str
        assert "/hour" in status_str

    def test_calc_one_compatibility(self):
        """Test calc_one compatibility method"""
        worker = TestWorker(log_dir=self.temp_dir)
        worker.setup_logging()

        # Use calc_one (compatibility method)
        with worker.calc_one("compat_op"):
            time.sleep(0.1)

        # Should create stats just like track_operation
        assert "compat_op" in worker.stats_dict
        op_stats = worker.stats_dict["compat_op"]
        assert op_stats["count"] == 1
        assert op_stats["total_duration"] > 0.1
