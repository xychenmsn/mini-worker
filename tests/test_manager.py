#!/usr/bin/env python3
"""
Tests for MiniWorkerManager
"""

import os
import time
import tempfile
import shutil
import json
from unittest.mock import patch, MagicMock
import pytest

from mini_worker import BaseMiniWorker, MiniWorkerManager


class TestWorker(BaseMiniWorker):
    """Test worker implementation"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.work_calls = 0
        
    def get_worker_id(self):
        return "test_worker"
        
    def do_work(self):
        self.work_calls += 1
        time.sleep(0.1)  # Simulate some work


class TestMiniWorkerManager:
    """Test cases for MiniWorkerManager"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = MiniWorkerManager(
            log_dir=self.temp_dir,
            stats_dir=self.temp_dir
        )
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_manager_initialization(self):
        """Test manager initialization"""
        assert self.manager.log_dir == self.temp_dir
        assert self.manager.stats_dir == self.temp_dir
        assert os.path.exists(self.temp_dir)
        assert self.manager.available_workers == {}
        
    def test_register_worker(self):
        """Test worker registration"""
        # Register a valid worker
        self.manager.register_worker("test_worker", "tests.test_manager.TestWorker")
        assert "test_worker" in self.manager.available_workers
        assert self.manager.available_workers["test_worker"] == "tests.test_manager.TestWorker"
        
    def test_register_invalid_worker(self):
        """Test registration of invalid worker"""
        with pytest.raises(ValueError, match="Cannot register worker"):
            self.manager.register_worker("invalid", "nonexistent.Worker")
            
    def test_get_unique_id(self):
        """Test unique ID generation"""
        unique_id = self.manager.get_unique_id("test_worker")
        assert unique_id == "worker_manager_test_worker"
        
    def test_get_available_workers(self):
        """Test getting available workers"""
        assert self.manager.get_available_workers() == []
        
        self.manager.register_worker("worker1", "tests.test_manager.TestWorker")
        self.manager.register_worker("worker2", "tests.test_manager.TestWorker")
        
        workers = self.manager.get_available_workers()
        assert set(workers) == {"worker1", "worker2"}
        
    @patch('subprocess.Popen')
    def test_start_worker(self, mock_popen):
        """Test starting a worker"""
        self.manager.register_worker("test_worker", "tests.test_manager.TestWorker")
        
        # Mock successful process start
        mock_process = MagicMock()
        mock_popen.return_value = mock_process
        
        self.manager.start_worker("test_worker")
        
        # Verify subprocess.Popen was called
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        
        # Check command structure
        assert "mini_worker.cli" in call_args
        assert "run" in call_args
        assert "--worker-class" in call_args
        assert "tests.test_manager.TestWorker" in call_args
        
    @patch('subprocess.Popen')
    def test_start_worker_with_params(self, mock_popen):
        """Test starting a worker with parameters"""
        self.manager.register_worker("test_worker", "tests.test_manager.TestWorker")
        
        mock_process = MagicMock()
        mock_popen.return_value = mock_process
        
        params = {"param1": "value1", "param2": 123}
        self.manager.start_worker_with_params("test_worker", params)
        
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        
        # Check that parameters are included
        assert "--worker-params" in call_args
        param_index = call_args.index("--worker-params") + 1
        param_json = call_args[param_index]
        assert json.loads(param_json) == params
        
    def test_start_unknown_worker(self):
        """Test starting an unknown worker"""
        with pytest.raises(ValueError, match="Unknown worker"):
            self.manager.start_worker("unknown_worker")
            
    @patch('mini_worker.manager.is_worker_running')
    def test_start_already_running_worker(self, mock_is_running):
        """Test starting a worker that's already running"""
        self.manager.register_worker("test_worker", "tests.test_manager.TestWorker")
        mock_is_running.return_value = True
        
        with pytest.raises(ValueError, match="already running"):
            self.manager.start_worker("test_worker")
            
    @patch('subprocess.Popen')
    def test_start_worker_subprocess_error(self, mock_popen):
        """Test handling subprocess errors when starting worker"""
        self.manager.register_worker("test_worker", "tests.test_manager.TestWorker")
        mock_popen.side_effect = OSError("Process failed")
        
        with pytest.raises(RuntimeError, match="Failed to start worker"):
            self.manager.start_worker("test_worker")
            
    @patch('psutil.process_iter')
    def test_stop_worker(self, mock_process_iter):
        """Test stopping a worker"""
        # Mock a running process
        mock_process = MagicMock()
        mock_process.info = {'pid': 12345, 'cmdline': ['python', '-m', 'mini_worker.cli', 'worker_manager_test_worker']}
        mock_process_iter.return_value = [mock_process]
        
        with patch.object(self.manager, 'is_worker_running', return_value=True):
            self.manager.stop_worker("test_worker")
            
        mock_process.terminate.assert_called_once()
        
    def test_stop_not_running_worker(self):
        """Test stopping a worker that's not running"""
        with patch.object(self.manager, 'is_worker_running', return_value=False):
            with pytest.raises(ValueError, match="not running"):
                self.manager.stop_worker("test_worker")
                
    @patch('mini_worker.manager.is_worker_running')
    def test_is_worker_running(self, mock_is_running):
        """Test checking if worker is running"""
        mock_is_running.return_value = True
        assert self.manager.is_worker_running("test_worker") is True
        
        mock_is_running.return_value = False
        assert self.manager.is_worker_running("test_worker") is False
        
    @patch('mini_worker.manager.get_worker_status')
    @patch('psutil.process_iter')
    def test_get_worker_status_running(self, mock_process_iter, mock_get_status):
        """Test getting status of a running worker"""
        # Mock worker stats
        mock_stats = {"total_work_cycles": 5, "status": "running"}
        mock_get_status.return_value = mock_stats
        
        # Mock running process
        mock_process = MagicMock()
        mock_process.info = {
            'pid': 12345, 
            'cmdline': ['python', '-m', 'mini_worker.cli', 'worker_manager_test_worker'],
            'create_time': 1234567890
        }
        mock_process_iter.return_value = [mock_process]
        
        with patch.object(self.manager, 'is_worker_running', return_value=True):
            status = self.manager.get_worker_status("test_worker")
            
        assert status["name"] == "test_worker"
        assert status["status"] == "running"
        assert status["pid"] == 12345
        assert status["stats"] == mock_stats
        
    @patch('mini_worker.manager.get_worker_status')
    def test_get_worker_status_stopped(self, mock_get_status):
        """Test getting status of a stopped worker"""
        mock_stats = {"total_work_cycles": 3, "status": "stopped"}
        mock_get_status.return_value = mock_stats
        
        with patch.object(self.manager, 'is_worker_running', return_value=False):
            status = self.manager.get_worker_status("test_worker")
            
        assert status["name"] == "test_worker"
        assert status["status"] == "stopped"
        assert "pid" not in status
        assert status["stats"] == mock_stats
        
    def test_get_worker_statuses(self):
        """Test getting status of all workers"""
        self.manager.register_worker("worker1", "tests.test_manager.TestWorker")
        self.manager.register_worker("worker2", "tests.test_manager.TestWorker")
        
        with patch.object(self.manager, 'get_worker_status') as mock_get_status:
            mock_get_status.return_value = {"status": "stopped"}
            
            statuses = self.manager.get_worker_statuses()
            
        assert "worker1" in statuses
        assert "worker2" in statuses
        assert mock_get_status.call_count == 2
        
    def test_reload_worker_config(self):
        """Test reload worker config (no-op for compatibility)"""
        # Should not raise any exceptions
        self.manager.reload_worker_config()
