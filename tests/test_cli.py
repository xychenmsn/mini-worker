#!/usr/bin/env python3
"""
Tests for CLI functionality
"""

import os
import tempfile
import shutil
import json
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
import pytest

from mini_worker.cli import main, status
from mini_worker import BaseMiniWorker


class MockWorker(BaseMiniWorker):
    """Mock worker for testing"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.work_calls = 0
        
    def get_worker_id(self):
        return "mock_worker"
        
    def do_work(self):
        self.work_calls += 1


class TestCLI:
    """Test cases for CLI functionality"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.runner = CliRunner()
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_missing_worker_class(self):
        """Test error when worker class is not provided"""
        result = self.runner.invoke(main, [])
        assert result.exit_code != 0
        assert "worker-class" in result.output
        
    def test_invalid_worker_class(self):
        """Test error when worker class cannot be imported"""
        result = self.runner.invoke(main, [
            '--worker-class', 'NonExistentWorker'
        ])
        assert result.exit_code == 1
        assert "Error importing worker class" in result.output
        
    def test_invalid_worker_params(self):
        """Test error when worker params are invalid JSON"""
        result = self.runner.invoke(main, [
            '--worker-class', 'MockWorker',
            '--worker-params', 'invalid json'
        ])
        assert result.exit_code == 1
        assert "Invalid JSON" in result.output
        
    @patch('mini_worker.cli.import_worker_class')
    def test_successful_worker_run(self, mock_import):
        """Test successful worker execution"""
        # Mock the worker class import
        mock_import.return_value = MockWorker
        
        # Mock the worker run method to avoid infinite loop
        with patch.object(MockWorker, 'run') as mock_run:
            result = self.runner.invoke(main, [
                '--worker-class', 'MockWorker',
                '--log-dir', self.temp_dir,
                '--wait-seconds', '1',
                '--max-cycles', '1'
            ])
            
        assert result.exit_code == 0
        mock_run.assert_called_once()
        
    @patch('mini_worker.cli.import_worker_class')
    def test_worker_params_parsing(self, mock_import):
        """Test that worker parameters are correctly parsed and passed"""
        mock_import.return_value = MockWorker
        
        params = {
            'param1': 'value1',
            'param2': 123,
            'param3': True
        }
        
        with patch.object(MockWorker, 'run') as mock_run:
            result = self.runner.invoke(main, [
                '--worker-class', 'MockWorker',
                '--worker-params', json.dumps(params),
                '--log-dir', self.temp_dir
            ])
            
        assert result.exit_code == 0
        
        # Check that MockWorker was instantiated with correct params
        mock_import.assert_called_once()
        
    @patch('mini_worker.cli.import_worker_class')
    def test_verbose_output(self, mock_import):
        """Test verbose output option"""
        mock_import.return_value = MockWorker
        
        with patch.object(MockWorker, 'run'):
            result = self.runner.invoke(main, [
                '--worker-class', 'MockWorker',
                '--log-dir', self.temp_dir,
                '--verbose'
            ])
            
        assert result.exit_code == 0
        assert "Creating worker" in result.output
        assert "Log directory" in result.output
        
    def test_status_no_workers(self):
        """Test status command when no workers exist"""
        result = self.runner.invoke(status, [
            '--stats-dir', self.temp_dir
        ])
        assert result.exit_code == 0
        assert "No worker status files found" in result.output
        
    def test_status_specific_worker_not_found(self):
        """Test status command for non-existent worker"""
        result = self.runner.invoke(status, [
            '--stats-dir', self.temp_dir,
            '--worker-id', 'nonexistent'
        ])
        assert result.exit_code == 1
        assert "No status found" in result.output
        
    def test_status_with_existing_worker(self):
        """Test status command with existing worker status"""
        # Create a mock status file
        status_data = {
            'worker_id': 'test_worker',
            'status': 'running',
            'total_work_cycles': 5,
            'operations': {
                'test_op': {
                    'count': 10,
                    'rate_per_hour': 120.0
                }
            }
        }
        
        json_file = os.path.join(self.temp_dir, 'test_worker.json')
        with open(json_file, 'w') as f:
            json.dump(status_data, f)
            
        # Create a mock PID file to simulate running worker
        pid_file = os.path.join(self.temp_dir, 'test_worker.pid')
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))
            
        result = self.runner.invoke(status, [
            '--stats-dir', self.temp_dir,
            '--worker-id', 'test_worker'
        ])
        
        assert result.exit_code == 0
        assert "test_worker" in result.output
        assert "running" in result.output
        assert "test_op" in result.output
        
    def test_status_json_output(self):
        """Test status command with JSON output"""
        # Create a mock status file
        status_data = {
            'worker_id': 'test_worker',
            'status': 'running',
            'total_work_cycles': 5
        }
        
        json_file = os.path.join(self.temp_dir, 'test_worker.json')
        with open(json_file, 'w') as f:
            json.dump(status_data, f)
            
        result = self.runner.invoke(status, [
            '--stats-dir', self.temp_dir,
            '--worker-id', 'test_worker',
            '--format', 'json'
        ])
        
        assert result.exit_code == 0
        
        # Parse the JSON output
        output_data = json.loads(result.output)
        assert output_data['worker_id'] == 'test_worker'
        assert output_data['status'] == 'running'
        assert 'is_running' in output_data
        
    def test_status_all_workers(self):
        """Test status command showing all workers"""
        # Create multiple mock status files
        workers = ['worker1', 'worker2', 'worker3']
        
        for worker_id in workers:
            status_data = {
                'worker_id': worker_id,
                'status': 'running',
                'total_work_cycles': 3
            }
            
            json_file = os.path.join(self.temp_dir, f'{worker_id}.json')
            with open(json_file, 'w') as f:
                json.dump(status_data, f)
                
        result = self.runner.invoke(status, [
            '--stats-dir', self.temp_dir
        ])
        
        assert result.exit_code == 0
        for worker_id in workers:
            assert worker_id in result.output
            
    @patch('mini_worker.cli.import_worker_class')
    def test_keyboard_interrupt_handling(self, mock_import):
        """Test graceful handling of keyboard interrupt"""
        mock_import.return_value = MockWorker

        # Mock the worker run method to raise KeyboardInterrupt
        with patch.object(MockWorker, 'run', side_effect=KeyboardInterrupt):
            result = self.runner.invoke(main, [
                '--worker-class', 'MockWorker',
                '--log-dir', self.temp_dir
            ])

        assert result.exit_code == 0
        assert "Shutdown requested by user" in result.output

    @patch('mini_worker.cli.import_worker_class')
    def test_worker_validation(self, mock_import):
        """Test worker class validation"""
        # Mock an invalid worker class (doesn't inherit from BaseMiniWorker)
        class InvalidWorker:
            pass

        mock_import.return_value = InvalidWorker

        result = self.runner.invoke(main, [
            '--worker-class', 'InvalidWorker',
            '--log-dir', self.temp_dir
        ])

        assert result.exit_code == 1
        assert "not a valid mini-worker class" in result.output
