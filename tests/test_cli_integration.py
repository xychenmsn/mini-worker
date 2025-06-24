#!/usr/bin/env python3
"""
CLI integration tests for mini-worker
"""

import os
import time
import tempfile
import shutil
import subprocess
import json
import signal
import threading
from pathlib import Path
import pytest

from mini_worker import BaseMiniWorker


class TestCLIIntegration:
    """End-to-end CLI integration tests"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_worker_file = None
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def create_test_worker_file(self, worker_code):
        """Create a test worker file"""
        self.test_worker_file = os.path.join(self.temp_dir, 'test_worker.py')
        with open(self.test_worker_file, 'w') as f:
            f.write(worker_code)
        return self.test_worker_file
        
    def test_cli_run_basic_worker(self):
        """Test running a basic worker via CLI"""
        worker_code = '''
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mini_worker import BaseMiniWorker
import time

class TestWorker(BaseMiniWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.work_count = 0
        
    def get_worker_id(self):
        return "cli_test_worker"
        
    def do_work(self):
        with self.track_operation('cli_test_operation'):
            self.work_count += 1
            time.sleep(0.1)
'''
        
        worker_file = self.create_test_worker_file(worker_code)
        
        # Run worker via CLI
        cmd = [
            'python', '-m', 'mini_worker.cli', 'run',
            '--worker-class', 'test_worker.TestWorker',
            '--log-dir', self.temp_dir,
            '--wait-seconds', '1',
            '--max-cycles', '3'
        ]
        
        # Add the temp directory to Python path
        env = os.environ.copy()
        env['PYTHONPATH'] = self.temp_dir + ':' + env.get('PYTHONPATH', '')
        
        result = subprocess.run(
            cmd,
            cwd=self.temp_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        assert result.returncode == 0
        
        # Check that log files were created
        log_file = os.path.join(self.temp_dir, 'cli_test_worker.log')
        stats_file = os.path.join(self.temp_dir, 'cli_test_worker.stats')
        json_file = os.path.join(self.temp_dir, 'cli_test_worker.json')
        
        assert os.path.exists(log_file)
        assert os.path.exists(stats_file)
        assert os.path.exists(json_file)
        
        # Check stats content
        with open(json_file, 'r') as f:
            stats = json.load(f)
            
        assert stats['worker_id'] == 'cli_test_worker'
        assert stats['total_work_cycles'] == 3
        assert 'cli_test_operation' in stats['operations']
        
    def test_cli_run_with_parameters(self):
        """Test running worker with custom parameters via CLI"""
        worker_code = '''
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mini_worker import BaseMiniWorker

class ParameterizedWorker(BaseMiniWorker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.custom_param = kwargs.get('custom_param', 'default')
        self.batch_size = kwargs.get('batch_size', 1)
        self.processed_items = 0
        
    def get_worker_id(self):
        return "parameterized_worker"
        
    def do_work(self):
        with self.track_operation('parameterized_processing'):
            for i in range(self.batch_size):
                self.processed_items += 1
'''
        
        worker_file = self.create_test_worker_file(worker_code)
        
        # Prepare parameters
        params = {
            'custom_param': 'test_value',
            'batch_size': 5
        }
        
        cmd = [
            'python', '-m', 'mini_worker.cli', 'run',
            '--worker-class', 'test_worker.ParameterizedWorker',
            '--log-dir', self.temp_dir,
            '--worker-params', json.dumps(params),
            '--wait-seconds', '1',
            '--max-cycles', '2'
        ]
        
        env = os.environ.copy()
        env['PYTHONPATH'] = self.temp_dir + ':' + env.get('PYTHONPATH', '')
        
        result = subprocess.run(
            cmd,
            cwd=self.temp_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        assert result.returncode == 0
        
        # Check that parameters were used
        json_file = os.path.join(self.temp_dir, 'parameterized_worker.json')
        with open(json_file, 'r') as f:
            stats = json.load(f)
            
        assert 'parameterized_processing' in stats['operations']
        assert stats['operations']['parameterized_processing']['count'] == 2
        
    def test_cli_status_command(self):
        """Test CLI status command"""
        # First, create some mock status files
        worker_stats = {
            'worker_id': 'test_status_worker',
            'status': 'running',
            'total_work_cycles': 10,
            'operations': {
                'test_operation': {
                    'count': 25,
                    'total_duration': 12.5,
                    'rate_per_hour': 7200.0
                }
            },
            'timestamp': time.time()
        }
        
        json_file = os.path.join(self.temp_dir, 'test_status_worker.json')
        with open(json_file, 'w') as f:
            json.dump(worker_stats, f)
            
        # Test status command for specific worker
        cmd = [
            'python', '-m', 'mini_worker.cli', 'status',
            '--stats-dir', self.temp_dir,
            '--worker-id', 'test_status_worker'
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        assert 'test_status_worker' in result.stdout
        assert 'test_operation' in result.stdout
        assert '25' in result.stdout  # operation count
        
    def test_cli_status_json_format(self):
        """Test CLI status command with JSON output"""
        worker_stats = {
            'worker_id': 'json_test_worker',
            'status': 'stopped',
            'total_work_cycles': 5,
            'operations': {},
            'timestamp': time.time()
        }
        
        json_file = os.path.join(self.temp_dir, 'json_test_worker.json')
        with open(json_file, 'w') as f:
            json.dump(worker_stats, f)
            
        cmd = [
            'python', '-m', 'mini_worker.cli', 'status',
            '--stats-dir', self.temp_dir,
            '--worker-id', 'json_test_worker',
            '--format', 'json'
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        
        # Parse JSON output
        output_data = json.loads(result.stdout)
        assert output_data['worker_id'] == 'json_test_worker'
        assert output_data['status'] == 'stopped'
        assert 'is_running' in output_data
        
    def test_cli_status_all_workers(self):
        """Test CLI status command for all workers"""
        # Create multiple worker status files
        workers = ['worker1', 'worker2', 'worker3']
        
        for worker_id in workers:
            stats = {
                'worker_id': worker_id,
                'status': 'running',
                'total_work_cycles': 3,
                'operations': {},
                'timestamp': time.time()
            }
            
            json_file = os.path.join(self.temp_dir, f'{worker_id}.json')
            with open(json_file, 'w') as f:
                json.dump(stats, f)
                
        cmd = [
            'python', '-m', 'mini_worker.cli', 'status',
            '--stats-dir', self.temp_dir
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        
        # All workers should be listed
        for worker_id in workers:
            assert worker_id in result.stdout
            
    def test_cli_error_handling(self):
        """Test CLI error handling scenarios"""
        # Test with non-existent worker class
        cmd = [
            'python', '-m', 'mini_worker.cli', 'run',
            '--worker-class', 'NonExistentWorker',
            '--log-dir', self.temp_dir
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 1
        assert 'Error importing worker class' in result.stderr
        
    def test_cli_invalid_json_params(self):
        """Test CLI with invalid JSON parameters"""
        cmd = [
            'python', '-m', 'mini_worker.cli', 'run',
            '--worker-class', 'SomeWorker',
            '--worker-params', 'invalid json',
            '--log-dir', self.temp_dir
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 1
        assert 'Invalid JSON' in result.stderr
        
    def test_cli_help_commands(self):
        """Test CLI help commands"""
        # Test main help
        result = subprocess.run(
            ['python', '-m', 'mini_worker.cli', '--help'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        assert 'Usage:' in result.stdout
        
        # Test run command help
        result = subprocess.run(
            ['python', '-m', 'mini_worker.cli', 'run', '--help'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        assert '--worker-class' in result.stdout
        
        # Test status command help
        result = subprocess.run(
            ['python', '-m', 'mini_worker.cli', 'status', '--help'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        assert result.returncode == 0
        assert '--stats-dir' in result.stdout
        
    def test_cli_verbose_output(self):
        """Test CLI verbose output"""
        worker_code = '''
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mini_worker import BaseMiniWorker

class VerboseTestWorker(BaseMiniWorker):
    def get_worker_id(self):
        return "verbose_test_worker"
        
    def do_work(self):
        pass
'''
        
        worker_file = self.create_test_worker_file(worker_code)
        
        cmd = [
            'python', '-m', 'mini_worker.cli', 'run',
            '--worker-class', 'test_worker.VerboseTestWorker',
            '--log-dir', self.temp_dir,
            '--max-cycles', '1',
            '--wait-seconds', '1',
            '--verbose'
        ]
        
        env = os.environ.copy()
        env['PYTHONPATH'] = self.temp_dir + ':' + env.get('PYTHONPATH', '')
        
        result = subprocess.run(
            cmd,
            cwd=self.temp_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        assert result.returncode == 0
        assert 'Creating worker' in result.stdout
        assert 'Log directory' in result.stdout
