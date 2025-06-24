#!/usr/bin/env python3
"""
Deployment and packaging tests for mini-worker
"""

import os
import sys
import tempfile
import shutil
import subprocess
import importlib
import json
from pathlib import Path
import pytest

from mini_worker import BaseMiniWorker, MiniWorkerManager


class TestDeploymentPackaging:
    """Test deployment and packaging scenarios"""
    
    def setup_method(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_package_imports(self):
        """Test that all package components can be imported correctly"""
        # Test main imports
        from mini_worker import BaseMiniWorker, MiniWorkerManager
        from mini_worker.base_worker import BaseMiniWorker as BaseWorkerDirect
        from mini_worker.manager import MiniWorkerManager as ManagerDirect
        from mini_worker.cli import main, status
        from mini_worker.utils import import_worker_class, get_worker_status, is_worker_running
        
        # Verify classes are the same
        assert BaseMiniWorker is BaseWorkerDirect
        assert MiniWorkerManager is ManagerDirect
        
        # Verify they are callable
        assert callable(BaseMiniWorker)
        assert callable(MiniWorkerManager)
        assert callable(main)
        assert callable(status)
        
    def test_cli_entry_point(self):
        """Test that CLI entry point is properly installed"""
        # Test that mini-worker command is available
        result = subprocess.run(
            ['python', '-m', 'mini_worker.cli', '--help'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        assert result.returncode == 0
        assert 'mini-worker' in result.output or 'Usage:' in result.output
        
    def test_package_metadata(self):
        """Test package metadata is correct"""
        import mini_worker
        
        # Check that package has required attributes
        assert hasattr(mini_worker, '__version__') or hasattr(mini_worker, '__version_info__')
        
        # Test that main classes are available at package level
        assert hasattr(mini_worker, 'BaseMiniWorker')
        assert hasattr(mini_worker, 'MiniWorkerManager')
        
    def test_dependency_requirements(self):
        """Test that required dependencies are available"""
        # Test click dependency
        import click
        assert hasattr(click, 'command')
        assert hasattr(click, 'option')
        
        # Test psutil dependency
        import psutil
        assert hasattr(psutil, 'Process')
        assert hasattr(psutil, 'process_iter')
        
    def test_external_project_simulation(self):
        """Simulate using mini-worker as a dependency in another project"""
        # Create a simulated external project structure
        project_dir = os.path.join(self.temp_dir, 'external_project')
        os.makedirs(project_dir)
        
        # Create a worker in the external project
        worker_code = '''
from mini_worker import BaseMiniWorker
import time

class ExternalWorker(BaseMiniWorker):
    """Worker defined in external project"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.external_config = kwargs.get('external_config', {})
        self.processed_items = 0
        
    def get_worker_id(self):
        return "external_project_worker"
        
    def do_work(self):
        """External project work logic"""
        with self.track_operation('external_processing'):
            # Simulate external project logic
            config = self.external_config
            batch_size = config.get('batch_size', 10)
            
            for i in range(batch_size):
                with self.track_operation('process_external_item'):
                    time.sleep(0.001)  # Simulate processing
                    self.processed_items += 1
                    
        self.logger.info(f"Processed {batch_size} items. Total: {self.processed_items}")

if __name__ == "__main__":
    worker = ExternalWorker(
        log_dir="/tmp/external_logs",
        external_config={"batch_size": 5},
        max_cycles=2,
        wait_seconds=0.1
    )
    worker.run()
'''
        
        worker_file = os.path.join(project_dir, 'external_worker.py')
        with open(worker_file, 'w') as f:
            f.write(worker_code)
            
        # Test that the external worker can be imported and used
        sys.path.insert(0, project_dir)
        try:
            import external_worker
            
            # Create and test the external worker
            worker = external_worker.ExternalWorker(
                log_dir=self.temp_dir,
                external_config={'batch_size': 3},
                max_cycles=1,
                wait_seconds=0.1
            )
            
            # Mock signal handlers for testing
            from unittest.mock import patch
            with patch.object(worker, 'setup_signal_handlers'):
                worker.run()
                
            # Verify external worker worked correctly
            assert worker.processed_items == 3
            assert 'external_processing' in worker.stats_dict
            assert 'process_external_item' in worker.stats_dict
            
        finally:
            sys.path.remove(project_dir)
            
    def test_manager_with_external_workers(self):
        """Test manager working with externally defined workers"""
        # Create external worker module
        external_module_dir = os.path.join(self.temp_dir, 'external_modules')
        os.makedirs(external_module_dir)
        
        # Create __init__.py
        with open(os.path.join(external_module_dir, '__init__.py'), 'w') as f:
            f.write('')
            
        # Create external worker module
        worker_module_code = '''
from mini_worker import BaseMiniWorker

class DataProcessor(BaseMiniWorker):
    def get_worker_id(self):
        return "data_processor"
        
    def do_work(self):
        with self.track_operation('data_processing'):
            pass

class ReportGenerator(BaseMiniWorker):
    def get_worker_id(self):
        return "report_generator"
        
    def do_work(self):
        with self.track_operation('report_generation'):
            pass
'''
        
        with open(os.path.join(external_module_dir, 'workers.py'), 'w') as f:
            f.write(worker_module_code)
            
        # Test manager with external workers
        sys.path.insert(0, self.temp_dir)
        try:
            manager = MiniWorkerManager(
                log_dir=self.temp_dir,
                stats_dir=self.temp_dir
            )
            
            # Register external workers
            manager.register_worker('data_processor', 'external_modules.workers.DataProcessor')
            manager.register_worker('report_generator', 'external_modules.workers.ReportGenerator')
            
            # Verify registration
            available_workers = manager.get_available_workers()
            assert 'data_processor' in available_workers
            assert 'report_generator' in available_workers
            
        finally:
            sys.path.remove(self.temp_dir)
            
    def test_configuration_file_support(self):
        """Test worker configuration through files"""
        # Create configuration file
        config = {
            'worker_settings': {
                'log_level': 'INFO',
                'wait_seconds': 60,
                'max_cycles': 100
            },
            'database': {
                'host': 'localhost',
                'port': 5432,
                'database': 'myapp'
            },
            'api': {
                'base_url': 'https://api.example.com',
                'timeout': 30,
                'retries': 3
            }
        }
        
        config_file = os.path.join(self.temp_dir, 'worker_config.json')
        with open(config_file, 'w') as f:
            json.dump(config, f)
            
        # Create worker that uses configuration file
        class ConfigurableWorker(BaseMiniWorker):
            def __init__(self, **kwargs):
                # Load configuration from file
                config_path = kwargs.pop('config_file', None)
                if config_path and os.path.exists(config_path):
                    with open(config_path, 'r') as f:
                        file_config = json.load(f)
                    
                    # Merge file config with kwargs
                    worker_settings = file_config.get('worker_settings', {})
                    for key, value in worker_settings.items():
                        kwargs.setdefault(key, value)
                        
                    # Store other config for use in worker
                    self.app_config = {
                        'database': file_config.get('database', {}),
                        'api': file_config.get('api', {})
                    }
                else:
                    self.app_config = {}
                    
                super().__init__(**kwargs)
                
            def get_worker_id(self):
                return "configurable_worker"
                
            def do_work(self):
                with self.track_operation('configured_processing'):
                    # Use configuration in work
                    db_config = self.app_config.get('database', {})
                    api_config = self.app_config.get('api', {})
                    
                    self.logger.info(f"Using database: {db_config.get('database', 'default')}")
                    self.logger.info(f"Using API: {api_config.get('base_url', 'default')}")
                    
        # Test worker with configuration
        worker = ConfigurableWorker(
            log_dir=self.temp_dir,
            config_file=config_file,
            max_cycles=1
        )
        
        # Verify configuration was loaded
        assert worker.wait_seconds == 60
        assert worker.max_cycles == 100
        assert worker.app_config['database']['host'] == 'localhost'
        assert worker.app_config['api']['base_url'] == 'https://api.example.com'
        
        # Test worker execution
        from unittest.mock import patch
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        assert 'configured_processing' in worker.stats_dict
        
    def test_logging_configuration(self):
        """Test custom logging configuration"""
        import logging
        
        class CustomLoggingWorker(BaseMiniWorker):
            def get_worker_id(self):
                return "custom_logging_worker"
                
            def setup_logging(self):
                super().setup_logging()
                
                # Add custom handler
                custom_handler = logging.FileHandler(
                    os.path.join(self.log_dir, 'custom.log')
                )
                custom_handler.setFormatter(
                    logging.Formatter('CUSTOM: %(asctime)s - %(message)s')
                )
                self.logger.addHandler(custom_handler)
                
            def do_work(self):
                self.logger.info("Custom logging test message")
                with self.track_operation('custom_logging_test'):
                    pass
                    
        worker = CustomLoggingWorker(
            log_dir=self.temp_dir,
            max_cycles=1
        )
        
        from unittest.mock import patch
        with patch.object(worker, 'setup_signal_handlers'):
            worker.run()
            
        # Verify custom log file was created
        custom_log_file = os.path.join(self.temp_dir, 'custom.log')
        assert os.path.exists(custom_log_file)
        
        with open(custom_log_file, 'r') as f:
            content = f.read()
            assert 'CUSTOM:' in content
            assert 'Custom logging test message' in content
            
    def test_worker_inheritance_patterns(self):
        """Test different worker inheritance patterns"""
        # Base worker with common functionality
        class BaseAppWorker(BaseMiniWorker):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.app_name = kwargs.get('app_name', 'MyApp')
                self.version = kwargs.get('version', '1.0.0')
                
            def setup(self):
                super().setup()
                self.logger.info(f"Starting {self.app_name} v{self.version}")
                
            def cleanup(self):
                self.logger.info(f"Stopping {self.app_name}")
                super().cleanup()
                
        # Specific worker implementations
        class EmailWorker(BaseAppWorker):
            def get_worker_id(self):
                return "email_worker"
                
            def do_work(self):
                with self.track_operation('send_emails'):
                    self.logger.info("Processing email queue")
                    
        class NotificationWorker(BaseAppWorker):
            def get_worker_id(self):
                return "notification_worker"
                
            def do_work(self):
                with self.track_operation('send_notifications'):
                    self.logger.info("Processing notifications")
                    
        # Test both workers
        for WorkerClass in [EmailWorker, NotificationWorker]:
            worker = WorkerClass(
                log_dir=self.temp_dir,
                app_name='TestApp',
                version='2.0.0',
                max_cycles=1
            )
            
            from unittest.mock import patch
            with patch.object(worker, 'setup_signal_handlers'):
                worker.run()
                
            assert worker.app_name == 'TestApp'
            assert worker.version == '2.0.0'
